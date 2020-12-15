/*Copyright [2019] housepower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package output

import (
	"context"
	"database/sql"
	std_errors "errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
)

var (
	selectSQLTemplate    = `select name, type, default_kind from system.columns where database = '%s' and table = '%s'`
	lowCardinalityRegexp = regexp.MustCompile(`LowCardinality\((.+)\)`)
)

// ClickHouse is an output service consumers from kafka messages
type ClickHouse struct {
	Dims []*model.ColumnWithType
	Dms  []string
	// Table Configs
	taskCfg *config.TaskConfig
	chCfg   *config.ClickHouseConfig

	prepareSQL string
}

// NewClickHouse new a clickhouse instance
func NewClickHouse(cfg *config.Config, taskName string) *ClickHouse {
	taskCfg := cfg.Tasks[taskName]
	chCfg := cfg.Clickhouse[taskCfg.Clickhouse]
	return &ClickHouse{taskCfg: taskCfg, chCfg: chCfg}
}

// Init the clickhouse intance
func (c *ClickHouse) Init() (err error) {
	if err = pool.InitConn(c.taskCfg.Clickhouse, c.chCfg.Hosts, c.chCfg.Port, c.chCfg.DB, c.chCfg.Username, c.chCfg.Password, c.chCfg.DsnParams); err != nil {
		return
	}
	if err = c.initSchema(); err != nil {
		return err
	}
	return nil
}

// Send a batch to clickhouse
func (c *ClickHouse) Send(batch *model.Batch) {
	statistics.WritingPoolBacklog.WithLabelValues(c.taskCfg.Name).Inc()
	_ = util.GlobalWritingPool.Submit(func() {
		c.loopWrite(batch)
		statistics.WritingPoolBacklog.WithLabelValues(c.taskCfg.Name).Dec()
	})
}

// Write kvs to clickhouse
func (c *ClickHouse) write(batch *model.Batch) error {
	var numErr int
	var err, tmpErr error
	var stmt *sql.Stmt
	var tx *sql.Tx
	if len(*batch.Rows) == 0 {
		return nil
	}

	conn := pool.GetConn(c.taskCfg.Clickhouse, batch.BatchIdx)
	if tx, err = conn.Begin(); err != nil {
		goto ERR
	}
	if stmt, err = tx.Prepare(c.prepareSQL); err != nil {
		goto ERR
	}
	defer stmt.Close()
	for _, row := range *batch.Rows {
		if _, tmpErr = stmt.Exec(*row...); tmpErr != nil {
			numErr++
			err = tmpErr
		}
	}
	if err != nil {
		log.Errorf("%s: stmt.Exec failed %d times with errors %+v", c.taskCfg.Name, numErr, err)
		goto ERR
	}

	if err = tx.Commit(); err != nil {
		goto ERR
	}
	statistics.FlushMsgsTotal.WithLabelValues(c.taskCfg.Name).Add(float64(batch.RealSize))
	return err
ERR:
	if shouldReconnect(err) {
		_ = conn.ReConnect()
		statistics.ClickhouseReconnectTotal.WithLabelValues(c.taskCfg.Name).Inc()
	}
	return err
}

func shouldReconnect(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "bad connection") {
		return true
	}
	return false
}

// LoopWrite will dead loop to write the records
func (c *ClickHouse) loopWrite(batch *model.Batch) {
	var err error
	var times int
	for {
		if err = c.write(batch); err == nil {
			if err = batch.Commit(); err == nil {
				return
			}
			// TODO: kafka_go and sarama commit give different error when context is cancceled.
			// How to unify them?
			if std_errors.Is(err, context.Canceled) || std_errors.Is(err, io.ErrClosedPipe) {
				log.Infof("%s: ClickHouse.loopWrite quit due to the context has been cancelled", c.taskCfg.Name)
				return
			}
			log.Errorf("%s: committing offset failed with permanent error %+v", c.taskCfg.Name, err)
			os.Exit(-1)
		}
		if std_errors.Is(err, context.Canceled) {
			log.Infof("%s: ClickHouse.loopWrite quit due to the context has been cancelled", c.taskCfg.Name)
			return
		}
		log.Errorf("%s: flush batch(try #%d) failed with error %+v", c.taskCfg.Name, c.chCfg.RetryTimes-times, err)
		statistics.FlushMsgsErrorTotal.WithLabelValues(c.taskCfg.Name).Add(float64(batch.RealSize))
		times++
		if shouldReconnect(err) && (c.chCfg.RetryTimes <= 0 || times < c.chCfg.RetryTimes) {
			time.Sleep(10 * time.Second)
		} else {
			os.Exit(-1)
		}
	}
}

// Stop free clickhouse connections
func (c *ClickHouse) Stop() error {
	pool.FreeConn(c.taskCfg.Clickhouse)
	return nil
}

func (c *ClickHouse) initSchema() (err error) {
	if c.taskCfg.AutoSchema {
		conn := pool.GetConn(c.taskCfg.Clickhouse, 0)
		var rs *sql.Rows
		if rs, err = conn.Query(fmt.Sprintf(selectSQLTemplate, c.chCfg.DB, c.taskCfg.TableName)); err != nil {
			err = errors.Wrapf(err, "")
			return err
		}
		defer rs.Close()

		c.Dims = make([]*model.ColumnWithType, 0, 10)
		var name, typ, defaultKind string
		for rs.Next() {
			if err = rs.Scan(&name, &typ, &defaultKind); err != nil {
				err = errors.Wrapf(err, "")
				return err
			}
			typ = lowCardinalityRegexp.ReplaceAllString(typ, "$1")
			if !util.StringContains(c.taskCfg.ExcludeColumns, name) && defaultKind != "MATERIALIZED" {
				c.Dims = append(c.Dims, &model.ColumnWithType{Name: name, Type: typ, SourceName: util.GetSourceName(name)})
			}
		}
	} else {
		c.Dims = make([]*model.ColumnWithType, 0)
		for _, dim := range c.taskCfg.Dims {
			c.Dims = append(c.Dims, &model.ColumnWithType{
				Name:       dim.Name,
				Type:       dim.Type,
				SourceName: dim.SourceName,
			})
		}
	}
	//根据 Dms 生成prepare的sql语句
	c.Dms = make([]string, 0, len(c.Dims))
	quotedDms := make([]string, 0, len(c.Dims))
	for _, d := range c.Dims {
		c.Dms = append(c.Dms, d.Name)
		quotedDms = append(quotedDms, fmt.Sprintf("`%s`", d.Name))
	}
	var params = make([]string, len(c.Dims))
	for i := range params {
		params[i] = "?"
	}
	c.prepareSQL = "INSERT INTO " + c.chCfg.DB + "." + c.taskCfg.TableName + " (" + strings.Join(quotedDms, ",") + ") " +
		"VALUES (" + strings.Join(params, ",") + ")"

	log.Infof("%s: Prepare sql=> %s", c.taskCfg.Name, c.prepareSQL)
	return nil
}

func (c *ClickHouse) ChangeSchema(newKeys *sync.Map) (err error) {
	var sqls []string
	var onCluster string
	if c.taskCfg.DynamicSchema.Cluster != "" {
		onCluster = fmt.Sprintf("ON CLUSTER %s", c.taskCfg.DynamicSchema.Cluster)
	}
	maxDims := math.MaxInt16
	if c.taskCfg.DynamicSchema.MaxDims > 0 {
		maxDims = c.taskCfg.DynamicSchema.MaxDims
	}
	newKeysQuota := maxDims - len(c.Dims)
	if newKeysQuota <= 0 {
		log.Warnf("number of columns reaches upper limit %d", maxDims)
		return
	}
	var i int
	newKeys.Range(func(key, value interface{}) bool {
		i++
		if i > newKeysQuota {
			log.Warnf("number of columns reaches upper limit %d", maxDims)
			return false
		}
		strKey := key.(string)
		strVal := value.(string)
		switch strVal {
		case "int":
			strVal = "Nullable(Int64)"
		case "float":
			strVal = "Nullable(Float64)"
		case "string":
			strVal = "Nullable(String)"
		default:
			err = errors.Errorf("%s: BUG: unsupported column type %s", c.taskCfg.Name, strVal)
			return false
		}
		sql := fmt.Sprintf("ALTER TABLE %s.%s %s ADD COLUMN IF NOT EXISTS %s %s", c.chCfg.DB, c.taskCfg.TableName, onCluster, strKey, strVal)
		sqls = append(sqls, sql)
		return true
	})
	if err != nil {
		return
	}
	if c.taskCfg.DynamicSchema.DistTableName != "" {
		sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s %s", c.chCfg.DB, c.taskCfg.DynamicSchema.DistTableName, onCluster))
		sqls = append(sqls, fmt.Sprintf("CREATE TABLE %s.%s %s AS %s ENGINE = Distributed(%s, %s, %s);",
			c.chCfg.DB, c.taskCfg.DynamicSchema.DistTableName, onCluster, c.taskCfg.TableName,
			c.taskCfg.DynamicSchema.Cluster, c.chCfg.DB, c.taskCfg.TableName))
	}

	conn := pool.GetConn(c.taskCfg.Clickhouse, 0)
	for _, sql := range sqls {
		log.Infof("%s: executing sql=> %s", c.taskCfg.Name, sql)
		if _, err = conn.Exec(sql); err != nil {
			err = errors.Wrapf(err, sql)
			return err
		}
	}
	return
}
