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

package task

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"

	log "github.com/sirupsen/logrus"
)

// TaskService holds the configuration for each task
type Service struct {
	sync.Mutex

	parentCtx  context.Context
	ctx        context.Context
	cancel     context.CancelFunc
	started    bool
	stopped    chan struct{}
	inputer    input.Inputer
	clickhouse *output.ClickHouse
	pp         *parser.Pool
	cfg        *config.Config
	taskCfg    *config.TaskConfig
	dims       []*model.ColumnWithType

	knownKeys  sync.Map
	newKeys    sync.Map
	cntNewKeys int32 // size of newKeys
	tid        goetty.Timeout

	rings     []*Ring
	sharder   *Sharder
	batchChan chan *model.Batch
	limiter1  *rate.Limiter
	limiter2  *rate.Limiter
	limiter3  *rate.Limiter
}

// NewTaskService creates an instance of new tasks with kafka, clickhouse and paser instances
func NewTaskService(inputer input.Inputer, clickhouse *output.ClickHouse, pp *parser.Pool, cfg *config.Config, taskName string) *Service {
	return &Service{
		stopped:    make(chan struct{}),
		inputer:    inputer,
		clickhouse: clickhouse,
		started:    false,
		pp:         pp,
		cfg:        cfg,
		taskCfg:    cfg.Tasks[taskName],
	}
}

// Init initializes the kafak and clickhouse task associated with this service

func (service *Service) Init() (err error) {
	if err = service.clickhouse.Init(); err != nil {
		return
	}

	service.dims = service.clickhouse.Dims
	service.batchChan = make(chan *model.Batch, 32)
	service.limiter1 = rate.NewLimiter(rate.Every(10*time.Second), 1)
	service.limiter2 = rate.NewLimiter(rate.Every(10*time.Second), 1)
	service.limiter3 = rate.NewLimiter(rate.Every(10*time.Second), 1)

	service.rings = make([]*Ring, 0)
	if service.taskCfg.ShardingKey != "" {
		if service.sharder, err = NewSharder(service); err != nil {
			return
		}
	}

	err = service.inputer.Init(service.cfg, service.taskCfg.Name, service.put)

	if service.taskCfg.DynamicSchema.Enable {
		maxDims := math.MaxInt16
		if service.taskCfg.DynamicSchema.MaxDims > 0 {
			maxDims = service.taskCfg.DynamicSchema.MaxDims
		}
		if maxDims <= len(service.dims) {
			service.taskCfg.DynamicSchema.Enable = false
			log.Warnf("%s: disabled DynamicSchema since the number of columns reaches upper limit %d", service.taskCfg.Name, maxDims)
		} else {
			for _, dim := range service.dims {
				service.knownKeys.Store(dim.SourceName, nil)
			}
			for _, dim := range service.taskCfg.ExcludeColumns {
				service.knownKeys.Store(dim, nil)
			}
			service.newKeys = sync.Map{}
			atomic.StoreInt32(&service.cntNewKeys, 0)
		}
	}
	return
}

// Run starts the task
func (service *Service) Run(ctx context.Context) {
	var err error
	service.started = true
	service.parentCtx = ctx
	service.ctx, service.cancel = context.WithCancel(ctx)
	log.Infof("%s: task started", service.taskCfg.Name)
	go service.inputer.Run(service.ctx)
	if service.sharder != nil {
		// schedule a delayed ForceFlush
		if service.sharder.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(service.taskCfg.FlushInterval)*time.Second, service.sharder.ForceFlush, nil); err != nil {
			err = errors.Wrap(err, "")
			log.Fatalf("%s: got error %+v", service.taskCfg.Name, err)
		}
	}

LOOP:
	for {
		select {
		case <-service.ctx.Done():
			break LOOP
		case batch := <-service.batchChan:
			if err := service.flush(batch); err != nil {
				log.Errorf("%s: got error %+v", service.taskCfg.Name, err)
			}
		}
	}
	service.stopped <- struct{}{}
}

func (service *Service) fnCommit(partition int, offset int64) error {
	msg := model.InputMessage{Topic: service.taskCfg.Topic, Partition: partition, Offset: offset}
	return service.inputer.CommitMessages(service.ctx, &msg)
}

func (service *Service) put(msg model.InputMessage) {
	statistics.ConsumeMsgsTotal.WithLabelValues(service.taskCfg.Name).Inc()
	// ensure ring for this message exist
	service.Lock()
	var ring *Ring
	if msg.Partition < len(service.rings) {
		ring = service.rings[msg.Partition]
	} else {
		for i := len(service.rings); i < msg.Partition+1; i++ {
			service.rings = append(service.rings, nil)
		}
	}

	var err error
	if ring == nil {
		batchSizeShift := util.GetShift(service.taskCfg.BufferSize)
		ringCap := int64(1 << (batchSizeShift + 1))
		ring := &Ring{
			ringBuf:          make([]model.MsgRow, ringCap),
			ringCap:          ringCap,
			ringGroundOff:    msg.Offset,
			ringCeilingOff:   msg.Offset,
			ringFilledOffset: msg.Offset,
			batchSizeShift:   batchSizeShift,
			idleCnt:          0,
			isIdle:           false,
			partition:        msg.Partition,
			batchSys:         model.NewBatchSys(service.taskCfg, service.fnCommit),
			service:          service,
		}
		// schedule a delayed ForceBatchOrShard
		if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(service.taskCfg.FlushInterval)*time.Second, ring.ForceBatchOrShard, nil); err != nil {
			err = errors.Wrap(err, "")
			log.Fatalf("%s: got error %+v", service.taskCfg.Name, err)
		}
		service.rings[msg.Partition] = ring
		service.Unlock()
	} else {
		service.Unlock()
		var ringGroundOff, ringFilledOffset int64
		ring.mux.Lock()
		ringGroundOff, ringFilledOffset = ring.ringGroundOff, ring.ringFilledOffset
		ring.mux.Unlock()
		if msg.Offset < ringFilledOffset {
			statistics.RingMsgsOffTooSmallErrorTotal.WithLabelValues(service.taskCfg.Name).Inc()
			if service.limiter2.Allow() {
				log.Warnf("%s: got a message(topic %v, partition %d, offset %v) left to %v",
					service.taskCfg.Name, msg.Topic, msg.Partition, msg.Offset, ringFilledOffset)
			}
			return
		}
		if msg.Offset >= ringGroundOff+ring.ringCap && atomic.LoadInt32(&service.cntNewKeys) == 0 {
			statistics.RingMsgsOffTooLargeErrorTotal.WithLabelValues(service.taskCfg.Name).Inc()
			if service.limiter3.Allow() {
				log.Warnf("%s: got a message(topic %v, partition %d, offset %v) right to the range [%v, %v)",
					service.taskCfg.Name, msg.Topic, msg.Partition, msg.Offset, ring.ringGroundOff, ring.ringGroundOff+ring.ringCap)
			}
			time.Sleep(1 * time.Second)
			ring.ForceBatchOrShard(&msg)
		}
	}

	// submit message to a goroutine pool
	statistics.ParsingPoolBacklog.WithLabelValues(service.taskCfg.Name).Inc()
	_ = util.GlobalParsingPool.Submit(func() {
		var row *model.Row
		p := service.pp.Get()
		metric, err := p.Parse(msg.Value)
		if err != nil {
			statistics.ParseMsgsErrorTotal.WithLabelValues(service.taskCfg.Name).Inc()
			if service.limiter1.Allow() {
				log.Errorf("%s: failed to parse message(topic %v, partition %d, offset %v) %+v, string(value) <<<%+v>>>, got error %+v",
					service.taskCfg.Name, msg.Topic, msg.Partition, msg.Offset, msg, string(msg.Value), err)
			}
		} else {
			row = model.MetricToRow(metric, msg, service.dims)
		}
		service.pp.Put(p)

		if service.taskCfg.DynamicSchema.Enable {
			found := metric.GetNewKeys(&service.knownKeys, &service.newKeys)
			if found {
				cntNewKeys := atomic.AddInt32(&service.cntNewKeys, 1)
				if cntNewKeys == 1 {
					// The first message which contains new keys triggers flushing
					// all messages and scheduling a delayed func to apply schema change.
					for _, ring := range service.rings {
						if ring != nil {
							ring.ForceBatchOrShard(nil)
						}
					}
					if service.sharder != nil {
						service.sharder.ForceFlush(nil)
					}
					if service.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(service.taskCfg.FlushInterval)*time.Second, service.changeSchema, nil); err != nil {
						log.Fatalf("got error %+v", err)
						os.Exit(-1)
					}
				}
			}
		}
		if atomic.LoadInt32(&service.cntNewKeys) == 0 {
			var ring *Ring
			service.Lock()
			ring = service.rings[msg.Partition]
			service.Unlock()
			ring.PutElem(model.MsgRow{Msg: &msg, Row: row})
		}
		statistics.ParsingPoolBacklog.WithLabelValues(service.taskCfg.Name).Dec()
	})
}

func (service *Service) flush(batch *model.Batch) (err error) {
	if (len(*batch.Rows)) == 0 {
		return batch.Commit()
	}
	service.clickhouse.Send(batch)
	return nil
}

func (service *Service) changeSchema(arg interface{}) {
	var err error
	// change schema
	if err = service.clickhouse.ChangeSchema(&service.newKeys); err != nil {
		log.Fatalf("%s: clickhouse.ChangeSchema failed with error: %+v", service.taskCfg.Name, err)
		os.Exit(-1)
	}
	// restart myself
	service.Stop()
	if err = service.Init(); err != nil {
		log.Fatalf("%s: init failed with error: %+v", service.taskCfg.Name, err)
		os.Exit(-1)
	}
	go service.Run(service.parentCtx)
}

// NotifyStop notify task to stop, This is non-blocking.
func (service *Service) NotifyStop() {
	log.Infof("%s: notified to stop", service.taskCfg.Name)
	service.cancel()
}

// Stop stop kafka and clickhouse client. This is blocking.
func (service *Service) Stop() {
	log.Infof("%s: stopping task service...", service.taskCfg.Name)
	service.cancel()
	if err := service.inputer.Stop(); err != nil {
		panic(err)
	}
	log.Infof("%s: stopped input", service.taskCfg.Name)

	_ = service.clickhouse.Stop()
	log.Infof("%s: stopped output", service.taskCfg.Name)

	if service.sharder != nil {
		service.sharder.tid.Stop()
	}
	for _, ring := range service.rings {
		if ring != nil {
			ring.tid.Stop()
		}
	}
	service.tid.Stop()
	log.Infof("%s: stopped internal timers", service.taskCfg.Name)

	if service.started {
		<-service.stopped
	}
	log.Infof("%s: stopped", service.taskCfg.Name)
}

// GoID returns goroutine id
func GoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}
