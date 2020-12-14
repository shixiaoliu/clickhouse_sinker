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

package parser

import (
	"fmt"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fastjson"
)

var _ Parser = (*FastjsonParser)(nil)

// FastjsonParser, parser for get data in json format
type FastjsonParser struct {
	tsLayout []string
	fjp      fastjson.Parser
}

func (p *FastjsonParser) Parse(bs []byte) (metric model.Metric, err error) {
	var value *fastjson.Value
	if value, err = p.fjp.ParseBytes(bs); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	metric = &FastjsonMetric{value: value, tsLayout: p.tsLayout}
	return
}

type FastjsonMetric struct {
	value    *fastjson.Value
	tsLayout []string
}

func (c *FastjsonMetric) Get(key string) interface{} {
	return c.value.Get(key)
}

func (c *FastjsonMetric) GetString(key string, nullable bool) interface{} {
	v := c.value.GetStringBytes(key)
	if nullable && v == nil {
		return nil
	}
	return string(v)
}

func (c *FastjsonMetric) GetFloat(key string, nullable bool) interface{} {
	if nullable && !c.value.Exists(key) {
		return nil
	}
	return c.value.GetFloat64(key)
}

func (c *FastjsonMetric) GetInt(key string, nullable bool) interface{} {
	if nullable && !c.value.Exists(key) {
		return nil
	}
	return int64(c.value.GetInt(key))
}

func (c *FastjsonMetric) GetArray(key string, t string) interface{} {
	array := c.value.GetArray(key)
	if array == nil {
		return nil
	}
	switch t {
	case "float":
		results := make([]float64, 0, len(array))
		for _, e := range array {
			v, _ := e.Float64()
			results = append(results, v)
		}
		return results
	case "int":
		results := make([]int, 0, len(array))
		for _, e := range array {
			v, _ := e.Int()
			results = append(results, v)
		}
		return results
	case "string":
		results := make([]string, 0, len(array))
		for _, e := range array {
			v, _ := e.StringBytes()
			results = append(results, string(v))
		}
		return results
	default:
		panic("not supported array type " + t)
	}
}

func (c *FastjsonMetric) String() string {
	return c.value.String()
}

func (c *FastjsonMetric) GetDate(key string, nullable bool) interface{} {
	if nullable && !c.value.Exists(key) {
		return nil
	}

	val := c.GetString(key, false).(string)
	t, _ := time.Parse(c.tsLayout[0], val)
	return t
}

func (c *FastjsonMetric) GetDateTime(key string, nullable bool) interface{} {
	if nullable && !c.value.Exists(key) {
		return nil
	}

	if v := c.GetFloat(key, false).(float64); v != 0 {
		return time.Unix(int64(v), int64(v*1e9)%1e9)
	}

	val := c.GetString(key, false).(string)
	t, _ := time.Parse(c.tsLayout[1], val)
	return t
}

func (c *FastjsonMetric) GetDateTime64(key string, nullable bool) interface{} {
	if nullable && !c.value.Exists(key) {
		return nil
	}

	if v := c.GetFloat(key, false).(float64); v != 0 {
		return time.Unix(int64(v), int64(v*1e9)%1e9)
	}

	val := c.GetString(key, false).(string)
	t, _ := time.Parse(c.tsLayout[2], val)
	return t
}

func (c *FastjsonMetric) GetElasticDateTime(key string, nullable bool) interface{} {
	val := c.GetString(key, nullable)
	if val == nil {
		return nil
	}
	t, _ := time.Parse(time.RFC3339, val.(string))

	return t.Unix()
}

func (c *FastjsonMetric) GetNewKeys(knownKeys *sync.Map, newKeys *sync.Map) (foundNew bool) {
	var obj *fastjson.Object
	var err error
	if obj, err = c.value.Object(); err != nil {
		return
	}
	obj.Visit(func(key []byte, v *fastjson.Value) {
		strKey := string(key)
		if _, loaded := knownKeys.LoadOrStore(strKey, nil); !loaded {
			v := c.value.Get(strKey)
			if v == nil {
				panic(fmt.Sprintf("BUG: fastjson.Object.Visit got an unexpected key: %s", strKey))
			}
			if _, err = v.Int64(); err == nil {
				newKeys.Store(strKey, "int")
				foundNew = true
			} else if _, err = v.Float64(); err == nil {
				newKeys.Store(strKey, "float")
				foundNew = true
			} else if _, err = v.StringBytes(); err == nil {
				newKeys.Store(strKey, "string")
				foundNew = true
			} else {
				log.Warnf("FastjsonMetric.GetNewKeys found a kv not be int/float/string, key: %s, value: %s", strKey, v.String())
			}
		}
	})
	return
}
