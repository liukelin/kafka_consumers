package consumers

import (
	"encoding/json"
	"fmt"
	"kafka_consumers/config"
	"strings"
	"time"

	"github.com/golang/glog"

	redisC "kafka_consumers/redis/cluster"  // cluster
	redis "kafka_consumers/redis/default"   // 单点
	redisS "kafka_consumers/redis/sentinel" // sentinel
)

// redis操作通用接口
type RedisClass interface {
	Push(string, string) error
	Pop(string) (string, error)
	Len(string) (int64, error)
	Cmd(...interface{}) (interface{}, error)
}

// 消费者配置, 单个消费者连接属性
type ConsumersConf struct {
	Topics []string
	// ChPool chan []string // 每个消费者单独的channel
}

// 传输出来的消息格式
type OutputData struct {
	Attempts int    `json:"attempts"`
	Data     string `json:"data"`
}

// 初始化redis连接
func InitRedis() RedisClass {

	var redisClass RedisClass
	var redisAddr []string
	// 根据配置判断是 单点、cluster、sentinel
	if len(config.Config.RedisHost) > 1 && len(config.Config.RedisPort) > 1 && len(config.Config.RedisHost) == len(config.Config.RedisPort) {
		glog.Infof("连接cluster redis;")

		// 组合连接参数
		for k, v := range config.Config.RedisHost {
			redisAddr = append(redisAddr, fmt.Sprintf("%s:%s", v, config.Config.RedisPort[k]))
		}

		redisClass = &redisC.Queue{
			Conf: &redisC.Conf{
				Servers: redisAddr,
			},
		}
	} else if len(config.Config.RedisHost) > 1 && len(config.Config.RedisPort) == 1 {
		glog.Infof("连接sentinel redis;")

		// 组合连接参数
		for _, v := range config.Config.RedisHost {
			redisAddr = append(redisAddr, fmt.Sprintf("%s:%s", v, config.Config.RedisPort[0]))
		}

		redisClass = &redisS.Queue{
			Conf: &redisS.Conf{
				MasterName: config.Config.SentinelMasterName,
				Servers:    redisAddr,
			},
		}

	} else if len(config.Config.RedisHost) == 1 && len(config.Config.RedisPort) == 1 {
		glog.Infof("连接单点redis;")
		// 组合连接参数
		redisAddr = append(redisAddr, fmt.Sprintf("%s:%s", config.Config.RedisHost[0], config.Config.RedisPort[0]))

		redisClass = &redis.Queue{
			Conf: &redis.Conf{
				Servers: redisAddr,
			},
		}
	}
	return redisClass
}

// 消费控制器
// 每隔1s检查redis list是否已满
// 并且更新 topic积压信息
// 消费
func Controller(qcache RedisClass) {

	go func() {
		glog.Warning("消费控制器 start.")
		for {
			// 所有topic
			for _, topic := range config.Config.Topics {
				chOpen := true
				data, err := qcache.Len(fmt.Sprintf("%s%s", config.Config.PrefixRedis, topic))
				if err != nil {
					glog.Errorf("error redis len:%v", err)
					chOpen = false // 获取长度失败，先暂停消费
				} else {
					//int64(len(config.Config.ChPool))+
					if data > config.Config.CacheSize {
						chOpen = false
					}

					// 更新长度
					// UpdateTopicLen(topic, data)
				}
				UpChOpen(topic, chOpen)
			}
			time.Sleep(time.Duration(config.Config.ControllerTime) * time.Second)
		}
	}()

	glog.Warning("同步kafka数据到redis控制器 start.")
	go func() {
		ConsumerToCache(qcache)
	}()

	glog.Warning("同步Topic堆积信息到redis控制器 start.")
	go func() {
		TopicLenToCache(qcache)
	}()

	// 维护锁
	glog.Warning("维护运行锁 start.")
	go func() {
		for {
			UpdateLockTime(qcache)
			time.Sleep(time.Duration(config.Config.StartLockCheckTime) * time.Second)
		}
	}()

}

// 消费channel到redis
func ConsumerToCache(qcache RedisClass) {
	for i := range config.Config.ChPool {
		// 切割字符串
		if len(i) > 1 {
			json, err0 := Parsing(i[1])
			if err0 != nil {
				glog.Errorf("Parsing error:, %v", err0)
			} else {
				var rePush int64 = 1
				for {
					err := qcache.Push(fmt.Sprintf("%s%s", config.Config.PrefixRedis, i[0]), json)
					if err != nil {
						glog.Warning("redis 重试执行push:%v: %v", rePush, i)
						rePush++
						time.Sleep(2 * time.Second)
					} else {
						glog.Infof("[push] redis->%v:%v", i[0], json)
						break
					}
				}
			}
		}
	}
}

// 包装json
func Parsing(data string) (string, error) {
	d := &OutputData{
		Attempts: 1,
		Data:     data,
	}
	b, err := json.Marshal(d)
	if err != nil {
		glog.Errorf("Umarshal failed:%v", err)
		return "", err
	}
	return string(b), nil
}

// 获取开关状态
func GetChOpen(topic string) bool {
	v, ok := (config.Config.ChOpenMap).Load(topic)
	if ok {
		switch vv := v.(type) {
		case string:
			if v == "1" {
				return true
			}
		default:
			glog.Errorf("config.Config.ChOpenMap type: %v", vv)
			return false
		}
	}
	return false
}

// 开启和关闭开关
// 0关 1开
func UpChOpen(topic string, open bool) {
	nowChOpen := GetChOpen(topic)
	if open { // 可写
		if nowChOpen == false {
			config.Config.ChOpenMap.Store(topic, "1")
			glog.Warning("继续消费:Topic:", topic)
		}
	} else { // 不可写
		if nowChOpen == true {
			config.Config.ChOpenMap.Store(topic, "0")
			glog.Warning("暂停消费:Topic:", topic)
		}
	}
}

/*
获得锁
*/
func GetLock(qcache RedisClass) error {
	_, lock := qcache.Cmd("set", config.Config.StartLock, 1, "nx", "ex", config.Config.StartLockTimeOut)
	return lock
}

/**
更新锁时间
*/
func UpdateLockTime(qcache RedisClass) {
	qcache.Cmd("expire", config.Config.StartLock, config.Config.StartLockTimeOut)
}

// 定时将topic堆积信息更新到redis
func TopicLenToCache(qcache RedisClass) {
	for {

		d := make(map[string]int64)

		// 循环sync.map不可取,弃用
		f := func(k, v interface{}) bool {
			strs := k.(string)
			offset := v.(int64)

			// 切割
			arr := strings.Split(strs, "##")
			if len(arr) > 2 {
				if _, ok := d[arr[2]]; !ok {
					d[arr[2]] = 0
				}
				if _, ok := d[arr[2]]; ok {
					if arr[0] == "<all>" {
						d[arr[2]] = d[arr[2]] + offset
					} else {
						d[arr[2]] = d[arr[2]] - offset
					}
				}
			}
			return true
		}
		config.Config.TopicLenMap.Range(f)

		/*
			data, err := qcache.Cmd("hgetall", "kafka_offset_infos")
			if err != nil {
				glog.Errorf("hgetall kafka_offset_infos: %v", err)
			}

			switch list := data.(type) {
			case []interface{}:
				for i := 0; i < len(list); i++ {
					if i%2 == 0 {
						arr := strings.Split(list[i].(string), "##")

						if len(arr) > 2 {
							offset, _ := strconv.ParseInt(list[i+1].(string), 10, 64)
							if _, ok := d[arr[2]]; !ok {
								d[arr[2]] = 0
							}

							if arr[0] == "<all>" {
								d[arr[2]] = d[arr[2]] + offset
							} else {
								d[arr[2]] = d[arr[2]] - offset
							}
						}
					}
				}
			default:
				glog.Errorf("hgetall kafka_offset_infos type: %v", list)
			}
		*/
		for topic, offset := range d {
			if offset < 0 {
				offset = 0
			}
			qcache.Cmd("hmset", config.Config.TopicsQueueLen, topic, offset)
		}
		time.Sleep(time.Duration(config.Config.TopicLenToCacheTime) * time.Second)
	}
}

// 记录各个分区的offset
// <formal>##<0>##{topic} {"0"=>1, "2"=>1} 正式消费信息
// <all>##<0>##{topic}    {"0"=>1, "2"=>1} 全部消费信息
func SetTopicLen(types string, topic string, partition string, offset int64) {
	key := fmt.Sprintf("<%s>##<%s>##%s", types, partition, topic)
	config.Config.TopicLenMap.Store(key, offset)
	// qcache.Cmd("hmset", "kafka_offset_infos", key, offset)
}

/**
更新topic堆积情况
如果当前是停止消费状态，说明压力很大，则每次触发累加 cache_size, 最大100000
如果当前是正常消费状态，说明比较空闲，则每次触发 减半
*/
func UpdateTopicLen(topic string, len int64) {
	var num int64 = 0
	v, ok := (config.Config.TopicLenMap).Load(topic)
	if ok {
		switch vv := v.(type) {
		case int64:
			num = num + v.(int64)
		default:
			glog.Errorf("config.Config.TopicLenMap type: %v", vv)
			num = 0
		}
	}

	// redis_cache 持续超过长度xx后开始累加值，经过测试 cache_size=1000 持续堆积的数值>700
	if len < config.Config.MinAccumulated { // 正常消费
		// num = num / 2
		num = num - config.Config.Accumulated
	} else { // 停止消费
		num = num + config.Config.Accumulated
	}
	if num > config.Config.TopicLenMax {
		num = config.Config.TopicLenMax
	}
	if len == 0 || num < 0 {
		num = 0
	}
	config.Config.TopicLenMap.Store(topic, num)
}
