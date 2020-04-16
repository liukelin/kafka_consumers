package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/golang/glog"
	"github.com/joho/godotenv"
)

type InitConfig struct {
	KafkaAddr           []string      `json:"kafka_addr"`
	RedisHost           []string      `json:"redis_host"`
	RedisPort           []string      `json:"redis_port"`
	SentinelMasterName  string        `json:"sentinel_master_name"`
	CacheSize           int64         `json:"cache_size"`
	Topics              []string      `json:"topics"`
	ConsumerGroup       string        `json:"consumer_group"`
	TopicsQueueLen      string        `json:"topics_queue_len"` // 记录每个topic的长度
	StartLock           string        `json:"start_lock"`       // 锁名称
	PrefixRedis         string        `json:"prefix_redis"`     // 输出redis前缀
	Accumulated         int64         `json:"accumulated"`      // 暂停消费时候累加值
	MinAccumulated      int64         `json:"min_accumulated"`  // 超过XX后开始累加值，经过测试 cache_size=1000 持续堆积的数值>700
	ConsunerNum         int64         `json:"consumer_num"`     // 每个topic消费者数量， 该值应该小于分区数
	StartLockTimeOut    int64         // 运行锁失效时间
	StartLockCheckTime  int64         // 运行锁维护间隔时间
	ChPool              chan []string // 缓存池子 kafka与redis 数据传输介质，长度可以长点
	ChOpenMap           *sync.Map     // 全局消费开关，指针类型
	TopicLenMap         *sync.Map     // 记录所有topic堆积情况，指针类型
	TopicLenMax         int64         // 记录所有topic堆积情况，最大长度
	TopicLenToCacheTime int64         // 将topic堆积数据同步到redis间隔时间
	ControllerTime      int64         // 检查redis list长度 间隔时间
}

var Config *InitConfig
var Sigchan = make(chan os.Signal, 1)

func init() {
	signal.Notify(Sigchan, syscall.SIGINT, syscall.SIGTERM)
	chPool := make(chan []string, 1) // 缓存池子 kafka与redis 数据传输介质，长度可以长点
	chOpenMap := new(sync.Map)
	topicLenMap := new(sync.Map)
	Config = &InitConfig{
		ChOpenMap:           chOpenMap,
		ChPool:              chPool,
		TopicLenMap:         topicLenMap,
		StartLockTimeOut:    20,
		StartLockCheckTime:  15,
		TopicLenMax:         1000000,
		TopicLenToCacheTime: 10,
		ControllerTime:      1,
	}
}

// 解析config配置文件
func LoadConfig(confPath string) error {
	data, err := ioutil.ReadFile(confPath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, &Config)
	return err
}

// 解析env配置文件
func LoadEnv(envPath string) error {
	err := godotenv.Load(envPath)
	if err != nil {
		glog.Warningf("Error loading .env file, %v", err)
		return err
	}

	kafkaAddr := os.Getenv("KAFKA_BROKERS")
	redisHost := os.Getenv("REDIS_HOST") // 存在 127.0.0.1;127.0.0.2
	redisPort := os.Getenv("REDIS_PORT") // 存在 单个
	redisSentinelService := os.Getenv("REDIS_SENTINEL_SERVICE")

	// 填充数据
	if kafkaAddr != "" {
		Config.KafkaAddr = strings.Split(kafkaAddr, ",")
	}
	if redisHost != "" {
		Config.RedisHost = strings.Split(redisHost, ";")
	}
	if redisPort != "" {
		Config.RedisPort = []string{redisPort}
	}
	if redisSentinelService != "" {
		Config.SentinelMasterName = redisSentinelService
	}

	return nil
}
