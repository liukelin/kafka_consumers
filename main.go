package main

import (
	"flag"
	"kafka_consumers/config"
	"kafka_consumers/consumers"
	"os"
	"path/filepath"
	"time"

	"github.com/golang/glog"
)

var (
	ConfName             = "conf.json"
	KafkaAddr            = []string{"localhost:9092"}
	RedisHost            = []string{"localhost"}
	RedisPort            = []string{"6379"}
	CacheSize      int64 = 10                    //允许redis缓存默认数量
	StartLock            = "kafka_consumers_run" // 运行锁名称 redis key
	TopicsQueueLen       = "topics_queue_len"    // 记录topics消息堆积情况 redis key
	RedisClass     consumers.RedisClass
	Accumulated    int64 = 1000 // 暂停消费时候每秒累加值
	MinAccumulated int64 = 700  // redis堆积>700开始累加值
	ConsunerNum    int   = 1    // topic 消费者数量
	// ChOpen             *bool    // 全局总开关，指针类型
)

func init() {

	confName := flag.String("config", "", "配置文件路径")
	envName := flag.String("env", "", "ENV配置文件路径")
	flag.Parse()

	glog.Warning("-----------Start kafka consumers----------")

	if confName == nil {
		ConfName = getCurrentPath(ConfName) // 默认为跟目录
	} else {
		ConfName = *confName
	}

	err := config.LoadConfig(ConfName)
	if err != nil {
		glog.Fatal(err)
	}

	if envName != nil {
		err2 := config.LoadEnv(*envName)
		if err2 != nil {
			glog.Warning(err2)
		}
	}

	// 组织缺省参数, 以及必要参数检查
	if len(config.Config.KafkaAddr) == 0 {
		config.Config.KafkaAddr = KafkaAddr
	}
	if len(config.Config.RedisHost) == 0 {
		config.Config.RedisHost = RedisHost
	}
	if len(config.Config.RedisPort) == 0 {
		config.Config.RedisPort = RedisPort
	}
	if len(config.Config.Topics) == 0 {
		glog.Fatal("topics is nil;")
	}
	if config.Config.StartLock == "" {
		config.Config.StartLock = StartLock
	}
	if config.Config.TopicsQueueLen == "" {
		config.Config.TopicsQueueLen = TopicsQueueLen
	}
	if config.Config.CacheSize == 0 {
		config.Config.CacheSize = CacheSize
	}
	if config.Config.Accumulated == 0 {
		config.Config.Accumulated = Accumulated
	}
	if config.Config.MinAccumulated == 0 {
		config.Config.MinAccumulated = MinAccumulated
	}
	if config.Config.ConsunerNum == 0 {
		config.Config.ConsunerNum = ConsunerNum
	}

	glog.Warningf("%#v", config.Config)

	// 连接redis
	RedisClass = consumers.InitRedis()

}

// 获取项目当前路径
func getCurrentPath(path ...string) string {
	selfDir, err := filepath.Abs(os.Args[0])
	if err != nil {
		panic(err)
	}
	return filepath.Join(filepath.Dir(selfDir), filepath.Join(path...))
}

func main() {

	// 排他锁
	check := consumers.GetLock(RedisClass)
	if check != nil {
		glog.Infof("等待运行...")
		// 5秒后退出
		time.Sleep(5 * time.Second)
		os.Exit(1)
	}

	// 启动总消费, 用于获取最新的offsets
	go func() {
		consumers.ConfluentConsumerAll()
	}()

	// 启动kafka消费者
	for _, topic := range config.Config.Topics {
		// 初始化消费开关
		consumers.UpChOpen(topic, true)

		for i := 0; i < config.Config.ConsunerNum; i++ {
			// 初始化配置
			consumerConf := &consumers.ConsumersConf{
				Topics: []string{topic},
			}
			go func() {
				consumers.ConfluentConsumer(consumerConf)
			}()
		}
	}

	// 启动控制器
	consumers.Controller(RedisClass)

	for {
		select {
		case sig := <-config.Sigchan:
			glog.Errorf("signal信号:%v.", sig)
			os.Exit(1)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}
