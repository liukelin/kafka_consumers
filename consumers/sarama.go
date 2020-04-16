package consumers

// /**
// 	kafka消费者
//  	@auth liukelin
// */
// import (
// 	"context"
// 	"kafka_consumers/config"
// 	"os"
// 	"os/signal"
// 	"sync"
// 	"syscall"
// 	"time"

// 	"github.com/Shopify/sarama"
// 	"github.com/golang/glog"
// )

// // 所有topic消费入口
// func Start(chPool chan string) {

// 	run := NewKafka(chPool).Init()
// 	//Ctrl+c 正常退出获取
// 	sigterm := make(chan os.Signal, 1)
// 	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
// 	select {
// 	case <-sigterm:
// 		glog.Warningf("terminating: via signal")
// 	}
// 	run()
// }

// type Consumer struct {
// 	brokers []string
// 	topics  []string
// 	//OffsetNewest int64 = -1
// 	//OffsetOldest int64 = -2
// 	startOffset       int64
// 	version           string
// 	ready             chan bool
// 	group             string
// 	channelBufferSize int
// 	chPool            chan string // 用于消费控制
// 	chOpen            *bool
// }

// func NewKafka(chPool chan string) *Consumer {
// 	// glog.Infof("====chOpen:%v, %v",, &chOpen)
// 	return &Consumer{
// 		brokers:           config.Config.KafkaAddr,
// 		topics:            config.Config.Topics,
// 		group:             config.Config.ConsumerGroup,
// 		channelBufferSize: 2,
// 		ready:             make(chan bool),
// 		version:           "1.1.1",
// 		chPool:            chPool,
// 		chOpen:            config.Config.ChOpen,
// 	}
// }

// func (p *Consumer) Init() func() {
// 	glog.Warningf("kafka Consumer init...")

// 	version, err := sarama.ParseKafkaVersion(p.version)
// 	if err != nil {
// 		glog.Fatal("Error parsing Kafka  version: %v", err)
// 	}
// 	config := sarama.NewConfig()
// 	config.Version = version
// 	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange // 分区分配策略
// 	config.Consumer.Offsets.Initial = -2                                   // 未找到组消费位移的时候从哪边开始消费
// 	config.ChannelBufferSize = p.channelBufferSize                         // channel长度

// 	ctx, cancel := context.WithCancel(context.Background())
// 	client, err := sarama.NewConsumerGroup(p.brokers, p.group, config)
// 	if err != nil {
// 		glog.Fatal("Error creating consumer group client: %v", err)
// 	}

// 	wg := &sync.WaitGroup{}
// 	wg.Add(1)

// 	// 退出执行, 保证在系统退出时，通道里面的消息被消费
// 	close := func() {
// 		glog.Warningf("kafka consumers close...")
// 		close(p.chPool)
// 		glog.Warningf("kafka consumers close cancel...")
// 		cancel()
// 		glog.Warningf("kafka consumers close Wait...")
// 		wg.Wait()

// 		if err = client.Close(); err != nil {
// 			glog.Errorf("Error closing client: %v", err)
// 		}
// 	}

// 	go func() {
// 		defer func() {
// 			wg.Done()
// 		}()
// 		for {
// 			if err := client.Consume(ctx, p.topics, p); err != nil {
// 				time.Sleep(time.Second)
// 				glog.Errorf("Error from consumer: %v", err)
// 			}
// 			// check if context was cancelled, signaling that the consumer should stop
// 			if ctx.Err() != nil {
// 				glog.Errorf("ctx Error:%v", ctx.Err())
// 				return
// 			}
// 			p.ready = make(chan bool)

// 			// debug
// 			glog.Infof("consumer suspended, open:%v; ", p.chOpen)
// 			time.Sleep(time.Second)
// 			// debug end
// 		}
// 	}()
// 	<-p.ready
// 	glog.Warningf("kafka consumer up and running...")

// 	return close
// }

// // Setup is run at the beginning of a new session, before ConsumeClaim
// func (p *Consumer) Setup(sarama.ConsumerGroupSession) error {
// 	// Mark the consumer as ready
// 	close(p.ready)
// 	return nil
// }

// // Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// func (p *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
// 	return nil
// }

// // ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// func (p *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
// 	// NOTE:
// 	// Do not move the code below to a goroutine.
// 	// The `ConsumeClaim` itself is called within a goroutine, see:
// 	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
// 	// 具体消费消息

// 	if *config.Config.ChOpen {
// 		for message := range claim.Messages() {
// 			msg := string(message.Value)

// 			p.chPool <- msg

// 			glog.Infof("msg: %s, %v, %v", msg, config.Config.ChOpen)
// 			time.Sleep(time.Second)

// 			// 更新位移
// 			session.MarkMessage(message, "")
// 		}
// 	} else {
// 		glog.Infof("暂停消费msg:%v.", config.Config.ChOpen)
// 		time.Sleep(time.Second)
// 	}

// 	/*
// 		for {
// 			// if len(p.chOpen) == 0 {
// 			select {
// 			case p.chOpen <- 1:

// 			case message, ok := <-claim.Messages():
// 				if !ok {
// 					continue
// 				}

// 				msg := string(message.Value)

// 				<-p.chOpen

// 				p.chPool <- msg

// 				glog.Infof("msg: %s, %v, %v", msg, len(p.chOpen), &p.chOpen)
// 				time.Sleep(time.Second)

// 				// 更新位移
// 				session.MarkMessage(message, "")

// 				continue
// 			default:
// 				glog.Infof("msg default.")
// 				// time.Sleep(time.Second)
// 				break
// 			}
// 			// } else {
// 			// 	time.Sleep(time.Second)
// 			// }
// 		}
// 	*/
// 	glog.Infof("ConsumeClaim...")
// 	return nil
// }

// // func main() {
// // 	k := NewKafka()
// // 	f := k.Init()

// // 	sigterm := make(chan os.Signal, 1)
// // 	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
// // 	select {
// // 	case <-sigterm:
// // 		glog.Infof("terminating: via signal")
// // 	}
// // 	f()
// // }

// /*
// // 单个topic消费
// func (conf *Conf) Consumers(topic string) {
// 	// 根据给定的代理地址和配置创建一个消费者
// 	consumer, err := sarama.NewConsumer(conf.Addrs, nil)
// 	if err != nil {
// 		glog.Infof("创建消费者失败")
// 		panic(err)
// 	}
// 	//Partitions(topic):该方法返回了该topic的所有分区id
// 	partitionList, err := consumer.Partitions(topic)
// 	if err != nil {
// 		panic(err)
// 	}
// 	// 消费所有分区
// 	// var wg sync.WaitGroup
// 	wg := &sync.WaitGroup{}
// 	for partition := range partitionList {
// 		//ConsumePartition方法根据主题，分区和给定的偏移量创建创建了相应的分区消费者
// 		//如果该分区消费者已经消费了该信息（没有新消息）将会返回error
// 		//sarama.OffsetNewest:表明了为最新消息偏移
// 		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
// 		if err != nil {
// 			panic(err)
// 		}
// 		defer pc.AsyncClose()

// 		wg.Add(1)
// 		go func(sarama.PartitionConsumer) {
// 			defer wg.Done()
// 			//Messages()该方法返回一个消费消息类型的只读通道，由代理产生
// 			for msg := range pc.Messages() {
// 				glog.Infof("%s---Partition:%d, Offset:%d, Key:%s, Value:%s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
// 				conf.ChPool <- string(msg.Value)
// 			}
// 		}(pc)
// 	}
// 	wg.Wait()
// 	consumer.Close()
// }
// */
