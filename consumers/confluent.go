package consumers

/*
	消费kafka
	自动提交commit
	@auth liukelin
*/
import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"kafka_consumers/config"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/glog"
)

/*

  自动commit https://github.com/confluentinc/confluent-kafka-go/tree/master/examples/consumer_example
  手动commit offset https://github.com/confluentinc/confluent-kafka-go/tree/master/examples/consumer_offset_metadata
*/
func ConfluentConsumer(consumerConf *ConsumersConf) {
	glog.Warningf("Consumer init...")

	topics := consumerConf.Topics
	if len(consumerConf.Topics) == 0 {
		topics = config.Config.Topics
	}
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     strings.Join(config.Config.KafkaAddr, ","),
		"broker.address.family": "v4",
		"group.id":              config.Config.ConsumerGroup,
		"session.timeout.ms":    60000,
		"auto.offset.reset":     "earliest",
		"enable.auto.commit":    false,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s.", err)
		// os.Exit(1)
		return
	}

	glog.Infof("Consumer Created ok. %v", c)

	// LOOP:
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to SubscribeTopics: %s.", err)
		// os.Exit(1)
		return
	}

	glog.Infof("Consumer runing.. Topic:%v; ", topics)

	run := true
	chOpenOld := true
	oldOffsets := []kafka.TopicPartition{}

	for run == true {
		chOpen := GetChOpen(topics[0])

		// 开关开启/或者关闭了，重新发起一次订阅
		if chOpenOld != chOpen && len(oldOffsets) > 0 {
			chOpenOld = chOpen
			// goto LOOP
			if chOpen {
				// 恢复消费
				c.Resume(oldOffsets)
			} else {
				// 暂停消费
				c.Pause(oldOffsets)
			}
		}

		ev := c.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:

			// [work_queue_ngx2php[0]@unset work_queue_ngx2php[1]@45 work_queue_ngx2php[2]@unset];
			// [work_queue_ngx2php[0]@unset work_queue_ngx2php[1]@unset work_queue_ngx2php[2]@48];
			offsets, errComm := c.Commit()
			if errComm != nil {
				glog.Errorf("Kafka Commit Error: %v.", errComm)
				// 失败后异步再试一次
			} else {
				oldOffsets = offsets
			}

			d := []string{*e.TopicPartition.Topic, string(e.Value)}
			config.Config.ChPool <- d
			glog.Infof("[Message][Topic[Partition]@Offsets]: %v; msg: %v;", e.TopicPartition.String(), d[1])
			glog.Infof("[Metadata]:%v, %v", e.TopicPartition.Metadata, e.String())

			offits, _ := strconv.ParseInt(e.TopicPartition.Offset.String(), 10, 64)
			partition := strconv.FormatInt(int64(e.TopicPartition.Partition), 10)
			SetTopicLen("formal", *e.TopicPartition.Topic, partition, offits)

			// _, errCommitOffset := c.CommitOffsets(offsets)
			// if errCommitOffset == nil {
			// 	d := []string{*e.TopicPartition.Topic, string(e.Value)}
			// 	config.Config.ChPool <- d
			// 	glog.Infof("[Message][Topic[Partition]@Offsets]: %v; msg: %v;", e.TopicPartition.String(), d[1])
			// } else {
			// 	glog.Errorf("Kafka CommitOffsets Error: offsets:%v; error: %v.", offsets, errCommitOffset)
			// }

			// } else {
			// 	// 提交上一次offset
			// 	var metadata string
			// 	metadata = "" // *e.TopicPartition.Metadata
			// 	offits, _ := strconv.Atoi(e.TopicPartition.Offset.String())
			// 	partition := int(e.TopicPartition.Partition)
			// 	commitOffset(c, *e.TopicPartition.Topic, partition, offits, metadata)

			// 	// 避免太频繁的连接订阅，这里暂停下
			// 	// time.Sleep(time.Duration(config.Config.ControllerTime) * time.Second)
			// 	// goto LOOP // 从上一次的位置开始消费，相当于重复读取该消息
			// }

		case kafka.Error:
			// fmt.Fprintf(os.Stderr, "%% Error: %v: %v", e.Code(), e)
			glog.Errorf("Error: [%v] %v; %v.", topics[0], e.Code(), e)
			if e.Code() == kafka.ErrAllBrokersDown {
				glog.Errorf("Kafka ErrAllBrokersDown: %v: %v.", e.Code(), e)
				// os.Exit(1)
			}
			// 发起重连
		default:
			glog.Infof("default: %v", e)
		}
	}

	glog.Warningf("Closing consumer.")
	c.Close()
}

// 获取分区消息堆积数量
func getLag(consumer *kafka.Consumer, topicPartition kafka.TopicPartition) (int64, error) {
	kafkaTimeout := 10000
	// lowOffset: The offset of the earliest message in the topic/partition. If no messages have been written to the topic, the low
	//  watermark offset is set to 0. The low watermark will also be 0 if one message has been written to the partition (with offset 0).
	// highOffset: The high watermark offset, which is the offset of the latest message in the topic/partition available for consumption + 1.
	_, highOffset, err := consumer.QueryWatermarkOffsets(*topicPartition.Topic, topicPartition.Partition, kafkaTimeout)
	if err != nil {
		return -1, err
	}
	offset := int64(topicPartition.Offset)
	return highOffset - offset, nil
}

// 订阅全部topic
func ConfluentConsumerAll() {
	glog.Warningf("Consumer init...")

	topics := config.Config.Topics

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     strings.Join(config.Config.KafkaAddr, ","),
		"broker.address.family": "v4",
		"group.id":              fmt.Sprintf("%s_all", config.Config.ConsumerGroup),
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s.", err)
		os.Exit(1)
	}

	// LOOP:
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to SubscribeTopics: %s.", err)
		os.Exit(1)
	}

	run := true
	for run == true {

		ev := c.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:

			offits, _ := strconv.ParseInt(e.TopicPartition.Offset.String(), 10, 64)
			partition := strconv.FormatInt(int64(e.TopicPartition.Partition), 10)
			SetTopicLen("all", *e.TopicPartition.Topic, partition, offits)

		case kafka.Error:
			// fmt.Fprintf(os.Stderr, "%% Error: %v: %v", e.Code(), e)
			glog.Errorf("Error: [%v] %v; %v.", topics[0], e.Code(), e)
			if e.Code() == kafka.ErrAllBrokersDown {
				glog.Errorf("Kafka ErrAllBrokersDown: %v: %v.", e.Code(), e)
				os.Exit(1)
			}
			// 发起重连
		default:
			// glog.Infof("default: %v", e)
		}
	}

	glog.Warningf("Closing consumer.")
	c.Close()
}

// 手动提交commit
func commitOffset(c *kafka.Consumer, topic string, partition int, offset int, metadata string) {
	res, err := c.CommitOffsets([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: int32(partition),
		Metadata:  &metadata,
		Offset:    kafka.Offset(offset),
	}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to commit offset: %s\n", err)
		os.Exit(1)
	}
	glog.Infof("回滚一次 Topic %v Partition %d offset %v committed successfully", topic, res[0].Partition, offset)
}

// 获取Offset
func showPartitionOffset(c *kafka.Consumer, topic string, partition int) {
	committedOffsets, err := c.Committed([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: int32(partition),
	}}, 5000)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to fetch offset: %s\n", err)
		os.Exit(1)
	}

	committedOffset := committedOffsets[0]

	fmt.Printf("Committed partition %d offset: %d", committedOffset.Partition, committedOffset.Offset)

	if committedOffset.Metadata != nil {
		fmt.Printf(" metadata: %s", *committedOffset.Metadata)
	} else {
		fmt.Println("\n Looks like we fetch empty metadata. Ensure that librdkafka version > v1.1.0")
	}
}

// 获取partition lag
func getPartitionLag(c *kafka.Consumer, topic string, partition int) {

}
