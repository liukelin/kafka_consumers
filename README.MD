
# 消费kafka 到 redis的工具

    概况
        针对项目中kafka消费者数量受限于kafka的partition数（一个partition只能有一个consumer）。
        如果业务进程直接连接kafka消费的话极大的限制了业务进程的并行计算的数量。
        并且业务进程如果频繁退出和启动，kafka为这些消费者存在频繁分配partition的情况. 
        以此可能会造成时间段内的消费者消费不上的问题。

    目前的解决思路是
        1. 引入一个工具，减少同消费组消费者的数量。
        2. 保证一个topic/partition只有1个消费者，并且是常驻连接，尽可能的减少重复连接消费者的情况。
        3. redis对并发请求数量几乎没有限制，redis的list结构也基本满足作为一个临时队列的要求。
        4. 所以该工具将kafka消费后的数据push到redis list，而业务进程从redis list获取数据进行消费。
        5. 为了避免redis list数据堆积，增加redis list堆积长度判断，如果达到该长度，则暂停从kafka消费数据。
        6. 并提供一个获取topic LAG信息的功能，将topic LAG信息记录到redis。
        7. golang对并发有天然的支持，并且静态编译也保证了运行环境的简洁，所以该工具使用golang进行开发。


    解决问题
        1. 解决多进程消费速度受限partition数量的问题，因为一个partition只能由一个消费者消费，限制了起消费者进程数量。
        2. 解决 同消费组下 多消费者id 对同个topic消费情况下，重连时候partition动态分配所造成的问题。
        3. 引入redis list结构作为消息缓冲，为业务进程提供数据,。因为redis特性，获取redis list的数据天然不存在竞争问题，这样业务进程的数量就可以随需求来动态增减，灵活的控制消费速度。
        4. redis（内存）消息堆积存在挤爆内存的风险，为了避免导致堆积，这里对写入数据的redis list 的长度做了限制，避免各个topic数据无限制的写入到redis key，保证等待消费的消息尽可能的待在安全的kafka(磁盘)保证不丢失。

![image](https://github.com/liukelin/kafka_consumers/raw/master/img/1587032745802.jpg)

```
    调试运行
    go run main.go --config=conf.json  -alsologtostderr
```

```
    配置文件说明
    {
        "kafka_addr":["127.0.0.1:9092","127.0.0.1:9093"],
        "redis_host":[
            "10.91.49.20","10.91.49.21","10.91.49.22"
        ],
        "redis_port":[
            "27000"
        ],
        "sentinel_master_name":"redis_cache_master",    // redis sentinel模式下必填
        "cache_size":5,                                 // redis list允许堆积的数量
        "topics":["lkl_test","lkl_make"],
        "consumer_group":"test-group",
        "start_lock":"kafka_consumers_run",
        "prefix_redis":"queues:",
        "consumer_num":2,                               // 每个topic的消费者数量，该值应<=partition
        "topics_queue_len":"topics_queue_len"          // 记录topic堆积情况key, 这个是预估值
    }

    // redis_host 和 redis_port 数量都=1的话，就是单点； 
    // redis_host 和 redis_port 数量相同 并且>1 就是cluster；
    // redis_host 数量>1, redis_port 数量=1 就是 sentinel;
```
