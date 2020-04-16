
# 消费kafka 到 redis的工具

    针对项目中kafka消费者数量受限于kafka的partition数（一个partition只能有一个consumer），如果业务进程直接连接kafka消费的话极大的限制了业务进程的并行计算的数量。并且业务进程如果频繁退出和启动，kafka为这些消费者存在频繁分配partition的情况，以此可能会造成时间段内的消费者消费不上的问题。

    目前的解决思路是，引入一个工具，减少同消费组消费者的数量，保证一个topic/partition只有1个消费者，并且是常驻连接，尽可能的减少重复连接消费者的情况。而redis对并发请求数量几乎没有限制，redis的list结构也基本满足作为一个临时队列的要求。所以该工具将kafka消费后的数据push到redis list，而业务进程从redis list获取数据进行消费。

    golang对并发有天然的自持，所以该工具使用golang进行开发。



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
        "consumer_num":2                                // 每个topic的消费者数量，该值应<=partition
    }
```