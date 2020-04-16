
# 消费kafka 到 redis的工具

```
    调试运行
    go run main.go --config=/Users/liukelin/Desktop/360/galaxy/src/kafka_consumers/conf.json  -alsologtostderr
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
        "cache_size":5,
        "topics":["work_queue_ngx2php", "lkl_test","lkl_make"],
        "consumer_group":"test-group",
        "output":"lkl_test"
    }
```