package rediscluster

/*
	redis list 消息队列类(cluster版)
	@auth liukelin
*/
import (
	"fmt"

	redis "github.com/go-redis/redis/v7"
	"github.com/golang/glog"
)

/**
 * 必要方法
 */
type QueueInterface interface {
	// 连接
	Connect() error
	// push
	Push(string, string) error
	// pop
	Pop(string) (string, error)
	// 常驻get
	// GetCallback(string, func(string) bool)
	// 获取list长度
	Len(string) (string, error)
	// setnxex
	Cmd(...interface{}) (interface{}, error)
}

type Queue struct {
	Conf *Conf
	Pool *redis.ClusterClient
}

type Conf struct {
	Servers []string
}

func (q *Queue) Connect() error {
	err := q.initConnection()
	return err
}

// 检测重连
func (q *Queue) initConnection() error {

	recon := true
	if q.Pool != nil {
		_, err0 := q.Pool.Ping().Result()
		if err0 == nil {
			recon = false
		}
	}
	if recon {
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: q.Conf.Servers,
		})

		pong, err := client.Ping().Result()
		if err != nil {
			glog.Errorf("connect reids error, %v, %v", err, pong)
			return err
		}
		q.Pool = client
	}
	return nil
}

// 写入元素
func (q *Queue) Push(queueName string, data string) error {

	err := q.initConnection()
	if err != nil {
		glog.Errorf("redis Push client error:, %v", err)
		return fmt.Errorf("redis Push client error:%v", err)
	}
	// defer conn.Close()
	// _, err = q.Pool.Do("rpush", data, queueName).Result()
	_, err = q.Pool.RPush(queueName, data).Result()
	return err
}

// 从list获取元素
func (q *Queue) Pop(queueName string) (string, error) {

	err := q.initConnection()
	if err != nil {
		glog.Errorf("connect redis Get client error:, %v", err)
		return "", fmt.Errorf("connect redis Get client error:%v", err)
	}

	// defer conn.Close()
	data, _err := q.Pool.Do("lpop", queueName).Result()
	if _err != nil {
		glog.Errorf(" redis Get error:, %v", _err)
		return "", _err
	}

	if data == nil {
		return "", nil
	}

	// 格式判断
	switch vv := data.(type) {
	case []uint8: // 字节切片类型
		return string(vv), nil
	case string:
		return data.(string), nil
	default:
		glog.Errorf(" redis Get value type error:, %v", vv)
		return "", fmt.Errorf("redis Get value type error:%v", vv)
	}
}

// 获取list长度
func (q *Queue) Len(queueName string) (int64, error) {
	err := q.initConnection()
	if err != nil {
		glog.Errorf("Connect redis Get client error:, %v", err)
		return 0, fmt.Errorf("Connect redis Get client error:%v", err)
	}
	// defer conn.Close()

	data, _err := q.Pool.Do("llen", queueName).Result()
	if _err != nil {
		glog.Errorf("redis Get lpop error:", _err)
		return 0, _err
	}
	if data == nil {
		return 0, nil
	}
	switch vv := data.(type) {
	case []uint8: // 字节切片类型
		return data.(int64), nil
	case int64:
		return data.(int64), nil
	default:
		glog.Errorf("redis Get value type error: %v", vv)
		return 0, fmt.Errorf("redis Get value type error:%v", vv)
	}
}

// 执行命令
/*
func (q *Queue) Cmd(args ...interface{}) error {
	err := q.initConnection()
	if err != nil {
		glog.Errorf("Connect redis client error:, %v", err)
		return fmt.Errorf("Connect client error:%v", err)
	}
	data, _err := q.Pool.Do(args).Result()
	if _err != nil {
		glog.Errorf("redis Cmd error:%v", _err)
		return _err
	}
	switch vv := data.(type) {
	case []uint8: // 字节切片类型
		return nil
	case int64:
		return nil
	default:
		glog.Errorf("redis Cmd value type error: %v", vv)
		return fmt.Errorf("redis Cmd value type error:%v", vv)
	}
}
*/
func (q *Queue) Cmd(args ...interface{}) (interface{}, error) {
	err := q.initConnection()
	if err != nil {
		glog.Errorf("Connect redis client error:, %v", err)
		return nil, fmt.Errorf("Connect client error:%v", err)
	}
	d, _err := q.Pool.Do(args...).Result()
	if _err != nil {
		return nil, _err
	}
	return d, nil
}

// func dial(addr string) (redis.Conn, error) {
// 	return redis.Dial("tcp", addr)
// }
