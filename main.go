// Command cloudwatch-redis-queues publishes size of redis queues as CloudWatch
// metrics.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/artyom/autoflags"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/pubsub"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/pkg/errors"
)

func main() {
	redisAddr := "localhost:6379"

	if env := os.Getenv("REDIS_ADDR"); env != "" {
		redisAddr = env
	}

	args := struct {
		Redis     string `flag:"redis,redis address (REDIS_ADDR env)"`
		Namespace string `flag:"namespace,CloudWatch namespace"`
		WithMax   bool   `flag:"withMax,create extra computed MAX metric holding size of largest queue"`
		WithStats bool   `flag:"withCommandStats,export per-queue statistics of ZADD/ZREM commands (CPU hungry)"`
	}{
		Redis:     redisAddr,
		Namespace: "Redis queues",
	}

	autoflags.Parse(&args)
	if err := run(args.Redis, args.Namespace, args.WithMax, args.WithStats); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(addr, namespace string, withMax, withStats bool) error {
	pool, err := pool.NewCustom("tcp", addr, 1, func(network, addr string) (*redis.Client, error) {
		conn, err := net.DialTimeout(network, addr, time.Second)
		if err != nil {
			return nil, err
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetKeepAlive(true)
			tc.SetKeepAlivePeriod(time.Minute)
		}
		return redis.NewClient(conn)
	})
	if err != nil {
		return errors.WithMessage(err, "redis connect")
	}
	sess, err := session.NewSession()
	if err != nil {
		return errors.WithMessage(err, "AWS session create")
	}

	svc := cloudwatch.New(sess)

	if withStats {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go statIngestLoop(ctx, svc, pool, namespace)
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	var i uint
	var keys []string
	for now := range ticker.C {
		if i%10 == 0 {
			if keys, err = pool.Cmd("keys", "*:queue").List(); err != nil {
				return errors.WithMessage(err, "keys *:queue command")
			}
		}
		i++
		if len(keys) == 0 {
			continue
		}
		m, err := metrics(pool, keys, now, withMax)
		if err != nil {
			return err
		}
		input := cloudwatch.PutMetricDataInput{
			Namespace:  &namespace,
			MetricData: m,
		}
		if err := putMetricData(svc, &input, 30*time.Second); err != nil {
			return errors.WithMessage(err, "CloudWatch metrics put")
		}
	}
	return nil
}

func putMetricData(svc *cloudwatch.CloudWatch, input *cloudwatch.PutMetricDataInput, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html
	// "Each request is also limited to no more than 20 different metrics."
	const maxSize = 20
	data := input.MetricData
	for len(data) > maxSize {
		input.MetricData = data[:maxSize]
		data = data[maxSize:]
		if _, err := svc.PutMetricDataWithContext(ctx, input); err != nil {
			return err
		}
	}
	input.MetricData = data
	_, err := svc.PutMetricDataWithContext(ctx, input)
	return err
}

func metrics(pool *pool.Pool, keys []string, now time.Time, withMax bool) ([]*cloudwatch.MetricDatum, error) {
	l := len(keys)
	if withMax {
		l++
	}
	out := make([]*cloudwatch.MetricDatum, 0, l)
	unit := aws.String(cloudwatch.StandardUnitCount)
	stamp := now.Unix()
	var max int
	for _, key := range keys {
		val, err := pool.Cmd("ZCOUNT", key, 0, stamp).Int()
		if err != nil {
			return nil, errors.Wrapf(err, "ZCOUNT %s 0 %d", key, stamp)
		}
		if val > max {
			max = val
		}
		out = append(out, &cloudwatch.MetricDatum{
			MetricName: aws.String(strings.TrimSuffix(key, ":queue")),
			Timestamp:  &now,
			Unit:       unit,
			Value:      aws.Float64(float64(val)),
		})
	}
	if withMax {
		out = append(out, &cloudwatch.MetricDatum{
			MetricName: aws.String("MAX"),
			Timestamp:  &now,
			Unit:       unit,
			Value:      aws.Float64(float64(max)),
		})
	}
	return out, nil
}

func statIngestLoop(ctx context.Context, svc *cloudwatch.CloudWatch, pool *pool.Pool, namespace string) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		switch err := statIngest(ctx, svc, pool, namespace); err {
		case context.Canceled, context.DeadlineExceeded:
			return err
		case nil:
		default:
			log.Print("command stats ingest: ", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func statIngest(ctx context.Context, svc *cloudwatch.CloudWatch, pool *pool.Pool, namespace string) error {
	conn, err := pool.Get()
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.Cmd("config", "set", "notify-keyspace-events", "Ez").Err; err != nil {
		return err
	}
	pclient := pubsub.NewSubClient(conn)
	if err := pclient.Subscribe(zaddChannel, zremChannel).Err; err != nil {
		return err
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	stats := make(map[string]counts)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			go func(stats map[string]counts) {
				if err := publishCommandStats(ctx, svc, namespace, stats); err != nil {
					log.Println("publish command stats: ", err)
				}
			}(stats)
			stats = make(map[string]counts, len(stats))
		default:
		}
		resp := pclient.Receive()
		if resp.Err != nil {
			return err
		}
		if resp.Type != pubsub.Message ||
			!strings.HasSuffix(resp.Message, ":queue") {
			continue
		}
		v := stats[resp.Message]
		switch resp.Channel {
		case zaddChannel:
			v.zadd++
		case zremChannel:
			v.zrem++
		}
		stats[resp.Message] = v
	}
}

func publishCommandStats(ctx context.Context, svc *cloudwatch.CloudWatch, namespace string, stats map[string]counts) error {
	if len(stats) == 0 {
		return nil
	}
	metrics := make([]*cloudwatch.MetricDatum, 0, len(stats)*2)
	unit := aws.String(cloudwatch.StandardUnitCount)
	now := time.Now()
	dimName := aws.String("Command")
	for k, v := range stats {
		name := strings.TrimSuffix(k, ":queue")
		metrics = append(metrics, &cloudwatch.MetricDatum{
			MetricName: &name,
			Timestamp:  &now,
			Unit:       unit,
			Value:      aws.Float64(float64(v.zadd)),
			Dimensions: []*cloudwatch.Dimension{{
				Name: dimName, Value: aws.String("zadd"),
			}},
		}, &cloudwatch.MetricDatum{
			MetricName: &name,
			Timestamp:  &now,
			Unit:       unit,
			Value:      aws.Float64(float64(v.zrem)),
			Dimensions: []*cloudwatch.Dimension{{
				Name: dimName, Value: aws.String("zrem"),
			}},
		})
	}

	input := cloudwatch.PutMetricDataInput{
		Namespace:  &namespace,
		MetricData: metrics,
	}
	return putMetricData(svc, &input, 30*time.Second)
}

const (
	zaddChannel = "__keyevent@0__:zadd"
	zremChannel = "__keyevent@0__:zrem"
)

type counts struct {
	zadd, zrem int
}
