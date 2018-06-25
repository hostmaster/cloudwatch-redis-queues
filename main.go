// Command cloudwatch-redis-queues publishes size of redis queues as CloudWatch
// metrics.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/artyom/autoflags"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/pkg/errors"
)

func main() {
	args := struct {
		Redis     string `flag:"redis,redis address"`
		Namespace string `flag:"namespace,CloudWatch namespace"`
	}{
		Redis:     "localhost:6379",
		Namespace: "Redis queues",
	}
	autoflags.Parse(&args)
	if err := run(args.Redis, args.Namespace); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(addr, namespace string) error {
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
	meta, err := ec2metadata.New(sess).GetInstanceIdentityDocument()
	if err != nil {
		return errors.WithMessage(err, "ec2 instance metadata fetch")
	}
	svc := cloudwatch.New(sess, aws.NewConfig().WithRegion(meta.Region))
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	var i uint
	var keys []string
	for now := range ticker.C {
		if i%60 == 0 {
			if keys, err = pool.Cmd("keys", "*:queue").List(); err != nil {
				return errors.WithMessage(err, "keys *:queue command")
			}
		}
		i++
		m, err := metrics(pool, keys, now)
		if err != nil {
			return err
		}
		if len(m) == 0 {
			continue
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

func metrics(pool *pool.Pool, keys []string, now time.Time) ([]*cloudwatch.MetricDatum, error) {
	out := make([]*cloudwatch.MetricDatum, 0, len(keys))
	unit := aws.String(cloudwatch.StandardUnitCount)
	stamp := now.Unix()
	for _, key := range keys {
		val, err := pool.Cmd("ZCOUNT", key, 0, stamp).Int()
		if err != nil {
			return nil, errors.Wrapf(err, "ZCOUNT %s 0 %d", key, stamp)
		}
		out = append(out, &cloudwatch.MetricDatum{
			MetricName: aws.String(strings.TrimSuffix(key, ":queue")),
			Timestamp:  &now,
			Unit:       unit,
			Value:      aws.Float64(float64(val)),
		})
	}
	return out, nil
}
