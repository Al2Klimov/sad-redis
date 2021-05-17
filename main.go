package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"os"
	"sync"
	"time"
)

func main() {
	addr := flag.String("redis", "127.0.0.1:6379", "HOST:PORT")
	pipelines := flag.Int("pipelines", 10, "AMOUNT")
	chunks := flag.Int("chunks", 10, "AMOUNT")
	chunk := flag.Int("chunk", 10, "AMOUNT")
	flag.Parse()

	client := redis.NewClient(&redis.Options{
		Addr:         *addr,
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	})
	bench := make([]time.Duration, *pipelines)
	g, ctx := errgroup.WithContext(context.Background())

	barrier := &sync.WaitGroup{}
	barrier.Add(*pipelines)

	for i := 0; i < *pipelines; i++ {
		i := i
		g.Go(func() error {
			rand, err := uuid.NewRandom()
			if err != nil {
				return err
			}

			pipe := client.Pipeline()
			defer pipe.Close()

			for j := 0; j < *chunks; j++ {
				members := make([]interface{}, 0, *chunk)
				for k := 0; k < *chunk; k++ {
					rand, err := uuid.NewRandom()
					if err != nil {
						return err
					}

					members = append(members, rand.String())
				}

				pipe.SAdd(ctx, rand.String(), members...)
			}

			barrier.Done()
			barrier.Wait()

			{
				start := time.Now()
				_, err := pipe.Exec(ctx)
				bench[i] = time.Since(start)

				return err
			}
		})
	}

	if err := g.Wait(); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	fmt.Println(bench)
}
