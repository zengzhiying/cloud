package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/hashicorp/go-cleanhttp"
)

var signal = make(chan int)

// 源码参考: https://github.com/hashicorp/consul/blob/main/api/watch/funcs_test.go
// godoc: https://pkg.go.dev/github.com/hashicorp/consul/api
func main() {
	conf := &api.Config{
		Address:    "127.0.0.1:8500",
		Scheme:     "http",
		Datacenter: "dc1",
		Transport:  cleanhttp.DefaultPooledTransport(),
		Token:      "031e3288-4543-4c2b-8fee-de0443ea0ab2",
	}

	client, err := api.NewClient(conf)
	if err != nil {
		fmt.Println(err)
		return
	}

	kv := client.KV()

	p1 := &api.KVPair{Key: "db/redis/maxclients", Value: []byte("100")}
	p2 := &api.KVPair{Key: "db/redis/maxmemory", Value: []byte("1024")}

	if _, err := kv.Put(p1, nil); err != nil {
		fmt.Println(err)
		return
	}
	if _, err := kv.Put(p2, nil); err != nil {
		fmt.Println(err)
		return
	}

	pair, _, err := kv.Get("db/redis/maxclients", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(pair)
	fmt.Printf("key: %s value: %s\n", pair.Key, string(pair.Value))

	pairs, _, err := kv.List("db/redis", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	// fmt.Println(pairs)
	for i, pair := range pairs {
		fmt.Printf("%d. %s ==> %s\n", i, pair.Key, string(pair.Value))
	}

	// watches keyprefix
	plan, err := watch.Parse(map[string]interface{}{
		"type":   "keyprefix",
		"prefix": "db/",
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	updateCh := make(chan *api.KVPairs, 1)

	plan.Handler = func(idx uint64, raw interface{}) {
		if raw == nil {
			fmt.Println("watch nil")
			return
		}

		pairs, ok := raw.(api.KVPairs)
		if !ok {
			fmt.Println("watch type error")
			return
		}
		fmt.Println("watch of changes.")
		// for i, pair := range pairs {
		// 	fmt.Printf("watch %d. %s ==> %s\n", i, pair.Key, string(pair.Value))
		// }
		updateCh <- &pairs
	}
	plan.Datacenter = conf.Datacenter
	plan.Token = conf.Token

	go notifyUpdate(updateCh)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := plan.Run(conf.Address); err != nil {
			fmt.Println("plan run error:", err)
		}
	}()

	if _, err := kv.Put(&api.KVPair{Key: "db/mysql/address",
		Value: []byte("127.0.0.1:3306")}, nil); err != nil {
		fmt.Println(err)
	}

	time.Sleep(time.Second)

	if _, err := kv.Put(&api.KVPair{Key: "db/mysql/address",
		Value: []byte("192.168.11.12:3306")}, nil); err != nil {
		fmt.Println(err)
	}

	time.Sleep(time.Second)

	// 不符合前缀，不会 watch 到
	if _, err := kv.Put(&api.KVPair{Key: "message/kafka/topic",
		Value: []byte("foo")}, nil); err != nil {
		fmt.Println(err)
	}

	plan.Stop()
	wg.Done()
	close(updateCh)
	<-signal
}

func notifyUpdate(updateCh <-chan *api.KVPairs) {
	for pairs := range updateCh {
		for i, pair := range *pairs {
			fmt.Printf("%d. %s ==> %s\n", i, pair.Key, string(pair.Value))
		}
	}
	fmt.Println("notify exit")
	signal <- 1
}
