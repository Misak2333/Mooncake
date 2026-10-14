// SPDX-License-Identifier: Apache-2.0
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"

	conductor "github.com/kvcache-ai/Mooncake/mooncake-conductor/go/mooncakeconductor"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:13334", "Conductor RPC address")
	model := flag.String("model", "model", "registered model name")
	block := flag.Int64("block-size", 16, "registered prefix block size")
	flag.Parse()
	client, err := conductor.New()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	if err = client.Setup(conductor.ClientConfig{Address: *addr}); err != nil {
		log.Fatal(err)
	}
	result, err := client.Query(conductor.QueryRequest{ModelName: *model, BlockSize: *block, TokenIDs: []int32{1, 2, 3}})
	if err != nil {
		log.Fatal(err)
	}
	data, err := json.Marshal(result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(data))
}
