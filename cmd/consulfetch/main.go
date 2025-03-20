package main

import (
	"flag"
	"log"

	"github.com/WqyJh/consulsync/syncer"
)

var (
	localPath   = flag.String("local-path", "", "path of the local files")
	consulPath  = flag.String("consul-path", "", "path of the consul files")
	consulAddr  = flag.String("consul-addr", "", "consul address")
	consulToken = flag.String("consul-token", "", "consul token")
)

func main() {
	flag.Parse()

	syncer := syncer.NewFetcher(
		*consulAddr,
		*consulToken,
		*consulPath,
		*localPath,
	)
	err := syncer.Fetch()
	if err != nil {
		log.Fatalf("failed to fetch: %+v", err)
	}
}
