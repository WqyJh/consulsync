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
	casTry      = flag.Int("cas-try", 3, "number of times to try cas")
)

func main() {
	flag.Parse()

	syncer := syncer.NewSyncer(
		*consulAddr,
		*consulToken,
		*consulPath,
		*localPath,
		*casTry,
	)
	err := syncer.Sync()
	if err != nil {
		log.Fatalf("failed to sync: %+v", err)
	}
}
