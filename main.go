package main

import (
	"flag"
	"os"
	"os/signal"

	"github.com/twitchscience/blueprint/api"
	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
)

var (
	staticFileDir     = flag.String("staticfiles", "./static", "the location to serve static files from")
	postgresURL       = flag.String("postgresURL", "", "The login url for the postgres DB")
	postgresTableName = flag.String("postgresTableName", "", "The name of the postgres table")
)

func main() {
	flag.Parse()
	pgBackend, err := bpdb.New("postgres", *postgresURL, *postgresTableName)
	if err != nil {
		panic(err)
	}
	apiProcess := api.New(*staticFileDir, pgBackend)

	manager := &core.SubprocessManager{
		Processes: []core.Subprocess{
			apiProcess,
		},
	}
	manager.Start()

	shutdownSignal := make(chan os.Signal)
	signal.Notify(shutdownSignal)
	go func() {
		<-shutdownSignal
		manager.Stop()
	}()

	manager.Wait()
}
