package main

import (
	"flag"
	"os"
	"os/signal"

	"github.com/twitchscience/blueprint/api"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/blueprint/postgres_client"
	cachingscoopclient "github.com/twitchscience/blueprint/scoopclient/cachingclient"
)

var (
	scoopURL        = flag.String("scoopURL", "", "the base url for scoop")
	staticFileDir   = flag.String("staticfiles", "./static", "the location to serve static files from")
	transformConfig = flag.String("transformConfig", "transforms_available.json", "config for available transforms in spade")
	postgresURL = flag.String("postgresURL", "", "The login url for the postgres DB")
)

func main() {
	flag.Parse()
	scoopClient := cachingscoopclient.New(*scoopURL, *transformConfig)
	pgBackend := postgres_client.BuildPostgresBackend(*postgresURL)
	apiProcess := api.New(*staticFileDir, scoopClient)
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
