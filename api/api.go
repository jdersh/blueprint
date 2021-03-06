package api

import (
	"flag"

	"github.com/gorilla/context"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/auth"
	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	"github.com/zenazn/goji"
	"github.com/zenazn/goji/graceful"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
)

type server struct {
	docRoot        string
	bpdbBackend    bpdb.Bpdb
	configFilename string
}

var (
	loginURL        = "/login"
	logoutURL       = "/logout"
	authCallbackURL = "/github_oauth_cb"
	enableAuth      bool
	readonly        bool
	cookieSecret    string
	clientID        string
	clientSecret    string
	githubServer    string
	requiredOrg     string
	ingesterURL     string
)

func init() {
	flag.BoolVar(&enableAuth, "enableAuth", true, "enable authentication when not in readonly mode")
	flag.BoolVar(&readonly, "readonly", false, "run in readonly mode and disable auth")
	flag.StringVar(&cookieSecret, "cookieSecret", "", "32 character secret for signing cookies")
	flag.StringVar(&clientID, "clientID", "", "Google API client id")
	flag.StringVar(&clientSecret, "clientSecret", "", "Google API client secret")
	flag.StringVar(&githubServer, "githubServer", "http://github.com", "Github server to use for auth")
	flag.StringVar(&requiredOrg, "requiredOrg", "", "Org user need to belong to to use auth")
	flag.StringVar(&ingesterURL, "ingesterURL", "", "URL to the ingester")
}

// New returns an API process.
func New(docRoot string, bpdbBackend bpdb.Bpdb, configFilename string) core.Subprocess {
	return &server{
		docRoot:        docRoot,
		bpdbBackend:    bpdbBackend,
		configFilename: configFilename,
	}
}

// Setup route handlers.
func (s *server) Setup() error {
	healthcheck := web.New()
	healthcheck.Get("/health", s.healthCheck)

	api := web.New()
	api.Use(jsonResponse)
	api.Get("/schemas", s.allSchemas)
	api.Get("/schema/:id", s.schema)
	api.Get("/migration/:schema", s.migration)
	api.Get("/types", s.types)
	api.Get("/suggestions", s.listSuggestions)
	api.Get("/suggestion/:id", s.suggestion)

	goji.Handle("/health", healthcheck)
	goji.Handle("/schemas", api)
	goji.Handle("/schema/*", api)
	goji.Handle("/migration/*", api)
	goji.Handle("/suggestions", api)
	goji.Handle("/suggestion/*", api)
	goji.Handle("/types", api)

	if !readonly {
		api.Use(context.ClearHandler)

		api.Post("/ingest", s.ingest)
		api.Put("/schema", s.createSchema)
		api.Post("/schema/:id", s.updateSchema)
		api.Post("/removesuggestion/:id", s.removeSuggestion)

		goji.Handle("/ingest", api)
		goji.Handle("/schema", api)
		goji.Handle("/removesuggestion/*", api)

		files := web.New()
		files.Use(context.ClearHandler)

		if enableAuth {
			a := auth.New(githubServer,
				clientID,
				clientSecret,
				cookieSecret,
				requiredOrg,
				loginURL)

			api.Use(a.AuthorizeOrForbid)

			goji.Handle(loginURL, a.LoginHandler)
			goji.Handle(logoutURL, a.LogoutHandler)
			goji.Handle(authCallbackURL, a.AuthCallbackHandler)

			files.Use(a.AuthorizeOrRedirect)
		}

		goji.Handle("/*", files)
		files.Get("/*", s.fileHandler)
	}
	goji.NotFound(fourOhFour)

	// The default logger logs in colour which makes CloudWatch hard to read.
	// Replace with a custom logger that does not use colour.
	err := goji.DefaultMux.Abandon(middleware.Logger)
	if err != nil {
		logger.WithError(err).Warn("Couldn't abandon default logger; will continue as is")
	} else {
		goji.DefaultMux.Use(SimpleLogger)
	}

	// Stop() provides our shutdown semantics
	graceful.ResetSignals()

	return nil
}

// Start the API server.
func (s *server) Start() {
	goji.Serve()
}

// Stop the API server.
func (s *server) Stop() {
	graceful.Shutdown()
}
