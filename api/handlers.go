package api

import (
	"database/sql"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/twitchscience/scoop_protocol/schema"

	"github.com/zenazn/goji/web"
)

func (s *server) updateSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	r.ParseForm()
	eventName := c.URLParams["id"]
	eventVersion := r.FormValue("version")

	if eventVersion == "" {
		http.Error(w, "Must provide version with migration", http.StatusNotAcceptable)
		return
	}

	version, err := strconv.Atoi(eventVersion)
	if err != nil {
		fourOhFour(w, r)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var migration schema.Migration
	err = json.Unmarshal(b, &migration)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if migration.Name != eventName {
		http.Error(w, "Migration event name and URL arg event name Must match", http.StatusNotAcceptable)
		return
	}

	currentEvent, err := s.backend.NewestEvent(eventName)
	if err == sql.ErrNoRows {
		currentEvent = []schema.Event{schema.MakeNewEvent(eventName, version)}
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if currentEvent[0].Version != version {
		http.Error(w, "Newer version of schema already exists", http.StatusNotAcceptable)
		return
	}

	migrator := schema.BuildMigratorBackend(migration, currentEvent[0])

	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	currentEvent, err = s.backend.NewestEvent(eventName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if currentEvent[0].Version != version {
		http.Error(w, "Newer version of schema already exists", http.StatusNotAcceptable)
		return
	}
	s.backend.PutEvent(*newEvent)
}

func (s *server) deleteSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	r.ParseForm()
	eventName := c.URLParams["id"]
	eventVersion := r.FormValue("version")

	if eventVersion == "" {
		http.Error(w, "Must provide version with migration", http.StatusNotAcceptable)
		return
	}

	version, err := strconv.Atoi(eventVersion)
	if err != nil {
		fourOhFour(w, r)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var migration schema.Migration
	err = json.Unmarshal(b, &migration)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if migration.Name != eventName {
		http.Error(w, "Migration event name and URL arg event name Must match", http.StatusNotAcceptable)
		return
	}

	currentEvent, err := s.backend.NewestEvent(eventName)
	if err != nil {
		http.Error(w, "Cannot delete event that does not exist", http.StatusInternalServerError)
		return
	}

	if currentEvent[0].Version != version {
		http.Error(w, "Newer version of schema already exists", http.StatusNotAcceptable)
		return
	}

	migrator := schema.BuildMigratorBackend(migration, currentEvent[0])

	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	currentEvent, err = s.backend.NewestEvent(eventName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if currentEvent[0].Version != version {
		http.Error(w, "Newer version of schema already exists", http.StatusNotAcceptable)
		return
	}
	s.backend.PutEvent(*newEvent)
}

func (s *server) allSchemas(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	events, err := s.backend.Events()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeEvent(w, events)
}

func (s *server) schema(c web.C, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	r.ParseForm()
	eventName := c.URLParams["id"]
	eventVersion := r.FormValue("version")

	var event []schema.Event
	var err error
	var version int
	if eventVersion == "" {
		event, err = s.backend.NewestEvent(eventName)
	} else {
		version, err = strconv.Atoi(eventVersion)
		if err != nil {
			fourOhFour(w, r)
			return
		}
		event, err = s.backend.VersionedEvent(eventName, version)
	}
	if err == sql.ErrNoRows {
		http.Error(w, "Event requested does not exist", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeEvent(w, event)
}

func (s *server) fileHandler(w http.ResponseWriter, r *http.Request) {
	fh, err := os.Open(staticPath(s.docRoot, r.URL.Path))
	if err != nil {
		fourOhFour(w, r)
		return
	}
	io.Copy(w, fh)
}

func (s *server) types(w http.ResponseWriter, r *http.Request) {
	types := schema.TransformList
	keys := []string{}
	for k := range types {
		keys = append(keys, k)
	}
	data := make(map[string][]string)
	data["result"] = keys
	b, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

func (s *server) listSuggestions(w http.ResponseWriter, r *http.Request) {
	availableSuggestions, err := getAvailableSuggestions(s.docRoot)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(availableSuggestions) == 0 {
		w.Write([]byte("[]"))
		return
	}

	b, err := json.Marshal(availableSuggestions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

func (s *server) suggestion(c web.C, w http.ResponseWriter, r *http.Request) {
	if !validSuggestion(strings.TrimSuffix(c.URLParams["id"], ".json"), s.docRoot) {
		fourOhFour(w, r)
		return
	}
	fh, err := os.Open(s.docRoot + "/events/" + c.URLParams["id"])
	if err != nil {
		fourOhFour(w, r)
		return
	}
	io.Copy(w, fh)
}

func (s *server) removeSuggestion(c web.C, w http.ResponseWriter, r *http.Request) {
	if !validSuggestion(strings.TrimSuffix(c.URLParams["id"], ".json"), s.docRoot) {
		fourOhFour(w, r)
		return
	}

	err := os.Remove(s.docRoot + "/events/" + c.URLParams["id"])
	if err != nil {
		fourOhFour(w, r)
		return
	}
}

func (s *server) healthCheck(c web.C, w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Healthy")
}
