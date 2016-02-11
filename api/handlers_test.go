package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/twitchscience/blueprint/bpdb"
	bpdbTest "github.com/twitchscience/blueprint/bpdb/test"

	"github.com/twitchscience/scoop_protocol/schema"
	schemaTest "github.com/twitchscience/scoop_protocol/schema/test"

	"github.com/zenazn/goji/web"
)

var (
	testEventTable = "test_event_schemas"
	backend        bpdb.Backend
	testServer     server
)

func setupTestDB() (bpdb.Backend, *sql.DB, string, error) {
	flag.Parse()

	driverName := "sqlite3"
	urlName := ":memory:"

	connection, err := sql.Open(driverName, urlName)
	if err != nil {
		return bpdb.Backend{}, nil, "", fmt.Errorf("Could not extablish connection to DB: %v", err)
	}
	backend, err := bpdb.New(connection, testEventTable)

	bpdbTest.CreateTestTable(connection, testEventTable)
	backend.PutEvent(schemaTest.SimEvent1Version1())
	backend.PutEvent(schemaTest.SimEvent1Version2())
	backend.PutEvent(schemaTest.SimEvent1Version3())
	backend.PutEvent(schemaTest.SimEvent1Version4())
	backend.PutEvent(schemaTest.SimEvent1Version5())

	backend.PutEvent(schemaTest.SimEvent2Version3())
	backend.PutEvent(schemaTest.SimEvent2Version4())
	return backend, connection, testEventTable, nil
}

func setupTestServer() (*server, *sql.DB, string, error) {
	backend, connection, tableName, err := setupTestDB()

	server := &server{backend: backend}
	return server, connection, tableName, err
}

func jsonEventsDeepEqualsChecker(testEvents, expectedEvents []byte) (bool, error) {
	var testEventsObject, expectedEventsObject []schema.Event
	err := json.Unmarshal(testEvents, &testEventsObject)
	if err != nil {
		return false, errors.New("Could not unmarshal testEventsObject")
	}
	err = json.Unmarshal(expectedEvents, &expectedEventsObject)
	if err != nil {
		return false, errors.New("Could not unmarshal expectedEventsObject")
	}
	if !reflect.DeepEqual(testEventsObject, expectedEventsObject) {
		return false, nil
	}
	return true, nil
}

func TestSchemas(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	defer connection.Close()
	defer bpdbTest.DropTestTable(connection, tableName)

	if err != nil {
		t.Error("could not setup test, sql connection did not open")
	}

	body := strings.NewReader("")
	r, err := http.NewRequest("GET", "/schemas", body)
	if err != nil {
		t.Error(err)
	}

	w := httptest.NewRecorder()

	server.allSchemas(w, r)

	t.Logf("%d - %s", w.Code, w.Body.String())

	expectedEvents, err := json.Marshal([]schema.Event{
		schemaTest.SimEvent1Version5(), schemaTest.SimEvent2Version4(),
	})
	if err != nil {
		t.Error("Could not marshal expected events object")
	}

	equalsStatus, err := jsonEventsDeepEqualsChecker(w.Body.Bytes(), expectedEvents)
	if err != nil {
		t.Errorf("Could not unmarshal objects in deep equals checker: %s", err.Error())
	}

	if !equalsStatus {
		t.Error("Schemas could not match")
	}

}

func TestNewestSchema(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	defer connection.Close()
	defer bpdbTest.DropTestTable(connection, tableName)

	if err != nil {
		t.Error("could not setup test, sql connection did not open")
	}

	body := strings.NewReader("")
	r, err := http.NewRequest("GET", "/schema/login_success", body)
	if err != nil {
		t.Error(err)
	}

	w := httptest.NewRecorder()

	c := web.C{
		URLParams: map[string]string{
			"id": "login_success",
		},
	}

	server.schema(c, w, r)

	t.Logf("%d - %s", w.Code, w.Body.String())

	expectedEvents, err := json.Marshal([]schema.Event{
		schemaTest.SimEvent2Version4(),
	})
	if err != nil {
		t.Error("Could not marshal expected events object")
	}

	equalsStatus, err := jsonEventsDeepEqualsChecker(w.Body.Bytes(), expectedEvents)
	if err != nil {
		t.Errorf("Could not unmarshal objects in deep equals checker: %s", err.Error())
	}

	if !equalsStatus {
		t.Error("Schemas could not match")
	}
}

func TestValidSchemaVersion(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	defer connection.Close()
	defer bpdbTest.DropTestTable(connection, tableName)

	if err != nil {
		t.Error("could not setup test, sql connection did not open")
	}

	body := strings.NewReader("")
	r, err := http.NewRequest("GET", "/schema/login_success?version=3", body)
	if err != nil {
		t.Error(err)
	}

	w := httptest.NewRecorder()

	c := web.C{
		URLParams: map[string]string{
			"id": "login_success",
		},
	}

	server.schema(c, w, r)

	t.Logf("%d - %s", w.Code, w.Body.String())

	expectedEvents, err := json.Marshal([]schema.Event{
		schemaTest.SimEvent2Version3(),
	})
	if err != nil {
		t.Error("Could not marshal expected events object")
	}

	equalsStatus, err := jsonEventsDeepEqualsChecker(w.Body.Bytes(), expectedEvents)
	if err != nil {
		t.Errorf("Could not unmarshal objects in deep equals checker: %s", err.Error())
	}

	if !equalsStatus {
		t.Error("Schemas could not match")
	}
}

func TestValidSchemaVersionDoesNotExist(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	defer connection.Close()
	defer bpdbTest.DropTestTable(connection, tableName)

	if err != nil {
		t.Error("could not setup test, sql connection did not open")
	}

	body := strings.NewReader("")
	r, err := http.NewRequest("GET", "/schema/login_success?version=7", body)
	if err != nil {
		t.Error(err)
	}

	w := httptest.NewRecorder()

	c := web.C{
		URLParams: map[string]string{
			"id": "login_success",
		},
	}

	server.schema(c, w, r)

	t.Logf("%d - %s", w.Code, w.Body.String())
	if w.Code != http.StatusNotFound {
		t.Errorf("Expected http code %d but recieved vode %d.", http.StatusNotFound, w.Code)
	}
}

func TestValidSchemaVersionNotInt(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	defer connection.Close()
	defer bpdbTest.DropTestTable(connection, tableName)

	if err != nil {
		t.Error("could not setup test, sql connection did not open")
	}

	body := strings.NewReader("")
	r, err := http.NewRequest("GET", "/schema/login_success?version=foobar", body)
	if err != nil {
		t.Error(err)
	}

	w := httptest.NewRecorder()

	c := web.C{
		URLParams: map[string]string{
			"id": "login_success",
		},
	}

	server.schema(c, w, r)

	t.Logf("%d - %s", w.Code, w.Body.String())
	if w.Code != http.StatusNotFound {
		t.Errorf("Expected http code %d but recieved vode %d.", http.StatusNotFound, w.Code)
	}
}

// func TestDeleteSchema(t *testing.T) {
// 	server, connection, tableName, err := setupTestServer()
// 	if err != nil {
// 		t.Error("could not setup test, sql connection did not open")
// 	}

// 	defer connection.Close()
// 	defer bpdbTest.DropTestTable(connection, tableName)

// }

// func TestUpdateSchema(t *testing.T) {
// 	server, connection, tableName, err := setupTestServer()
// 	if err != nil {
// 		t.Error("could not setup test, sql connection did not open")
// 	}

// 	defer connection.Close()
// 	defer bpdbTest.DropTestTable(connection, tableName)

// }
