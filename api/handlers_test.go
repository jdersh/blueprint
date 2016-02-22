package api

import (
	"bytes"
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
	if err != nil {
		return bpdb.Backend{}, nil, "", fmt.Errorf("Could not extablish connection to DB: %v", err)
	}

	err = bpdbTest.CreateTestTable(connection, testEventTable)
	if err != nil {
		return bpdb.Backend{}, nil, "", fmt.Errorf("Could not create test table in DB: %v", err)
	}

	return backend, connection, testEventTable, nil
}

func setupTestServer() (*server, *sql.DB, string, error) {
	backend, connection, tableName, err := setupTestDB()

	server := &server{backend: backend}
	return server, connection, tableName, err
}

func fillTestDB(backend bpdb.Backend, t *testing.T) {
	putEventAndCheck(backend, schemaTest.SimEvent1Version1(), t)
	putEventAndCheck(backend, schemaTest.SimEvent1Version2(), t)
	putEventAndCheck(backend, schemaTest.SimEvent1Version3(), t)
	putEventAndCheck(backend, schemaTest.SimEvent1Version4(), t)
	putEventAndCheck(backend, schemaTest.SimEvent1Version5(), t)

	putEventAndCheck(backend, schemaTest.SimEvent2Version1(), t)
	putEventAndCheck(backend, schemaTest.SimEvent2Version2(), t)
	putEventAndCheck(backend, schemaTest.SimEvent2Version3(), t)
	putEventAndCheck(backend, schemaTest.SimEvent2Version4(), t)
}

func putEventAndCheck(backend bpdb.Backend, event schema.Event, t *testing.T) {
	err := backend.PutEvent(event)
	if err != nil {
		t.Errorf("Could not put test event in DB: %v", err)
	}
}

func jsonEventsDeepEqualsChecker(testEvents, expectedEvents []byte, t *testing.T) (bool, error) {
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
		t.Logf("Expected: %+v", expectedEventsObject)
		t.Logf("Recieved: %+v", testEventsObject)
		return false, nil
	}
	return true, nil
}

func TestSchemas(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	fillTestDB(server.backend, t)
	defer func() {
		err := connection.Close()
		if err != nil {
			t.Error("Could not close sql connection")
		}
	}()

	defer func() {
		err := bpdbTest.DropTestTable(connection, tableName)
		if err != nil {
			t.Errorf("Could not drop table: %s", err.Error())
		}
	}()

	if err != nil {
		t.Errorf("could not setup test: %s", err.Error())
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

	equalsStatus, err := jsonEventsDeepEqualsChecker(w.Body.Bytes(), expectedEvents, t)
	if err != nil {
		t.Errorf("Could not unmarshal objects in deep equals checker: %s", err.Error())
	}

	if !equalsStatus {
		t.Error("Schemas could not match")
	}

}

func TestNewestSchema(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	fillTestDB(server.backend, t)
	defer func() {
		err := connection.Close()
		if err != nil {
			t.Error("Could not close sql connection")
		}
	}()

	defer func() {
		err := bpdbTest.DropTestTable(connection, tableName)
		if err != nil {
			t.Errorf("Could not drop table: %s", err.Error())
		}
	}()

	if err != nil {
		t.Errorf("could not setup test: %s", err.Error())
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

	equalsStatus, err := jsonEventsDeepEqualsChecker(w.Body.Bytes(), expectedEvents, t)
	if err != nil {
		t.Errorf("Could not unmarshal objects in deep equals checker: %s", err.Error())
	}

	if !equalsStatus {
		t.Error("Schemas could not match")
	}
}

func TestValidSchemaVersion(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	fillTestDB(server.backend, t)
	defer func() {
		err := connection.Close()
		if err != nil {
			t.Error("Could not close sql connection")
		}
	}()

	defer func() {
		err := bpdbTest.DropTestTable(connection, tableName)
		if err != nil {
			t.Errorf("Could not drop table: %s", err.Error())
		}
	}()

	if err != nil {
		t.Errorf("could not setup test: %s", err.Error())
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

	equalsStatus, err := jsonEventsDeepEqualsChecker(w.Body.Bytes(), expectedEvents, t)
	if err != nil {
		t.Errorf("Could not unmarshal objects in deep equals checker: %s", err.Error())
	}

	if !equalsStatus {
		t.Error("Schemas could not match")
	}
}

func TestValidSchemaVersionDoesNotExist(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	fillTestDB(server.backend, t)
	defer func() {
		err := connection.Close()
		if err != nil {
			t.Error("Could not close sql connection")
		}
	}()

	defer func() {
		err := bpdbTest.DropTestTable(connection, tableName)
		if err != nil {
			t.Errorf("Could not drop table: %s", err.Error())
		}
	}()

	if err != nil {
		t.Errorf("could not setup test: %s", err.Error())
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
	fillTestDB(server.backend, t)
	defer func() {
		err := connection.Close()
		if err != nil {
			t.Error("Could not close sql connection")
		}
	}()

	defer func() {
		err := bpdbTest.DropTestTable(connection, tableName)
		if err != nil {
			t.Errorf("Could not drop table: %s", err.Error())
		}
	}()

	if err != nil {
		t.Errorf("could not setup test: %s", err.Error())
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

func TestValidUpdateSchemaAddTable(t *testing.T) {
	server, connection, tableName, err := setupTestServer()

	defer func() {
		err := connection.Close()
		if err != nil {
			t.Error("Could not close sql connection")
		}
	}()

	defer func() {
		err := bpdbTest.DropTestTable(connection, tableName)
		if err != nil {
			t.Errorf("Could not drop table: %s", err.Error())
		}
	}()

	if err != nil {
		t.Errorf("could not setup test: %s", err.Error())
	}

	jsonMigration1, err := json.Marshal(schemaTest.SimEvent1Migration1())
	if err != nil {
		t.Error("Could not marshal test migration")
	}

	body := bytes.NewReader(jsonMigration1)
	r, err := http.NewRequest("POST", "/schema/chat_ignore_client?version=1", body)
	if err != nil {
		t.Error(err)
	}

	w := httptest.NewRecorder()

	c := web.C{
		URLParams: map[string]string{
			"id": "chat_ignore_client",
		},
	}

	server.updateSchema(c, w, r)

	t.Logf("%d - %s", w.Code, w.Body.String())

	expectedEvents, err := json.Marshal([]schema.Event{
		schemaTest.SimEvent1Version2(),
	})
	if err != nil {
		t.Error("Could not marshal expected events object")
	}

	equalsStatus, err := jsonEventsDeepEqualsChecker(w.Body.Bytes(), expectedEvents, t)
	if err != nil {
		t.Errorf("Could not unmarshal objects in deep equals checker: %s", err.Error())
	}

	if !equalsStatus {
		t.Error("Schemas could not match")
	}
}

func TestValidUpdateSchemaUpdateTable(t *testing.T) {
	server, connection, tableName, err := setupTestServer()

	defer func() {
		err := connection.Close()
		if err != nil {
			t.Error("Could not close sql connection")
		}
	}()

	defer func() {
		err := bpdbTest.DropTestTable(connection, tableName)
		if err != nil {
			t.Errorf("Could not drop table: %s", err.Error())
		}
	}()

	putEventAndCheck(server.backend, schemaTest.SimEvent1Version1(), t)
	putEventAndCheck(server.backend, schemaTest.SimEvent1Version2(), t)
	putEventAndCheck(server.backend, schemaTest.SimEvent1Version3(), t)

	if err != nil {
		t.Errorf("could not setup test: %s", err.Error())
	}

	jsonMigration1, err := json.Marshal(schemaTest.SimEvent1Migration3())
	if err != nil {
		t.Error("Could not marshal test migration")
	}

	body := bytes.NewReader(jsonMigration1)
	r, err := http.NewRequest("POST", "/schema/chat_ignore_client?version=3", body)
	if err != nil {
		t.Error(err)
	}

	w := httptest.NewRecorder()

	c := web.C{
		URLParams: map[string]string{
			"id": "chat_ignore_client",
		},
	}

	server.updateSchema(c, w, r)

	t.Logf("%d - %s", w.Code, w.Body.String())

	expectedEvents, err := json.Marshal([]schema.Event{
		schemaTest.SimEvent1Version4(),
	})
	if err != nil {
		t.Error("Could not marshal expected events object")
	}

	equalsStatus, err := jsonEventsDeepEqualsChecker(w.Body.Bytes(), expectedEvents, t)
	if err != nil {
		t.Errorf("Could not unmarshal objects in deep equals checker: %s", err.Error())
	}

	if !equalsStatus {
		t.Error("Schemas could not match")
	}
}

func TestValidUpdateSchemaRemoveTable(t *testing.T) {
	server, connection, tableName, err := setupTestServer()

	defer func() {
		err := connection.Close()
		if err != nil {
			t.Error("Could not close sql connection")
		}
	}()

	defer func() {
		err := bpdbTest.DropTestTable(connection, tableName)
		if err != nil {
			t.Errorf("Could not drop table: %s", err.Error())
		}
	}()

	putEventAndCheck(server.backend, schemaTest.SimEvent2Version1(), t)
	putEventAndCheck(server.backend, schemaTest.SimEvent2Version2(), t)
	putEventAndCheck(server.backend, schemaTest.SimEvent2Version3(), t)
	putEventAndCheck(server.backend, schemaTest.SimEvent2Version4(), t)

	if err != nil {
		t.Errorf("could not setup test: %s", err.Error())
	}

	jsonMigration1, err := json.Marshal(schemaTest.SimEvent2Migration4())
	if err != nil {
		t.Error("Could not marshal test migration")
	}

	body := bytes.NewReader(jsonMigration1)
	r, err := http.NewRequest("DELETE", "/schema/login_success?version=4", body)
	if err != nil {
		t.Error(err)
	}

	w := httptest.NewRecorder()

	c := web.C{
		URLParams: map[string]string{
			"id": "login_success",
		},
	}

	server.deleteSchema(c, w, r)

	t.Logf("%d - %s", w.Code, w.Body.String())

	expectedEvents, err := json.Marshal([]schema.Event{
		schemaTest.SimEvent2Version5(),
	})
	if err != nil {
		t.Error("Could not marshal expected events object")
	}

	equalsStatus, err := jsonEventsDeepEqualsChecker(w.Body.Bytes(), expectedEvents, t)
	if err != nil {
		t.Errorf("Could not unmarshal objects in deep equals checker: %s", err.Error())
	}

	if !equalsStatus {
		t.Error("Schemas could not match")
	}
}
