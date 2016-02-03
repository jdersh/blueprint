package bpdb

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/twitchscience/scoop_protocol/schema"

	"github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type DBBackendObject struct {
	connection *sql.DB
	tableName  string
}

type eventRow struct {
	name    string
	version int
	payload string
}

func NewDBBackend(driverName, urlName, tableName string) (DBBackendObject, error) {
	db, err := sql.Open(driverName, urlName)

	if err != nil {
		return DBBackendObject{}, fmt.Errorf("Error '%v' establishing connection to %s DB", err, driverName)
	}

	return DBBackendObject{
		connection: db,
		tableName:  tableName,
	}, nil
}

func (b *DBBackendObject) Events() ([]schema.Event, error) {
	jsonEvents, err := b.items()
	if err != nil {
		return []schema.Event{}, fmt.Errorf("Error '%v' getting events from database", err)
	}

	var events []schema.Event

	for _, jsonEvent := range jsonEvents {
		var event schema.Event

		err := json.Unmarshal([]byte(jsonEvent), &event)
		if err != nil {
			return []schema.Event{}, fmt.Errorf("Error '%v' unmarshalling json: %s", err, jsonEvent)
		}

		events = append(events, event)
	}

	return events, nil
}

func (b *DBBackendObject) NewestEvent(name string) (schema.Event, error) {
	jsonEvent, err := b.newestItem(name)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error '%v' getting newest event from DB", err)
	}

	var event schema.Event
	err = json.Unmarshal([]byte(jsonEvent), &event)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error '%v' unmarshalling newest event json: %s ", err, jsonEvent)
	}
	return event, nil
}

func (b *DBBackendObject) EventVersion(name string, version int) (schema.Event, error) {
	jsonEvent, err := b.itemVersion(name, version)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error '%v' getting specific event from DB", err)
	}

	var event schema.Event
	err = json.Unmarshal([]byte(jsonEvent), &event)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error '%v' unmarshalling specific event json: %s", err, jsonEvent)
	}
	return event, nil
}

func (b *DBBackendObject) PutEvent(event schema.Event) error {
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("Error '%v' marshalling event", err)
	}

	err = b.putItem(event.Name, event.Version, string(eventPayload))
	if err != nil {
		return fmt.Errorf("Error '%v' loading event into DB", err)
	}

	return nil
}

func (b *DBBackendObject) items() ([]string, error) {
	//Retrieve all the events sorted by newest version, and use distinct to filter out old versions
	// query := fmt.Sprintf(`	select distinct on (name)
	//                    			name,
	//                    			version,
	//                    			payload
	//                 			from %s
	//                 			order by name asc, version desc;`, pq.QuoteIdentifier(b.tableName))
	query := fmt.Sprintf(`	select schemas.name, schemas.version, schemas.payload
							from (select name, max(version) as max_version 
									from %s group by name) versions
							join %s schemas
							on versions.name = schemas.name AND 
								versions.max_version = schemas.version;`, pq.QuoteIdentifier(b.tableName),
		pq.QuoteIdentifier(b.tableName))

	rawEventRows, err := b.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("Error '%v' querying newest items from DB", err)
	}

	defer rawEventRows.Close()

	var events []string

	for rawEventRows.Next() {
		var row eventRow
		err := rawEventRows.Scan(
			&row.name,
			&row.version,
			&row.payload)

		if err != nil {
			return nil, fmt.Errorf("Error '%v' storing rows from DB", err)
		}

		events = append(events, row.payload)
	}

	return events, nil
}

func (b *DBBackendObject) newestItem(name string) (string, error) {
	query := fmt.Sprintf(`select name, version, payload from %s
                where name = $1
                order by version desc limit 1;`, pq.QuoteIdentifier(b.tableName))

	var row eventRow
	err := b.connection.QueryRow(query, name).Scan(
		&row.name,
		&row.version,
		&row.payload)

	switch {
	case err == sql.ErrNoRows:
		return "", sql.ErrNoRows
	case err != nil:
		return "", fmt.Errorf("Error '%v' querying newest item from DB", err)
	}

	return row.payload, nil
}

func (b *DBBackendObject) itemVersion(name string, version int) (string, error) {
	query := fmt.Sprintf(`select name, version, payload from %s
                where name = $1 and version = $2
                order by version desc limit 1;`, pq.QuoteIdentifier(b.tableName))

	var row eventRow
	err := b.connection.QueryRow(query, name, version).Scan(
		&row.name,
		&row.version,
		&row.payload)

	switch {
	case err == sql.ErrNoRows:
		return "", sql.ErrNoRows
	case err != nil:
		return "", fmt.Errorf("Error '%v' querying specific item from DB", err)
	}

	return row.payload, nil
}

func (b *DBBackendObject) putItem(name string, version int, payload string) error {
	query := fmt.Sprintf(`insert into %s values ($1, $2, $3);`, pq.QuoteIdentifier(b.tableName))

	_, err := b.connection.Exec(query, name, version, payload)
	if err != nil {
		return fmt.Errorf("Error '%v' executing insert into DB", err)
	}

	return nil
}
