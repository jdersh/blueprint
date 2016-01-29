package bpdb

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/twitchscience/scoop_protocol/schema"

	"github.com/lib/pq"
)

type PostgresBackendObject struct {
	connection *sql.DB
	tableName  string
}

type eventRow struct {
	name    string
	version int
	payload string
}

func NewPostgresBackend(db *sql.DB, tableName string) (PostgresBackendObject, error) {
	err := db.Ping()
	if err != nil {
		return PostgresBackendObject{}, fmt.Errorf("Error %v establishing connection to DB", err)
	}

	return PostgresBackendObject{
		connection: db,
		tableName:  tableName,
	}, nil
}

func (pg *PostgresBackendObject) Events() ([]schema.Event, error) {
	jsonEvents, err := pg.items()
	if err != nil {
		return []schema.Event{}, fmt.Errorf("Error %v getting events from database", err)
	}

	var events []schema.Event

	for _, jsonEvent := range jsonEvents {
		var event schema.Event

		err := json.Unmarshal([]byte(jsonEvent), &event)
		if err != nil {
			return []schema.Event{}, fmt.Errorf("Error %v unmarshalling json: %s", err, jsonEvent)
		}

		events = append(events, event)
	}

	return events, nil
}

func (pg *PostgresBackendObject) NewestEvent(name string) (schema.Event, error) {
	jsonEvent, err := pg.newestItem(name)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error %v getting newest event from DB", err)
	}

	var event schema.Event
	err = json.Unmarshal([]byte(jsonEvent), &event)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error %v unmarshalling newest event json: %s ", err, jsonEvent)
	}
	return event, nil
}

func (pg *PostgresBackendObject) EventVersion(name string, version int) (schema.Event, error) {
	jsonEvent, err := pg.itemVersion(name, version)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error %v getting specific event from DB", err)
	}

	var event schema.Event
	err = json.Unmarshal([]byte(jsonEvent), &event)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error %v unmarshalling specific event json: %s", err, jsonEvent)
	}
	return event, nil
}

func (pg *PostgresBackendObject) PutEvent(event schema.Event) error {
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("Error %v marshalling event", err)
	}

	err = pg.putItem(event.Name, event.Version, string(eventPayload))
	if err != nil {
		return fmt.Errorf("Error %v loading event into DB", err)
	}

	return nil
}

func (pg *PostgresBackendObject) items() ([]string, error) {
	//Retrieve all the events sorted by newest version, and use distinct to filter out old versions
	query := fmt.Sprintf(`	select distinct on (event_name)
                    			event_name,
                    			event_version,
                    			event_payload
                 			from %s 
                 			order by event_name, event_version desc;`, pq.QuoteIdentifier(pg.tableName))

	rawEventRows, err := pg.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("Error %v querying newest items from DB", err)
	}

	defer rawEventRows.Close()

	var events []string

	for rawEventRows.Next() {
		var stringEventRow eventRow
		err := rawEventRows.Scan(
			&stringEventRow.name,
			&stringEventRow.version,
			&stringEventRow.payload)

		if err != nil {
			return nil, fmt.Errorf("Error %v storing rows from DB", err)
		}

		events = append(events, stringEventRow.payload)
	}

	return events, nil
}

func (pg *PostgresBackendObject) newestItem(name string) (string, error) {
	query := fmt.Sprintf(`select event_name, event_version, event_payload from %s
                where event_name = $1
                order by event_version desc limit 1;`, pq.QuoteIdentifier(pg.tableName))

	var stringEventRow eventRow
	err := pg.connection.QueryRow(query, name).Scan(
		&stringEventRow.name,
		&stringEventRow.version,
		&stringEventRow.payload)

	switch {
	case err == sql.ErrNoRows:
		temp, err := json.Marshal(schema.MakeNewEvent(name, 1))
		if err != nil {
			return "", fmt.Errorf("Error %v marshalling json for new event", err)
		}
		return string(temp), nil

	case err != nil:
		return "", fmt.Errorf("Error %v querying newest item from DB", err)
	}

	return stringEventRow.payload, nil
}

func (pg *PostgresBackendObject) itemVersion(name string, version int) (string, error) {
	query := fmt.Sprintf(`select event_name, event_version, event_payload from %s
                where event_name = $1 and event_version = $2
                order by event_version desc limit 1;`, pq.QuoteIdentifier(pg.tableName))

	var stringEventRow eventRow
	err := pg.connection.QueryRow(query, name, version).Scan(
		&stringEventRow.name,
		&stringEventRow.version,
		&stringEventRow.payload)

	switch {
	case err == sql.ErrNoRows:
		return "", fmt.Errorf("Specific item does not exist in DB: %v", err)
	case err != nil:
		return "", fmt.Errorf("Error %v querying specific item from DB", err)
	}

	return stringEventRow.payload, nil
}

func (pg *PostgresBackendObject) putItem(name string, version int, payload string) error {
	query := fmt.Sprintf(`insert into %s values ($1, $2, $3);`, pq.QuoteIdentifier(pg.tableName))

	_, err := pg.connection.Exec(query, name, version, payload)
	if err != nil {
		return fmt.Errorf("Error %v executing insert into DB", err)
	}

	return nil
}
