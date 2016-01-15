package PostgresClient

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/twitchscience/scoop_protocol/schema"

	"errors"

	_ "github.com/lib/pq"
)

type PostgresBackendObject struct {
	connection *sql.DB
	tableName  string
}

type eventRow struct {
	eventName    string
	eventVersion int
	eventPayload string
}

func BuildPostgresBackend(blueprintDBURL, tableName string) (PostgresBackendObject, error) {

	db, err := sql.Open("postgres", blueprintDBURL)

	if err != nil {
		return PostgresBackendObject{}, errors.New("Could not open connection to DB: " + err.Error())
	}
	err = db.Ping()
	if err != nil {
		return PostgresBackendObject{}, errors.New("Could not check link to DB: " + err.Error())
	}

	return PostgresBackendObject{
		connection: db,
		tableName:  tableName,
	}, nil
}

func (pg *PostgresBackendObject) GetEvents() ([]schema.Event, error) {
	jsonEvents, err := pg.getItems()
	if err != nil {
		return []schema.Event{}, errors.New("Failed to get events from database: " + err.Error())
	}

	var events []schema.Event
	var event schema.Event

	for _, jsonEvent := range jsonEvents {

		err := json.Unmarshal(jsonEvent, &event)
		if err != nil {
			return []schema.Event{}, errors.New("Unable to unmarshal json data into Events. Possibly incorrect json formatting.")
		}

		events = append(events, event)
	}

	return events, nil
}

func (pg *PostgresBackendObject) GetNewestEvent(eventName string) (schema.Event, error) {
	jsonEvent, err := pg.getNewestItem(eventName)
	if err != nil {
		return schema.Event{}, errors.New("Failed to get specific event from database: " + err.Error())
	}

	var event schema.Event
	err = json.Unmarshal(jsonEvent, &event)
	if err != nil {
		return schema.Event{}, errors.New("Unable to unmarshal json data into Event. Possibly incorrect json formatting.")
	}
	return event, nil
}

func (pg *PostgresBackendObject) GetSpecificEvent(eventName string, eventVersion int) (schema.Event, error) {
	jsonEvent, err := pg.getSpecificItem(eventName, eventVersion)
	if err != nil {
		return schema.Event{}, errors.New("Failed to get specific event from database: " + err.Error())
	}

	var event schema.Event
	err = json.Unmarshal(jsonEvent, &event)
	if err != nil {
		return schema.Event{}, errors.New("Unable to unmarshal json data into Event. Possibly incorrect json formatting.")
	}
	return event, nil
}

func (pg *PostgresBackendObject) UpdateEvent(event schema.Event) error {
	eventName := event.Name
	eventVersion := event.Version
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return errors.New("Unable to marshal event as json.")
	}

	err = pg.putItem(eventName, eventVersion, eventPayload)
	if err != nil {
		return errors.New("An error occured trying to load the event into the DB: " + err.Error())
	}

	return nil
}

func (pg *PostgresBackendObject) getItems() ([][]byte, error) {
	query := `select distinct on (event_name)
                    event_name,
                    event_version,
                    event_payload
                 from ` + pg.tableName + ` 
                 order by event_name, event_version desc;`

	rawEventRows, err := pg.connection.Query(query)
	if err != nil {
		return nil, errors.New("Could not get items from db: " + err.Error())
	}
	defer rawEventRows.Close()

	var stringEventRow eventRow
	var byteEvents [][]byte

	for rawEventRows.Next() {
		err := rawEventRows.Scan(
			&stringEventRow.eventName,
			&stringEventRow.eventVersion,
			&stringEventRow.eventPayload)

		if err != nil {
			return nil, errors.New("Items from DB were not what was expected: " + err.Error())
		}

		byteEventPayload := []byte(stringEventRow.eventPayload)
		byteEvents = append(byteEvents, byteEventPayload)
	}

	return byteEvents, nil
}

func (pg *PostgresBackendObject) getNewestItem(eventName string) ([]byte, error) {
	query := `select event_name, event_version, event_payload from ` + pg.tableName + ` 
                where event_name = '` + eventName + `  
                order by event_version desc limit 1;`

	var stringEventRow eventRow
	err := pg.connection.QueryRow(query).Scan(&stringEventRow)

	switch {
	case err == sql.ErrNoRows:
		temp, err := json.Marshal(schema.MakeNewEvent(eventName, 1))
		if err != nil {
			return nil, errors.New("Could not create json for new event: " + err.Error())
		}
		return temp, nil
	case err != nil:
		return nil, errors.New("Problem occured querying event from db: " + err.Error())
	}

	return []byte(stringEventRow.eventPayload), nil
}

func (pg *PostgresBackendObject) getSpecificItem(eventName string, eventVersion int) ([]byte, error) {
	query := `select event_name, event_version, event_payload from ` + pg.tableName + ` 
                where event_name = '` + eventName + `' and event_version = ` + string(eventVersion) + ` 
                order by event_version desc limit 1;`

	var stringEventRow eventRow
	err := pg.connection.QueryRow(query).Scan(&stringEventRow)

	switch {
	case err == sql.ErrNoRows:
		return nil, errors.New("Event with specific version does not exist in DB")
	case err != nil:
		return nil, errors.New("Problem occured querying event from db: " + err.Error())
	}

	return []byte(stringEventRow.eventPayload), nil
}

func (pg *PostgresBackendObject) putItem(eventName string, eventVersion int, eventPayload []byte) error {
	query := fmt.Sprintf(`insert into dev_event_schemas values ('%q', %d, '%q'); `, eventName, eventVersion, eventPayload)

	_, err := pg.connection.Exec(query)
	if err != nil {
		return errors.New("Could not add new event into DB: " + err.Error())
	}

	return nil
}

func (pg *PostgresBackendObject) createTestTable() error {
	query := fmt.Sprintf(`create table %q (
                            event_name varchar(127) not null, 
                            event_version integer not null, 
                            event_payload JSONB);`, pg.tableName)

	_, err := pg.connection.Exec(query)
	if err != nil {
		return errors.New("Could not add table to DB " + err.Error())
	}

	return nil
}

func (pg *PostgresBackendObject) dropTestTable() error {
	query := fmt.Sprintf(`drop table %q `, pg.tableName)

	_, err := pg.connection.Exec(query)
	if err != nil {
		return errors.New("Could not drop table from DB " + err.Error())
	}

	return nil
}
