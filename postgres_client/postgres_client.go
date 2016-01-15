package PostgresClient

import "github.com/twitchscience/scoop_protocol/schema"

type PostgresAdaptor interface {
	GetEvents() ([]schema.Event, error)
	GetNewestEvent(eventName string) (schema.Event, error)
	GetSpecificEvent(eventName string, eventVersion int) (schema.Event, error)
	PutEvent(schema.Event) error
}
