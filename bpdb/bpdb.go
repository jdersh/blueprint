package bpdb

import "github.com/twitchscience/scoop_protocol/schema"

type Adapter interface {
	New(driverName, urlName, tableName string) (Backend, error)
	Events() ([]schema.Event, error)
	NewestEvent(name string) (schema.Event, error)
	EventVersion(name string, version int) (schema.Event, error)
	PutEvent(schema.Event) error
}
