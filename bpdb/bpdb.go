package bpdb

import "github.com/twitchscience/scoop_protocol/schema"

//Adapter specifies the interface that stores event versions into a DB
type Adapter interface {
	Events() ([]schema.Event, error)
	NewestEvent(name string) ([]schema.Event, error)
	EventVersion(name string, version int) ([]schema.Event, error)
	PutEvent(schema.Event) error
}
