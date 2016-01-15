package PostgresClient

import (
	"flag"
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	postgresURL = flag.String("postgresURL", "", "The login url for the postgres DB")
	pgBackend   PostgresBackendObject

	testEventTable = "test_event_schemas"
)

func TestGeneric(t *testing.T) {
	flag.Parse()

	Convey("Top Level", t, func() {
		pgBackend, err := BuildPostgresBackend(*postgresURL, testEventTable)
		fmt.Println("Open connection")
		So(err, ShouldEqual, nil)

		err = pgBackend.createTestTable()
		fmt.Println("create Table")
		So(err, ShouldEqual, nil)

		Convey("Test a ping1", func() {
			err := pgBackend.connection.Ping()
			So(err, ShouldEqual, nil)
		})

		Convey("Test a ping2", func() {
			err := pgBackend.connection.Ping()
			So(err, ShouldEqual, nil)
		})

		Reset(func() {
			pgBackend.connection.Close()
			fmt.Println("TEARDOWN")
		})

		err = pgBackend.dropTestTable()
		fmt.Println("Drop table")
		So(err, ShouldEqual, nil)

	})

	fmt.Println("Success!!!")
}
