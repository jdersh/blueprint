package dynamo_client

type DynamoBackend interface {

}

type DynamoClient interface {
    GetTables() []string
    GetSchema(tableName string) (*schema_storer.TableSchema, error)
    DoesExist() bool
    UpdateTable(schema_storer.TableSchema) error
}