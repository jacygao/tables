## Overview
Package tables is an useful tool to validate, migrate and reset table schema for DynamoDB.

## Basics
### Configuration
Modify `tables.yaml` file to add/edit table schemas.

### Initialisation
```go
// Initialise a dynamodb client via the aws-sdk-go.
// Information on how to initialse a dynamodb client please refer to the offcial documentation 
// of Dynamodb Go SDK: https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/
dynamodbCli := dynamodb.New(session.New())

// NewController loads the config file and retrieves table schema for comparison.
// env represents Environment which is used as table prefix
// You can optionally pass a logger implementation.
// If no logging implementation is passed the default logger is used.
controller := tables.NewController(dynamodbCli, "sandbox", nil)
```

### Validate Table Schema
```go
validationResult, err := controller.Validate()
if err != nil {
	// handle error
}
```

### Migrate Table Schema
```go
migrationResult := controller.Migrate(validationResult)
for _, res := range migrationResult {
	if len(res.Errors) > 0 {
		// handle error
	}
}
```

### Reset Tables
```go
// Reset removes all configured tables from DynamoDB
resetResults := controller.Reset()
```

### Console Output
The sample output shows the following information:
- table escrow is missing
- table alliance-member has mismatch table schema
- table alliance-member cannot be updated due to specific error
- table escrow has been updated without any error
```
2018/12/05 16:59:22 INFO: Validate table [escrow] with diff: missing table: escrow
2018/12/05 16:59:22 INFO: Validate table [savedata] with diff:
2018/12/05 16:59:22 INFO: Validate table [transactions] with diff:
2018/12/05 16:59:22 INFO: Validate table [token] with diff:
2018/12/05 16:59:22 INFO: Validate table [integration-test] with diff:
2018/12/05 16:59:22 INFO: Validate table [replays] with diff:
2018/12/05 16:59:22 INFO: Validate table [alliance-member] with diff: GSI: Attribute Definition: *{[]*dynamodb.AttributeDefinition}[0].AttributeName:
        -: "alliance_id"
        +: "alliance_ids"
*{[]*dynamodb.KeySchemaElement}[0].AttributeName:
        -: "alliance_id"
        +: "alliance_ids"
*{[]*dynamodb.KeySchemaElement}[0].AttributeName:
        -: "alliance_id"
        +: "alliance_ids"
2018/12/05 16:59:22 INFO: Migrate table [alliance-member] with errors: [cannot migrate table input with unrecoverable errors]
2018/12/05 16:59:22 INFO: Migrate table [escrow] with errors: []
```

### Table Schema update currently supported by Migrate
- new table
- new GSIs
- new TTL
- update table throughput
- update GSI throughput
- enable/disable TTL
