package tables

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Controller struct {
	DynamoDB *dynamodb.DynamoDB
	// TableInfo gets loaded from config
	Tables []TableInfo
	// Environment string used as table prefix
	env string
	// Default logger if no logging implementation is defined.
	Log Logger
}

// ValidationResult contains result information of a single table schema validation.
type ValidationResult struct {
	// TableInfo loaded from config file
	TableInput TableInfo
	// If any table is missing, CreateTableInput will contain an input for creating the table.
	CreateTableInput *dynamodb.CreateTableInput
	// If table schemas mismatch, such as updated table throughput or newly added GSI,
	// UpdateTableInput will contain an input for updating the table.
	// nil if schemas mismatches cannot be fixed by updating table.
	UpdateTableInput *dynamodb.UpdateTableInput
	// If TTL is missing or the status of TTL is changed, UpdateTTLInput wil contain an input for
	// updating the TTL.
	UpdateTTLInput *dynamodb.UpdateTimeToLiveInput
	// A diff string that shows all the mismatched table schemas
	Diff string
	// true if table schema can be migrated.
	CanMigrate bool
	// Error contains error information when a table schema can not be migrated.
	Error error
}

// MigrationResult contains result information of a single table schema migration
type MigrationResult struct {
	// TableInfo loaded from config file
	TableInput TableInfo
	// Errors occurred during migration
	Errors []error
}

// NewController initialises a new table schema controller
// NewController loads the config file and retrieves table schema for comparison.
// env represents Environment which is used as table prefix
// You can optionally pass a logger implementation.
// If no logging implementation is passed the default logger is used.
func NewController(db *dynamodb.DynamoDB, env string, logger Logger) (*Controller, error) {
	tableInfo, err := Load()
	if err != nil {
		return nil, err
	}

	if logger == nil {
		logger = &DefaultLogger{}
	}

	return &Controller{
		DynamoDB: db,
		Tables:   tableInfo,
		env:      env,
		Log:      logger,
	}, nil
}

// Validate compares the table schemas in the config file to
// the table descriptions in the current database.
// A common error ErrValidationFailed is also returned if
// any comparison contains schema mismatches.
func (c *Controller) Validate() ([]*ValidationResult, error) {
	resultChan := make(chan *ValidationResult, len(c.Tables))

	var wg sync.WaitGroup
	for _, tbl := range c.Tables {
		wg.Add(1)
		go func(tbl TableInfo, resultChan chan *ValidationResult) {
			defer wg.Done()
			result, err := c.compare(tbl)
			if err != nil {
				result.CanMigrate = false
				result.Error = err
			}
			c.Log.Infof("Validate table [%s] with diff: %v", tbl.TableName, result.Diff)
			resultChan <- result
		}(tbl, resultChan)
	}
	wg.Wait()
	close(resultChan)

	res := []*ValidationResult{}
	isValid := true
	for r := range resultChan {
		res = append(res, r)
		if r.Error != nil {
			isValid = false
		}
		if len(r.Diff) > 0 {
			isValid = false
		}
	}

	if isValid {
		return res, nil
	}
	return res, ErrValidationFailed
}

// Migrate attempts to update table schemas based on given validation result.
// Validate() must be called prior to Migrate in order to get the Validation Result.
// Any Validation Result that contains schema mismatches which cannot be migrated
// will be skipped.
// Any errors occur during migration process are included in the Migration Result.
func (c *Controller) Migrate(results []*ValidationResult) []*MigrationResult {
	ms := make([]*MigrationResult, len(results))
	var wg sync.WaitGroup
	for i, res := range results {
		if len(res.Diff) > 0 {
			wg.Add(1)
			go func(i int, res *ValidationResult) {
				defer wg.Done()
				ms[i] = &MigrationResult{
					TableInput: res.TableInput,
				}
				errs := c.migrate(res)
				if len(errs) > 0 {
					ms[i].Errors = errs
				}
				c.Log.Infof("Migrate table [%s] with errors: %+v", res.TableInput.TableName, ms[i].Errors)
			}(i, res)
		}
	}
	wg.Wait()

	return ms
}

func (c *Controller) migrate(r *ValidationResult) []error {
	errs := []error{}

	if r.Error != nil {
		return []error{ErrInvalidMigrationInput}
	}
	if !r.CanMigrate {
		return []error{ErrInvalidMigrationInput}
	}
	// migrate
	if r.CreateTableInput != nil {
		if err := c.createTable(r.TableInput); err != nil {
			errs = append(errs, err)
		}
	}
	if r.UpdateTTLInput != nil {
		if err := c.updateTTL(r.UpdateTTLInput); err != nil {
			errs = append(errs, err)
		}
	}
	if r.UpdateTableInput != nil {
		if err := c.updateTable(r.TableInput, r.UpdateTableInput); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// compare compares table schema
// The first returning value contains diff string
// The second returning value indicates whether the schema is suitable for auto migration.
func (c *Controller) compare(tbl TableInfo) (*ValidationResult, error) {
	diff := ""
	canMigrate := true
	result := &ValidationResult{
		TableInput: tbl,
	}

	// Check if table exists. If not, append input for table creation and return.
	desc, err := c.describeTable(withPrefix(c.env, tbl.Title, tbl.TableName))
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if ok {
			// Table doesn't exist
			if aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
				result.CreateTableInput = CreateTableInput(tbl, c.env)
				result.CanMigrate = true
				result.Diff = fmt.Sprintf("missing table: %s", tbl.TableName)
				return result, nil
			}
		}
		return nil, err
	}

	// Table exists, compare table description
	input := CreateTableInput(tbl, c.env)
	d := DiffTableDesc(desc, input)
	if len(d) > 0 {
		// Table descriptions mismatch
		// This is unlikely to happen
		canMigrate = false
		diff = d
	}

	diffPt := DiffProvisionedThroughput(&dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  desc.ProvisionedThroughput.ReadCapacityUnits,
		WriteCapacityUnits: desc.ProvisionedThroughput.WriteCapacityUnits,
	}, input.ProvisionedThroughput)
	if len(diffPt) > 0 {
		diff = fmt.Sprintf("Throughput: %v%v", diff, diffPt)
		result.UpdateTableInput = UpdateTableInputBase(tbl, c.env)
		result.UpdateTableInput.ProvisionedThroughput = input.ProvisionedThroughput
	}

	// Compare GSI
	diffGSI := DiffGSI(desc.GlobalSecondaryIndexes, input.GlobalSecondaryIndexes)
	if diffGSI != nil {
		if len(diffGSI.Diff) > 0 {
			diff = fmt.Sprintf("GSI: %v%v", diff, diffGSI.Diff)
			if len(diffGSI.GSIInput) == 0 {
				// GSI can not be updated by Migrate
				canMigrate = false
			}
			if result.UpdateTableInput == nil {
				result.UpdateTableInput = UpdateTableInputBase(tbl, c.env)
			}
			result.UpdateTableInput.GlobalSecondaryIndexUpdates = diffGSI.GSIInput
		}
	}

	// Compare TTL
	if tbl.TTL != nil {
		ttl, err := c.describeTTL(withPrefix(c.env, tbl.Title, tbl.TableName))
		if err != nil {
			c.Log.Error(err.Error())
			return result, err
		}
		// Missing TTL
		if ttl == nil {
			result.UpdateTTLInput = NewUpdateTimeToLiveInput(tbl, c.env, tbl.TTL)
			return result, nil
		}
		// TTL exists, compare TTLs
		ttlStatus := "ENABLED"
		if tbl.TTL.Enabled == false {
			ttlStatus = "DISABLED"
		}
		expected := &dynamodb.TimeToLiveDescription{
			AttributeName:    aws.String(tbl.TTL.AttributeName),
			TimeToLiveStatus: aws.String(ttlStatus),
		}
		d := DiffTTL(ttl, expected)
		if len(d) > 0 {
			diff = fmt.Sprintf("TTL: %v%v", diff, d)
			result.UpdateTTLInput = NewUpdateTimeToLiveInput(tbl, c.env, tbl.TTL)
		}
	}

	result.Diff = diff
	result.CanMigrate = canMigrate
	return result, nil
}

func (c *Controller) describeTable(tblName string) (*dynamodb.TableDescription, error) {
	output, err := c.DynamoDB.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tblName),
	})
	if err != nil {
		return nil, err
	}
	return output.Table, nil
}

func (c *Controller) describeTTL(tblName string) (*dynamodb.TimeToLiveDescription, error) {
	output, err := c.DynamoDB.DescribeTimeToLive(&dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(tblName),
	})
	if err != nil {
		return nil, err
	}
	return output.TimeToLiveDescription, nil
}

func (c *Controller) createTable(ti TableInfo) error {
	input := CreateTableInput(ti, c.env)
	if _, err := c.DynamoDB.CreateTable(input); err != nil {
		return err
	}

	if ti.TTL != nil {
		ttlInfo := NewUpdateTimeToLiveInput(ti, c.env, ti.TTL)
		if err := c.updateTTL(ttlInfo); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) updateTTL(input *dynamodb.UpdateTimeToLiveInput) error {
	if _, err := c.DynamoDB.UpdateTimeToLive(input); err != nil {
		return err
	}
	return nil
}

func (c *Controller) updateTable(ti TableInfo, input *dynamodb.UpdateTableInput) error {
	if _, err := c.DynamoDB.UpdateTable(input); err != nil {
		return err
	}
	return nil
}
