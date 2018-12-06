package tables

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type TableInfo struct {
	Title           string            `yaml:"title"`
	TableName       string            `yaml:"table_name"`
	PrimaryKey      string            `yaml:"primary_key"`
	SortKey         string            `yaml:"sort_key"`
	SortKeyType     string            `yaml:"sort_key_type"`
	ReadThroughput  int64             `yaml:"read_throughput"`
	WriteThroughput int64             `yaml:"write_throughput"`
	Indexes         []IndexInfo       `yaml:"indexes"`
	TTL             *TTLAttributeInfo `yaml:"ttl"`
}

type IndexInfo struct {
	IndexName       string   `yaml:"index_name"`
	PrimaryKey      string   `yaml:"primary_key"`
	PrimaryKeyType  string   `yaml:"primary_key_type"`
	SortKey         string   `yaml:"sort_key"`
	SortKeyType     string   `yaml:"sort_key_type"`
	ReadThroughput  int64    `yaml:"read_throughput"`
	WriteThroughput int64    `yaml:"write_throughput"`
	ProjectedFields []string `yaml:"projection_fields"`
}

type TTLAttributeInfo struct {
	AttributeName string `yaml:"attribute_name"`
	Enabled       bool   `yaml:"enabled"`
}

// CreateTableInput is a helper function to create a base CreateTableInput type
func CreateTableInput(table TableInfo, envPrefix string) *dynamodb.CreateTableInput {
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(withPrefix(envPrefix, table.Title, table.TableName)),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(table.PrimaryKey),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(table.PrimaryKey),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(table.ReadThroughput),
			WriteCapacityUnits: aws.Int64(table.WriteThroughput),
		},
	}
	if table.SortKey != "" {
		input.AttributeDefinitions = append(input.AttributeDefinitions,
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String(table.SortKey),
				AttributeType: aws.String(table.SortKeyType),
			},
		)

		input.KeySchema = append(input.KeySchema,
			&dynamodb.KeySchemaElement{
				AttributeName: aws.String(table.SortKey),
				KeyType:       aws.String("RANGE"),
			},
		)
	}
	if len(table.Indexes) > 0 {
		gsi := []*dynamodb.GlobalSecondaryIndex{}
		for _, index := range table.Indexes {
			gsi = append(gsi, NewGlobalSecondaryIndex(index))
			if !contains(input.AttributeDefinitions, index.PrimaryKey) {
				input.AttributeDefinitions = append(input.AttributeDefinitions,
					&dynamodb.AttributeDefinition{
						AttributeName: aws.String(index.PrimaryKey),
						AttributeType: aws.String(index.PrimaryKeyType),
					},
				)
			}
			if index.SortKey != "" {
				if !contains(input.AttributeDefinitions, index.SortKey) {
					input.AttributeDefinitions = append(input.AttributeDefinitions,
						&dynamodb.AttributeDefinition{
							AttributeName: aws.String(index.SortKey),
							AttributeType: aws.String(index.SortKeyType),
						},
					)
				}
			}
		}
		input.GlobalSecondaryIndexes = gsi
	}
	return input
}

// NewGlobalSecondaryIndex is a helper function to create a base GlobalSecondaryIndex type
func NewGlobalSecondaryIndex(index IndexInfo) *dynamodb.GlobalSecondaryIndex {
	projectedAttributes := []*string{
		aws.String("id"),
	}
	if len(index.ProjectedFields) > 0 {
		for _, pf := range index.ProjectedFields {
			projectedAttributes = append(projectedAttributes, aws.String(pf))
		}
	}

	input := &dynamodb.GlobalSecondaryIndex{
		IndexName: aws.String(index.IndexName),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(index.PrimaryKey),
				KeyType:       aws.String("HASH"),
			},
		},
		Projection: &dynamodb.Projection{
			NonKeyAttributes: projectedAttributes,
			ProjectionType:   aws.String(dynamodb.ProjectionTypeInclude),
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(index.ReadThroughput),
			WriteCapacityUnits: aws.Int64(index.WriteThroughput),
		},
	}
	if index.SortKey != "" {
		input.KeySchema = append(input.KeySchema,
			&dynamodb.KeySchemaElement{
				AttributeName: aws.String(index.SortKey),
				KeyType:       aws.String("RANGE"),
			},
		)
	}
	return input
}

// UpdateTableInputBase is a helper function to create a base UpdateTableInput type
func UpdateTableInputBase(table TableInfo, envPrefix string) *dynamodb.UpdateTableInput {
	base := &dynamodb.UpdateTableInput{
		TableName: aws.String(withPrefix(envPrefix, table.Title, table.TableName)),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(table.PrimaryKey),
				AttributeType: aws.String("S"),
			},
		},
	}
	if table.SortKey != "" {
		base.AttributeDefinitions = append(base.AttributeDefinitions,
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String(table.SortKey),
				AttributeType: aws.String(table.SortKeyType),
			},
		)
	}

	if len(table.Indexes) > 0 {
		gsi := []*dynamodb.GlobalSecondaryIndex{}
		for _, index := range table.Indexes {
			gsi = append(gsi, NewGlobalSecondaryIndex(index))
			if !contains(base.AttributeDefinitions, index.PrimaryKey) {
				base.AttributeDefinitions = append(base.AttributeDefinitions,
					&dynamodb.AttributeDefinition{
						AttributeName: aws.String(index.PrimaryKey),
						AttributeType: aws.String(index.PrimaryKeyType),
					},
				)
			}
			if index.SortKey != "" {
				if !contains(base.AttributeDefinitions, index.SortKey) {
					base.AttributeDefinitions = append(base.AttributeDefinitions,
						&dynamodb.AttributeDefinition{
							AttributeName: aws.String(index.SortKey),
							AttributeType: aws.String(index.SortKeyType),
						},
					)
				}
			}
		}
	}

	return base
}

// UpdateTableInputBase is a helper function to create a base UpdateTimeToLiveInput type
func NewUpdateTimeToLiveInput(table TableInfo, envPrefix string, ttl *TTLAttributeInfo) *dynamodb.UpdateTimeToLiveInput {
	if ttl != nil {
		return &dynamodb.UpdateTimeToLiveInput{
			TableName: aws.String(withPrefix(envPrefix, table.Title, table.TableName)),
			TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
				AttributeName: aws.String(ttl.AttributeName),
				Enabled:       aws.Bool(ttl.Enabled),
			},
		}
	}
	return nil
}

func withPrefix(env, title, tableName string) string {
	if len(env) > 0 && len(title) > 0 {
		return fmt.Sprintf("%s-%s-%s", title, env, tableName)
	}
	return tableName
}

func contains(attributes []*dynamodb.AttributeDefinition, attributeName string) bool {
	for _, a := range attributes {
		if *a.AttributeName == attributeName {
			return true
		}
	}
	return false
}
