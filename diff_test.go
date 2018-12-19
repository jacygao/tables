package tables

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"testing"
)

func TestDiffIndexName(t *testing.T) {
	str1 := aws.String("test")
	str2 := aws.String("should_fail")

	if diff := DiffIndexName(str1, str1); diff != "" {
		t.Fatalf("expected empty diff but got %s", diff)
	}

	if diff := DiffIndexName(str1, str2); diff == "" {
		t.Fatal("expected valid diff but got empty diff")
	}
}

func TestDiffProvisionedThroughput(t *testing.T) {
	obj1 := &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(1),
		WriteCapacityUnits: aws.Int64(1),
	}

	obj2 := &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(2),
		WriteCapacityUnits: aws.Int64(2),
	}

	if diff := DiffProvisionedThroughput(obj1, obj1); diff != "" {
		t.Fatalf("expected empty diff but got %s", diff)
	}

	if diff := DiffProvisionedThroughput(obj1, obj2); diff == "" {
		t.Fatalf("expected valid diff but got empty diff")
	}
}

func TestDiffKeySchema(t *testing.T) {
	obj1 := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String("test"),
			KeyType:       aws.String("test"),
		},
	}

	obj2 := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String("should_fail"),
			KeyType:       aws.String("should_fail"),
		},
	}

	if diff := DiffKeySchema(obj1, obj1); diff != "" {
		t.Fatalf("expected empty diff but got %s", diff)
	}

	if diff := DiffKeySchema(obj1, obj2); diff == "" {
		t.Fatal("expected valid diff but got empty diff")
	}
}

func TestDiffAttributeDefinitions(t *testing.T) {
	obj1 := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String("test1"),
			AttributeType: aws.String("test1"),
		},
		{
			AttributeName: aws.String("test2"),
			AttributeType: aws.String("test2"),
		},
	}

	obj2 := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String("test2"),
			AttributeType: aws.String("test2"),
		},
		{
			AttributeName: aws.String("test1"),
			AttributeType: aws.String("test1"),
		},
	}

	obj3 := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String("should_fail"),
			AttributeType: aws.String("should_fail"),
		},
	}

	if diff := DiffAttributeDefinitions(obj1, obj2); diff != "" {
		t.Fatalf("expected empty diff but got %s", diff)
	}

	if diff := DiffAttributeDefinitions(obj1, obj3); diff == "" {
		t.Fatal("expected valid diff but got empty diff")
	}
}

func TestDiffProjection(t *testing.T) {
	obj1 := &dynamodb.Projection{
		NonKeyAttributes: []*string{
			aws.String("test1"),
			aws.String("test2"),
		},
		ProjectionType: aws.String("test"),
	}

	obj2 := &dynamodb.Projection{
		NonKeyAttributes: []*string{
			aws.String("test3"),
			aws.String("test4"),
		},
		ProjectionType: aws.String("test"),
	}

	obj3 := &dynamodb.Projection{
		NonKeyAttributes: []*string{
			aws.String("test2"),
			aws.String("test1"),
		},
		ProjectionType: aws.String("test"),
	}

	if diff := DiffProjection(obj1, obj1); diff != "" {
		t.Fatalf("expected empty diff but got %s", diff)
	}

	if diff := DiffProjection(obj1, obj2); diff == "" {
		t.Fatal("expected valid diff but got empty diff")
	}

	if diff := DiffProjection(obj1, obj3); diff != "" {
		t.Fatalf("expected empty diff but got %s", diff)
	}
}

func TestDiffGSI(t *testing.T) {
	obj1 := []*dynamodb.GlobalSecondaryIndex{
		{
			IndexName: aws.String("test"),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("test"),
					KeyType:       aws.String("test"),
				},
			},
			Projection: &dynamodb.Projection{
				NonKeyAttributes: []*string{
					aws.String("test1"),
					aws.String("test2"),
				},
				ProjectionType: aws.String("test"),
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		},
	}

	obj2 := []*dynamodb.GlobalSecondaryIndexDescription{
		{
			IndexName: aws.String("test"),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("test"),
					KeyType:       aws.String("test"),
				},
			},
			Projection: &dynamodb.Projection{
				NonKeyAttributes: []*string{
					aws.String("test1"),
					aws.String("test2"),
				},
				ProjectionType: aws.String("test"),
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughputDescription{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
			ItemCount: aws.Int64(100),
		},
	}

	obj3 := []*dynamodb.GlobalSecondaryIndexDescription{
		{
			IndexName: aws.String("test2"),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("test2"),
					KeyType:       aws.String("test2"),
				},
			},
			Projection: &dynamodb.Projection{
				NonKeyAttributes: []*string{
					aws.String("test1"),
					aws.String("test2"),
				},
				ProjectionType: aws.String("test"),
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughputDescription{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
			ItemCount: aws.Int64(100),
		},
	}

	obj4 := []*dynamodb.GlobalSecondaryIndexDescription{
		{
			IndexName: aws.String("test"),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("test3"),
					KeyType:       aws.String("test3"),
				},
			},
			Projection: &dynamodb.Projection{
				NonKeyAttributes: []*string{
					aws.String("test1"),
					aws.String("test2"),
				},
				ProjectionType: aws.String("test"),
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughputDescription{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
			ItemCount: aws.Int64(100),
		},
	}

	res := DiffGSI(obj2, obj1)
	if len(res.Diff) > 0 {
		t.Fatalf("expected empty diff but got %s", res.Diff)
	}

	res = DiffGSI(obj3, obj1)
	if len(res.Diff) == 0 {
		t.Fatalf("expected empty diff but got %s", res.Diff)
	}

	res = DiffGSI(obj4, obj1)
	if len(res.Diff) == 0 {
		t.Fatal("expected valid diff but got empty")
	}
}

func TestDiffLSI(t *testing.T) {
	obj1 := []*dynamodb.LocalSecondaryIndex{
		{
			IndexName: aws.String("test"),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("test"),
					KeyType:       aws.String("test"),
				},
			},
			Projection: &dynamodb.Projection{
				NonKeyAttributes: []*string{
					aws.String("test1"),
					aws.String("test2"),
				},
				ProjectionType: aws.String("test"),
			},
		},
	}

	obj2 := []*dynamodb.LocalSecondaryIndex{
		{
			IndexName: aws.String("test"),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("test"),
					KeyType:       aws.String("test"),
				},
			},
			Projection: &dynamodb.Projection{
				NonKeyAttributes: []*string{
					aws.String("test1"),
					aws.String("test2"),
				},
				ProjectionType: aws.String("test"),
			},
		},
	}

	obj3 := []*dynamodb.LocalSecondaryIndex{
		{
			IndexName: aws.String("test2"),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("test2"),
					KeyType:       aws.String("test2"),
				},
			},
			Projection: &dynamodb.Projection{
				NonKeyAttributes: []*string{
					aws.String("test1"),
					aws.String("test2"),
				},
				ProjectionType: aws.String("test"),
			},
		},
	}

	diff := DiffLSI(obj2, obj1)
	if diff != "" {
		t.Fatalf("expected empty diff but got %s", diff)
	}

	diff = DiffLSI(obj3, obj1)
	if len(diff) == 0 {
		t.Fatal("expected valid diff but got empty")
	}
}
