package tables

import (
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

type GSIResult struct {
	GSIInput []*dynamodb.GlobalSecondaryIndexUpdate
	Diff     string
}

// DiffTableDesc gets the diff string of two table descriptions
func DiffTableDesc(desc *dynamodb.TableDescription, input *dynamodb.CreateTableInput) string {
	diff := ""

	if d := DiffKeySchema(desc.KeySchema, input.KeySchema); len(d) > 0 {
		diff = fmt.Sprintf("Key Schedma: %v%v", diff, d)
	}

	if l := len(desc.LocalSecondaryIndexes); l > 0 {
		lsi := make([]*dynamodb.LocalSecondaryIndex, l)
		for _, i := range desc.LocalSecondaryIndexes {
			lsi = append(lsi, &dynamodb.LocalSecondaryIndex{
				IndexName:  i.IndexName,
				KeySchema:  i.KeySchema,
				Projection: i.Projection,
			})
		}
		d := DiffLSI(lsi, input.LocalSecondaryIndexes)
		if len(d) > 0 {
			diff = fmt.Sprintf("LSI: %v%v", diff, d)
		}
	}
	return diff
}

// DiffGSI compares two GlobalSecondaryIndexDescription slices and returns the diff string.
// GSIResult also contains a list GSIInput. This data is used for Migrate() and only
// overridable GSIInputs are appended to the list.
func DiffGSI(desc []*dynamodb.GlobalSecondaryIndexDescription, input []*dynamodb.GlobalSecondaryIndex) *GSIResult {
	diff := ""
	result := &GSIResult{}

	if len(desc) == 0 && len(input) == 0 {
		return result
	}
	// Converting GSI slice into a map
	newObj := make(map[string]*dynamodb.GlobalSecondaryIndex, len(input))

	for _, gsi := range desc {
		newObj[aws.StringValue(gsi.IndexName)] = &dynamodb.GlobalSecondaryIndex{
			IndexName:  gsi.IndexName,
			KeySchema:  gsi.KeySchema,
			Projection: gsi.Projection,
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  gsi.ProvisionedThroughput.ReadCapacityUnits,
				WriteCapacityUnits: gsi.ProvisionedThroughput.WriteCapacityUnits,
			},
		}
	}

	for _, gsi := range input {
		obj, ok := newObj[aws.StringValue(gsi.IndexName)]
		if !ok {
			// Index does not exist in dynamoDB, we queue an input to create missing index.
			result.GSIInput = append(result.GSIInput, &dynamodb.GlobalSecondaryIndexUpdate{
				Create: &dynamodb.CreateGlobalSecondaryIndexAction{
					IndexName:             gsi.IndexName,
					KeySchema:             gsi.KeySchema,
					Projection:            gsi.Projection,
					ProvisionedThroughput: gsi.ProvisionedThroughput,
				},
			})

			diff = fmt.Sprintf("missing index: %s", aws.StringValue(gsi.IndexName))
			result.Diff = diff
			continue
		}

		if d := DiffIndexName(obj.IndexName, gsi.IndexName); len(d) > 0 {
			diff = fmt.Sprintf("%v%v", diff, d)
		}
		if d := DiffKeySchema(obj.KeySchema, gsi.KeySchema); len(d) > 0 {
			diff = fmt.Sprintf("%v%v", diff, d)
		}
		if d := DiffProjection(obj.Projection, gsi.Projection); len(d) > 0 {
			diff = fmt.Sprintf("%v%v", diff, d)
		}

		if len(diff) > 0 {

		}

		if d := DiffProvisionedThroughput(obj.ProvisionedThroughput, &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  gsi.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: gsi.ProvisionedThroughput.WriteCapacityUnits,
		}); len(d) > 0 {
			diff = fmt.Sprintf("%v%v", diff, d)
			result.GSIInput = append(result.GSIInput, &dynamodb.GlobalSecondaryIndexUpdate{
				Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
					IndexName:             gsi.IndexName,
					ProvisionedThroughput: gsi.ProvisionedThroughput,
				},
			})
		}

		if d := DiffIndexName(obj.IndexName, gsi.IndexName); len(d) > 0 {
			diff = fmt.Sprintf("%v%v", diff, d)
		}
		if d := DiffKeySchema(obj.KeySchema, gsi.KeySchema); len(d) > 0 {
			diff = fmt.Sprintf("%v%v", diff, d)
		}
		if d := DiffProjection(obj.Projection, gsi.Projection); len(d) > 0 {
			diff = fmt.Sprintf("%v%v", diff, d)
		}
		if d := DiffProvisionedThroughput(obj.ProvisionedThroughput, &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  gsi.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: gsi.ProvisionedThroughput.WriteCapacityUnits,
		}); len(d) > 0 {
			diff = fmt.Sprintf("%v%v", diff, d)
		}
	}
	if len(diff) > 0 {
		result.Diff = diff
	}

	return result
}

// DiffIndexName gets the diff string of two index names
func DiffIndexName(name1, name2 *string) string {
	return cmp.Diff(name1, name2)
}

// DiffProvisionedThroughput gets the diff string of two ProvisionedThroughputs
func DiffProvisionedThroughput(pt1, pt2 *dynamodb.ProvisionedThroughput) string {
	return cmp.Diff(
		pt1,
		pt2,
		cmpopts.IgnoreTypes(struct{}{}),
	)
}

// DiffKeySchema gets the diff string of two KeySchema slices
func DiffKeySchema(obj1, obj2 []*dynamodb.KeySchemaElement) string {
	return cmp.Diff(
		obj1,
		obj2,
		cmpopts.IgnoreTypes(struct{}{}),
	)
}

// DiffAttributeDefinitions gets the diff string of two AttributeDefinition slices.
// If two slices have same values but in different orders, the result will be the same.
func DiffAttributeDefinitions(obj1, obj2 []*dynamodb.AttributeDefinition) string {
	sort.Slice(obj1, func(i, j int) bool {
		return aws.StringValue(obj1[i].AttributeName) < aws.StringValue(obj1[j].AttributeName)
	})
	sort.Slice(obj2, func(i, j int) bool {
		return aws.StringValue(obj2[i].AttributeName) < aws.StringValue(obj2[j].AttributeName)
	})
	return cmp.Diff(
		obj1,
		obj2,
		cmpopts.IgnoreTypes(struct{}{}),
	)
}

// DiffProject gets the diff string of two Projects objects
func DiffProjection(p1, p2 *dynamodb.Projection) string {
	sort.Slice(p1.NonKeyAttributes, func(i, j int) bool {
		return aws.StringValue(p1.NonKeyAttributes[i]) < aws.StringValue(p1.NonKeyAttributes[j])
	})
	sort.Slice(p2.NonKeyAttributes, func(i, j int) bool {
		return aws.StringValue(p2.NonKeyAttributes[i]) < aws.StringValue(p2.NonKeyAttributes[j])
	})
	return cmp.Diff(
		p1,
		p2,
		cmpopts.IgnoreTypes(struct{}{}),
	)
}

// DiffLSI gets the diff string of two LocalSecondaryIndexDescription slices
func DiffLSI(input1, input2 []*dynamodb.LocalSecondaryIndex) string {
	return cmp.Diff(
		input1,
		input2,
		cmpopts.IgnoreTypes(struct{}{}),
	)
}

// DiffTTL gets the diff string of two TimeToLiveDescription objects
func DiffTTL(desc1, desc2 *dynamodb.TimeToLiveDescription) string {
	return cmp.Diff(
		desc1,
		desc2,
		cmpopts.IgnoreTypes(struct{}{}),
	)
}
