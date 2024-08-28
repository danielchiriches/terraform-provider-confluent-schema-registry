package schemaregistry

import (
	"context"

	"github.com/ashleybill/srclient"
	"fmt"
	"encoding/json"
)

const IDSeparator = "___"

func formatSchemaVersionID(subject string) string {
	return subject
}

func extractSchemaVersionID(id string) string {
	return id
}

// getSchemaByCustomVersionField gets the schema that contains a specific customVersionField and desiredVersion
funct getSchemaByCustomVersionField(client *srclient.SchemaRegistryClient, subject string, customVersionField string, desiredVersion string) (*srclient.Schema, error) {
	versions, err := client.GetSchemaVersions(subject)
	if err != nil {
		return nil, err
	}

	var latestSchema *srclient.Schema
	var latestVersion int

	for _, version := range versions {
		schema, err := client.GetSchemaByVersion(subject, version)
		if err != nil {
			return nil, err
		}

		var schemaData map[string]interface{}
		err = json.Unmarshal([]byte(schema.Schema()), &schemaData)
		if err != nil {
			return nil, err
		}

		if schemaData[customVersionField] == desiredVersion {
			// Check if this is the latest version found so far
			if latestSchema == nil || version > latestVersion {
				latestSchema = schema
				latestVersion = version
			}
		}
	}

	if latestSchema == nil {
		return nil, fmt.Errorf("No schema found with %s=%s", customVersionField, desiredVersion)
	}

	return latestSchema, nil
}
