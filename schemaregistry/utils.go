package schemaregistry

import (
	"encoding/json"
	"fmt"
	"github.com/ashleybill/srclient"
)

const IDSeparator = "___"

func formatSchemaVersionID(subject string) string {
	return subject
}

func extractSchemaVersionID(id string) string {
	return id
}

// getSchemaByCustomVersionField gets the schema that contains a specific customVersionField and desiredVersion
func getSchemaByCustomVersionField(client *srclient.SchemaRegistryClient, subject string, customVersionField string, desiredVersion int) (*srclient.Schema, error) {
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

		// Look for the custom version field in the schema data
		if fields, ok := schemaData["fields"].([]interface{}); ok {
			for _, field := range fields {
				if fieldMap, ok := field.(map[string]interface{}); ok {
					if name, ok := fieldMap["name"].(string); ok && name == customVersionField {
						if fieldVersion, ok := fieldMap["default"].(float64); ok && int(fieldVersion) == desiredVersion {
							if latestSchema == nil || version > latestVersion {
								latestSchema = schema
								latestVersion = version
							}
						}
					}
				}
			}
		}
	}

	if latestSchema == nil {
		return nil, fmt.Errorf("No schema found with %s=%s for subject %s", customVersionField, desiredVersion, subject)
	}

	return latestSchema, nil
}
