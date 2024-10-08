package schemaregistry

import (
	"context"

	"github.com/ashleybill/srclient"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataSourceSchema() *schema.Resource {
	return &schema.Resource{
		ReadContext: dataSourceSubjectRead,
		Schema: map[string]*schema.Schema{
			"subject": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The subject related to the schema",
			},
			"version": {
				Type:        schema.TypeInt,
				Optional:    true,
				Description: "The version of the schema",
			},
			"desired_version": {
				Type:        schema.TypeInt,
				Optional:    true,
				Description: "The custom field version of the schema",
			},
			"custom_version_field": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The subject related to the schema",
			},
			"schema_id": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "The schema ID",
			},
			"schema": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The schema string",
			},
			"references": {
				Type:        schema.TypeList,
				Computed:    true,
				Description: "The referenced schema names list",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:        schema.TypeString,
							Computed:    true,
							Description: "The referenced schema name",
						},
						"subject": {
							Type:        schema.TypeString,
							Computed:    true,
							Description: "The subject related to the schema",
						},
						"version": {
							Type:        schema.TypeInt,
							Computed:    true,
							Description: "The version of the schema",
						},
					},
				},
			},
		},
	}
}

func dataSourceSubjectRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	subject := d.Get("subject").(string)
	version := d.Get("version").(int)
	customVersionField := d.Get("custom_version_field").(string)
	desiredVersion := d.Get("desired_version").(int)

	client := m.(*srclient.SchemaRegistryClient)
	var schema *srclient.Schema
	var err error

	if version > 0 {
		schema, err = client.GetSchemaByVersion(subject, version)
	} else if customVersionField != "" && desiredVersion > 0 {
		schema, err = getSchemaByCustomVersionField(client, subject, customVersionField, desiredVersion)
	} else {
		schema, err = client.GetLatestSchema(subject)
	}

	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("schema", schema.Schema())
	d.Set("schema_id", schema.ID())
	d.Set("version", schema.Version())

	if err = d.Set("references", FromRegistryReferences(schema.References())); err != nil {
		return diag.FromErr(err)
	}

	d.SetId(formatSchemaVersionID(subject))

	return diags
}
