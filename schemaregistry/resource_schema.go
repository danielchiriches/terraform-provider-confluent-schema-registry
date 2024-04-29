package schemaregistry

import (
	"context"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/customdiff"

	"github.com/ashleybill/srclient"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/structure"
)

func resourceSchema() *schema.Resource {
	return &schema.Resource{
		CreateContext: schemaCreate,
		UpdateContext: schemaUpdate,
		ReadContext:   schemaRead,
		DeleteContext: schemaDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		CustomizeDiff: customdiff.ComputedIf("version", func(ctx context.Context, d *schema.ResourceDiff, meta interface{}) bool {
			oldState, newState := d.GetChange("schema")
			newJSON, _ := structure.NormalizeJsonString(newState)
			oldJSON, _ := structure.NormalizeJsonString(oldState)
			schemaHasChange := newJSON != oldJSON

			// explicitly set a version change on schema change and make dependencies aware of a
			// version changed at `plan` time (computed field)
			return schemaHasChange || d.HasChange("version")
		}),
		Schema: map[string]*schema.Schema{
			"subject": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The subject related to the schema",
				ForceNew:    true,
			},
			"schema": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The schema string",
				DiffSuppressFunc: func(k, old, new string, d *schema.ResourceData) bool {
					newJSON, _ := structure.NormalizeJsonString(new)
					oldJSON, _ := structure.NormalizeJsonString(old)
					return newJSON == oldJSON
				},
			},
			"schema_id": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "The ID of the schema",
			},
			"version": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "The schema string",
			},
			"reference": {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "The referenced schema list",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "The referenced schema name",
						},
						"subject": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "The referenced schema subject",
						},
						"version": {
							Type:        schema.TypeInt,
							Required:    true,
							Description: "The referenced schema version",
						},
					},
				},
			},
			"schema_type": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The schema type",
				Default:     "avro",
			},
			"compatibility_level": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The compatibiltiy level of the schema",
				Default:     "FORWARD_TRANSITIVE",
			},
		},
	}
}

func schemaCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	subject := d.Get("subject").(string)
	schemaString := d.Get("schema").(string)
	references := ToRegistryReferences(d.Get("reference").([]interface{}))
	schemaType := ToSchemaType(d.Get("schema_type"))
	compatibilityLevel := ToCompatibilityLevelType(d.Get("compatibility_level"))

	client := meta.(*srclient.SchemaRegistryClient)

	schema, err := client.CreateSchema(subject, schemaString, schemaType, references...)
	if err != nil {
		return diag.FromErr(err)
	}

	_, err = client.ChangeSubjectCompatibilityLevel(subject, compatibilityLevel)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(formatSchemaVersionID(subject))
	d.Set("schema_id", schema.ID())
	d.Set("schema", schema.Schema())
	d.Set("version", schema.Version())

	if err = d.Set("reference", FromRegistryReferences(schema.References())); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func schemaUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	subject := d.Get("subject").(string)
	schemaString := d.Get("schema").(string)
	references := ToRegistryReferences(d.Get("reference").([]interface{}))
	schemaType := ToSchemaType(d.Get("schema_type"))
	compatibilityLevel := ToCompatibilityLevelType(d.Get("compatibility_level"))

	client := meta.(*srclient.SchemaRegistryClient)

	schema, err := client.CreateSchema(subject, schemaString, schemaType, references...)
	if err != nil {
		if strings.Contains(err.Error(), "409") {
			return diag.Errorf(`invalid "schema": incompatible`)
		}
		return diag.FromErr(err)
	}

	_, err = client.ChangeSubjectCompatibilityLevel(subject, compatibilityLevel)
	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("schema_id", schema.ID())
	d.Set("schema", schema.Schema())
	d.Set("version", schema.Version())

	if err = d.Set("reference", FromRegistryReferences(schema.References())); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func schemaRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	client := meta.(*srclient.SchemaRegistryClient)
	subject := extractSchemaVersionID(d.Id())

	newSchema := d.Get("schema")
	references := ToRegistryReferences(d.Get("reference").([]interface{}))
	schemaType := ToSchemaType(d.Get("schema_type"))

	var schema *srclient.Schema
	var err error

	if newSchema == nil {
		schema, err = client.GetLatestSchema(subject)
	} else {
		schema, err = client.LookupSchema(subject, newSchema.(string), schemaType, references...)
	}
	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("schema", schema.Schema())
	d.Set("schema_id", schema.ID())
	d.Set("subject", subject)
	d.Set("version", schema.Version())

	if err = d.Set("reference", FromRegistryReferences(schema.References())); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func schemaDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	client := meta.(*srclient.SchemaRegistryClient)
	subject := extractSchemaVersionID(d.Id())

	err := client.DeleteSubject(subject, true)
	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func FromRegistryReferences(references []srclient.Reference) []interface{} {
	if len(references) == 0 {
		return make([]interface{}, 0)
	}

	refs := make([]interface{}, 0, len(references))
	for _, reference := range references {
		refs = append(refs, map[string]interface{}{
			"name":    reference.Name,
			"subject": reference.Subject,
			"version": reference.Version,
		})
	}

	return refs
}

func ToSchemaType(schemaType interface{}) srclient.SchemaType {
	returnType := srclient.Avro

	if schemaType == "json" {
		returnType = srclient.Json
	}
	if schemaType == "protobuf" {
		returnType = srclient.Protobuf
	}

	return returnType
}

func ToCompatibilityLevelType(compatibilityLevel interface{}) srclient.CompatibilityLevel {
	returnType := srclient.ForwardTransitive

	if compatibilityLevel == "NONE" {
		returnType = srclient.None
	}
	if compatibilityLevel == "BACKWARD" {
		returnType = srclient.Backward
	}
	if compatibilityLevel == "BACKWARD_TRANSITIVE" {
		returnType = srclient.BackwardTransitive
	}
	if compatibilityLevel == "FORWARD" {
		returnType = srclient.Forward
	}
	if compatibilityLevel == "FULL" {
		returnType = srclient.Full
	}
	if compatibilityLevel == "FULL_TRANSITIVE" {
		returnType = srclient.FullTransitive
	}

	return returnType
}

func ToRegistryReferences(references []interface{}) []srclient.Reference {

	if len(references) == 0 {
		return make([]srclient.Reference, 0)
	}

	refs := make([]srclient.Reference, 0, len(references))
	for _, reference := range references {
		r := reference.(map[string]interface{})

		refs = append(refs, srclient.Reference{
			Name:    r["name"].(string),
			Subject: r["subject"].(string),
			Version: r["version"].(int),
		})
	}

	return refs
}
