curl -L -X POST '{{host}}/object/category/definition/v4/create' \
-H 'Content-Type: application/json' \
--data-raw '{
  "request": {
    "objectCategoryDefinition": {
      "categoryId": "obj-cat:course-assessment",
      "targetObjectType": "QuestionSet",
      "objectMetadata": {
        "config": {},
        "schema": {
          "properties": {
            "trackable": {
              "type": "object",
              "properties": {
                "enabled": {
                  "type": "string",
                  "enum": [
                    "Yes",
                    "No"
                  ],
                  "default": "No"
                },
                "autoBatch": {
                  "type": "string",
                  "enum": [
                    "Yes",
                    "No"
                  ],
                  "default": "No"
                }
              },
              "additionalProperties": false
            }
          }
        }
      }
    }
  }
}'