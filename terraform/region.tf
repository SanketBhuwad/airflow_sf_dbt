resource "snowflake_table" "region" {
  database = snowflake_database.practice_db.name
  schema   = "RAW"
  name     = "REGION"

  column {
    name = "R_REGIONKEY"
    type = "NUMBER(38,0)"
  }
  column {
    name = "R_NAME"
    type = "VARCHAR(25)"
  }
  column {
    name = "R_COMMENT"
    type = "VARCHAR(152)"
  }

  # Optional ETL tracking columns for data lineage
  column {
    name = "DAG_ID"
    type = "VARCHAR(255)"
  }
  column {
    name = "LOAD_TIME"
    type = "TIMESTAMP_NTZ(9)"
  }
}
