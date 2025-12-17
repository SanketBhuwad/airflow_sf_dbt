resource "snowflake_table" "nation" {
  database = snowflake_database.practice_db.name
  schema   = "RAW"
  name     = "NATION"

  column {
    name = "N_NATIONKEY"
    type = "VARCHAR(255)"
  }
  column {
    name = "N_NAME"
    type = "VARCHAR(255)"
  }
  column {
    name = "N_REGIONKEY"
    type = "VARCHAR(255)"
  }
  column {
    name = "N_COMMENT"
    type = "VARCHAR(16777216)" # Only for very long text
  }
  column {
    name = "DAG_ID"
    type = "VARCHAR(255)"
  }
  column {
    name = "LOAD_TIME"
    type = "TIMESTAMP_NTZ(9)"
  }
}
