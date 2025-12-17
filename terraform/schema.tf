resource "snowflake_schema" "raw" {
    name = "RAW"
    database = snowflake_database.practice_db.name
    comment = "schema for landing file"
}

resource "snowflake_schema" "curated" {
    name = "CURATED"
    database = snowflake_database.practice_db.name
    comment = "schema for CURATED file"
}

resource "snowflake_schema" "consumption" {
    name = "CONSUMPTION"
    database = snowflake_database.practice_db.name
    comment = "schema for CONSUMPTION file"
}