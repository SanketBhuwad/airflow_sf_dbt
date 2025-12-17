resource "snowflake_table" "lineitem" {
  database = snowflake_database.practice_db.name
  schema   = "RAW"
  name     = "LINEITEM"

  column {
    name = "L_ORDERKEY"
    type = "NUMBER(38,0)"
  }
  column {
    name = "L_PARTKEY"
    type = "NUMBER(38,0)"
  }
  column {
    name = "L_SUPPKEY"
    type = "NUMBER(38,0)"
  }
  column {
    name = "L_LINENUMBER"
    type = "NUMBER(38,0)"
  }
  column {
    name = "L_QUANTITY"
    type = "NUMBER(12,2)"
  }
  column {
    name = "L_EXTENDEDPRICE"
    type = "NUMBER(12,2)"
  }
  column {
    name = "L_DISCOUNT"
    type = "NUMBER(12,2)"
  }
  column {
    name = "L_TAX"
    type = "NUMBER(12,2)"
  }
  column {
    name = "L_RETURNFLAG"
    type = "VARCHAR(1)"
  }
  column {
    name = "L_LINESTATUS"
    type = "VARCHAR(1)"
  }
  column {
    name = "L_SHIPDATE"
    type = "DATE"
  }
  column {
    name = "L_COMMITDATE"
    type = "DATE"
  }
  column {
    name = "L_RECEIPTDATE"
    type = "DATE"
  }
  column {
    name = "L_SHIPINSTRUCT"
    type = "VARCHAR(25)"
  }
  column {
    name = "L_SHIPMODE"
    type = "VARCHAR(10)"
  }
  column {
    name = "L_COMMENT"
    type = "VARCHAR(44)"
  }

  # Optional columns for ETL tracking (as per your pattern)
  column {
    name = "DAG_ID"
    type = "VARCHAR(255)"
  }
  column {
    name = "LOAD_TIME"
    type = "TIMESTAMP_NTZ(9)"
  }
}
