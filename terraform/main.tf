terraform {
  required_providers {
    vault = {
      source = "hashicorp/vault"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
    }
  }
}

# Vault provider
provider "vault" {
  address = "http://localhost:8200"
  token   = "root"
}

# Read secrets from Vault KV
data "vault_kv_secret_v2" "snowflake" {
  mount = "secret"
  name  = "snowflake"
}

# Extract values + split account identifier
locals {
  snowflake_user       = data.vault_kv_secret_v2.snowflake.data["user"]
  snowflake_password   = data.vault_kv_secret_v2.snowflake.data["password"]
  snowflake_account_id = data.vault_kv_secret_v2.snowflake.data["account"]   # e.g. "xy12345.ap-south-1"
  snowflake_role       = data.vault_kv_secret_v2.snowflake.data["role"]
  snowflake_warehouse  = data.vault_kv_secret_v2.snowflake.data["warehouse"]
  snowflake_account_name = data.vault_kv_secret_v2.snowflake.data["account_name"]
  snowflake_organization_name = data.vault_kv_secret_v2.snowflake.data["organization"]
}

# ►► Correct Snowflake provider configuration (matches your example)
provider "snowflake" {
  organization_name = local.snowflake_organization_name
  account_name      = local.snowflake_account_name
  user              = local.snowflake_user
  password          = local.snowflake_password
  role              = local.snowflake_role
  warehouse         = local.snowflake_warehouse

  # Enable preview features (REQUIRED for snowflake_table)
  preview_features_enabled = ["snowflake_table_resource"]
}
