import hvac

def get_snowflake_credentials_from_vault():
    
    client = hvac.Client(url='http://vault:8200', token='root')
    secret = client.secrets.kv.v2.read_secret_version(path='snowflake')
    return secret['data']['data']