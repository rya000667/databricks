'''
Get Secret from AKV via python
'''
from azure.keyvault.secrets import SecretClient
from azure.identity import AzureCliCredential

'''
Variables
secretName = databricks-prod-tokengi
'''
kvName = 'test-key-vault-app-reg'
secretName = 'test-pat-token'


'''
Get Secret from key vault
'''
KVUri = f"https://{kvName}.vault.azure.net"

# Using AzureCliCredential assumes that az login has already been done and principal is logged into azure
credential = AzureCliCredential()
client = SecretClient(vault_url=KVUri, credential=credential)

# get secret
secret = client.get_secret(secretName)

print('secret has been retrieved')


