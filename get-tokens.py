# Given the client ID and tenant ID for an app registered in Azure,
# provide an Azure AD access token and a refresh token.

# If the caller is not already signed in to Azure, the caller's
# web browser will prompt the caller to sign in first.

# pip install msal
from msal import PublicClientApplication
import sys
import requests
import json
from azure.keyvault.secrets import SecretClient
from azure.identity import AzureCliCredential

'''
Get AAD Token
'''
client_id = 'ea2aaefe-efc8-49f7-86a0-ca3298db64b8'
tenant_id = 'cd583730-b42a-4459-9923-c3fdb83d43d1'
scope_name = 'simple-azure-kv-scope'
resourceID = '/subscriptions/dcc586ee-e6f5-49bb-961d-46e52119e293/resourceGroups/ryanTestrg/providers/Microsoft.KeyVault/vaults/test-key-vault-app-reg' 
dns_name = 'https://test-key-vault-app-reg.vault.azure.net/'
url = 'https://adb-1041994189564461.1.azuredatabricks.net/api/2.0/secrets/scopes/create'


scopes = [ '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default' ]


app = PublicClientApplication(
  client_id = client_id,
  authority = "https://login.microsoftonline.com/" + tenant_id
)

acquire_tokens_result = app.acquire_token_interactive(
  scopes = scopes
)

# grab access token
access_token = acquire_tokens_result['access_token']

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

# build post
dataFile = 'create-scope.json'
headers = {"Content-Type": "application/json", 
"Authorization": f'Bearer {secret.value}',
"X-Databricks-Azure-SP-Management-Token": f'{access_token}'}

data = '''{\"scope\": \"%s\", \"scope_backend_type\": \"AZURE_KEYVAULT", \"backend_azure_keyvault\": {\"resource_id\": \"%s\", \"dns_name\": \"%s\"}, \"initial_manage_principal\": \"users\"}'''%(scope_name, resourceID, dns_name)

response = requests.post(url=url, data=data, headers=headers)
print(f'response: {response.content}')

