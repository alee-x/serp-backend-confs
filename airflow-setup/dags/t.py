import requests
from airflow.hooks.base_hook import BaseHook
import json

if __name__ == '__main__':
	airflow_oauth2_conn = BaseHook.get_connection('airflow_oauth2')
	nrda_backend_api_conn = BaseHook.get_connection('nrdav2_backend_api')

	token_url = airflow_oauth2_conn.host
	#client (application) credentials on keycloak
	client_id = airflow_oauth2_conn.login
	client_secret = airflow_oauth2_conn.password
	#step A, B - single call with client credentials as the basic auth header - will return access_token
	data = {'grant_type': 'client_credentials'}
	access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False, auth=(client_id, client_secret))
	tokens = json.loads(access_token_response.text)
	print(tokens)
	print("access token: " + tokens['access_token'])