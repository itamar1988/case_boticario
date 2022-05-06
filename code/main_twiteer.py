import requests
import os
import json
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from datetime import datetime,timedelta,timezone

# Criacao das variaveis
project_id = 'noted-hangout-346322'
table_destination = 'raw.twitter_gb'
path_auth = 'auth.json'
creds_bq = service_account.Credentials.from_service_account_file(path_auth)

#Funcao de autenticacao na API, chamda do BEARER_TOKEN
def auth():
    return os.environ.get("BEARER_TOKEN")

#Funcao de criacao da URL para utilizacao do GET da API
def create_url():
    query = "Boticário Maquiagem lang:pt"
    tweet_fields = "tweet.fields=text"
    user_fields = "expansions=author_id&user.fields=name"
    now =datetime.now()
    start_time = (now-timedelta(days=4,hours=26,minutes=59)).strftime("%Y-%m-%-dT%H:%M:%SZ")
    end_time = (now-timedelta(hours=3)).strftime("%Y-%m-%-dT%H:%M:%SZ")           
    filter = "start_time={}&end_time={}".format(start_time,end_time)
    max_results = "max_results=50"
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(
        query, user_fields,tweet_fields,filter,max_results
    )
    return url

# Funcao valida autorizacao na API
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

#Funcao que valida endpoint da solicitacao
def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# Funcao de paginacao dos resultados da solicicacao GET na API
def paginate(url,headers,next_token=""):
  if next_token:
    full_url=f"{url}&next_token={next_token}"
  else:
    full_url = url
  data = connect_to_endpoint(full_url, headers)
  yield data
  if "next_token" in data.get("meta",{}):
    yield from paginate(url, headers, data['meta']['next_token'])

# Funcao resonsavel por percorrer json original da responsta e retornar um novo json com apenas os campos solicitados no case
def search_name(json_response,author_id):
  for i in json_response['includes']['users']:
    if i ['id'] == author_id:
      return i ['name']

# Funcao responsavel por gerar um novo json com apenas os campos solicitados no case, preparando-os para carga no BQ
def generate_json(json_list,json_response):
  temp_json = {}
  for i in json_response['data']:
    temp_json = i
    temp_json['name'] = search_name(json_response,temp_json['author_id'])
    json_list.append(temp_json)
  return json_list

""" 
  Funcao main, valida Auth, envia solicitacao GET para a URL valida
  percorre todas as paginas de retorno e carrega o resultado em uma tabela no BigQuery
"""
def func_twitter(event, context):
  bearer_token = "AAAAAAAAAAAAAAAAAAAAAAMccQEAAAAAnxWQDOmOSUKn%2FarrGIOZochBnrc%3DFsfNZZ6LZ9cbG45DpzdO9N40VLidz6pMVKIa4wd5FsIWbXccbU"
  url = create_url()
  headers = create_headers(bearer_token)
  json_list = []
  for json_response in paginate(url,headers):
    json_list = generate_json(json_list,json_response)
    df_tweet = pd.DataFrame.from_records(json_list)
    pandas_gbq.to_gbq(df_tweet, table_destination, project_id = project_id, if_exists = 'append',credentials=creds_bq)