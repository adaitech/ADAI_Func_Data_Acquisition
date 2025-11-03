import requests
import pandas as pd
import logging
import io
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import os

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#Variaveis que se alteram por conjunto de dados
###################
url = "https://inradar.com.br/api/v1/visitors"

params = {
    "limit": 100,
    "offset": 0
}

name_file = "visitantes_gc"


###################

data_formatada = datetime.today().strftime('%Y%m%d')

ACCOUNT_NAME = os.getenv("ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
CONTAINER_NAME = "raw"
BLOB_NAME = f"{name_file}/{name_file}_{data_formatada}.csv"

# Construir a connection string
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={ACCOUNT_NAME};"
    f"AccountKey={ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

API_KEY = os.getenv("API_KEY")

# Cabeçalhos de autenticação
headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json;charset=UTF-8",
    "Channel": "control_panel",
    "Origin": "https://admin.inchurch.com.br",
    "Referer": "https://admin.inchurch.com.br/",
    "Accept": "application/json, text/plain, /"
}

todos_size = []

while True:
    logger.info(f"Linhas de dados coletadas: {params['offset']}")
    response = requests.get(url, headers=headers, params=params)

    try:
        data = response.json()
    except Exception as e:
        logger.error("Erro ao converter JSON:", e)
        logger.error("Resposta crua:", response.text)
        break

    data = data.get("objects", [])  # Ponto-chave da resposta

    if not data:
        logger.info("Nenhum dado retornado. Fim da coleta.")
        break

    todos_size.extend(data)
    params["offset"] += params["limit"]

# Se dados foram coletados
if todos_size:
    # Converte todos os dados em DataFrame
    df = pd.json_normalize(todos_size)

    print(f"Quantidade de dados: {df.shape[0]}")

    logger.info("Colunas disponíveis:")
    print(df.columns.tolist())

    # Salvar tudo em StringIO
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    # Conectar ao Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

    # Enviar o arquivo como bytes
    blob_client.upload_blob(buffer.getvalue().encode('utf-8'), overwrite=True)

    logger.info(f"Arquivo completo salvo como '{name_file}.csv'")
else:
    logger.info("Nenhum dado coletado da API.")