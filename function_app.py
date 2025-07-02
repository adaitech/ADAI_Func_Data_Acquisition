import logging
import azure.functions as func
from datetime import datetime
import os
import io
import csv
import requests
# from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()


# Configuração do logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Função auxiliar para achatar dicts
def flatten_dict(d, parent_key='', sep='.'):
    """
    Achata um dict aninhado em um dict plano.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


@app.timer_trigger(schedule="0 0 5 * * *", arg_name="myTimer", run_on_startup=False,
                   use_monitor=False)
def Func_Data_Acquisition(myTimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function started.')

    logging.info('Variaveis que se alteram por conjunto de dados')
    url = "https://inradar.com.br/api/v1/member_history"

    params = {
        "limit": 50,
        "offset": 0
    }

    name_file = "member_history"

    data_formatada = datetime.today().strftime('%Y%m%d')

    ACCOUNT_NAME = "adairawdatastore"
    ACCOUNT_KEY = "9rpvbn2UDWIdEZ7CxbuVZwNCjU0PSJ2X1BYqukn2fSULlmRfs3r/nxUYBSqMOocEKoxGlUl+5KxM+AStni+Uvw=="
    CONTAINER_NAME = "raw"
    BLOB_NAME = f"{name_file}/{name_file}_{data_formatada}.csv"

    logging.info(f'ACCOUNT_NAME: {ACCOUNT_NAME}')
    logging.info(f'ACCOUNT_KEY: {ACCOUNT_KEY}')
    logging.info(f'BLOB_NAME: {BLOB_NAME}')

    logging.info('Construir a connection string')
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={ACCOUNT_NAME};"
        f"AccountKey={ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )

    logging.info('Cabeçalhos de autenticação')
    headers = {
        "Authorization": "ApiKey yurifillippo:9d20cd67a03a19a2bbcf7cac0ea11b2409e11bc7",
        "Content-Type": "application/json;charset=UTF-8",
        "Channel": "control_panel",
        "Origin": "https://admin.inchurch.com.br",
        "Referer": "https://admin.inchurch.com.br/",
        "Accept": "application/json, text/plain, */*"
    }

    todos_size = []

    while True:
        logging.info(f"Linhas de dados coletadas: {params['offset']}")
        response = requests.get(url, headers=headers,
                                params=params, verify=False)

        try:
            data = response.json()
        except Exception as e:
            logging.error("Erro ao converter JSON:", e)
            logging.error("Resposta crua:", response.text)
            break

        logging.info('data' + str(data))

        data = data.get("objects", [])  # Ponto-chave da resposta

        if not data:
            logging.info("Nenhum dado retornado. Fim da coleta.")
            break

        todos_size.extend(data)
        params["offset"] += params["limit"]

    logging.info('Se dados foram coletados')
    if todos_size:

        logging.info('Flatten dos dados')
        flattened_data = [flatten_dict(item) for item in todos_size]

        logging.info(
            f"Quantidade de registros coletados: {len(flattened_data)}")

        if flattened_data:
            # Pegar todos os campos (headers) presentes nos dados flatten
            all_keys = set()
            for item in flattened_data:
                all_keys.update(item.keys())

            # Organiza as colunas em ordem alfabética (opcional)
            fieldnames = sorted(list(all_keys))

            logging.info("Colunas disponíveis: %s", fieldnames)

            # Escreve CSV no buffer
            buffer = io.StringIO()
            writer = csv.DictWriter(buffer, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flattened_data)

            csv_data = buffer.getvalue().encode('utf-8')
            bytes_buffer = io.BytesIO(csv_data)

            # Conectar ao Azure Blob Storage
            logging.info('Conectar ao Azure Blob Storage')
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string)
            blob_client = blob_service_client.get_blob_client(
                container=CONTAINER_NAME, blob=BLOB_NAME)

            # Enviar o arquivo para o blob
            blob_client.upload_blob(bytes_buffer, overwrite=True)

            logging.info("Arquivo completo salvo como '%s'", BLOB_NAME)
        else:
            logging.info("Não há dados para salvar.")
    else:
        logging.info("Nenhum dado coletado da API.")

    logging.info('Python timer trigger function executed.')


# if __name__ == '__main__':
#     # For local testing, you can call the function directly
#     # Func_Data_Acquisition(func.TimerRequest())
