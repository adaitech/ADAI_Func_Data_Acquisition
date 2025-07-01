import logging
import azure.functions as func
from datetime import datetime
# import os
import requests
# import pandas as pd
# import io
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
    logger.info('Python timer trigger function started.')

    logger.info('Variaveis que se alteram por conjunto de dados')
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

    logger.info(f'ACCOUNT_NAME: {ACCOUNT_NAME}')
    logger.info(f'ACCOUNT_KEY: {ACCOUNT_KEY}')
    logger.info(f'BLOB_NAME: {BLOB_NAME}')

    logger.info('Construir a connection string')
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={ACCOUNT_NAME};"
        f"AccountKey={ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )

    logger.info('Cabeçalhos de autenticação')
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
        logger.info(f'Fazendo request para a URL: {url} com params: {params}')
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            logger.error(
                f'Erro ao fazer request: {response.status_code} - {response.text}')
            break

        data = response.json()

        if not data['data']:
            logger.info('Nenhum dado encontrado, encerrando a coleta.')
            break

        todos_size.append(len(data['data']))
        logger.info(
            f'Tamanho dos dados coletados nesta iteração: {len(data["data"])}')

        # Achatar os dados
        flattened_data = [flatten_dict(item) for item in data['data']]

        # Aqui você pode salvar os dados em um arquivo ou banco de dados
        # Exemplo: salvar em CSV (pode ser adaptado para salvar no Azure Blob Storage)
        # df = pd.DataFrame(flattened_data)
        # df.to_csv(f"{name_file}_{data_formatada}.csv", index=False)

        params['offset'] += params['limit']
    logger.info('Se dados foram coletados')

    logger.info('Python timer trigger function executed.')


# if __name__ == '__main__':
#     # For local testing, pass None or a mock TimerRequest
#     Func_Data_Acquisition(None)
