import logging
import azure.functions as func
from datetime import datetime
import os
import requests
# import pandas as pd
import io
# from azure.storage.blob import BlobServiceClient


app = func.FunctionApp()


@app.timer_trigger(schedule="0 5 * * * *", arg_name="myTimer", run_on_startup=False,
                   use_monitor=False)
def ADAI_Func_Data_Acquisition(myTimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function started.')

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Configuração do logger')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger(__name__)

    logging.info('Variaveis que se alteram por conjunto de dados')
    url = "https://inradar.com.br/api/v1/member_history"
    params = {"limit": 50, "offset": 0}
    name_file = "member_history"
    data_formatada = datetime.today().strftime('%Y%m%d')

    ACCOUNT_NAME = os.environ.get(ACCOUNT_NAME)
    ACCOUNT_KEY = os.environ.get(ACCOUNT_KEY)
    CONTAINER_NAME = "raw"
    BLOB_NAME = f"{name_file}/{name_file}_{data_formatada}.csv"

    logging.info('ACCOUNT_NAME' + ACCOUNT_NAME)
    logging.info('ACCOUNT_KEY' + ACCOUNT_KEY)
    logging.info('BLOB_NAME' + BLOB_NAME)

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

        logging.info('data' + str(data))

        data = data.get("objects", [])  # Ponto-chave da resposta

        if not data:
            logger.info("Nenhum dado retornado. Fim da coleta.")
            break

        todos_size.extend(data)
        params["offset"] += params["limit"]

    logging.info('Se dados foram coletados')
    if todos_size:

        logging.info('Converte todos os dados em DataFrame')
        df = pd.json_normalize(todos_size)

        print(f"Quantidade de dados: {df.shape[0]}")

        logger.info("Colunas disponíveis:")
        print(df.columns.tolist())

        logging.info('Salvar tudo')
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        logging.info('Conectar ao Azure Blob Storage')
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string)
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME, blob=BLOB_NAME)

        logger.info('Enviar o arquivo para o blob')
        blob_client.upload_blob(buffer, overwrite=True)

        logger.info("Arquivo completo salvo como '{name_file}.csv'")
    else:
        logger.info("Nenhum dado coletado da API.")

    logging.info('Python timer trigger function executed.')
