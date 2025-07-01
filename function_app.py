import logging
import azure.functions as func
from datetime import datetime
# import os
# import requests
# import pandas as pd
# import io
# from azure.storage.blob import BlobServiceClient


app = func.FunctionApp()


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

    logging.info(
        'Acessar os dados do Blob Storage e carregar no banco de dados relacional.')

    logging.info('Formatar os dados com as regras de negócios.')

    logging.info('Criar todas as tabelas para leituras dos dados.')

    logging.info('Executar o script de carga.')

    logging.info('Executar a confereência da carga.')

    logging.info('Python timer trigger function executed.')


# if __name__ == '__main__':
#     # For local testing, pass None or a mock TimerRequest
#     Func_Data_Acquisition(None)
