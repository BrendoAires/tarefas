import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import requests
import time
from datetime import datetime, timezone, time as dt_time
import logging
import streamlit as st

from dotenv import load_dotenv
import os

load_dotenv()




# Configurações Gerais
CONFIG = {
    "authentication": os.getenv("CLICKUP_API_KEY"),
    "team_id": 9013069666,
    "created_date": None,
    "due_date": None,
    "time_zone": "America/Sao_Paulo",
}

GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")

HEADERS = {
    "Authorization": CONFIG["authentication"],
    "Content-Type": "application/json"
}

# Configurações de Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Funções Auxiliares




# Função para converter data e hora em timestamp Unix (milissegundos)
def to_unix_milliseconds(date_string):
    """
    Converte uma string de data no formato "%d/%m/%Y %H:%M:%S" para milissegundos Unix (timestamp).

    Args:
        date_string (str): Data no formato "%d/%m/%Y %H:%M:%S".

    Returns:
        int: Timestamp Unix em milissegundos.
    """
    # Converta a string para um objeto datetime
    dt = datetime.strptime(date_string, "%d/%m/%Y %H:%M:%S")

    # Obtenha o timestamp em segundos e converta para milissegundos
    timestamp_ms = int(dt.timestamp() * 1000)

    return timestamp_ms



# Função para converter data e hora em timestamp Unix (milissegundos)
def to_unix_milliseconds(date_string):
    """
    Converte uma string de data no formato "%d/%m/%Y %H:%M:%S" para milissegundos Unix (timestamp).

    Args:
        date_string (str): Data no formato "%d/%m/%Y %H:%M:%S".

    Returns:
        int: Timestamp Unix em milissegundos.
    """
    # Converta a string para um objeto datetime
    dt = datetime.strptime(date_string, "%d/%m/%Y %H:%M:%S")

    # Obtenha o timestamp em segundos e converta para milissegundos
    timestamp_ms = int(dt.timestamp() * 1000)

    return timestamp_ms


def make_request_with_backoff(url, headers, params=None, retries=5, initial_backoff=2, multiplier=2):
    """
    Faz requisições com backoff exponencial.
    """
    backoff_time = initial_backoff
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Rate limit
                logging.warning(f"Rate limit atingido. Tentando novamente em {backoff_time} segundos")
                time.sleep(backoff_time)
                backoff_time *= multiplier
            else:
                response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro na requisição: {e}")
    raise Exception("Número máximo de tentativas atingido.")

def convert_timestamp(timestamp):
    """
    Converte um timestamp em milissegundos para uma data no formato dd/mm/yyyy HH:MM:SS.
    """
    if not timestamp:
        return None
    return datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc).strftime('%d/%m/%Y %H:%M:%S')



# Funções ClickUp
def get_task_name(task_id):
    """
    Busca o nome da tarefa usando o endpoint /task/{task_id}.
    """
    url = f"https://api.clickup.com/api/v2/task/{task_id}"
    try:
        response = make_request_with_backoff(url, HEADERS)
        return response.get('name', 'N/A')
    except Exception as e:
        logging.error(f"Erro ao buscar nome da tarefa {task_id}: {e}")
        return 'N/A'





def get_spaces(team_id, archived):
    url = f"https://api.clickup.com/api/v2/team/{team_id}/space"
    response = make_request_with_backoff(url, HEADERS, {"archived": str(archived).lower()})
    return [space['id'] for space in response.get('spaces', [])]

def get_folders(space_ids, archived):
    folders = []
    for space_id in space_ids:
        url = f"https://api.clickup.com/api/v2/space/{space_id}/folder"
        response = make_request_with_backoff(url, HEADERS, {"archived": str(archived).lower()})
        folders.extend(folder['id'] for folder in response.get('folders', []))
    return folders

def get_lists(folder_ids, archived):
    lists = []
    for folder_id in folder_ids:
        url = f"https://api.clickup.com/api/v2/folder/{folder_id}/list"
        response = make_request_with_backoff(url, HEADERS, {"archived": str(archived).lower()})
        lists.extend(lst['id'] for lst in response.get('lists', []))
    return lists




def get_tasks(list_ids, created_date, due_date, archived):
    tasks = []
    query = {
        "archived": str(archived).lower(),
        "subtasks": "true",
        "include_closed": "true",
        "include_stats": "true",
        "date_created_gt": created_date if created_date else "",
        "due_date_lt": due_date if due_date else "",
    }
    for list_id in list_ids:
        url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
        response = make_request_with_backoff(url, HEADERS, query)
        tasks.extend(task['id'] for task in response.get('tasks', []))
    return tasks

def get_task_details(task_ids):
    """
    Busca os detalhes das tarefas com todos os campos necessários.
    """
    task_data = []
    i = 0
    for task_id in task_ids:
        i += 1
        url = f"https://api.clickup.com/api/v2/task/{task_id}"
        try:
            response = make_request_with_backoff(url, HEADERS)
            # Adicionar os campos detalhados
            task_data.extend(
                {
                    'task_id': response.get('id'),
                    'task_name': response.get('name'),
                    'task_status': response.get('status', {}).get('status'),
                    'task_status_type': response.get('status', {}).get('type'),
                    'task_date_created': convert_timestamp(response.get('date_created')),
                    'task_date_updated': convert_timestamp(response.get('date_updated')),
                    'task_date_closed': convert_timestamp(response.get('date_closed')),
                    'task_date_done': convert_timestamp(response.get('date_done')),
                    'archived': response.get('archived', False),
                    'assignee_id': assignee.get('id', None),
                    'assignee_username': assignee.get('username', None),
                    'parent': response.get('parent'),
                    'due_date': convert_timestamp(response.get('due_date')),
                    'start_date': convert_timestamp(response.get('start_date')),

                    #'priority': response.get('priority', {}).get('priority', ""),  # Campo priority
                    'tags': [tag.get('name', "") for tag in response.get('tags', [])],  # Campo tags
                    #'points': response.get('points', ""),  # Campo points

                    'time_estimate': response.get('time_estimate'),
                    'time_spent': response.get('time_spent'),
                    'custom_fields_Eficiência': next(
                        (cf.get('value') for cf in response.get('custom_fields', []) if cf.get('name') == 'Eficiência'
                        ), None),
                    'custom_fields_Tipo de Tarefa': next(
                        (
                            cf.get('value') for cf in response.get('custom_fields', [])
                            if cf.get('id') == 'be41cb2a-63fd-4607-abb1-680685d0a581'
                        ),
                        None
                    ),
                    'custom_fields_Data realizada': convert_timestamp(next(
                        (cf.get('value') for cf in response.get('custom_fields', []) if cf.get('name') == 'Data realizada'
                        ), None)),


                    'custom_fields_Progresso': next(
                        (cf.get('value', {}).get('current') for cf in response.get('custom_fields', []) if
                         cf.get('name') == 'Progresso'
                         ), None),
                    'custom_fields_Reuniao': next(
                        (
                            cf.get('value') for cf in response.get('custom_fields', [])
                            if cf.get('id') == '6ddf4f21-a59d-4ac1-bcba-f7d9769da471'
                        ),
                        None
                    ),
                    'custom_fields_Relação com o RC': next(
                        (cf.get('value') for cf in response.get('custom_fields', []) if cf.get('name') == 'Relação com o RC'
                        ), None),
                    'creator': response.get('creator', {}).get('username', None),
                    'creator_id': response.get('creator', {}).get('id', None),
                    'url': response.get('url'),
                    'id_list': response.get('list', {}).get('id', None),
                    'list': response.get('list', {}).get('name', None),
                    #'tag': response.get('list', {}).get('tag', None),
                    'id_folder': response.get('folder', {}).get('id', None),
                    'folder': response.get('folder', {}).get('name', None),
                    'id_project': response.get('project', {}).get('id', None),
                    'project': response.get('project', {}).get('name', None),
                    'space': response.get('space', {}).get('id', None),
                    'watchers': [watcher.get('username') for watcher in response.get('watchers', [])],
                }
                for assignee in response.get('assignees', [])

            )
        except Exception as e:
            logging.error(f"Erro ao buscar detalhes da tarefa {task_id}: {e}")

    st.write(f"Tarefas encontradas: {i}")
    return pd.DataFrame(task_data)

def get_task_time_entries(task_ids):
    all_data = []
    i = 0
    for task_id in task_ids:
        i += 1
        print(f'Processando detalhes da tarefa: {task_id}, {i}')
        url = f"https://api.clickup.com/api/v2/task/{task_id}/time"
        try:
            response_data = make_request_with_backoff(url, HEADERS)
            if 'data' in response_data and response_data['data']:
                task_name = get_task_name(task_id)  # Busca o nome da tarefa
                task_data = [
                    {
                        'task_id': task_id,
                        'task_name': task_name,
                        'user_id': user['user']['id'],
                        'nome': user['user']['username'],
                        'interval_id': interval['id'],
                        'start': convert_timestamp(interval['start']),
                        'end': convert_timestamp(interval['end']),
                        'time_spent': int(interval['time']),
                    }
                    for user in response_data['data']
                    for interval in user.get('intervals', [])
                ]
                all_data.extend(task_data)
                logging.debug(f'{task_id}')
            else:
                logging.warning(f"Nenhum dado de tempo encontrado para task_id {task_id}.")
        except Exception as e:
            logging.error(f"Erro ao buscar dados de tempo para task_id {task_id}: {e}")
    return pd.DataFrame(all_data)

# Funções Google Sheets
#def authenticate_google_sheets():
 #   credentials = Credentials.from_service_account_file(
  #      GOOGLE_CREDENTIALS_PATH,
   #     scopes=[
    #        'https://www.googleapis.com/auth/spreadsheets',
     #       'https://www.googleapis.com/auth/drive'
      #  ]
    #)
    #return gspread.authorize(credentials)

def authenticate_google_sheets():
    # Obter o conteúdo das credenciais do secrets
    credentials_info = st.secrets["google_credentials"]

    # Converter para o formato JSON
    credentials = Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]

    )
    gc = gspread.authorize(credentials)
    return gc

#Função para subscrever os dados na planilha


def xport_to_google_sheets(dataframe, spreadsheet_id, worksheet_index=0):
    gc = authenticate_google_sheets()
    spreadsheet = gc.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.get_worksheet(worksheet_index)

    dataframe = dataframe.fillna("")

    #data_to_export = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
    data_to_export = [dataframe.columns.tolist()] + dataframe.astype(str).values.tolist()
    # Garantir que todos os valores são strings e remover caracteres especiais
    data_to_export = [[str(value) if pd.notna(value) else "" for value in row] for row in data_to_export]


    worksheet.clear()
    worksheet.update(range_name='A1', values=data_to_export)
    print(f"Dados exportados com sucesso para a planilha: {spreadsheet_id}")


def export_to_google_sheets(dataframe, spreadsheet_id, worksheet_name):
    """
    Exporta os dados para o Google Sheets, criando ou atualizando uma aba específica.

    Args:
        dataframe (pd.DataFrame): Dados a serem exportados.
        spreadsheet_id (str): ID da planilha no Google Sheets.
        worksheet_name (str): Nome da aba para criar ou atualizar.
    """
    gc = authenticate_google_sheets()
    spreadsheet = gc.open_by_key(spreadsheet_id)

    # Verificar se a aba já existe
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()  # Limpa a aba existente
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="26")

    # Preparar os dados para exportação
    dataframe = dataframe.fillna("")
    data_to_export = [dataframe.columns.tolist()] + dataframe.astype(str).values.tolist()

    # Atualizar os dados na aba
    worksheet.update(range_name='A1', values=data_to_export)
    st.write(f"Dados exportados com sucesso para a aba: {worksheet_name} na planilha: {spreadsheet_id}")


def processar_form(CONFIG):
    # Formulário em Streamlit
    with st.form(key="date_form"):
        col1, col2 = st.columns([4,4])
        with col1:
            data_inicio = st.date_input(
                "Data Inicial",
                value=datetime.today().date(),  # Data padrão
                min_value=datetime(2024, 1, 1).date(),  # Data mínima
                max_value=datetime(2025, 12, 31).date()  # Data máxima
            )
        with col2:
            hora_inicio = st.time_input("Hora Inicial", value=dt_time(0, 0))  # Para selecionar a hora

        col1, col2 = st.columns([4, 4])
        with col1:
            data_vencimento = st.date_input(
                "Data de Vencimento",
                value=datetime.today().date(),  # Data padrão
                min_value=datetime(2024, 1, 1).date(),  # Data mínima
                max_value=datetime(2025, 12, 31).date()  # Data máxima
            )
        with col2:
            hora_vencimento = st.time_input("Hora de Vencimento", value=dt_time(23, 59))  # Para selecionar a hora
        archived_option = st.radio(
            "Arquivado?:",
            (["Sim", "Não"])
        )
        archived = {"Sim": "true", "Não": "false"}[archived_option]
        # Botão para submeter o formulário
        submit_button = st.form_submit_button(label="Enviar")

        if submit_button:
            data_hora_inicio = datetime.combine(data_inicio, hora_inicio)
            created_date = int(data_hora_inicio.timestamp() * 1000)

            data_hora_vencimento = datetime.combine(data_vencimento, hora_vencimento)
            due_date = int(data_hora_vencimento.timestamp() * 1000)

            CONFIG["created_date"] = created_date
            CONFIG["due_date"] = due_date
            CONFIG["archived"] = archived

            return True, {"created_date": created_date, "due_date": due_date, "archived": archived}
    return False, {}




def main():
    st.title("Integração ClickUp com Google Sheets")
    logging.info("Iniciando aplicação...")

    st.write("Preencha os campos abaixo para configurar o intervalo de datas:")
    form_submitted, form_data = processar_form(CONFIG)



    if form_submitted:
        # Extrair o mês e ano do formulário
        data_inicio_dt = datetime.fromtimestamp(CONFIG["created_date"] / 1000)
        data_vencimento_dt = datetime.fromtimestamp(CONFIG["due_date"] / 1000)

        mes_ano_inicio = data_inicio_dt.strftime("%m-%Y")
        mes_ano_vencimento = data_vencimento_dt.strftime("%m-%Y")

        # Obter dados do ClickUp
        logging.info("Obtendo espaços...")
        spaces = get_spaces(CONFIG["team_id"], CONFIG["archived"])
        folders = get_folders(spaces, CONFIG["archived"])
        lists = get_lists(folders, CONFIG["archived"])

        logging.info("Obtendo pacotes de tarefas...")
        tasks = get_tasks(lists, CONFIG["created_date"], CONFIG["due_date"], CONFIG["archived"])
        task_details_df = get_task_details(tasks)
        time_entries_df = get_task_time_entries(tasks)

        # IDs das planilhas
        task_spreadsheet_id = "1cBmj0EoxdmQbhdb3LWj1cmso0-pUEX9h6x3nk4fGZC4"
        time_spreadsheet_id = "1eZLzEq0KB24GzTxqjFTZMzN4Z7SZSdhgUrU0fdA5Xl4"

        # Exportar dados
        if not task_details_df.empty:
            export_to_google_sheets(task_details_df, task_spreadsheet_id, mes_ano_inicio)

        if not time_entries_df.empty:
            export_to_google_sheets(time_entries_df, time_spreadsheet_id, mes_ano_inicio)

        st.success("Processo concluído com sucesso!")


    else:
        st.info("Preencha o formulário para iniciar o processo.")





if __name__ == "__main__":
    main()
    
    
