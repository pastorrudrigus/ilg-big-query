import requests
import pandas as pd
import re
import csv
from io import BytesIO
from google.cloud import bigquery

def run_etl():
    # 1. Obter o mapeamento dos campos
    fields_url = "https://ilgcomex.bitrix24.com.br/rest/96/alvz97lfjlgbne97/crm.deal.fields"
    response_fields = requests.get(fields_url)
    if response_fields.status_code == 200:
        fields_json = response_fields.json()
        fields_result = fields_json.get("result", {})
        field_mapping = {field: info.get("listLabel", field) for field, info in fields_result.items()}
        print("Mapeamento de campos obtido:")
        for k, v in field_mapping.items():
            print(f"{k} -> {v}")
    else:
        print("Erro ao obter os campos. Status:", response_fields.status_code)
        field_mapping = {}

    # 2. Obter os dados dos negócios (deals) via POST (com paginação)
    deal_list = []
    count = 0
    while True:
        list_url = f"https://ilgcomex.bitrix24.com.br/rest/96/alvz97lfjlgbne97/crm.deal.list?start={count}"
        data_payload = {"SELECT": ["*", "UF_*"]}
        response = requests.post(list_url, json=data_payload)
        print("Response da listagem:", response)
        if response.status_code != 200:
            print("Erro ao obter os dados. Status:", response.status_code)
            break
        response_json = response.json()
        deals = response_json.get("result", [])
        deal_list.extend(deals)
        next_start = response_json.get("next")
        if next_start is None:
            break
        else:
            count = next_start
    print(f"Total de registros obtidos: {len(deal_list)}")

    # 3. Converter para DataFrame e ajustar colunas
    df = pd.DataFrame(deal_list)
    print("Colunas originais do DataFrame:")
    print(df.columns.tolist())
    df.rename(columns=field_mapping, inplace=True)
    print("Colunas renomeadas do DataFrame:")
    print(df.columns.tolist())

    status_url = "https://ilgcomex.bitrix24.com.br/rest/96/alvz97lfjlgbne97/crm.status.list"
    status_fields = requests.get(status_url)
    dic = {}
    for i in status_fields.json().get('result'):
        dic[i['STATUS_ID']] = i['NAME']
    df['STAGE_ID'] = df['STAGE_ID'].map(dic)

    def make_columns_unique(df):
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            dup_idx = cols[cols == dup].index.tolist()
            for i, idx in enumerate(dup_idx):
                if i > 0:
                    cols[idx] = f"{cols[idx]}_{i}"
        df.columns = cols
        return df

    def fix_column_values(x):
        if isinstance(x, list):
            return ', '.join(map(str, x))
        return x

    def sanitize_column_names(df):
        df.columns = [re.sub(r'[^0-9A-Za-z_]', '_', col) for col in df.columns]
        return df

    df = make_columns_unique(df)
    for col in df.columns:
        df[col] = df[col].apply(fix_column_values)
    df = sanitize_column_names(df)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)
    df = df.reset_index(drop=True)

    def remove_newlines(x):
        if isinstance(x, str):
            return re.sub(r'[\r\n]+', ' ', x)
        return x
    for col in df.columns:
        df[col] = df[col].apply(remove_newlines)

    # 4. Carregar os dados no BigQuery
    SERVICE_ACCOUNT_JSON = 'tickets-carol-e53dd744385e.json'  # Certifique-se de incluir este arquivo no pacote da function
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
    project_id = client.project
    dataset_id = 'ilg'
    table_id = 'deals'
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8', sep=';', quoting=csv.QUOTE_ALL)
    csv_buffer.seek(0)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=';',
        max_bad_records=50,
        autodetect=True
    )
    job = client.load_table_from_file(csv_buffer, table_ref, job_config=job_config)
    job.result()
    print("Dados importados com sucesso para o BigQuery!")

# Handler da function
def handler(event, context):
    try:
        run_etl()
        return "ETL executado com sucesso!"
    except Exception as e:
        print("Erro na execução do ETL:", e)
        return f"Erro: {e}"
