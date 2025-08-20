import os
import io
import re
import csv
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv

s3_client = boto3.client('s3')  

load_dotenv()

SENTRY_ORGANIZATION_ID = os.getenv('SENTRY_ORGANIZATION_ID')
SENTRY_PROJECT_SLUG = os.getenv('SENTRY_PROJECT_SLUG')
SENTRY_AUTH_TOKEN = os.getenv('SENTRY_AUTH_TOKEN')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')


def clean_quoted_strings(data):
    """
    Remove as aspas simples extras de strings dentro de um dicionário ou lista,
    assumindo que as aspas extras são parte do valor da string.
    Ex: "'valor'" se torna "valor".
    """
    if isinstance(data, dict):
        # Cria um novo dicionário para evitar problemas de modificação durante a iteração
        return {k: clean_quoted_strings(v) for k, v in data.items()}
    elif isinstance(data, list):
        # Mapeia a função para cada item da lista
        return [clean_quoted_strings(elem) for elem in data]
    elif isinstance(data, str):
        # Verifica se a string começa e termina com aspas simples
        if len(data) >= 2 and data.startswith("'") and data.endswith("'"):
            # Remove o primeiro e o último caractere (as aspas)
            return data[1:-1]
        return data
    else:
        # Retorna outros tipos de dados como estão (int, float, bool, None, etc.)
        return data


def get_collect_info(entries):
    for entry in entries:
        threads_data = entry.get('data', {})
        threads_list = threads_data.get('values', [])
        for thread in threads_list:
            stacktrace = thread.get('stacktrace', {}) or {}
            frames = stacktrace.get('frames', [])

            for frame in frames:
                frame_vars = frame.get('vars', {})
                if 'body' in frame_vars:
                    return clean_quoted_strings(frame_vars['body'])
    return {}


def get_all_events():
    url = f'https://sentry.io/api/0/projects/{SENTRY_ORGANIZATION_ID}/{SENTRY_PROJECT_SLUG}/events/?full=true'

    headers = {
        'Authorization': f'Bearer {SENTRY_AUTH_TOKEN}',
        'Accept': 'application/json',
    }

    events = []
    while url and len(events) < 1000:
        try:
            req = urllib.request.Request(url, headers=headers, method='GET')
            with urllib.request.urlopen(req) as response:
                response_data = response.read().decode('utf-8')
                for event in json.loads(response_data):
                    collect_info = get_collect_info(event['entries'])
                    
                    try:
                        created_at = datetime.strptime(event['dateCreated'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        created_at = datetime.strptime(event['dateCreated'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')

                    events.append({
                        'issue_id': event['groupID'],
                        'event_id': event['eventID'],
                        'project_id': event['projectID'],
                        'event_type': event['type'],
                        'title': event['title'],
                        'message': event['message'],
                        'platform': event['platform'],
                        'culprit': event['culprit'],
                        'created_at': created_at,
                        'collect_id': collect_info.get('id'),
                        'kind_of_material': collect_info.get('material'),
                        'type_of_packaging': collect_info.get('packaging'),
                        'hauler_cnpj': re.sub(r'[^0-9]', '', collect_info['hauler']['document']) if 'hauler' in collect_info and collect_info['hauler'].get('document') else None,
                        'receiver_cnpj': re.sub(r'[^0-9]', '', collect_info['receiver']['document']) if 'receiver' in collect_info and collect_info['receiver'].get('document') else None,
                        'sentry_url': f"https://musa-tecnologia.sentry.io/issues/{event['groupID']}/events/{event['eventID']}/?project={event['projectID']}",
                    })

                link_header = response.getheader('Link')
                url = None

                if link_header:
                    for link_part in link_header.split(', '):
                        match = re.search(r'<(.*?)>; rel="next"', link_part)
                        if match:
                            url = match.group(1) if 'results="true"' in link_header else None

        except urllib.error.HTTPError as e:
            print(f"Erro HTTP: {e.code} - {e.reason}")
            print(e.read().decode('utf-8')) # Mostra o corpo do erro para depuração
        except urllib.error.URLError as e:
            print(f"Erro de URL: {e.reason}")
        except Exception as e:
            print(f"Um erro inesperado ocorreu: {e}")

    return events


def transform_data_to_csv(data):
    csv_headers = [
        'issue_id', 'event_id', 'project_id', 'event_type', 'title', 'message',
        'platform', 'culprit', 'created_at', 'collect_id', 'kind_of_material',
        'type_of_packaging', 'hauler_cnpj', 'receiver_cnpj', 'sentry_url',
    ]

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=csv_headers, extrasaction='ignore', delimiter=';')

    writer.writeheader()
    writer.writerows(data)

    csv_output = csv_buffer.getvalue()
    csv_buffer.close()

    return csv_output


def lambda_handler(event, context):
    events = get_all_events()

    if len(events):
        csv_output = transform_data_to_csv(events)

        datetime_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        filename = f'{SENTRY_PROJECT_SLUG}_backup/events_{datetime_str}.csv'
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=filename, Body=csv_output)

        filename = f'{SENTRY_PROJECT_SLUG}/events.csv'
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=filename, Body=csv_output)

    return {
        'statusCode': 200,
        'body': 'Sucesso',
    }


if __name__ == '__main__':
    lambda_handler({}, {})