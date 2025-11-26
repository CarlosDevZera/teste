import msal
import requests
import json
import time
from datetime import datetime, timedelta

WEBHOOK_URL_MAKE = "https://hook.us2.make.com/g0js58bk9vvpxgfgp52vka69174iovfj"

CLIENT_ID = "e9179b26-ea99-457f-9f89-31e8d11ec5df"
CLIENT_SECRET = "MDN8Q~i34rawudT_RHEa7zjZUAz~ofwXRJSv1aDe"
DATASET_ID = "64e8911d-bce3-4913-8fe7-322dfc18ebee"
TENANT_ID = "a0457613-3949-4fe4-93de-090bdec2c768"

AUTORIDADE_URL = f"https://login.microsoftonline.com/{TENANT_ID}"
ESCOPO = ["https://analysis.windows.net/powerbi/api/.default"]
API_URL = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"

EXCEL_URL_OUTPUT = "https://docs.google.com/spreadsheets/d/1AexQHGLhNvat3jjSFT0j38i4-2p4wGl4v9rYMSDWtcM/edit?usp=sharing"

NOME_TABELA_BI = 'Tabela 1 (Página1)'
COLUNA_DIA_BI = 'DIA'
COLUNA_PARCEIRO_BI = 'Parceiro'
NUM_DIAS_LOOP = 30

MAPA_PARCEIROS = {
    'KMV | Ipiranga': 'KMV | Ipiranga',
    'Awin': 'Awin',
    'Premmia': 'Premmia',
    'CRM Bonus': 'CRM Bonus',
    'Epay': 'Epay',
    'PicPay': 'PicPay',
    'Easylive': 'Easylive',
    'Others': 'Others'
}

CHAVE_PARCEIRO = 'Tabela 1 (Página1)[Parceiro]'
CHAVE_VALOR = "[TotalValor]"


# =====================================================================
# --- FUNÇÕES (Autenticação, Query, Dias) ---
# =====================================================================

def get_auth_token():
    """Obtém o token de autenticação de cliente a partir do Azure AD."""
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTORIDADE_URL, client_credential=CLIENT_SECRET
    )
    resultado_token = app.acquire_token_for_client(scopes=ESCOPO)
    if "access_token" not in resultado_token:
        raise Exception(f"Falha ao obter o token: {resultado_token.get('error_description')}")
    return resultado_token['access_token']


def execute_dax_query(token, dax_query):
    """Executa uma consulta DAX no dataset do Power BI via API."""
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    data = {"queries": [{"query": dax_query}]}
    response = requests.post(API_URL, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"ERRO API Status {response.status_code}: {response.text}")
        response.raise_for_status()
    dados = response.json()
    if not dados.get('results') or not dados['results'][0].get('tables'):
        return []
    return dados['results'][0]['tables'][0].get('rows', [])


def get_dias_para_atualizar():
    """Retorna a lista de dias a serem processados: Dia 1 até Ontem."""
    hoje = datetime.now()
    ultimo_dia_consolidado = (hoje - timedelta(days=1)).day
    dias_coletados = list(range(1, ultimo_dia_consolidado + 1))
    return [d for d in dias_coletados if 1 <= d <= NUM_DIAS_LOOP]


def send_data_to_make(data):
    """Envia a lista de dados coletados para o Webhook do Make."""
    headers = {'Content-Type': 'application/json'}
    # Faz uma requisição POST para o URL do Make, enviando o JSON
    response = requests.post(WEBHOOK_URL_MAKE, headers=headers, data=json.dumps(data))
    print(response)
    print(data)
    if response.status_code == 200:
        return True
    else:
        # Se a fila estiver cheia, este erro será capturado
        raise Exception(
            f"❌ ERRO: Falha ao enviar dados para o Make. Status: {response.status_code}. Resposta: {response.text}")


def coleta_e_formata_dados():
    """Função principal que coleta dados do BI, formata e os retorna/envia."""
    token = get_auth_token()
    dias_para_atualizar = get_dias_para_atualizar()
    if not dias_para_atualizar:
        return []

    todos_resultados = []
    for dia_corrente in dias_para_atualizar:
        dax_query = f"""
EVALUATE
SUMMARIZECOLUMNS(
    '{NOME_TABELA_BI}'[{COLUNA_PARCEIRO_BI}],
    FILTER(
        '{NOME_TABELA_BI}',
        '{NOME_TABELA_BI}'[{COLUNA_DIA_BI}] = {dia_corrente}
    ),
    "TotalValor", CALCULATE(SUM('{NOME_TABELA_BI}'[Valor]))
)
""".strip()

        rows = execute_dax_query(token, dax_query)
        # Adiciona o dia corrente a cada linha de resultado para o Make mapear a linha correta
        for row in rows:
            parceiro_bi = row.get(CHAVE_PARCEIRO)
            valor = row.get(CHAVE_VALOR)
            # Formata a saída que o Make espera
            todos_resultados.append({
                "dia": dia_corrente,
                "parceiro_bi": parceiro_bi,
                "valor": valor if valor is not None else 0
            })

    return todos_resultados

def main():
    # BATCH_SIZE reduzido para 10 para processamento mais rápido
    BATCH_SIZE = 24
    # DELAY_SECONDS aumentado para 15 segundos para estabilizar a fila
    DELAY_SECONDS = 15

    try:
        # 1. Coletar e formatar os dados (ET)
        print("Iniciando coleta de dados...")
        dados_coletados = coleta_e_formata_dados()

        if dados_coletados:
            total_itens = len(dados_coletados)
            num_lotes = (total_itens + BATCH_SIZE - 1) // BATCH_SIZE
            print(f"Total de {total_itens} itens coletados.")
            print(f"INICIANDO ENVIO em {num_lotes} lotes de {BATCH_SIZE} itens com pausa de {DELAY_SECONDS}s.")

            # --- LÓGICA DE LOOP PARA ENVIAR TODOS OS LOTES COM PAUSA ---
            for i in range(0, total_itens, BATCH_SIZE):
                lote = dados_coletados[i:i + BATCH_SIZE]
                numero_lote_atual = (i // BATCH_SIZE) + 1

                if send_data_to_make(lote):
                    print(f"✅ LOTE {numero_lote_atual}/{num_lotes} de {len(lote)} itens ENVIADO COM SUCESSO.")

                if i + BATCH_SIZE < total_itens:
                    time.sleep(DELAY_SECONDS)

            print(f"✅ SUCESSO! {total_itens} itens foram enviados em {num_lotes} lotes para o Make.")

        else:
            print("INFO: Não há dados ou dias para processar. Rotina encerrada.")

    except Exception as e:
        print(f"\nERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()