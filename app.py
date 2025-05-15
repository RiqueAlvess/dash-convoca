import dash
from dash import dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import os
import requests
import json
import time
import gc
import warnings
from flask_caching import Cache
import psutil
import sys

# Suprimir avisos para logs mais limpos
warnings.filterwarnings('ignore')

# Configuração para uso mínimo de memória
pd.options.mode.chained_assignment = None

# Monitoramento de memória
def get_memory_usage():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    memory_mb = memory_info.rss / 1024 / 1024
    return f"Memória: {memory_mb:.1f} MB"

# Funções para gerenciar memória ativamente
def clear_memory():
    # Forçar coleta de lixo
    gc.collect()
    
    # Limpar cache do pandas (experimental)
    for name in list(sys.modules.keys()):
        if 'pandas' in name:
            try:
                sys.modules[name]._clear_cache()
            except:
                pass

# Inicialização mínima do app
app = dash.Dash(__name__, 
               external_stylesheets=[dbc.themes.BOOTSTRAP],
               assets_folder=None,  # Evitar carregamento de assets
               compress=True,  # Compressão para reduzir tráfego
               meta_tags=[
                   {"name": "viewport", "content": "width=device-width, initial-scale=1, maximum-scale=1"}
               ])

server = app.server

# Configuração de cache eficiente
cache_config = {
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': os.environ.get('CACHE_DIR', './minimal_cache'),
    'CACHE_THRESHOLD': 100,  # Limitar número de itens
    'CACHE_DEFAULT_TIMEOUT': 86400  # 24h
}

# Criar diretório de cache
if not os.path.exists(cache_config['CACHE_DIR']):
    os.makedirs(cache_config['CACHE_DIR'])

cache = Cache()
cache.init_app(server, config=cache_config)

# Arquivo para armazenar resultados agregados (muito menor que dados brutos)
SUMMARY_FILE = os.path.join(cache_config['CACHE_DIR'], 'data_summary.json')
LAST_UPDATE_FILE = os.path.join(cache_config['CACHE_DIR'], 'last_update.txt')

# Credenciais - prioritize variáveis de ambiente
API_CONFIG = {
    'URL_SOC': os.environ.get('URL_SOC', 'https://ws1.soc.com.br/WebSoc/exportadados?parametro='),
    'URL_CONNECT': os.environ.get('URL_CONNECT', 'https://www.grsconnect.com.br/'),
    'USERNAME': os.environ.get('API_USERNAME', '1'),
    'PASSWORD': os.environ.get('API_PASSWORD', 'ConnectBI@20482022'),
    'EMPRESA': os.environ.get('EMPRESA', '423'),
    'CODIGO': os.environ.get('CODIGO', '151346'),
    'CHAVE': os.environ.get('CHAVE', 'b5aa04943cd28ff155ed')
}

# Configuração de tipos otimizados para memória
OPTIMIZED_DTYPES = {
    'CODIGOEMPRESA': 'category',
    'NOMEABREVIADO': 'category', 
    'EXAME': 'category',
    'Status': 'category',
    'MesAnoVencimento': 'category',
    'DiasParaVencer': 'int16'  # -32768 a 32767 é suficiente para dias
}

# Função para extrair dados da API, processando em chunks para economia de memória
def extract_data_from_api(max_empresas=None):
    """Extrai dados direto da API, processando em chunks para economia de memória"""
    print(f"[{datetime.now()}] Iniciando extração de dados")
    
    try:
        # Obter token (reutilizável)
        token_response = requests.get(
            url=API_CONFIG['URL_CONNECT'] + 'get_token',
            params={
                'username': API_CONFIG['USERNAME'],
                'password': API_CONFIG['PASSWORD']
            },
            timeout=30
        )
        token = token_response.json()['token']
        
        # Inicialização para armazenar resumos
        summary_data = {
            'status_counts': {},
            'empresas': [],
            'exames': [],
            'meses_vencimento': {},
            'incompany_eligibility': {},
            'total_records': 0
        }
        
        # Obter empresas ativas
        print(f"[{datetime.now()}] Obtendo lista de empresas ativas")
        convoca_codigo = pd.json_normalize(requests.get(
            url=API_CONFIG['URL_CONNECT'] + 'get_ped_proc',
            params={"token": token},
            timeout=30
        ).json())
        
        empresas_ativas = convoca_codigo.query("ativo == True")
        
        # Limitar número de empresas para testes ou economia de memória
        if max_empresas and max_empresas > 0:
            empresas_ativas = empresas_ativas.head(max_empresas)
            
        total_empresas = len(empresas_ativas)
        print(f"[{datetime.now()}] Processando {total_empresas} empresas ativas")
        
        # Processar cada empresa separadamente para não carregar tudo na memória
        for i, row in enumerate(empresas_ativas.itertuples(index=False)):
            # Log de progresso a cada 10 empresas
            if i % 10 == 0:
                clear_memory()  # Limpar memória regularmente
                print(f"[{datetime.now()}] Processando empresa {i+1}/{total_empresas} - {get_memory_usage()}")
            
            try:
                # Preparar parâmetros para a API
                convoca = {
                    "empresa": API_CONFIG['EMPRESA'],
                    "codigo": API_CONFIG['CODIGO'],
                    "chave": API_CONFIG['CHAVE'],
                    "tipoSaida": "json",
                    "empresaTrabalho": str(row.cod_empresa),
                    "codigoSolicitacao": str(row.cod_solicitacao)
                }
                
                # Obter dados desta empresa
                response = requests.get(
                    url=API_CONFIG['URL_SOC'] + str(convoca),
                    timeout=30
                )
                data = json.loads(response.content.decode('latin-1'))
                
                # Verificar se data é lista ou dicionário e processar
                items_to_process = data if isinstance(data, list) else [data]
                
                if items_to_process:
                    # Processar dados diretamente, sem armazenar tudo em memória
                    process_chunk(items_to_process, summary_data)
                
                # Pequena pausa para não sobrecarregar a API
                time.sleep(0.1)
                
            except Exception as e:
                print(f"[{datetime.now()}] Erro ao processar empresa {row.cod_empresa}: {e}")
                continue
        
        # Salvar resumo agregado (muito menor que dados brutos)
        with open(SUMMARY_FILE, 'w') as f:
            json.dump(summary_data, f)
            
        # Registrar hora da atualização
        with open(LAST_UPDATE_FILE, 'w') as f:
            f.write(datetime.now().isoformat())
            
        return summary_data
        
    except Exception as e:
        print(f"[{datetime.now()}] Erro durante extração: {e}")
        
        # Se já existir um resumo, usar mesmo que esteja desatualizado
        if os.path.exists(SUMMARY_FILE):
            print(f"[{datetime.now()}] Usando resumo existente devido a erro")
            with open(SUMMARY_FILE, 'r') as f:
                return json.load(f)
        
        # Se não houver resumo, retornar estrutura vazia
        return {
            'status_counts': {},
            'empresas': [],
            'exames': [],
            'meses_vencimento': {},
            'incompany_eligibility': {},
            'total_records': 0
        }

def process_chunk(items, summary_data):
    """Processa um lote de dados atualizando as estatísticas agregadas"""
    if not items:
        return
        
    today = datetime.now()
    
    # Converter para DataFrame temporário (apenas este chunk)
    chunk_df = pd.json_normalize(items)
    
    # Atualizar total de registros
    summary_data['total_records'] += len(chunk_df)
    
    # Processar apenas se tiver colunas relevantes
    if 'NOMEABREVIADO' in chunk_df.columns:
        # Adicionar empresas únicas à lista
        for empresa in chunk_df['NOMEABREVIADO'].dropna().unique():
            if empresa and empresa not in summary_data['empresas']:
                summary_data['empresas'].append(empresa)
    
    if 'EXAME' in chunk_df.columns:
        # Adicionar exames únicos à lista
        for exame in chunk_df['EXAME'].dropna().unique():
            if exame and exame not in summary_data['exames']:
                summary_data['exames'].append(exame)
    
    # Converter colunas de data
    date_columns = ['REFAZER', 'ULTIMOPEDIDO', 'DATARESULTADO']
    for col in date_columns:
        if col in chunk_df.columns:
            chunk_df[col] = pd.to_datetime(chunk_df[col], errors='coerce')
    
    # Calcular dias para vencer
    if 'REFAZER' in chunk_df.columns:
        chunk_df['DiasParaVencer'] = (chunk_df['REFAZER'] - today).dt.days
        
        # Calcular status
        conditions = [
            chunk_df['REFAZER'].isna(),
            chunk_df['DiasParaVencer'] < 0,
            (chunk_df['DiasParaVencer'] >= 0) & (chunk_df['DiasParaVencer'] <= 30),
            (chunk_df['DiasParaVencer'] > 30) & (chunk_df['DiasParaVencer'] <= 60),
            (chunk_df['DiasParaVencer'] > 60) & (chunk_df['DiasParaVencer'] <= 90),
            chunk_df['REFAZER'].dt.year == today.year,
        ]
        choices = [
            'Pendente',
            'Vencido',
            'Vence em 30 dias',
            'Vence em 60 dias',
            'Vence em 90 dias',
            'A Vencer (ano atual)'
        ]
        chunk_df['Status'] = np.select(conditions, choices, default='Em dia')
        
        # Atualizar contagens por status
        status_counts = chunk_df['Status'].value_counts().to_dict()
        for status, count in status_counts.items():
            if status in summary_data['status_counts']:
                summary_data['status_counts'][status] += count
            else:
                summary_data['status_counts'][status] = count
        
        # Adicionar coluna de mês/ano de vencimento
        chunk_df['MesAnoVencimento'] = chunk_df['REFAZER'].dt.strftime('%Y-%m')
        
        # Atualizar contagens por mês
        month_counts = chunk_df['MesAnoVencimento'].value_counts().to_dict()
        for month, count in month_counts.items():
            if month and pd.notna(month):
                if month in summary_data['meses_vencimento']:
                    summary_data['meses_vencimento'][month] += count
                else:
                    summary_data['meses_vencimento'][month] = count
        
        # Processar estatísticas de incompany
        if 'NOMEABREVIADO' in chunk_df.columns:
            # Para cada empresa neste chunk
            for empresa in chunk_df['NOMEABREVIADO'].dropna().unique():
                if not empresa:
                    continue
                    
                # Filtrar apenas para esta empresa
                empresa_df = chunk_df[chunk_df['NOMEABREVIADO'] == empresa]
                
                # Contar por mês
                empresa_month_counts = empresa_df['MesAnoVencimento'].value_counts().to_dict()
                
                # Armazenar meses com 20+ exames (elegíveis para incompany)
                for month, count in empresa_month_counts.items():
                    if month and pd.notna(month) and count >= 20:
                        if empresa not in summary_data['incompany_eligibility']:
                            summary_data['incompany_eligibility'][empresa] = {}
                            
                        if month not in summary_data['incompany_eligibility'][empresa]:
                            summary_data['incompany_eligibility'][empresa][month] = count
                        else:
                            summary_data['incompany_eligibility'][empresa][month] += count

# Verificar se os dados estão atualizados (menos de 24h)
def is_data_updated():
    """Verifica se os dados estão atualizados (menos de 24h)"""
    if not os.path.exists(LAST_UPDATE_FILE):
        return False
        
    try:
        with open(LAST_UPDATE_FILE, 'r') as f:
            last_update = datetime.fromisoformat(f.read().strip())
            
        hours_since_update = (datetime.now() - last_update).total_seconds() / 3600
        return hours_since_update < 24
        
    except Exception as e:
        print(f"Erro ao verificar atualização: {e}")
        return False

# Carregar dados resumidos
@cache.memoize(timeout=3600)
def load_data_summary():
    """Carrega ou atualiza dados resumidos"""
    if os.path.exists(SUMMARY_FILE) and is_data_updated():
        # Usar dados em cache
        with open(SUMMARY_FILE, 'r') as f:
            return json.load(f)
    else:
        # Atualizar dados da API
        return extract_data_from_api()

# Função otimizada para filtrar estatísticas
def filter_status_stats(summary_data, empresa, start_date, end_date):
    """Filtra estatísticas de status com base nos critérios"""
    # Se não houver dados, retorna vazio
    if not summary_data or 'status_counts' not in summary_data:
        return {'Sem dados': 0}
    
    # Por enquanto, apenas retorna contagens gerais
    # Em uma versão mais avançada, podemos implementar filtros mais granulares
    return summary_data['status_counts']

# Cores para os gráficos
STATUS_COLORS = {
    'Vencido': '#FF5733',
    'Vence em 30 dias': '#FFC300',
    'Vence em 60 dias': '#DAF7A6',
    'Vence em 90 dias': '#C4E17F',
    'A Vencer (ano atual)': '#5DADE2',
    'Em dia': '#2ECC71',
    'Pendente': '#BDC3C7',
    'Sem dados': '#95A5A6'
}

# Componentes reutilizáveis para minimizar o código
def create_kpi_card(id_prefix, title, color_class):
    """Cria um cartão KPI reutilizável"""
    return html.Div([
        html.Div([
            html.H4(id=f'{id_prefix}-count', className=f"text-{color_class}"),
            html.P(title, className="text-muted small")
        ], className="border rounded p-2 text-center")
    ])

# Layout extremamente otimizado
app.layout = dbc.Container([
    # Header mínimo
    dbc.Row([
        dbc.Col([
            html.H2("Dashboard de Exames Ocupacionais", className="text-center text-primary my-3"),
            html.Div(id="memory-usage", className="text-muted small text-center")
        ])
    ]),
    
    # Filtros e info
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Filtros e Informações", className="bg-primary text-white py-1"),
                dbc.CardBody([
                    # Status dos dados
                    dbc.Row([
                        dbc.Col([
                            html.Div([
                                html.Strong("Status: ", className="small"),
                                html.Span(id="data-status", className="small")
                            ], className="mb-1"),
                            html.Button(
                                "Atualizar Dados",
                                id="refresh-button",
                                className="btn btn-sm btn-outline-primary"
                            ),
                        ], width=6),
                        dbc.Col([
                            html.Div(id="data-stats", className="small text-muted")
                        ], width=6)
                    ], className="mb-2"),
                    
                    # Filtros simplificados
                    dbc.Row([
                        dbc.Col([
                            html.Label("Empresa:", className="small mb-1"),
                            dcc.Dropdown(
                                id='empresa-dropdown',
                                options=[{'label': 'Todas Empresas', 'value': 'todas'}],
                                value='todas',
                                className="mb-2"
                            ),
                        ], width=12),
                    ]),
                    dbc.Row([
                        dbc.Col([
                            html.Label("Período:", className="small mb-1"),
                            dcc.DatePickerRange(
                                id='date-range',
                                start_date=datetime.now().date(),
                                end_date=(datetime.now() + timedelta(days=365)).date(),
                                display_format='DD/MM/YYYY',
                                className="mb-2"
                            ),
                        ], width=12),
                    ]),
                ], className="p-2")  # Padding reduzido
            ], className="mb-3")
        ])
    ]),
    
    # KPIs principais em cards pequenos
    dbc.Row([
        dbc.Col([create_kpi_card("vencidos", "Vencidos", "danger")], width=3),
        dbc.Col([create_kpi_card("a-vencer", "A Vencer", "warning")], width=3),
        dbc.Col([create_kpi_card("pendentes", "Pendentes", "info")], width=3),
        dbc.Col([create_kpi_card("em-dia", "Em Dia", "success")], width=3),
    ], className="mb-3"),
    
    # Gráficos principais - carregados sob demanda
    dcc.Loading(
        id="loading-main",
        type="circle",
        children=[
            # Mostrar contadores e texto em vez de gráficos pesados
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Resumo por Status", className="bg-primary text-white py-1"),
                        dbc.CardBody(id="status-summary", className="p-2")
                    ], className="mb-3 h-100")
                ], width=6),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Vencimentos próximos 6 meses", className="bg-primary text-white py-1"),
                        dbc.CardBody(id="vencimentos-proximos", className="p-2")
                    ], className="mb-3 h-100")
                ], width=6)
            ]),
            
            # Mostrar tabela em vez de gráfico para InCompany (mais leve)
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Elegibilidade para InCompany", className="bg-primary text-white py-1"),
                        dbc.CardBody(id="incompany-table", className="p-2")
                    ], className="mb-3")
                ], width=12)
            ]),
            
            # Recomendações e ações
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Ações Recomendadas", className="bg-primary text-white py-1"),
                        dbc.CardBody(id="acoes-recomendadas", className="p-2")
                    ], className="mb-3")
                ], width=12)
            ])
        ]
    ),
    
    # Footer mínimo
    dbc.Row([
        dbc.Col([
            html.Footer([
                html.P("Dashboard v2.0 - Otimizado para baixo consumo de memória", className="text-center text-muted small")
            ])
        ])
    ]),
    
    # Intervalo para atualizar uso de memória
    dcc.Interval(
        id='memory-interval',
        interval=10000,  # 10 segundos
        n_intervals=0
    ),
    
    # Armazenar dados do último refresh para evitar recargas
    dcc.Store(id='refresh-timestamp'),
    
    # Armazenar último status de memória
    dcc.Store(id='last-memory-usage', data=0),
    
], fluid=True)

# Callback para mostrar uso de memória
@app.callback(
    Output('memory-usage', 'children'),
    Input('memory-interval', 'n_intervals'),
    Input('last-memory-usage', 'data')
)
def update_memory_usage(n, last_usage):
    usage = get_memory_usage()
    return usage

# Callback para atualizar dropdown de empresas
@app.callback(
    Output('empresa-dropdown', 'options'),
    Input('refresh-button', 'n_clicks')
)
def update_dropdown(n_clicks):
    # Carregar dados resumidos
    summary_data = load_data_summary()
    
    # Lista base de opções
    options = [{'label': 'Todas Empresas', 'value': 'todas'}]
    
    # Adicionar empresas do resumo
    if summary_data and 'empresas' in summary_data:
        # Verificar tipo para lidar com dicionários ou listas
        if isinstance(summary_data['empresas'], dict):
            empresas = list(summary_data['empresas'].keys())
        else:
            empresas = summary_data['empresas']
            
        # Adicionar cada empresa como opção
        for empresa in sorted(empresas):
            if empresa and empresa.strip():
                options.append({'label': empresa, 'value': empresa})
    
    return options

# Callback para mostrar status dos dados
@app.callback(
    [Output('data-status', 'children'),
     Output('data-status', 'className'),
     Output('data-stats', 'children')],
    [Input('refresh-button', 'n_clicks'),
     Input('memory-interval', 'n_intervals')]
)
def update_data_status(n_clicks, n_intervals):
    # Verificar se dados estão atualizados
    is_updated = is_data_updated()
    
    # Obter estatísticas
    stats_text = ""
    if os.path.exists(SUMMARY_FILE):
        try:
            with open(SUMMARY_FILE, 'r') as f:
                summary = json.load(f)
                stats_text = f"Total de registros: {summary.get('total_records', 0):,} | " + \
                             f"Empresas: {len(summary.get('empresas', []))} | " + \
                             f"Exames: {len(summary.get('exames', []))}"
        except:
            stats_text = "Estatísticas não disponíveis"
    
    # Obter data da última atualização
    last_update = "Nunca"
    if os.path.exists(LAST_UPDATE_FILE):
        try:
            with open(LAST_UPDATE_FILE, 'r') as f:
                last_update = datetime.fromisoformat(f.read().strip()).strftime("%d/%m/%Y %H:%M")
        except:
            pass
    
    if is_updated:
        return f"Atualizado em {last_update}", "small text-success", stats_text
    else:
        return f"Desatualizado (>24h) - Última: {last_update}", "small text-warning", stats_text

# Callback para atualizar dados
@app.callback(
    Output('refresh-timestamp', 'data'),
    Input('refresh-button', 'n_clicks')
)
def refresh_data(n_clicks):
    if n_clicks:
        # Limpar caches
        cache.clear()
        
        # Recarregar dados (isso atualizará da API se necessário)
        summary_data = load_data_summary()
        
        # Forçar coleta de lixo
        clear_memory()
        
        return datetime.now().isoformat()
    
    return datetime.now().isoformat()

# Callback para atualizar KPIs
@app.callback(
    [Output('vencidos-count', 'children'),
     Output('a-vencer-count', 'children'),
     Output('pendentes-count', 'children'),
     Output('em-dia-count', 'children')],
    [Input('empresa-dropdown', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date'),
     Input('refresh-timestamp', 'data')]
)
def update_kpis(empresa, start_date, end_date, refresh_time):
    # Carregar dados resumidos
    summary_data = load_data_summary()
    
    # Filtrar estatísticas
    status_counts = filter_status_stats(summary_data, empresa, start_date, end_date)
    
    # Calcular valores dos KPIs
    vencidos = status_counts.get('Vencido', 0)
    
    a_vencer = status_counts.get('Vence em 30 dias', 0) + \
               status_counts.get('Vence em 60 dias', 0) + \
               status_counts.get('Vence em 90 dias', 0) + \
               status_counts.get('A Vencer (ano atual)', 0)
               
    pendentes = status_counts.get('Pendente', 0)
    
    em_dia = status_counts.get('Em dia', 0)
    
    return f"{vencidos:,}", f"{a_vencer:,}", f"{pendentes:,}", f"{em_dia:,}"

# Callback para mostrar resumo por status
@app.callback(
    Output('status-summary', 'children'),
    [Input('empresa-dropdown', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date'),
     Input('refresh-timestamp', 'data')]
)
def update_status_summary(empresa, start_date, end_date, refresh_time):
    # Carregar dados resumidos
    summary_data = load_data_summary()
    
    # Filtrar estatísticas
    status_counts = filter_status_stats(summary_data, empresa, start_date, end_date)
    
    # Criar tabela de resumo
    if not status_counts:
        return html.P("Sem dados disponíveis", className="text-muted")
    
    # Tabela simples em vez de gráfico (mais leve)
    rows = []
    for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
        if count > 0:
            # Determinar cor do status
            color = STATUS_COLORS.get(status, '#777777')
            
            # Criar linha da tabela
            rows.append(html.Tr([
                html.Td(html.Div(className="color-dot", style={"backgroundColor": color})),
                html.Td(status),
                html.Td(f"{count:,}", className="text-right"),
            ]))
    
    # Montar tabela completa
    table = html.Table([
        html.Thead(html.Tr([
            html.Th("", style={"width": "20px"}),
            html.Th("Status"),
            html.Th("Quantidade", className="text-right"),
        ])),
        html.Tbody(rows)
    ], className="table table-sm")
    
    return table

# Callback para mostrar vencimentos próximos
@app.callback(
    Output('vencimentos-proximos', 'children'),
    [Input('empresa-dropdown', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date'),
     Input('refresh-timestamp', 'data')]
)
def update_vencimentos_proximos(empresa, start_date, end_date, refresh_time):
    # Carregar dados resumidos
    summary_data = load_data_summary()
    
    # Se não houver dados, mostra mensagem
    if not summary_data or 'meses_vencimento' not in summary_data:
        return html.P("Sem dados disponíveis", className="text-muted")
    
    # Gerar sequência dos próximos 6 meses
    today = datetime.now()
    next_months = [(today + timedelta(days=30*i)).strftime('%Y-%m') for i in range(6)]
    
    # Filtrar apenas os próximos 6 meses
    month_counts = {
        month: summary_data['meses_vencimento'].get(month, 0)
        for month in next_months
    }
    
    # Criar tabela de resumo
    rows = []
    for month, count in month_counts.items():
        # Formatar mês para exibição
        month_date = datetime.strptime(month, '%Y-%m')
        month_formatted = month_date.strftime('%b/%Y')
        
        # Determinar status do mês
        bgcolor = "#ffffff"
        if count > 50:
            bgcolor = "#ffeeee"  # Vermelho claro para meses com muitos vencimentos
        
        # Criar linha da tabela
        rows.append(html.Tr([
            html.Td(month_formatted),
            html.Td(f"{count:,}", className="text-right"),
        ], style={"backgroundColor": bgcolor}))
    
    # Montar tabela completa
    table = html.Table([
        html.Thead(html.Tr([
            html.Th("Mês"),
            html.Th("Qtde Exames", className="text-right"),
        ])),
        html.Tbody(rows)
    ], className="table table-sm")
    
    return table

# Callback para tabela de incompany
@app.callback(
    Output('incompany-table', 'children'),
    [Input('empresa-dropdown', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date'),
     Input('refresh-timestamp', 'data')]
)
def update_incompany_table(empresa, start_date, end_date, refresh_time):
    # Carregar dados resumidos
    summary_data = load_data_summary()
    
    # Se não houver dados, mostra mensagem
    if not summary_data or 'incompany_eligibility' not in summary_data:
        return html.P("Sem dados disponíveis", className="text-muted")
    
    incompany_data = summary_data['incompany_eligibility']
    
    # Se for empresa específica
    if empresa != 'todas' and empresa in incompany_data:
        # Mostrar dados apenas para esta empresa
        months_data = incompany_data[empresa]
        
        if not months_data:
            return html.P(f"Empresa {empresa} não possui meses elegíveis para InCompany", className="text-muted")
        
        # Criar tabela de resumo
        rows = []
        for month, count in sorted(months_data.items()):
            # Formatar mês para exibição
            try:
                month_date = datetime.strptime(month, '%Y-%m')
                month_formatted = month_date.strftime('%b/%Y')
            except:
                month_formatted = month
            
            # Criar linha da tabela
            rows.append(html.Tr([
                html.Td(month_formatted),
                html.Td(f"{count:,}", className="text-right"),
                html.Td("Elegível" if count >= 20 else "Não elegível", 
                       className="text-success" if count >= 20 else "text-muted"),
            ]))
        
        # Montar tabela completa
        table = html.Table([
            html.Thead(html.Tr([
                html.Th("Mês"),
                html.Th("Qtde Exames", className="text-right"),
                html.Th("Status"),
            ])),
            html.Tbody(rows)
        ], className="table table-sm")
        
        return [
            html.H6(f"Elegibilidade para InCompany - {empresa}"),
            table
        ]
    else:
        # Mostrar dados agregados para todas as empresas
        # Limitado a 10 empresas para economizar espaço/memória
        eligible_count = 0
        top_companies = []
        
        for company, months in incompany_data.items():
            eligible_months = sum(1 for count in months.values() if count >= 20)
            if eligible_months > 0:
                eligible_count += 1
                top_companies.append((company, eligible_months, sum(months.values())))
        
        # Ordenar por número de meses elegíveis
        top_companies.sort(key=lambda x: x[1], reverse=True)
        
        # Limitar a 10 empresas
        top_companies = top_companies[:10]
        
        # Criação da tabela resumo
        if not top_companies:
            return html.P("Não há empresas elegíveis para InCompany no período", className="text-muted")
        
        # Criar tabela de resumo
        rows = []
        for company, eligible_months, total_exams in top_companies:
            rows.append(html.Tr([
                html.Td(company),
                html.Td(f"{eligible_months}", className="text-right"),
                html.Td(f"{total_exams:,}", className="text-right"),
            ]))
        
        # Montar tabela completa
        table = html.Table([
            html.Thead(html.Tr([
                html.Th("Empresa"),
                html.Th("Meses Elegíveis", className="text-right"),
                html.Th("Total Exames", className="text-right"),
            ])),
            html.Tbody(rows)
        ], className="table table-sm")
        
        return [
            html.H6(f"Top 10 Empresas Elegíveis para InCompany (de {eligible_count} total)"),
            html.P("Selecione uma empresa específica para ver detalhes", className="text-muted small mb-2"),
            table
        ]

# Callback para mostrar ações recomendadas
@app.callback(
    Output('acoes-recomendadas', 'children'),
    [Input('empresa-dropdown', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date'),
     Input('refresh-timestamp', 'data')]
)
def update_acoes_recomendadas(empresa, start_date, end_date, refresh_time):
    # Carregar dados resumidos
    summary_data = load_data_summary()
    
    # Filtrar estatísticas
    status_counts = filter_status_stats(summary_data, empresa, start_date, end_date)
    
    # Calcular valores dos KPIs
    vencidos = status_counts.get('Vencido', 0)
    a_vencer_30 = status_counts.get('Vence em 30 dias', 0)
    a_vencer_60 = status_counts.get('Vence em 60 dias', 0)
    a_vencer_90 = status_counts.get('Vence em 90 dias', 0)
    pendentes = status_counts.get('Pendente', 0)
    
    # Lista de recomendações
    recomendacoes = []
    
    if vencidos > 0:
        recomendacoes.append(html.Li([
            html.Span("URGENTE: ", className="text-danger font-weight-bold"),
            f"Regularizar {vencidos:,} exames vencidos."
        ]))
    
    if pendentes > 0:
        recomendacoes.append(html.Li([
            html.Span("ALTA PRIORIDADE: ", className="text-warning font-weight-bold"),
            f"Definir datas para {pendentes:,} exames pendentes."
        ]))
    
    if a_vencer_30 > 0:
        recomendacoes.append(html.Li([
            html.Span("PRIORIDADE: ", className="text-primary font-weight-bold"),
            f"Planejar {a_vencer_30:,} exames a vencer em 30 dias."
        ]))
    
    if a_vencer_60 + a_vencer_90 > 0:
        recomendacoes.append(html.Li([
            html.Span("PLANEJAMENTO: ", className="text-info font-weight-bold"),
            f"Preparar para {a_vencer_60 + a_vencer_90:,} exames a vencer em 60-90 dias."
        ]))
    
    # Verificar elegibilidade para InCompany
    if empresa != 'todas' and summary_data and 'incompany_eligibility' in summary_data:
        incompany_data = summary_data['incompany_eligibility']
        if empresa in incompany_data and any(count >= 20 for count in incompany_data[empresa].values()):
            recomendacoes.append(html.Li([
                html.Span("INCOMPANY: ", className="text-success font-weight-bold"),
                f"Empresa elegível para programa InCompany em alguns meses."
            ]))
    
    # Se não houver recomendações específicas
    if not recomendacoes:
        if sum(status_counts.values()) > 0:
            return html.P("Não há ações críticas necessárias no momento.", className="text-success")
        else:
            return html.P("Sem dados suficientes para gerar recomendações.", className="text-muted")
    
    return html.Ol(recomendacoes)

# Iniciar o servidor com configurações otimizadas
if __name__ == '__main__':
    # Verificar se estamos no Render
    if 'RENDER' in os.environ:
        # No Render, usar configurações de produção
        port = int(os.environ.get('PORT', 8080))
        app.run_server(host='0.0.0.0', port=port)
    else:
        # Localmente, usar configurações de desenvolvimento
        app.run_server(debug=False)  # Desativar debug para economia de memória
