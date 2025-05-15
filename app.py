import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
from datetime import datetime, timedelta
import os
from flask_caching import Cache
import gc
import warnings
import requests
import json
import time
from pathlib import Path
warnings.filterwarnings('ignore')

# Configuração inicial
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# Configurar o cache do Flask
cache_dir = './flask_cache'
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)

cache_config = {
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': cache_dir,
    'CACHE_DEFAULT_TIMEOUT': 86400  # 24 horas em segundos
}
    
server_cache = Cache()
server_cache.init_app(app.server, config=cache_config)

# Diretório para cache de dados da API
data_cache_dir = './data_cache'
if not os.path.exists(data_cache_dir):
    os.makedirs(data_cache_dir)

# Arquivo de cache dos dados
csv_cache_file = os.path.join(data_cache_dir, 'convocacao_grs_nucleo.csv')
cache_info_file = os.path.join(data_cache_dir, 'cache_info.json')

# Configuração da API (usando variáveis de ambiente ou valores padrão para desenvolvimento)
URL_SOC = os.environ.get('URL_SOC', 'https://ws1.soc.com.br/WebSoc/exportadados?parametro=')
URL_CONNECT = os.environ.get('URL_CONNECT', 'https://www.grsconnect.com.br/')
API_USERNAME = os.environ.get('API_USERNAME', '1')
API_PASSWORD = os.environ.get('API_PASSWORD', 'ConnectBI@20482022')
EMPRESA = os.environ.get('EMPRESA', '423')
CODIGO = os.environ.get('CODIGO', '151346')
CHAVE = os.environ.get('CHAVE', 'b5aa04943cd28ff155ed')

# Função para verificar se o cache está válido (menos de 24 horas)
def is_cache_valid():
    try:
        if not os.path.exists(csv_cache_file) or not os.path.exists(cache_info_file):
            return False
        
        with open(cache_info_file, 'r') as f:
            cache_info = json.load(f)
        
        cache_time = datetime.fromisoformat(cache_info['timestamp'])
        current_time = datetime.now()
        
        # Cache é válido se menos de 24 horas se passaram
        return (current_time - cache_time).total_seconds() < 86400
    except Exception as e:
        print(f"Erro ao verificar cache: {e}")
        return False

# Função para extrair dados da API e salvar no cache
def extract_data_from_api():
    print("Extraindo dados da API...")
    try:
        # Obter token
        token_response = requests.get(
            url=URL_CONNECT + 'get_token',
            params={
                'username': API_USERNAME,
                'password': API_PASSWORD
            }
        )
        
        token = token_response.json()['token']
        
        # Obter lista de empresas ativas
        convoca_codigo = pd.json_normalize(requests.get(
            url=URL_CONNECT + 'get_ped_proc',
            params={"token": token}
        ).json())
        
        # Filtrar empresas ativas
        empresas_ativas = convoca_codigo.query("ativo == True")
        
        # Coletar dados
        convocacao_grs_nucleo = []
        total_empresas = len(empresas_ativas)
        
        print(f"Processando {total_empresas} empresas ativas...")
        
        for i, row in enumerate(empresas_ativas.itertuples(index=False)):
            # Exibir progresso
            if i % 10 == 0:
                print(f"Processando empresa {i+1}/{total_empresas}...")
            
            convoca = {
                "empresa": EMPRESA,
                "codigo": CODIGO,
                "chave": CHAVE,
                "tipoSaida": "json",
                "empresaTrabalho": str(row.cod_empresa),
                "codigoSolicitacao": str(row.cod_solicitacao)
            }
            
            try:
                response = requests.get(url=URL_SOC + str(convoca))
                data = json.loads(response.content.decode('latin-1'))
                
                # Verificar se data é lista ou dicionário
                if isinstance(data, list):
                    convocacao_grs_nucleo.extend(data)  # Adicionar todos os itens da lista
                elif isinstance(data, dict):
                    convocacao_grs_nucleo.append(data)  # Adicionar o dicionário único
                
                # Pequena pausa para não sobrecarregar a API
                time.sleep(0.1)
            except Exception as e:
                print(f"Erro ao processar empresa {row.cod_empresa}: {e}")
                continue
        
        # Normalizar e salvar dados
        print("Normalizando e salvando dados...")
        df = pd.json_normalize(convocacao_grs_nucleo)
        df.to_csv(csv_cache_file, index=False, encoding="utf-8")
        
        # Salvar informações do cache
        with open(cache_info_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'rows': len(df),
                'columns': len(df.columns)
            }, f)
        
        print(f"Dados extraídos e salvos com sucesso. Total de {len(df)} registros.")
        return df
    
    except Exception as e:
        print(f"Erro durante extração de dados: {e}")
        # Se já existir um cache, usar ele mesmo que esteja expirado
        if os.path.exists(csv_cache_file):
            print("Usando cache existente devido a erro na extração.")
            return pd.read_csv(csv_cache_file, low_memory=False)
        raise e

# Função para carregar dados (do cache ou da API)
@server_cache.memoize(timeout=3600)  # Cache por 1 hora
def load_initial_data():
    try:
        # Verificar se cache é válido
        if is_cache_valid():
            print("Usando dados em cache...")
            df = pd.read_csv(csv_cache_file, low_memory=False)
        else:
            print("Cache expirado ou inexistente. Extraindo novos dados...")
            df = extract_data_from_api()
        
        # Reportar o tamanho do dataset
        print(f"Dataset carregado: {len(df)} linhas")
        
        # Converter colunas de datas
        date_columns = ['ULTIMOPEDIDO', 'DATARESULTADO', 'REFAZER']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Criar índice para melhorar performance de filtros
        if 'NOMEABREVIADO' in df.columns:
            df['NOMEABREVIADO_ORIGINAL'] = df['NOMEABREVIADO']  # manter original para uso futuro
            df['NOMEABREVIADO'] = df['NOMEABREVIADO'].astype(str)  # normalizar para string

        # Limpar memória
        gc.collect()
        
        return df
    
    except Exception as e:
        print(f"Erro ao carregar dados: {e}")
        # Retornar um DataFrame vazio com as colunas necessárias
        return pd.DataFrame(columns=['CODIGOEMPRESA', 'NOMEABREVIADO', 'EXAME', 'REFAZER'])

# Carregar dados inicialmente
df_full = load_initial_data()

# Manter uma lista de empresas para o dropdown (mais eficiente que recalcular)
try:
    empresas = sorted(list(set(df_full['NOMEABREVIADO'].dropna().unique())))
    empresas = [e for e in empresas if e and e.strip()]  # Filtrar valores vazios ou só com espaços
    print(f"Total de empresas: {len(empresas)}")
except Exception as e:
    print(f"Erro ao listar empresas: {e}")
    empresas = []

# Função otimizada para calcular status dos exames
@server_cache.memoize(timeout=600)  # Cache por 10 minutos
def process_dataframe(df):
    today = datetime.now()
    
    # Adicionar coluna de dias para vencer (método vetorizado é mais rápido)
    df['DiasParaVencer'] = (df['REFAZER'] - today).dt.days
    
    # Adicionar coluna de status usando numpy (mais rápido que apply)
    conditions = [
        df['REFAZER'].isna(),
        df['DiasParaVencer'] < 0,
        (df['DiasParaVencer'] >= 0) & (df['DiasParaVencer'] <= 30),
        (df['DiasParaVencer'] > 30) & (df['DiasParaVencer'] <= 60),
        (df['DiasParaVencer'] > 60) & (df['DiasParaVencer'] <= 90),
        df['REFAZER'].dt.year == today.year,
    ]
    choices = [
        'Pendente',
        'Vencido',
        'Vence em 30 dias',
        'Vence em 60 dias',
        'Vence em 90 dias',
        'A Vencer (ano atual)'
    ]
    df['Status'] = np.select(conditions, choices, default='Em dia')
    
    # Adicionar coluna de mês/ano de vencimento (mais eficiente usar strftime diretamente)
    df['MesAnoVencimento'] = df['REFAZER'].dt.strftime('%Y-%m')
    
    return df

# Função otimizada para identificar meses para InCompany
@server_cache.memoize(timeout=600)  # Cache por 10 minutos
def identify_incompany_months(df, min_exams=20):
    # Identificar meses potenciais para InCompany até dezembro de 2025
    today = datetime.now()
    end_date = datetime(2025, 12, 31)
    months = []
    
    current = datetime(today.year, today.month, 1)
    while current <= end_date:
        months.append(current.strftime('%Y-%m'))
        current = current + pd.DateOffset(months=1)
    
    # Dicionário para armazenar empresas elegíveis por mês
    incompany_months = {}
    
    # Para cada empresa, calcular de forma mais eficiente
    for empresa in df['NOMEABREVIADO'].unique():
        if pd.isna(empresa) or not empresa.strip():
            continue
            
        empresa_df = df[df['NOMEABREVIADO'] == empresa]
        
        # Agrupar e contar em uma única operação (muito mais eficiente)
        month_counts = empresa_df['MesAnoVencimento'].value_counts()
        
        # Filtrar apenas meses com contagem suficiente
        eligible_months = []
        for month in months:
            count = month_counts.get(month, 0)
            if count >= min_exams:
                eligible_months.append({
                    'month': month,
                    'count': count
                })
        
        if eligible_months:
            incompany_months[empresa] = eligible_months
    
    return incompany_months

# Cores para os gráficos
status_colors = {
    'Vencido': '#FF5733',
    'Vence em 30 dias': '#FFC300',
    'Vence em 60 dias': '#DAF7A6',
    'Vence em 90 dias': '#C4E17F',
    'A Vencer (ano atual)': '#5DADE2',
    'Em dia': '#2ECC71',
    'Pendente': '#BDC3C7',
    'Desconhecido': '#95A5A6'
}

# Adicionar informação sobre dados
data_info = html.Div([
    html.P([
        html.Strong("Status dos dados: "), 
        html.Span(id="data-status", className="text-success"),
    ], className="mb-1"),
    html.P([
        html.Strong("Última atualização: "), 
        html.Span(id="data-timestamp", className="text-muted"),
    ], className="mb-1"),
    html.Button(
        "Forçar Atualização de Dados", 
        id="refresh-data-button", 
        className="btn btn-sm btn-outline-primary mt-2"
    ),
    html.Div(id="refresh-status", className="mt-2 small")
], className="mt-3")

# Layout do Dashboard
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("Dashboard de Gestão de Exames Ocupacionais", 
                   className="text-center text-primary my-4")
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Filtros e Informações", className="bg-primary text-white"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Empresa:"),
                            dcc.Dropdown(
                                id='empresa-dropdown',
                                options=[{'label': 'Todas Empresas', 'value': 'todas'}] + 
                                        [{'label': emp, 'value': emp} for emp in empresas],
                                value='todas',
                                className="mb-3"
                            ),
                        ], width=8),
                        dbc.Col([
                            # Informações sobre os dados
                            data_info
                        ], width=4, className="border-left")
                    ]),
                    
                    html.Label("Período:"),
                    dcc.DatePickerRange(
                        id='date-range',
                        start_date=datetime.now().date(),
                        end_date=datetime(2025, 12, 31).date(),
                        display_format='DD/MM/YYYY',
                        className="mb-3"
                    ),
                    html.Div(id='data-size-info', className="text-muted small")
                ])
            ], className="mb-4")
        ])
    ]),
    
    # Indicador de carregamento para operações demoradas
    dcc.Loading(
        id="loading-1",
        type="circle",
        children=[
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Resumo por Status", className="bg-primary text-white"),
                        dbc.CardBody([
                            dcc.Graph(id='status-pie-chart')
                        ])
                    ], className="mb-4")
                ], width=6),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("KPIs", className="bg-primary text-white"),
                        dbc.CardBody([
                            dbc.Row([
                                dbc.Col([
                                    html.Div([
                                        html.H4(id='vencidos-count', className="text-danger"),
                                        html.P("Exames Vencidos", className="text-muted")
                                    ], className="border rounded p-3 text-center")
                                ], width=6),
                                dbc.Col([
                                    html.Div([
                                        html.H4(id='a-vencer-count', className="text-warning"),
                                        html.P("Exames a Vencer (90 dias)", className="text-muted")
                                    ], className="border rounded p-3 text-center")
                                ], width=6)
                            ], className="mb-3"),
                            dbc.Row([
                                dbc.Col([
                                    html.Div([
                                        html.H4(id='pendentes-count', className="text-info"),
                                        html.P("Exames Pendentes", className="text-muted")
                                    ], className="border rounded p-3 text-center")
                                ], width=6),
                                dbc.Col([
                                    html.Div([
                                        html.H4(id='em-dia-count', className="text-success"),
                                        html.P("Exames em Dia", className="text-muted")
                                    ], className="border rounded p-3 text-center")
                                ], width=6)
                            ])
                        ])
                    ], className="mb-4")
                ], width=6)
            ]),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Vencimentos por Mês", className="bg-primary text-white"),
                        dbc.CardBody([
                            dcc.Graph(id='vencimentos-bar-chart')
                        ])
                    ], className="mb-4")
                ])
            ]),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Meses Elegíveis para InCompany", className="bg-primary text-white"),
                        dbc.CardBody([
                            html.Div(id='incompany-months-table')
                        ])
                    ], className="mb-4")
                ], width=8),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Análise de Criticidade", className="bg-primary text-white"),
                        dbc.CardBody([
                            html.Div(id='criticality-gauge')
                        ])
                    ], className="mb-4")
                ], width=4)
            ]),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Análise por Tipo de Exame", className="bg-primary text-white"),
                        dbc.CardBody([
                            dcc.Graph(id='exames-bar-chart')
                        ])
                    ], className="mb-4")
                ])
            ]),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Ações Estratégicas Recomendadas", className="bg-primary text-white"),
                        dbc.CardBody([
                            html.Div(id='estrategias-recomendadas')
                        ])
                    ], className="mb-4")
                ])
            ])
        ]
    ),
    
    dbc.Row([
        dbc.Col([
            html.Footer([
                html.P("Dashboard de Gestão de Exames Ocupacionais | Dados atualizados em: " + 
                      datetime.now().strftime("%d/%m/%Y %H:%M"),
                      className="text-center text-muted")
            ])
        ])
    ])
    
], fluid=True)

# Função para obter informações do cache
def get_cache_info():
    try:
        if os.path.exists(cache_info_file):
            with open(cache_info_file, 'r') as f:
                return json.load(f)
        return None
    except Exception as e:
        print(f"Erro ao ler informações do cache: {e}")
        return None

# Callback para mostrar informações do cache
@app.callback(
    [Output("data-status", "children"),
     Output("data-timestamp", "children")],
    [Input("refresh-data-button", "n_clicks")]
)
def update_data_info(n_clicks):
    cache_info = get_cache_info()
    
    if cache_info:
        timestamp = datetime.fromisoformat(cache_info['timestamp'])
        formatted_time = timestamp.strftime("%d/%m/%Y %H:%M:%S")
        
        if is_cache_valid():
            status = "Atualizado"
            return status, formatted_time
        else:
            status = "Desatualizado (>24h)"
            return status, formatted_time
    
    return "Não disponível", "Nunca atualizado"

# Callback para atualizar dados quando solicitado
@app.callback(
    Output("refresh-status", "children"),
    [Input("refresh-data-button", "n_clicks")]
)
def refresh_data(n_clicks):
    if n_clicks is None or n_clicks == 0:
        return ""
    
    try:
        # Forçar atualização de dados
        extract_data_from_api()
        
        # Limpar cache de função
        server_cache.clear()
        
        # Recarregar dados
        global df_full, df_processed
        df_full = load_initial_data()
        if 'df_processed' in globals():
            del df_processed
        
        return html.Span("Dados atualizados com sucesso! Recarregue a página para ver as mudanças.", 
                         className="text-success")
    except Exception as e:
        return html.Span(f"Erro ao atualizar dados: {str(e)}", className="text-danger")

# Função otimizada para filtrar dados
@server_cache.memoize(timeout=300)  # Cache por 5 minutos
def filter_dataframe(empresa, start_date, end_date):
    # Converter parâmetros para formatos adequados
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    # Processar dados iniciais apenas uma vez e armazenar em cache
    global df_processed
    if 'df_processed' not in globals():
        print("Processando dataframe pela primeira vez...")
        df_processed = process_dataframe(df_full.copy())
    
    # Filtrar por empresa (mais eficiente usando índice)
    if empresa != 'todas':
        filtered_df = df_processed[df_processed['NOMEABREVIADO'] == empresa].copy()
    else:
        filtered_df = df_processed.copy()
    
    # Filtrar por período (com base na data REFAZER)
    date_mask = (filtered_df['REFAZER'].isna()) | (
        (filtered_df['REFAZER'] >= start_date) & (filtered_df['REFAZER'] <= end_date)
    )
    filtered_df = filtered_df[date_mask]
    
    # Forçar limpeza de memória para DataFrames temporários
    gc.collect()
    
    return filtered_df

# Callbacks mais eficientes
@app.callback(
    Output('data-size-info', 'children'),
    [Input('empresa-dropdown', 'value')]
)
def update_data_info(selected_empresa):
    if selected_empresa == 'todas':
        return f"Total de registros: {len(df_full):,}"
    else:
        count = len(df_full[df_full['NOMEABREVIADO'] == selected_empresa])
        return f"Registros para {selected_empresa}: {count:,}"

# Usamos múltiplos callbacks em vez de um grande para melhor desempenho
@app.callback(
    [
        Output('status-pie-chart', 'figure'),
        Output('vencidos-count', 'children'),
        Output('a-vencer-count', 'children'),
        Output('pendentes-count', 'children'),
        Output('em-dia-count', 'children')
    ],
    [
        Input('empresa-dropdown', 'value'),
        Input('date-range', 'start_date'),
        Input('date-range', 'end_date')
    ]
)
def update_kpis_and_status(selected_empresa, start_date, end_date):
    # Filtrar dados
    filtered_df = filter_dataframe(selected_empresa, start_date, end_date)
    
    # Calcular contagens por status (usando value_counts é mais rápido)
    status_counts = filtered_df['Status'].value_counts().reset_index()
    status_counts.columns = ['Status', 'Count']
    
    # Limitar a 1000 registros para o gráfico de pizza para performance
    if len(status_counts) > 0:
        # Criar gráfico de pizza
        pie_chart = px.pie(
            status_counts, 
            values='Count', 
            names='Status',
            color='Status',
            color_discrete_map=status_colors,
            title='Distribuição de Exames por Status',
            hole=0.4
        )
        pie_chart.update_traces(textinfo='percent+label')
    else:
        # Gráfico vazio se não há dados
        pie_chart = px.pie(
            pd.DataFrame({'Status': ['Sem dados'], 'Count': [1]}),
            values='Count',
            names='Status',
            title='Sem dados para exibir'
        )
    
    # Calcular contagens para KPIs
    vencidos = filtered_df[filtered_df['Status'] == 'Vencido'].shape[0]
    a_vencer_90 = filtered_df[
        (filtered_df['Status'] == 'Vence em 30 dias') |
        (filtered_df['Status'] == 'Vence em 60 dias') |
        (filtered_df['Status'] == 'Vence em 90 dias')
    ].shape[0]
    pendentes = filtered_df[filtered_df['Status'] == 'Pendente'].shape[0]
    em_dia = filtered_df[
        (filtered_df['Status'] == 'Em dia') | 
        (filtered_df['Status'] == 'A Vencer (ano atual)')
    ].shape[0]
    
    return pie_chart, f"{vencidos:,}", f"{a_vencer_90:,}", f"{pendentes:,}", f"{em_dia:,}"

@app.callback(
    Output('vencimentos-bar-chart', 'figure'),
    [
        Input('empresa-dropdown', 'value'),
        Input('date-range', 'start_date'),
        Input('date-range', 'end_date')
    ]
)
def update_vencimentos_chart(selected_empresa, start_date, end_date):
    # Filtrar dados
    filtered_df = filter_dataframe(selected_empresa, start_date, end_date)
    
    # Agrupar por mês/ano de vencimento (usar value_counts é mais eficiente)
    vencimentos_por_mes = filtered_df.dropna(subset=['MesAnoVencimento'])['MesAnoVencimento'].value_counts().reset_index()
    vencimentos_por_mes.columns = ['MesAno', 'Count']
    vencimentos_por_mes = vencimentos_por_mes.sort_values('MesAno')
    
    # Converter para formato mais legível
    vencimentos_por_mes['MesAnoFormatado'] = vencimentos_por_mes['MesAno'].apply(
        lambda x: pd.to_datetime(x).strftime('%b/%Y')
    )
    
    # Filtrar para mostrar apenas os próximos 12 meses
    today = datetime.now()
    next_12_months = [(today + pd.DateOffset(months=i)).strftime('%Y-%m') for i in range(12)]
    vencimentos_por_mes = vencimentos_por_mes[vencimentos_por_mes['MesAno'].isin(next_12_months)]
    
    # Limitar a 12 meses para melhor visualização
    vencimentos_por_mes = vencimentos_por_mes.head(12)
    
    # Criar gráfico de barras
    if len(vencimentos_por_mes) > 0:
        bar_chart = px.bar(
            vencimentos_por_mes,
            x='MesAnoFormatado',
            y='Count',
            title='Vencimentos por Mês (Próximos 12 Meses)',
            labels={'Count': 'Qtde Exames', 'MesAnoFormatado': 'Mês/Ano'},
            color_discrete_sequence=['#3498DB']
        )
    else:
        # Gráfico vazio se não há dados
        bar_chart = px.bar(
            pd.DataFrame({'Mês': ['Sem dados'], 'Count': [0]}),
            x='Mês',
            y='Count',
            title='Sem dados para exibir'
        )
    
    return bar_chart

@app.callback(
    Output('exames-bar-chart', 'figure'),
    [
        Input('empresa-dropdown', 'value'),
        Input('date-range', 'start_date'),
        Input('date-range', 'end_date')
    ]
)
def update_exames_chart(selected_empresa, start_date, end_date):
    # Filtrar dados
    filtered_df = filter_dataframe(selected_empresa, start_date, end_date)
    
    # Agrupar por tipo de exame e status
    try:
        exames_status = filtered_df.groupby(['EXAME', 'Status']).size().reset_index(name='Count')
        
        # Limitar a 15 tipos de exames mais comuns para melhor visualização
        top_exames = filtered_df['EXAME'].value_counts().nlargest(15).index.tolist()
        exames_status = exames_status[exames_status['EXAME'].isin(top_exames)]
        
        # Criar gráfico de barras
        if len(exames_status) > 0:
            exames_chart = px.bar(
                exames_status,
                x='EXAME',
                y='Count',
                color='Status',
                title='Top 15 Tipos de Exame por Status',
                color_discrete_map=status_colors,
                labels={'Count': 'Qtde', 'EXAME': 'Tipo de Exame'},
                height=500
            )
            exames_chart.update_layout(xaxis={'categoryorder':'total descending'})
        else:
            # Gráfico vazio se não há dados
            exames_chart = px.bar(
                pd.DataFrame({'Exame': ['Sem dados'], 'Count': [0]}),
                x='Exame',
                y='Count',
                title='Sem dados para exibir'
            )
    except Exception as e:
        print(f"Erro ao criar gráfico de exames: {e}")
        # Gráfico vazio em caso de erro
        exames_chart = px.bar(
            pd.DataFrame({'Exame': ['Erro ao processar dados'], 'Count': [0]}),
            x='Exame',
            y='Count',
            title='Erro ao processar dados'
        )
    
    return exames_chart

@app.callback(
    [
        Output('incompany-months-table', 'children'),
        Output('criticality-gauge', 'children'),
        Output('estrategias-recomendadas', 'children')
    ],
    [
        Input('empresa-dropdown', 'value'),
        Input('date-range', 'start_date'),
        Input('date-range', 'end_date')
    ]
)
def update_analysis_components(selected_empresa, start_date, end_date):
    # Filtrar dados
    filtered_df = filter_dataframe(selected_empresa, start_date, end_date)
    
    # Calcular incompany para conjunto filtrado
    incompany_months = identify_incompany_months(filtered_df)
    
    # 1. Tabela de meses elegíveis para InCompany
    if selected_empresa != 'todas' and selected_empresa in incompany_months:
        incompany_data = incompany_months[selected_empresa]
        
        # Converter para formato mais legível
        for item in incompany_data:
            item['month_formatted'] = pd.to_datetime(item['month']).strftime('%b/%Y')
        
        # Filtrar para meses dentro do período selecionado
        start_date_dt = pd.to_datetime(start_date)
        end_date_dt = pd.to_datetime(end_date)
        incompany_data = [
            item for item in incompany_data 
            if start_date_dt <= pd.to_datetime(item['month']) <= end_date_dt
        ]
        
        if incompany_data:
            incompany_table = dash_table.DataTable(
                columns=[
                    {'name': 'Mês', 'id': 'month_formatted'},
                    {'name': 'Quantidade de Exames', 'id': 'count'}
                ],
                data=incompany_data,
                style_header={
                    'backgroundColor': '#f8f9fa',
                    'fontWeight': 'bold'
                },
                style_cell={
                    'textAlign': 'center',
                    'padding': '10px'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': '#f2f2f2'
                    },
                    {
                        'if': {'column_id': 'count', 'filter_query': '{count} >= 20'},
                        'backgroundColor': '#d4edda',
                        'color': '#155724'
                    }
                ]
            )
            
            incompany_content = [
                html.H5(f"Meses Elegíveis para InCompany - {selected_empresa}"),
                incompany_table,
                html.P("Meses com 20 ou mais exames são elegíveis para InCompany.", 
                       className="mt-3 text-muted")
            ]
        else:
            incompany_content = [
                html.H5(f"Meses Elegíveis para InCompany - {selected_empresa}"),
                html.P("Não há meses elegíveis para InCompany no período selecionado.",
                      className="text-muted")
            ]
    else:
        if selected_empresa == 'todas':
            # Compilar todos os meses elegíveis por empresa (limite a 100 para performance)
            all_incompany = []
            count = 0
            for empresa, months in incompany_months.items():
                for month_data in months:
                    # Converter para formato mais legível
                    month_formatted = pd.to_datetime(month_data['month']).strftime('%b/%Y')
                    # Filtrar para meses dentro do período selecionado
                    start_date_dt = pd.to_datetime(start_date)
                    end_date_dt = pd.to_datetime(end_date)
                    if start_date_dt <= pd.to_datetime(month_data['month']) <= end_date_dt:
                        all_incompany.append({
                            'empresa': empresa,
                            'month': month_data['month'],
                            'month_formatted': month_formatted,
                            'count': month_data['count']
                        })
                        count += 1
                        if count >= 100:  # Limitar a 100 registros para performance
                            break
                if count >= 100:
                    break
            
            if all_incompany:
                incompany_table = dash_table.DataTable(
                    columns=[
                        {'name': 'Empresa', 'id': 'empresa'},
                        {'name': 'Mês', 'id': 'month_formatted'},
                        {'name': 'Quantidade de Exames', 'id': 'count'}
                    ],
                    data=all_incompany,
                    style_header={
                        'backgroundColor': '#f8f9fa',
                        'fontWeight': 'bold'
                    },
                    style_cell={
                        'textAlign': 'center',
                        'padding': '10px'
                    },
                    style_data_conditional=[
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': '#f2f2f2'
                        },
                        {
                            'if': {'column_id': 'count', 'filter_query': '{count} >= 20'},
                            'backgroundColor': '#d4edda',
                            'color': '#155724'
                        }
                    ],
                    sort_action='native',
                    filter_action='native',
                    page_size=10
                )
                
                incompany_content = [
                    html.H5("Meses Elegíveis para InCompany - Todas Empresas"),
                    incompany_table,
                    html.P("Meses com 20 ou mais exames são elegíveis para InCompany.", 
                           className="mt-3 text-muted")
                ]
            else:
                incompany_content = [
                    html.H5("Meses Elegíveis para InCompany - Todas Empresas"),
                    html.P("Não há meses elegíveis para InCompany no período selecionado.",
                          className="text-muted")
                ]
        else:
            incompany_content = [
                html.H5("Meses Elegíveis para InCompany"),
                html.P("Selecione uma empresa para ver os meses elegíveis para InCompany.",
                      className="text-muted")
            ]
    
    # 2. Medidor de criticidade
    total_exames = filtered_df.shape[0]
    if total_exames > 0:
        # Calcular contagens por status
        vencidos = filtered_df[filtered_df['Status'] == 'Vencido'].shape[0]
        a_vencer_90 = filtered_df[
            (filtered_df['Status'] == 'Vence em 30 dias') |
            (filtered_df['Status'] == 'Vence em 60 dias') |
            (filtered_df['Status'] == 'Vence em 90 dias')
        ].shape[0]
        pendentes = filtered_df[filtered_df['Status'] == 'Pendente'].shape[0]
        
        percentual_vencidos = (vencidos / total_exames) * 100
        percentual_a_vencer = (a_vencer_90 / total_exames) * 100
        percentual_pendentes = (pendentes / total_exames) * 100
        
        criticality_score = percentual_vencidos * 1 + percentual_a_vencer * 0.5 + percentual_pendentes * 0.3
        
        # Gauge para nível de criticidade
        gauge_fig = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = criticality_score,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "Nível de Criticidade", 'font': {'size': 16}},
            gauge = {
                'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                'bar': {'color': "darkblue"},
                'bgcolor': "white",
                'borderwidth': 2,
                'bordercolor': "gray",
                'steps': [
                    {'range': [0, 20], 'color': 'green'},
                    {'range': [20, 40], 'color': 'lime'},
                    {'range': [40, 60], 'color': 'yellow'},
                    {'range': [60, 80], 'color': 'orange'},
                    {'range': [80, 100], 'color': 'red'}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 80
                }
            }
        ))
        
        gauge_fig.update_layout(height=300, margin=dict(l=10, r=10, t=50, b=10))
        
        criticality_content = dcc.Graph(figure=gauge_fig)
    else:
        criticality_content = html.P("Sem dados para calcular criticidade.", className="text-muted")
    
    # 3. Recomendações estratégicas
    if total_exames > 0:
        # Calcular contagens por status
        vencidos = filtered_df[filtered_df['Status'] == 'Vencido'].shape[0]
        a_vencer_90 = filtered_df[
            (filtered_df['Status'] == 'Vence em 30 dias') |
            (filtered_df['Status'] == 'Vence em 60 dias') |
            (filtered_df['Status'] == 'Vence em 90 dias')
        ].shape[0]
        pendentes = filtered_df[filtered_df['Status'] == 'Pendente'].shape[0]
        
        recomendacoes = []
        
        if vencidos > 0:
            recomendacoes.append(html.Li([
                html.Span("Ação Imediata: ", className="font-weight-bold text-danger"),
                f"Regularizar os {vencidos:,} exames vencidos com prioridade."
            ]))
        
        if pendentes > 0:
            recomendacoes.append(html.Li([
                html.Span("Curto Prazo (30 dias): ", className="font-weight-bold text-warning"),
                f"Definir datas para os {pendentes:,} exames pendentes que não possuem programação."
            ]))
        
        if a_vencer_90 > 0:
            recomendacoes.append(html.Li([
                html.Span("Médio Prazo (90 dias): ", className="font-weight-bold text-info"),
                f"Planejar a realização dos {a_vencer_90:,} exames que vencem nos próximos 90 dias."
            ]))
        
        # Verificar se há meses com muitos vencimentos para recomendar InCompany
        if selected_empresa != 'todas' and selected_empresa in incompany_months:
            incompany_data = incompany_months[selected_empresa]
            
            # Filtrar para meses dentro do período selecionado
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            incompany_data = [
                item for item in incompany_data 
                if start_date_dt <= pd.to_datetime(item['month']) <= end_date_dt
            ]
            
            if incompany_data:
                next_incompany = sorted(incompany_data, key=lambda x: pd.to_datetime(x['month']))[0]
                month_formatted = pd.to_datetime(next_incompany['month']).strftime('%b/%Y')
                
                recomendacoes.append(html.Li([
                    html.Span("Planejamento InCompany: ", className="font-weight-bold text-primary"),
                    f"Programar InCompany para {month_formatted} com {next_incompany['count']:,} exames previstos."
                ]))
        
        # Adicionar recomendação contínua
        recomendacoes.append(html.Li([
            html.Span("Contínuo: ", className="font-weight-bold text-secondary"),
            "Implementar sistema de alertas automáticos para notificar quando exames estiverem a 60 dias do vencimento."
        ]))
        
        estrategias_content = [
            html.H5("Recomendações Estratégicas", className="mb-3"),
            html.Ol(recomendacoes, className="pl-3")
        ]
        
        # Se for MANSERV e tivermos pelo menos 5 exames
        if selected_empresa != 'todas' and 'MANSERV' in selected_empresa.upper() and filtered_df.shape[0] >= 5:
            # Adicionar análise específica
            estrategias_content.append(html.Div([
                html.H5("Análise Específica", className="mt-4 mb-3"),
                html.P([
                    "A empresa ",
                    html.Strong(selected_empresa),
                    " apresenta uma situação que requer atenção especial com ",
                    html.Strong(f"{vencidos:,} exames vencidos"),
                    " e ",
                    html.Strong(f"{pendentes:,} exames pendentes"),
                    ". Recomendamos agendar uma campanha InCompany imediata para regularização."
                ]),
                html.P([
                    "Sugerimos também um acompanhamento mensal do status dos exames para evitar acúmulo de vencimentos e garantir a conformidade legal."
                ])
            ], className="mt-3 p-3 bg-light border rounded"))
    else:
        estrategias_content = [
            html.H5("Recomendações Estratégicas", className="mb-3"),
            html.P("Sem dados suficientes para gerar recomendações.", className="text-muted")
        ]
    
    return incompany_content, criticality_content, estrategias_content

# Executar o aplicativo
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port)
