import streamlit as st
from PIL import Image
import pandas as pd
import requests
import traceback
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
import base64
from io import BytesIO
import time


# ==============================================
# CONFIGURAÇÕES GLOBAIS
# ==============================================
EVOLUTION_API_KEY = st.secrets["EVOLUTION_API_KEY"]
CHAVE_SECRETA_N8N = st.secrets["CHAVE_SECRETA_N8N"]
credenciais = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["google_sheets_credentials"], escopo)
EVOLUTION_API_URL = "https://evolution.simplefin.ia.br"
ID_PLANILHA_GOOGLE = "1xmXgoaDWUnOaqQRnqYi14OligNoq36LbLVahL2zY89M"
URL_WEBHOOK_N8N_GERAR = "https://app.simplefin.ia.br/webhook/cob"
URL_WEBHOOK_N8N_ENVIAR = "https://app.simplefin.ia.br/webhook/enviar-wa"
ABA_GOOGLE_SHEETS = "Dados de Cobrança"
MAX_REGISTROS = 500
MAX_CLIENTES = 100    # Máximo de clientes distintos
TIMEOUT_FILA = 300
MAX_USUARIOS_SIMULTANEOS = 3

# ==============================================
# INICIALIZAÇÃO DE ESTADOS
# ==============================================
defaults = {
    'tel_corporativo': "",
    'is_connected': False,
    'mensagens_recebidas': [],
    'processo_concluido': False,
    'processo_iniciado': False,
    'geracao_finalizada': False,
    'ultima_atualizacao': 0,
    'selecionar_todos': True,
    'progress_step': 0,
    'total_mensagens_previstas': 0,
    'instancias_ocupadas': {},
    'minha_instancia_atual': None,
    'mostrar_validacao_visual': False,
    'fila_usuarios': {},
    'minha_posicao_fila': None,
    'validacao_backend_concluida': False,
    'resultados_validacao': {},
}

def verificar_trava_instancia(instancia):
    ocupadas = st.session_state.get('instancias_ocupadas', {})
    if ocupadas.get(instancia, False):
        return False, "Instância em uso"
    return True, None

def definir_instancia_ocupada(instancia, ocupada=True):
    if 'instancias_ocupadas' not in st.session_state:
        st.session_state.instancias_ocupadas = {}
    st.session_state.instancias_ocupadas[instancia] = ocupada
    st.session_state.minha_instancia_atual = instancia if ocupada else None

# ==============================================
# SISTEMA DE FILA
# ==============================================
def verificar_fila(instancia):
    agora = time.time()
    fila = st.session_state.get('fila_usuarios', {})
    fila_limpa = {k: v for k, v in fila.items() if (agora - v) < TIMEOUT_FILA}
    st.session_state.fila_usuarios = fila_limpa

    if instancia in fila_limpa:
        st.session_state.fila_usuarios[instancia] = agora
        return True, 1, len(fila_limpa), "Processando"

    if len(fila_limpa) < MAX_USUARIOS_SIMULTANEOS:
        st.session_state.fila_usuarios[instancia] = agora
        return True, 1, len(fila_limpa) + 1, "Entrou na fila"

    instancias_ordenadas = sorted(fila_limpa.items(), key=lambda x: x[1])
    posicao = len([i for i, t in instancias_ordenadas if t < fila_limpa.get(instancia, agora)]) + 1
    return False, posicao, len(fila_limpa), f"Fila cheia. Sua posição: {posicao}"

def sair_da_fila(instancia):
    if instancia in st.session_state.get('fila_usuarios', {}):
        del st.session_state.fila_usuarios[instancia]

for key, val in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val

# ==============================================
# FUNÇÕES
# ==============================================
def salvar_no_google_sheets(df, sheet_id, instancia, aba_base=ABA_GOOGLE_SHEETS):
    try:
        escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credenciais = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", escopo)
        cliente = gspread.authorize(credenciais)
        planilha = cliente.open_by_key(sheet_id)

        nome_aba = f"{aba_base} - {instancia}"

        try:
            worksheet = planilha.worksheet(nome_aba)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = planilha.add_worksheet(title=nome_aba, rows="2000", cols="20")

        df_para_sheets = df.copy()
        for col in df_para_sheets.columns:
            if str(df_para_sheets[col].dtype).startswith("datetime"):
                df_para_sheets[col] = pd.to_datetime(df_para_sheets[col]).dt.strftime("%d/%m/%Y")

        df_para_sheets = df_para_sheets.fillna("").astype(str)

        for col_name in ["Mensagem Gerada", "Status Envio WA"]:
            if col_name not in df_para_sheets.columns:
                df_para_sheets[col_name] = "" if col_name == "Mensagem Gerada" else "Pendente"

        worksheet.clear()
        dados_lista = [df_para_sheets.columns.values.tolist()] + df_para_sheets.values.tolist()
        worksheet.update(dados_lista, value_input_option="RAW")

        return True, nome_aba

    except Exception as e:
        st.error(f"❌ Erro ao salvar no Google Sheets: {str(e)}")
        return False, None

def carregar_mensagens_do_sheets(sheet_id, instancia, aba_base=ABA_GOOGLE_SHEETS):
    try:
        escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credenciais = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", escopo)
        cliente = gspread.authorize(credenciais)

        nome_aba = f"{aba_base} - {instancia}"

        try:
            worksheet = cliente.open_by_key(sheet_id).worksheet(nome_aba)
        except gspread.exceptions.WorksheetNotFound:
            return []

        dados = worksheet.get_all_records()

        if not dados or not isinstance(dados, list):
            return []

        try:
            df = pd.DataFrame(dados)
        except Exception:
            return []

        if df.empty:
            return []

        df.columns = df.columns.astype(str).str.strip()

        col_nome = None
        col_telefone = None
        col_mensagem = None
        col_cliente = None

        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower in ['nome', 'name', 'cliente_nome']:
                col_nome = col
            elif col_lower in ['telefone', 'tel', 'fone', 'whatsapp', 'numero', 'phone', 'whats']:
                col_telefone = col
            elif col_lower in ['mensagem gerada', 'mensagem', 'message', 'msg', 'texto']:
                col_mensagem = col
            elif col_lower in ['cliente', 'codigo', 'codigo_cliente', 'id_cliente', 'código', 'cod_cliente']:
                col_cliente = col

        if col_mensagem is None:
            return []

        mensagens = []
        for _, row in df.iterrows():
            try:
                mensagem_texto = str(row.get(col_mensagem, '')) if col_mensagem else ''
                if mensagem_texto and mensagem_texto.strip() and mensagem_texto.lower() not in ['nan', 'none', '']:
                    mensagem = {
                        'nome': str(row.get(col_nome, ''))[:100] if col_nome else '',
                        'telefone': str(row.get(col_telefone, ''))[:20] if col_telefone else '',
                        'mensagem': mensagem_texto[:2000],
                        'codigo_cliente': str(row.get(col_cliente, ''))[:50] if col_cliente else '',
                    }
                    mensagens.append(mensagem)
            except Exception:
                continue

        return mensagens

    except Exception:
        return []

def normalizar_telefone_instancia(telefone):
    nums = re.sub(r'\D', '', telefone)
    if not nums.startswith('55') and len(nums) in (10, 11):
        nums = '55' + nums
    if len(nums) == 12:
        nums = nums[:4] + '9' + nums[4:]
    return nums

def check_status(instance):
    headers = {"apikey": EVOLUTION_API_KEY}
    try:
        res = requests.get(
            f"{EVOLUTION_API_URL}/instance/connectionState/{instance}",
            headers=headers,
            timeout=5
        )
        if res.status_code == 200:
            try:
                return res.json().get("instance", {}).get("state")
            except Exception:
                return "error"
        return "not_found"
    except requests.exceptions.RequestException:
        return "error"

def toggle_all_messages_selection():
    st.session_state.selecionar_todos = st.session_state.master_select_all_checkbox_key

# ==============================================
# INTERFACE
# ==============================================
st.set_page_config(page_title="Cobra AI", layout="wide")

with st.sidebar:
    st.header("Acesso")

    tel_input = st.text_input(
        "Telefone (DDD + Número)",
        value=st.session_state.get('tel_corporativo_input', ''),
        placeholder="Ex: 11 99999-9999",
        key="tel_corporativo_input"
    )

    tel_limpo = normalizar_telefone_instancia(tel_input) if tel_input else ""

    if len(tel_limpo) == 13:
        st.caption(f"ID: {tel_limpo}")
        st.session_state.tel_corporativo = tel_limpo

        status = check_status(tel_limpo)

        if status == "open":
            st.success("✅ WhatsApp Conectado")
            st.session_state.is_connected = True
        elif status == "connecting":
            st.info("🔄 Conectando...")
            st.session_state.is_connected = False
        else:
            st.warning("⚠️ Desconectado")
            st.session_state.is_connected = False

            if st.button("🔌 Conectar (QR Code)"):
                with st.spinner("Preparando..."):
                    headers = {"apikey": EVOLUTION_API_KEY, "Content-Type": "application/json"}

                    if status == "not_found":
                        try:
                            requests.post(
                                f"{EVOLUTION_API_URL}/instance/create",
                                json={"instanceName": tel_limpo, "qrcode": True, "integration": "WHATSAPP-BAILEYS"},
                                headers=headers,
                                timeout=10
                            )
                        except requests.exceptions.RequestException:
                            st.error("Erro ao tentar criar instância.")
                            st.stop()

                    try:
                        res_qr = requests.get(
                            f"{EVOLUTION_API_URL}/instance/connect/{tel_limpo}",
                            headers=headers,
                            timeout=10
                        )
                        if res_qr.status_code == 200:
                            try:
                                base64_qr = res_qr.json().get("base64", "").replace("data:image/png;base64,", "")
                            except Exception:
                                base64_qr = ""
                            if base64_qr:
                                st.image(base64.b64decode(base64_qr))
                                if st.button("🔄 Atualizar Status"):
                                    st.rerun()
                            else:
                                st.error("Não foi possível gerar QR Code.")
                        else:
                            st.error(f"Erro ao conectar: {res_qr.status_code}")
                    except requests.exceptions.RequestException as e:
                        st.error(f"Erro na API de conexão: {e}")
    elif tel_input:
        st.error("Número inválido")
        st.session_state.is_connected = False

if not st.session_state.tel_corporativo or not st.session_state.is_connected:
    st.title("Cobra AI")
    st.info("👈 Informe o WhatsApp corporativo.")
    st.stop()

# ==============================================
# CONTEÚDO PRINCIPAL
# ==============================================
st.title("Cobra AI")
st.caption(f"ID: 📱 {st.session_state.tel_corporativo}")

# Etapa 1: Upload
st.subheader("📁 1. Envie sua planilha")
with st.expander("Upload", expanded=False):
    uploaded_file = st.file_uploader(
        "Selecione o arquivo Excel ou CSV",
        type=["xlsx", "csv"],
        help="📋 Sua planilha precisa conter as seguintes colunas:\n\n"
            "• Cliente: 12345678 \n"
            "• Nome: Empório do Zé\n"
            "• Valor: 250.75\n"
            "• Vencimento: 12/12/2025\n"
            "• Telefone: 11999999999"
        ,
    )

if not uploaded_file:
    st.stop()

# Barra de progresso apenas para novo arquivo
# Logo após o upload, antes da validação
if st.session_state.get('ultimo_arquivo') != uploaded_file.name:
    # Novo arquivo: lê e valida colunas
    df = (
        pd.read_excel(uploaded_file, dtype=str)
        if uploaded_file.name.endswith(".xlsx")
        else pd.read_csv(uploaded_file, dtype=str)
    )
    
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.replace("", pd.NA)
    df = df.dropna(how="all")


    colunas_obrigatorias = ["Cliente", "Nome", "Valor", "Vencimento", "Telefone"]
    colunas_faltantes = [c for c in colunas_obrigatorias if c not in df.columns]

    if colunas_faltantes:
        st.error(f"❌ Colunas faltantes: {', '.join(colunas_faltantes)}")
        st.info("""
     📋 **Formato correto da planilha**

     **Cliente** → Código único do cliente (8 dígitos iniciais do CNPJ/CPF)

     **Nome** → Nome da pessoa ou razão social da empresa  
     
     **Valor** → Valor do boleto em aberto
     """)
        st.write("Colunas encontradas:", df.columns.tolist())
        st.stop()
  

    if 'Código_Cliente' in df.columns:
        clientes_unicos = df['Código_Cliente'].nunique()
    elif 'Cliente' in df.columns:
        clientes_unicos = df['Cliente'].nunique()
    else:
        clientes_unicos = len(df)
    # Valida limite de linhas
    if len(df) > MAX_REGISTROS:
        st.error(
            f"⚠️ Sua planilha possui **{len(df)} linhas**, o limite é **{MAX_REGISTROS}**. "
            f"Divida a planilha em partes menores e tente novamente."
        )
        st.stop()

    # Valida limite de clientes distintos
    if clientes_unicos > MAX_CLIENTES:
        st.error(
            f"⚠️ Sua planilha possui **{clientes_unicos}** clientes distintos, o limite é de **{MAX_CLIENTES}**. "
            f"Divida a planilha em partes menores e tente novamente."
        )
        st.stop()    

else:
    df = (
        pd.read_excel(uploaded_file, dtype=str)
        if uploaded_file.name.endswith(".xlsx")
        else pd.read_csv(uploaded_file, dtype=str)
    )

    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.replace("", pd.NA)
    df = df.dropna(how="all")

    colunas_obrigatorias = ["Cliente", "Nome", "Valor", "Vencimento", "Telefone"]
    colunas_faltantes = [c for c in colunas_obrigatorias if c not in df.columns]

    if colunas_faltantes:
        st.error(f"❌ Colunas faltantes: {', '.join(colunas_faltantes)}")
        st.write("Colunas encontradas:", df.columns.tolist())
        st.stop()

    if 'Código_Cliente' in df.columns:
        clientes_unicos = df['Código_Cliente'].nunique()
    elif 'Cliente' in df.columns:
        clientes_unicos = df['Cliente'].nunique()
    else:
        clientes_unicos = len(df)
        
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.replace("", pd.NA)
    df = df.dropna(how="all")    

# Inicializa estado para validação em background (SEMPRE EXECUTA)
if 'validacao_backend_concluida' not in st.session_state or st.session_state.get('ultimo_arquivo') != uploaded_file.name:
    st.session_state.validacao_backend_concluida = False
    st.session_state.resultados_validacao = {}
    st.session_state.indice_validacao = 0
    st.session_state.ultimo_arquivo = uploaded_file.name

    numeros_brutos = df['Telefone'].dropna().astype(str).str.replace('.0', '', regex=False).str.strip().tolist()
    st.session_state.lista_numeros = list(set(numeros_brutos))

coluna_telefone = None
for col in df.columns:
    if "tel" in col.lower() or "fone" in col.lower() or "whats" in col.lower():
        coluna_telefone = col
        break

if coluna_telefone is None and 'Telefone' in df.columns:
    coluna_telefone = 'Telefone'

    
# ==========================================
# VALIDAÇÃO EM BACKGROUND (SEMPRE EXECUTA)
# ==========================================
# SUBSTITUA o bloco de validação em background por este:
if not st.session_state.validacao_backend_concluida:
    total_numeros = len(st.session_state.lista_numeros)
    inicio = st.session_state.indice_validacao
    fim = min(inicio + 5, total_numeros)

    # Barra de progresso real vinculada à validação
    pct = int((inicio / total_numeros) * 100) if total_numeros > 0 else 0
    texto_progresso = f"🔍 Analisando dados enviados... {pct}%"
    
    st.progress(pct, text=texto_progresso)

    if inicio < total_numeros:
        lote = st.session_state.lista_numeros[inicio:fim]

        for numero in lote:
            chave = re.sub(r'\D', '', str(numero))
            if not chave or chave in st.session_state.resultados_validacao:
                continue

            try:
                numero_api = chave
                if not numero_api.startswith('55') and len(numero_api) in (10, 11):
                    numero_api = '55' + numero_api
                if len(numero_api) == 12:
                    numero_api = numero_api[:4] + '9' + numero_api[4:]

                url_check = f"{EVOLUTION_API_URL}/chat/whatsappNumbers/{st.session_state.tel_corporativo}"
                payload = {"numbers": [numero_api]}
                headers = {"apikey": EVOLUTION_API_KEY, "Content-Type": "application/json"}

                resp = requests.post(url_check, json=payload, headers=headers, timeout=5)

                valido = False
                if resp.status_code in [200, 201]:
                    try:
                        dados = resp.json()
                        if isinstance(dados, list) and len(dados) > 0:
                            valido = dados[0].get('exists', False)
                    except Exception:
                        valido = False

                st.session_state.resultados_validacao[chave] = {'valido': valido}

            except Exception:
                st.session_state.resultados_validacao[chave] = {'valido': False}

            time.sleep(0.2)

        st.session_state.indice_validacao = fim
        st.rerun()

    else:
        st.session_state.validacao_backend_concluida = True
        st.rerun()

# ==========================================
# EXIBIÇÃO FINAL COM VALIDAÇÃO VISUAL OPCIONAL
# ==========================================
if st.session_state.validacao_backend_concluida and coluna_telefone:
    total_registros = len(df)
    validos_count = 0
    invalidos_count = 0

    def get_status_bool(tel):
        chave = re.sub(r'\D', '', str(tel or ""))
        return st.session_state.resultados_validacao.get(chave, {}).get('valido', False)

    for tel in df[coluna_telefone]:
        if get_status_bool(tel):
            validos_count += 1
        else:
            invalidos_count += 1

    st.success(f"✅ Planilha carregada com sucesso! {total_registros} registros encontrados.")
# Toggle apenas para exibição visual
    st.session_state.mostrar_validacao_visual = st.toggle(
        "🔍 WhatsApp?",
        value=st.session_state.mostrar_validacao_visual,
        help="Destaca em vermelho os números sem WhatsApp."
    )
    if st.session_state.mostrar_validacao_visual and invalidos_count > 0:
        st.caption(f"⚠️ {invalidos_count} número(s) sem WhatsApp.")

    MAX_MENSAGENS = 500
    if validos_count > MAX_MENSAGENS:
        st.error(f"⚠️ Limite de {MAX_MENSAGENS} mensagens excedido. Você tem {validos_count} válidos.")
        st.stop()

    if validos_count == 0:
        st.error("❌ Nenhum telefone válido encontrado.")
        st.stop()

    def destacar_celula_numero(val):
        if not st.session_state.mostrar_validacao_visual:
            return ""
        chave = re.sub(r'\D', '', str(val))
        info = st.session_state.resultados_validacao.get(chave, {})
        if not info.get('valido', False):
            return "background-color: rgba(255, 80, 80, 0.2);"
        return ""

    if st.session_state.mostrar_validacao_visual:
        styled_df = df.style.applymap(destacar_celula_numero, subset=[coluna_telefone])
        st.write("📋 **Preview dos Dados:** (Números sem WhatsApp destacados em vermelho)")
    else:
        styled_df = df
        st.write("📋 **Preview dos Dados:**")

    st.dataframe(styled_df, use_container_width=True, hide_index=True, height=500)
    st.divider()

# Etapa 2: Tom da mensagem
st.subheader("⚙️ 2. Configurar Tom da Mensagem")
template = st.selectbox("Selecione o tom", ["Empático", "Formal", "Urgente"], index=0)

st.divider()

# Etapa 3: Gerar
st.subheader("🪄 3. Gerar mensagens personalizadas")

pode_gerar = not st.session_state.processo_iniciado and not st.session_state.geracao_finalizada
instancia_atual = st.session_state.tel_corporativo

# Verifica fila de lotação
pode_prosseguir, posicao, total_fila, msg_fila = verificar_fila(instancia_atual)

if not pode_prosseguir:
    st.info(f"⏳ {msg_fila}")
    st.progress(0)
    st.caption(f"Capacidade: {total_fila}/{MAX_USUARIOS_SIMULTANEOS} usuários simultâneos")
    time.sleep(3)
    st.rerun()

st.markdown("""
<style>
div.stButton > button:first-child {
    background-color: #ADD8E6;
    color: black;
    border-radius: 10px;
    border: none;
}
div.stButton > button:first-child:hover {
    background-color: #5F9EA0;
}
</style>
""", unsafe_allow_html=True)


if st.button("Gerar mensagens agora", type="primary", use_container_width=True, disabled=not pode_gerar):
    if len(df) > MAX_REGISTROS:
        st.warning(f"⚠️ Sua planilha tem {len(df)} registros, mas o limite por execução é {MAX_REGISTROS}.")
        st.stop()

    st.session_state.fila_usuarios[instancia_atual] = time.time()
    definir_instancia_ocupada(instancia_atual, True)

    # SEMPRE filtra apenas números válidos (independente do toggle visual)
    def eh_valido(tel):
        chave = re.sub(r'\D', '', str(tel or ""))
        return st.session_state.resultados_validacao.get(chave, {}).get('valido', False)

    df_filtrado = df[df['Telefone'].apply(eh_valido)].copy()

    if len(df_filtrado) == 0:
        sair_da_fila(instancia_atual)
        definir_instancia_ocupada(instancia_atual, False)
        st.error("❌ Nenhum número válido encontrado na planilha.")
        st.stop()

    # Salva no Google Sheets sem spinner
    sucesso, nome_aba_usada = salvar_no_google_sheets(df_filtrado, ID_PLANILHA_GOOGLE, instancia_atual)
    if not sucesso:
        sair_da_fila(instancia_atual)
        definir_instancia_ocupada(instancia_atual, False)
        st.info("💡 Verifique se o credentials.json está na pasta.")
        st.stop()

    # Aciona o webhook sem spinner
    try:
        resp = requests.post(
            URL_WEBHOOK_N8N_GERAR,
            json={
                "tom_mensagem": template.lower(),
                "total_clientes": len(df_filtrado),
                "data_execucao": datetime.now().isoformat(),
                "data_hoje": datetime.now().strftime("%d/%m/%Y"),
                "aba_google_sheets": nome_aba_usada,
                "remetente": instancia_atual
            },
            headers={
                "Content-Type": "application/json",
                "X-Chave-Secreta": CHAVE_SECRETA_N8N
            },
            timeout=(10, 60)  # (conexão, leitura)
        )

        resposta_n8n = None
        if resp.status_code == 200:
            try:
                resposta_n8n = resp.json()
            except Exception:
                resposta_n8n = {"total_mensagens_previstas": len(df_filtrado)}
        else:
            sair_da_fila(instancia_atual)
            definir_instancia_ocupada(instancia_atual, False)
            st.error(f"❌ Erro ao iniciar: Status {resp.status_code}")
            st.stop()

        if isinstance(resposta_n8n, list) and len(resposta_n8n) > 0:
            resposta_n8n = resposta_n8n[0]
        elif not isinstance(resposta_n8n, dict):
            resposta_n8n = {"total_mensagens_previstas": len(df_filtrado)}

        total_previsto = resposta_n8n.get('total_mensagens_previstas', len(df_filtrado))
        if total_previsto is None or not isinstance(total_previsto, (int, float)):
            total_previsto = len(df_filtrado)

        st.session_state.total_mensagens_previstas = int(total_previsto)
        st.session_state.processo_iniciado = True
        st.session_state.contagem_anterior = 0
        st.session_state.segundos_sem_mudanca = 0
        st.session_state.ultima_atualizacao = time.time()
        st.session_state.progress_step = 0
        st.session_state.mensagens_recebidas = []
        st.rerun()

    except Exception as e:
        sair_da_fila(instancia_atual)
        definir_instancia_ocupada(instancia_atual, False)
        st.error(f"❌ Erro inesperado: {str(e)}")
        st.code(traceback.format_exc())

# ==============================================
# POLLING
# ==============================================
processo_iniciado = st.session_state.get('processo_iniciado', False)
geracao_finalizada = st.session_state.get('geracao_finalizada', False)
mensagens_recebidas = st.session_state.get('mensagens_recebidas', []) or []
total_previsto = st.session_state.get('total_mensagens_previstas', 999999) or 999999

condicao_polling = processo_iniciado or (
    geracao_finalizada and
    isinstance(mensagens_recebidas, list) and
    len(mensagens_recebidas) < total_previsto
)

if condicao_polling:
    if instancia_atual in st.session_state.get('fila_usuarios', {}):
        st.session_state.fila_usuarios[instancia_atual] = time.time()

    try:
        mensagens = carregar_mensagens_do_sheets(ID_PLANILHA_GOOGLE, instancia_atual)
        if not isinstance(mensagens, list):
            mensagens = []
        st.session_state.mensagens_recebidas = mensagens
    except Exception:
        mensagens = st.session_state.get('mensagens_recebidas', []) or []
        st.session_state.mensagens_recebidas = mensagens

    total_gerado = len(mensagens) if isinstance(mensagens, list) else 0
    total_esperado = st.session_state.get("total_mensagens_previstas", len(df)) or 1

    if total_esperado > 0 and total_gerado >= total_esperado:
        st.session_state.processo_iniciado = False
        st.session_state.geracao_finalizada = True
        sair_da_fila(instancia_atual)
        definir_instancia_ocupada(instancia_atual, False)
        time.sleep(3)
        st.rerun()
    else:
        st.info(f"✨ IA escrevendo mensagens ... ({total_gerado}/{total_esperado})")
        if total_esperado > 0:
            progresso = min(100, int((total_gerado / total_esperado) * 100))
            st.progress(progresso)

        time.sleep(5)
        st.rerun()

# ==============================================
# SEÇÃO DE REVISÃO E ENVIO
# ==============================================
mensagens_recebidas = st.session_state.get('mensagens_recebidas', []) or []
geracao_finalizada = st.session_state.get('geracao_finalizada', False)

if not isinstance(mensagens_recebidas, list):
    mensagens_recebidas = []
    st.session_state.mensagens_recebidas = []

if geracao_finalizada and len(mensagens_recebidas) > 0:
    st.divider()
    st.subheader("✨ 4. Revisar e Enviar Mensagens")

    try:
        df_mensagens = pd.DataFrame(mensagens_recebidas)
    except Exception as e:
        st.error(f"❌ Erro ao processar mensagens: {str(e)}")
        st.stop()

    if df_mensagens.empty:
        st.warning("⚠️ Nenhuma mensagem para exibir.")
        st.stop()

    if 'nome' in df_mensagens.columns:
        try:
            df_mensagens = df_mensagens.sort_values('nome', ascending=True).reset_index(drop=True)
        except Exception:
            pass

    # Remove colunas desnecessárias antes de exibir
    colunas_remover = ['codigo_cliente', 'status_validacao']
    for col in colunas_remover:
        if col in df_mensagens.columns:
            df_mensagens.drop(columns=[col], inplace=True)

    # Adiciona coluna "Enviar"
    df_mensagens.insert(0, "Enviar", st.session_state.selecionar_todos)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.replace("", pd.NA)
    df = df.dropna(how="all")


    # Checkbox mestre
    st.checkbox(
        "Selecionar/Desmarcar todos",
        value=st.session_state.selecionar_todos,
        key="master_select_all_checkbox_key",
        on_change=toggle_all_messages_selection
    )


    st.write("📋 **Resumo das Mensagens:** (Dê dois cliques na mensagem para editar, se necessário. 😉)")
    altura_tabela = min(800, max(300, len(df_mensagens) * 60))


    column_config = {
        "Enviar": st.column_config.CheckboxColumn(
            "✓",
            width="small",
            help="Marque para incluir no envio"
        ),
        "nome": st.column_config.TextColumn(
            "👤 Cliente",
            disabled=True,
            width="medium"
        ),
        "telefone": st.column_config.TextColumn(
            "📱 WhatsApp",
            disabled=True,
            width="small"
        ),
        "mensagem": st.column_config.TextColumn(
            "💬 Mensagem",
            width="large",
            help="Clique duas vezes para editar",
        ),
    }

    # Remove colunas do config que não existem no DataFrame
    column_config = {k: v for k, v in column_config.items() if k in df_mensagens.columns}

    try:
        df_editado = st.data_editor(
            df_mensagens,
            column_config=column_config,
            use_container_width=True,
            hide_index=True,
            height=altura_tabela,
            num_rows="fixed",
            key="all_messages_table"
        )
    except Exception as e:
        st.error(f"❌ Erro ao exibir tabela: {str(e)}")
        st.stop()      


    total_aprovados = int(df_editado["Enviar"].sum()) if "Enviar" in df_editado.columns else 0
    st.caption(f"{total_aprovados} de {len(df_editado)} mensagens selecionadas.")
    
    st.divider()

    st.markdown("""
<style>
div.stButton > button:first-child {
    background-color: #FF4B4B;
    color: Black;
    border-radius: 10px;
    border: none;
}
div.stButton > button:first-child:hover {
    background-color: #E03E3E;
}
</style>
""", unsafe_allow_html=True)


    if st.button("🚀 Enviar Mensagens Aprovadas", use_container_width=True, disabled=total_aprovados == 0):
        df_aprovados = df_editado[df_editado["Enviar"] == True]

        with st.spinner(f"Enviando {total_aprovados} de {len(df_aprovados)} ..."):
            itens_envio = []
            for _, row in df_aprovados.iterrows():
                try:
                    itens_envio.append({
                        "destinatario": str(row.get("telefone", ""))[:20],
                        "mensagem": str(row.get("mensagem", ""))[:2000],
                        "codigo_cliente": str(row.get("codigo_cliente", ""))[:50],
                        "nome": str(row.get("nome", ""))[:100]
                    })
                except Exception:
                    continue

            if len(itens_envio) == 0:
                st.error("❌ Nenhuma mensagem válida para enviar.")
                st.stop()

            payload_envio = {
                "remetente": st.session_state.tel_corporativo,
                "itens": itens_envio,
                "intervalo_segundos": 21
            }

            try:
                resp_envio = requests.post(
                    URL_WEBHOOK_N8N_ENVIAR,
                    json=payload_envio,
                    headers={
                        "Content-Type": "application/json",
                        "X-Chave-Secreta": CHAVE_SECRETA_N8N,
                    },
                    timeout=(10,240)
                )

                if resp_envio.status_code == 200:
                    st.info(f"As mensagens serão enviadas com intervalo aleatório")
                    st.success(f"🎉 As {len(itens_envio)} mensagens foram enviadas com sucesso!")
                    st.session_state.mensagens_recebidas = []
                    st.session_state.processo_iniciado = False
                    st.session_state.geracao_finalizada = False
                    st.session_state.selecionar_todos = True
                    st.session_state.total_mensagens_previstas = 0
                    sair_da_fila(instancia_atual)
                    definir_instancia_ocupada(instancia_atual, False)
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error(f"❌ Falha ao iniciar o envio. Status: {resp_envio.status_code}")
                    try:
                        st.code(resp_envio.text[:500])
                    except Exception:
                        pass

            except requests.exceptions.RequestException as e:
                st.error(f"❌ Erro de conexão ao enviar: {str(e)}")
            except Exception as e:
                st.error(f"❌ Erro inesperado: {str(e)}")

# ==============================================
# RODAPÉ
# ==============================================
with st.expander("Precisa de ajuda?", expanded=False):
    st.markdown(f"""
    ### 📋 Colunas obrigatórias da planilha
    📋 Sua planilha precisa ter estas colunas:

| Coluna      | Descrição                                             | Exemplo         |
|-------------|-------------------------------------------------------|-----------------|
| Cliente     | Código único do cliente (8 dígitos do CNPJ ou CPF)    | 12345678        |
| Nome        | Nome do cliente ou razão social da empresa            | João Silva      |
| Valor       | Valor do boleto em aberto                             | 150,00          |
| Vencimento  | Data de vencimento do boleto                          | 28/02/2026      |
| Telefone    | WhatsApp com DDD (somente números)                    | 11999999999     |

⚠️ Atenção:
- Os nomes das colunas devem ser escritos exatamente como acima.
- Um cliente pode ter mais de um boleto em aberto (mais de uma linha).
- O campo telefone deve conter o WhatsApp do cliente.

### 🔐 Segurança
- O telefone corporativo informado no login será usado como remetente das mensagens.
- Certifique-se de que este número está conectado (status verdinho na barra lateral).
""")

st.markdown("---")
st.caption(f"Cobrança Inteligente | Atualizado em {datetime.now().strftime('%d/%m/%Y %H:%M')}")