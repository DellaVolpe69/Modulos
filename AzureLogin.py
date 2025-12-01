import streamlit as st
import pandas as pd
import os
import sys
from datetime import date
from pathlib import Path, PureWindowsPath
import itertools
import subprocess

# ================================================
# CARREGAR MÓDULOS E PARQUETS
# ================================================
# Caminho local onde o módulo será baixado
modulos_dir = Path(__file__).parent / "Modulos"
# Se o diretório ainda não existir, faz o clone direto do GitHub
if not modulos_dir.exists():
    print("📥 Clonando repositório Modulos do GitHub...")
    subprocess.run([
        "git", "clone",
        "https://github.com/DellaVolpe69/Modulos.git",
        str(modulos_dir)
    ], check=True)
# Garante que o diretório está no caminho de importação
if str(modulos_dir) not in sys.path:
    sys.path.insert(0, str(modulos_dir))
# Agora importa o módulo normalmente
from Modulos import AzureLogin
from Modulos import ConectionSupaBase

###################################
# Carregar Parquet do Minio
###################################
from Modulos.Minio.examples.MinIO import read_file  # ajuste o caminho se necessário

@st.cache_data(show_spinner="Carregando CV_FILIAL...")
def load_cv_bp():
    return read_file('dados/CV_FILIAL.parquet', 'calculation-view')

try:
    df_filial = load_cv_bp()
except Exception as e:
    st.error(f"Erro ao carregar df_filial: {e}")
    st.stop()

# 🔗 Conexão com o Supabase
supabase = ConectionSupaBase.conexao()

# ================================================
# CORREÇÃO JSON
# ================================================
def converter_tipos_json(d):
    novo = {}
    for k, v in d.items():
        if hasattr(v, "item"):
            novo[k] = v.item()
        elif isinstance(v, date):
            novo[k] = v.isoformat()
        else:
            novo[k] = v
    return novo

# ================================================
# SUPABASE — REGISTROS
# ================================================
def carregar_registros_supabase():
    resposta = supabase.table("RNC_Registros").select("*").execute()
    if resposta.data:
        df = pd.DataFrame(resposta.data)
        # Converter strings de data de volta para objetos date se necessário
        if 'data_envio_filial' in df.columns:
            df['data_envio_filial'] = pd.to_datetime(df['data_envio_filial'], errors='coerce').dt.date
        if 'data_retorno' in df.columns:
            df['data_retorno'] = pd.to_datetime(df['data_retorno'], errors='coerce').dt.date
        if 'data_encerramento' in df.columns:
            df['data_encerramento'] = pd.to_datetime(df['data_encerramento'], errors='coerce').dt.date
        return df
    else:
        return pd.DataFrame(columns=[
            "ID", "Motivo", "Empresa", "Filial", "oef", "nf", "valor_nota", "rnc", 
            "data_envio_filial", "status", "data_encerramento", "data_retorno", "id_acao", "excluido"
        ])

def inserir_registro_supabase(registro_dict):
    registro_dict = converter_tipos_json(registro_dict)
    supabase.table("RNC_Registros").insert(registro_dict).execute()

def atualizar_registro_supabase(id_registro, dados):
    dados = converter_tipos_json(dados)
    supabase.table("RNC_Registros").update(dados).eq("ID", id_registro).execute()

def excluir_registro_supabase(id_registro):
    supabase.table("RNC_Registros").update({"excluido": True}).eq("ID", id_registro).execute()

# ================================================
# SUPABASE — MOTIVOS
# ================================================
def carregar_motivos_supabase():
    resposta = supabase.table("RNC_Motivos").select("*").execute()
    if not resposta.data:
        return pd.DataFrame(columns=["ID", "Motivo", "excluido"])
    df = pd.DataFrame(resposta.data)
    return df

def inserir_motivo_supabase(nome):
    # Buscar o maior ID atual
    resposta = supabase.table("RNC_Motivos").select("ID").execute()
    if resposta.data:
        max_id = max([r["ID"] for r in resposta.data])
        novo_id = max_id + 1
    else:
        novo_id = 1

    novo = {"ID": novo_id, "Motivo": nome, "excluido": False}
    supabase.table("RNC_Motivos").insert(converter_tipos_json(novo)).execute()

def excluir_motivo_supabase(id_motivo):
    supabase.table("RNC_Motivos").update({"excluido": True}).eq("ID", id_motivo).execute()

# ================================================
# SESSION STATE
# ================================================
if "registros_df" not in st.session_state:
    st.session_state.registros_df = carregar_registros_supabase()
if "motivos_df" not in st.session_state:
    st.session_state.motivos_df = carregar_motivos_supabase()

# ================================================
# CONFIG VISUAL
# ================================================
# --- LINK DIRETO DA IMAGEM NO GITHUB ---
url_imagem = "https://raw.githubusercontent.com/DellaVolpe69/Images/main/AppBackground02.png"
url_logo = "https://raw.githubusercontent.com/DellaVolpe69/Images/main/DellaVolpeLogoBranco.png"
fox_image = "https://raw.githubusercontent.com/DellaVolpe69/Images/main/Foxy4.png"

st.markdown(
    f"""
    <style>
    /* Remove fundo padrão dos elementos de cabeçalho que às vezes 'brigam' com o BG */
    header, [data-testid="stHeader"] {{
        background: transparent;
    }}
    </style>
    """,
    unsafe_allow_html=True
)

###### CONFIGURAR O TÍTULO DA PÁGINA #######
st.set_page_config(
    page_title="Formulário RNC",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown(f"""
    <style>
        [data-testid="stAppViewContainer"] {{
            background: linear-gradient(rgba(0,0,0,0.7), rgba(0,0,0,0.7)),
                        url("{url_imagem}");
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
        }}
        /* Inputs padrão: text_input, number_input, date_input, etc */
        input, textarea {{
            border: 1px solid white !important;
            border-radius: 5px !important;
        }}

        /* Selectbox (parte fechada) */
        .stSelectbox div[data-baseweb="select"] > div {{
            border: 1px solid white !important;
            border-radius: 5px !important;
        }}

        /* Date input container */
        .stDateInput input {{
            border: 1px solid white !important;
            border-radius: 5px !important;
        }}
        .stButton > button {{
            background-color: #FF5D01 !important;
            color: white !important;
            border: 2px solid white !important;
            padding: 0.6em 1.2em;
            border-radius: 10px !important;
            font-size: 1rem;
            font-weight: 500;
            font-color: white !important;
            cursor: pointer;
            transition: 0.2s ease;
            text-decoration: none !important;   /* 👈 AQUI remove de vez */
            display: inline-block;
        }}
        .stButton > button:hover {{
            background-color: white !important;
            color: #FF5D01 !important;
            transform: scale(1.03);
            font-color: #FF5D01 !important;
            border: 2px solid #FF5D01 !important;
        }}
        /* RODAPÉ FIXO */
        .footer {{
            position: fixed;
            left: 0;
            bottom: 0;
            width: 100%;
            background: rgba(0, 0, 0, 0.6);
            color: white;
            text-align: center;
            font-size: 14px;
            padding: 8px 0;
            text-shadow: 1px 1px 2px black;
        }}
        .footer a {{
            color: #FF5D01;
            text-decoration: none;
            font-weight: bold;
        }}
        .footer a:hover {{
            text-decoration: underline;
        }}
    </style>
""", unsafe_allow_html=True)

# --- FUNÇÃO DE RODAPÉ ---
def rodape():
    st.markdown("""
        <div class="footer">
            © 2025 <b>Della Volpe</b> | Desenvolvido por <a href="#">Alan de Souza Bezerra</a>
        </div>
    """, unsafe_allow_html=True)

rodape()

# ================================================
# MENU
# ================================================
st.sidebar.title("📂 Menu")

# =============================
# EXIBIR USUÁRIO AUTENTICADO
# =============================
nome, email = AzureLogin.get_logged_user() if hasattr(AzureLogin, 'get_logged_user') else (None, None)
if nome and email:
    st.sidebar.markdown(f"**Usuário:** {nome}<br>**E-mail:** {email}", unsafe_allow_html=True)
else:
    st.sidebar.warning("Nenhum usuário autenticado.")

menu = st.sidebar.radio(
    "Navegação",
    ["Novo Registro", "Listar Registros", "Editar Registro", "Excluir Registro", "Adicionar Motivo"]
)

# ================================================
# LISTAR
# ================================================
if menu == "Listar Registros":
    st.title("📋 Lista de Registros")
    # Campo de busca
    col1, col2 = st.columns([4, 1])
    with col1:
        busca = st.text_input("Digite ID (numérico) ou RNC (alfanumérico) para buscar:")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento para alinhar
        buscar_btn = st.button("🔍 Buscar")
    df_visivel = st.session_state.registros_df[
        st.session_state.registros_df["excluido"] == False
    ].drop(columns=["excluido"]).sort_values(by="ID", ascending=True)
    if df_visivel.empty:
        st.info("Nenhum registro cadastrado ainda.")
    else:
        # Aplicar filtro se houver busca
        if buscar_btn and busca.strip() != "":
            if busca.isdigit():
                # Busca por ID
                df_filtrado = df_visivel[df_visivel["ID"] == int(busca)]
                if df_filtrado.empty:
                    st.warning(f"⚠️ Nenhum registro encontrado com ID: {busca}")
                else:
                    st.success(f"✅ Encontrado {len(df_filtrado)} registro(s) com ID: {busca}")
                    st.dataframe(df_filtrado, use_container_width=True, hide_index=True)
            else:
                # Busca por RNC (alfanumérico)
                df_filtrado = df_visivel[df_visivel["rnc"].str.contains(busca, case=False, na=False)]
                if df_filtrado.empty:
                    st.warning(f"⚠️ Nenhum registro encontrado com RNC contendo: {busca}")
                else:
                    st.success(f"✅ Encontrado {len(df_filtrado)} registro(s) com RNC contendo: {busca}")
                    st.dataframe(df_filtrado, use_container_width=True, hide_index=True)
        else:
            # Mostrar todos os registros sem índice, ordenado por ID
            st.dataframe(df_visivel, use_container_width=True, hide_index=True)

# ================================================
# NOVO REGISTRO
# ================================================

elif menu == "Novo Registro":
    st.title("📝 Formulário RNC")
    
    # Campo Empresa
    empresa = st.selectbox('Empresa:', ['Raizen', 'Oxiteno', 'Outros'])
    
    # Ordena as filiais antes de exibir no dropdown
    filiais_ordenadas = df_filial.sort_values(by="TXTMD_1")
    opcao2 = st.selectbox('Escolha uma Filial:', filiais_ordenadas["TXTMD_1"])
    
    # Ordena os motivos antes de exibir no dropdown
    motivos_visiveis = st.session_state.motivos_df[st.session_state.motivos_df["excluido"] == False]
    motivos_ordenados = motivos_visiveis.sort_values(by="Motivo")
    opcao1 = st.selectbox('Escolha o Motivo:', motivos_ordenados["Motivo"].tolist())
    
    # Campos em colunas para melhor organização
    col1, col2 = st.columns(2)
    
    with col1:
        oef = st.text_input("Número OEF:")
        nf = st.text_input("Número NF:")
        valor_nota = st.text_input("Valor Da Nota:")
        rnc = st.text_input("RNC:")
    
    with col2:
        id_acao = st.text_input("ID da Ação:")
        data_envio = st.date_input("Data do Envio para a Filial:", value=None, format="DD/MM/YYYY")
        status = st.text_input("Status:")
        data_encerramento = st.date_input("Data de Encerramento:", value=None, format="DD/MM/YYYY")
        data_retorno = st.date_input("Data Reprovação:", value=None, format="DD/MM/YYYY")
    
    if st.button('Enviar'):
        # Validações dos campos obrigatórios
        if not oef or not nf or not valor_nota or not rnc:
            st.error("❌ Os campos OEF, NF, Valor da Nota e RNC são obrigatórios!")
        else:
            # Validação do OEF (apenas números inteiros)
            try:
                int(oef)
            except ValueError:
                st.error("❌ O número OEF deve conter apenas números inteiros!")
                st.stop()
            
            # Validação do NF (apenas números inteiros)
            try:
                int(nf)
            except ValueError:
                st.error("❌ O número NF deve conter apenas números inteiros!")
                st.stop()
            
            # Validação e conversão do valor da nota (aceita formato brasileiro)
            try:
                valor_nota_convertido = valor_nota.replace('.', '').replace(',', '.')
                valor_nota_float = float(valor_nota_convertido)
            except ValueError:
                st.error("❌ O valor da nota deve ser numérico! Use o formato brasileiro (ex: 1.234,56) ou decimal (ex: 1234.56)")
                st.stop()
            
            # Validação do ID da Ação (se preenchido, deve ser inteiro)
            if id_acao.strip() != "":
                try:
                    int(id_acao)
                except ValueError:
                    st.error("❌ O ID da Ação deve ser um número inteiro!")
                    st.stop()
            
            if not st.session_state.registros_df.empty:
                novo_id = int(st.session_state.registros_df["ID"].max()) + 1
            else:
                novo_id = 1
            
            novo_registro = {
                "ID": novo_id,
                "Motivo": opcao1,
                "Empresa": empresa,
                "Filial": opcao2,
                "oef": oef,
                "nf": nf,
                "valor_nota": valor_nota_convertido,
                "rnc": rnc,
                "data_envio_filial": data_envio if data_envio else None,
                "status": status if status.strip() != "" else None,
                "data_encerramento": data_encerramento if data_encerramento else None,
                "data_retorno": data_retorno if data_retorno else None,
                "id_acao": int(id_acao) if id_acao.strip() != "" else None,
                "excluido": False
            }
            
            st.session_state.registros_df = pd.concat(
                [st.session_state.registros_df, pd.DataFrame([novo_registro])],
                ignore_index=True
            )
            inserir_registro_supabase(novo_registro)
            st.success("✅ Dados enviados com sucesso!")
            
# ================================================
# EDITAR
# ================================================
elif menu == "Editar Registro":
    st.title("✏️ Editar Registro")
    df_validos = st.session_state.registros_df[
        st.session_state.registros_df["excluido"] == False
    ]
    if df_validos.empty:
        st.warning("Nenhum registro para editar.")
    else:
        ids = df_validos["ID"].tolist()
        id_escolhido = st.selectbox("Escolha o ID:", sorted(ids))

        registro = df_validos.loc[df_validos["ID"] == id_escolhido].iloc[0]

        # ========= EMPRESA ==========
        empresa_atual = registro.get("Empresa", "Raizen")  # Default para Raizen se não existir
        opcoes_empresa = ['Raizen', 'Oxiteno', 'Outros']
        if empresa_atual not in opcoes_empresa:
            empresa_atual = "Raizen"
        index_empresa = opcoes_empresa.index(empresa_atual)
        nova_empresa = st.selectbox(
            'Empresa:',
            opcoes_empresa,
            index=index_empresa
        )

        # ========= FILIAL  ==========
        filiais_visiveis = df_filial["TXTMD_1"].tolist()
        filiais_ordenadas = sorted(filiais_visiveis)
        filial_atual = registro["Filial"]

        if filial_atual not in filiais_ordenadas:
            st.warning(f"⚠️ A filial '{filial_atual}' não existe mais. Selecione uma nova filial.")
            filiais_ordenadas = [filial_atual] + filiais_ordenadas
            index_filial = 0
        else:
            index_filial = filiais_ordenadas.index(filial_atual)
        nova_filial = st.selectbox(
            'Nova Filial:',
            filiais_ordenadas,
            index=index_filial
        )

        # ========= MOTIVO ==========
        motivos_visiveis = st.session_state.motivos_df[
            st.session_state.motivos_df["excluido"] == False
        ]
        motivos_ordenados = motivos_visiveis.sort_values(by="Motivo")
        lista_motivos = motivos_ordenados["Motivo"].tolist()

        motivo_atual = registro["Motivo"]
        if motivo_atual not in lista_motivos:
            st.warning(f"⚠️ O motivo '{motivo_atual}' foi excluído. Selecione um novo motivo.")
            lista_motivos.insert(0, motivo_atual)
            index_motivo = 0
        else:
            index_motivo = lista_motivos.index(motivo_atual)
        novo_motivo = st.selectbox(
            'Novo Motivo:',
            lista_motivos,
            index=index_motivo
        )

        # ========= CAMPOS EM COLUNAS ==========
        col1, col2 = st.columns(2)
        
        with col1:
            novo_texto = st.text_input("Número OEF:", value=registro["oef"])
            novo_texto2 = st.text_input("Número NF:", value=registro["nf"])
            novo_texto3 = st.text_input("Valor da Nota:", value=registro["valor_nota"])
            novo_texto4 = st.text_input("RNC:", value=registro["rnc"])
        
        with col2:
            # ID Ação
            id_acao_atual = registro.get("id_acao")
            if pd.isna(id_acao_atual):
                id_acao_atual = ""
            else:
                id_acao_atual = str(int(id_acao_atual))
            novo_id_acao = st.text_input("ID da Ação:", value=id_acao_atual)
            
            # Data Envio Filial
            data_envio_atual = registro.get("data_envio_filial")
            if pd.isna(data_envio_atual):
                data_envio_atual = None
            nova_data_envio = st.date_input(
                "Data do Envio para a Filial:", 
                value=data_envio_atual,
                format="DD/MM/YYYY"
            )
            
            # Status
            status_atual = registro.get("status")
            if pd.isna(status_atual):
                status_atual = ""
            novo_status = st.text_input("Status:", value=status_atual)
            
            # Data Encerramento
            data_encerramento_atual = registro.get("data_encerramento")
            if pd.isna(data_encerramento_atual):
                data_encerramento_atual = None
            nova_data_encerramento = st.date_input(
                "Data de Encerramento:", 
                value=data_encerramento_atual,
                format="DD/MM/YYYY"
            )
            
            # Data Retorno
            data_retorno_atual = registro.get("data_retorno")
            if pd.isna(data_retorno_atual):
                data_retorno_atual = None
            nova_data_retorno = st.date_input(
                "Data Reprovação:", 
                value=data_retorno_atual,
                format="DD/MM/YYYY"
            )

        # ========= SALVAR ==========
        if st.button("Salvar Alterações"):
            # Validação do ID da Ação
            if novo_id_acao.strip() != "":
                try:
                    novo_id_acao_int = int(novo_id_acao)
                except ValueError:
                    st.error("❌ O ID da Ação deve ser um número inteiro!")
                    st.stop()
            else:
                novo_id_acao_int = None
            
            st.session_state.registros_df.loc[
                st.session_state.registros_df["ID"] == id_escolhido,
                ["Motivo", "Empresa", "Filial", "oef", "nf", "valor_nota", "rnc", 
                 "data_envio_filial", "status", "data_encerramento", "data_retorno", "id_acao"]
            ] = [novo_motivo, nova_empresa, nova_filial, novo_texto, novo_texto2, novo_texto3, novo_texto4,
                 nova_data_envio, novo_status if novo_status.strip() != "" else None, 
                 nova_data_encerramento, nova_data_retorno, novo_id_acao_int]
            
            atualizar_registro_supabase(id_escolhido, {
                "Motivo": novo_motivo,
                "Empresa": nova_empresa,
                "Filial": nova_filial,
                "oef": novo_texto,
                "nf": novo_texto2,
                "valor_nota": novo_texto3,
                "rnc": novo_texto4,
                "data_envio_filial": nova_data_envio,
                "status": novo_status if novo_status.strip() != "" else None,
                "data_encerramento": nova_data_encerramento,
                "data_retorno": nova_data_retorno,
                "id_acao": novo_id_acao_int
            })
            st.success("✅ Registro atualizado com sucesso!")

# ================================================
# EXCLUIR
# ================================================
elif menu == "Excluir Registro":
    st.title("🗑️ Excluir Registro")
    df_validos = st.session_state.registros_df[
        st.session_state.registros_df["excluido"] == False
    ]
    if df_validos.empty:
        st.warning("Nenhum registro para excluir.")
    else:
        ids = df_validos["ID"].tolist()
        id_escolhido = st.selectbox("Escolha o ID:", sorted(ids))

        if "confirmar_exclusao" not in st.session_state:
            st.session_state.confirmar_exclusao = False
        if st.button("Confirmar Exclusão"):
            st.session_state.confirmar_exclusao = True
        if st.session_state.confirmar_exclusao:
            st.error("⚠️ Esta ação é permanente e não poderá ser desfeita. Deseja realmente excluir este registro?")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Confirmar Exclusão DEFINITIVA"):
                    st.session_state.registros_df.loc[
                        st.session_state.registros_df["ID"] == id_escolhido,
                        "excluido"
                    ] = True
                    excluir_registro_supabase(id_escolhido)
                    st.session_state.confirmar_exclusao = False
                    st.success("🗑️ Registro excluído com sucesso!")
            with col2:
                if st.button("Cancelar"):
                    st.session_state.confirmar_exclusao = False
                    st.info("Operação cancelada.")

# ================================================
# ADICIONAR MOTIVO (SUPABASE)
# ================================================
elif menu == "Adicionar Motivo":
    st.title("➕ Adicionar Novo Motivo")
    entrada_texto = st.text_input("Digite o nome do novo motivo ou ID para excluir:")
    colA, colB = st.columns(2)
    with colA:
        if st.button("Adicionar Motivo"):
            if entrada_texto.strip() == "":
                st.warning("⚠️ O campo não pode estar vazio.")
            else:
                inserir_motivo_supabase(entrada_texto)
                st.session_state.motivos_df = carregar_motivos_supabase()
                st.success(f"✅ Motivo '{entrada_texto}' adicionado!")
    with colB:
        if st.button("Excluir Motivo (ID)"):
            if entrada_texto.strip() == "" or not entrada_texto.isdigit():
                st.warning("⚠️ Insira um ID válido para excluir.")
            else:
                excluir_motivo_supabase(int(entrada_texto))
                st.session_state.motivos_df = carregar_motivos_supabase()
                st.success(f"🗑️ Motivo ID {entrada_texto} excluído!")
    st.subheader("📜 Motivos atuais:")
    motivos_visiveis = st.session_state.motivos_df[st.session_state.motivos_df["excluido"] == False]
    st.dataframe(motivos_visiveis[["ID", "Motivo"]], use_container_width=True, hide_index=True)
