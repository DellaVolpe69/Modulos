import sys
import subprocess
import importlib.util
import streamlit as st
import pandas as pd
from datetime import date
from pathlib import Path, PureWindowsPath
import itertools
from requests_oauthlib import OAuth2Session
import requests
import io
import tempfile
from urllib.parse import urlsplit, quote
import os

url_imagem = "https://raw.githubusercontent.com/DellaVolpe69/Images/main/AppBackground02.png"
url_logo = "https://raw.githubusercontent.com/DellaVolpe69/Images/main/DellaVolpeLogoBranco.png"

############################################

# aplique antes do conteúdo
st.markdown(
    f"""
    <style>
    /* Fundo da aplicação inteira */
    .stApp {{
        background: linear-gradient(rgba(0,0,0,0.7), rgba(0,0,0,0.7)),
            url("{url_imagem}");
        background-size: cover;
    }}

    /* Remove fundo padrão dos elementos de cabeçalho que às vezes ‘brigam’ com o BG */
    header, [data-testid="stHeader"] {{
        background: transparent;
    }}

    /* Opcional: clareia cards/expander se você usar esses componentes */
    .stExpander, .st-emotion-cache-16idsys, .stCard {{
        background: rgba(0,0,0,0.35) !important;
    }}
    </style>
    """,
    unsafe_allow_html=True
)

# Configurações do Azure AD OAuth2
client_id = st.secrets["AZURE_CLIENT_ID"]
client_secret = st.secrets["AZURE_CLIENT_SECRET"]
redirect_uri = st.secrets["AZURE_REDIRECT_URI"]
authorization_base_url = st.secrets["AZURE_AUTH_URL"]
token_url = st.secrets["AZURE_TOKEN_URL"]
scope = [
    "openid",
    "email",
    "profile",
    "https://graph.microsoft.com/User.Read",
]

# Autenticação OAuth
if "token" not in st.session_state:
    st.session_state["token"] = None

query_params = st.query_params
if "code" in query_params and st.session_state["token"] is None:
    code = query_params["code"]
    azure = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scope)
    try:
        token = azure.fetch_token(
            token_url,
            client_secret=client_secret,
            code=code
        )
        st.session_state["token"] = token
        st.query_params.clear()
        st.rerun()
    except Exception as e:
        if "Scope has changed" in str(e):
            st.warning("Escopos alterados. É necessário iniciar um novo login.")
            st.session_state["token"] = None
            st.query_params.clear()
            azure = OAuth2Session(client_id, scope=scope, redirect_uri=redirect_uri)
            authorization_url, state = azure.authorization_url(authorization_base_url, prompt="select_account")
            st.link_button("🔐 Iniciar novo login", authorization_url)
            st.stop()
        else:
            st.error(f"Erro ao obter token: {e}")
            st.stop()

if st.session_state["token"] is None:
    
    azure = OAuth2Session(client_id, scope=scope, redirect_uri=redirect_uri)
    authorization_url, state = azure.authorization_url(authorization_base_url, prompt="select_account")

    # Linha da logo centralizada
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.image(url_logo, caption=None, use_container_width=False)
    
    # Criar espaço vazio nas laterais e centralizar os botões
    esp1, centro, esp2 = st.columns([1, 1, 1])

    with centro:        
        #st.markdown(
        #    "<h3 style='text-align: center; color: white; !important'> Tela de Acesso</h3>",
        #    unsafe_allow_html=True
        #)

        st.markdown("""
        <style>
    
        /* Botão estilo Streamlit */
        .custom-login-btn {
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
        }

        /* Hover parecido com o do Streamlit */
        .custom-login-btn:hover {
            background-color: white !important;
            color: #FF5D01 !important;
            transform: scale(1.03);
            font-color: #FF5D01 !important;
            border: 2px solid #FF5D01 !important;
        }
    
        .center-container {
            text-align: center;
            margin-top: 10px;
        }
    
        </style>
        """, unsafe_allow_html=True)
    
        st.markdown(
            f"""
            <div class="center-container">
                <a href="{authorization_url}" class="custom-login-btn">
                    🔐 Login com Microsoft
                </a>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.stop()

# Usuário autenticado
# Botão para limpar sessão se necessário
#col_logout, _ = st.columns([1, 3])
#with col_logout:
#    if st.button("Sair e relogar"):
#        st.session_state["token"] = None
#        st.query_params.clear()
#        st.rerun()
azure = OAuth2Session(client_id, token=st.session_state["token"])
me_resp = azure.get("https://graph.microsoft.com/v1.0/me")
if me_resp.status_code != 200:
    st.error(f"Falha ao obter perfil do usuário ({me_resp.status_code}): {me_resp.text}")
    st.stop()
user_info = me_resp.json()
user_email = (
    user_info.get("userPrincipalName")
    or user_info.get("mail")
    or user_info.get("userPrincipalName")
    or "desconhecido"
)
#st.success(f"Bem-vindo, {user_email}!")

# Validação de domínio do e-mail
if not isinstance(user_email, str) or not user_email.lower().endswith("@dellavolpe.com.br"):
    st.error("Acesso restrito a usuários @dellavolpe.com.br. Não foi possível validar seu e-mail.")
    st.stop()

#################################
#################################
#################################

# ---- Informações do usuário autenticado ----
me_resp = azure.get("https://graph.microsoft.com/v1.0/me")
if me_resp.status_code != 200:
    st.error(f"Falha ao obter perfil do usuário ({me_resp.status_code}): {me_resp.text}")
    st.stop()

user_info = me_resp.json()
user_name = user_info.get("displayName", "Usuário")
user_email = (
    user_info.get("mail")
    or user_info.get("userPrincipalName")
    or "desconhecido"

)

#################################
#################################
#################################

# Salva no session_state – DISPONÍVEL NO APP TODO
st.session_state["user_name"] = user_name
st.session_state["user_email"] = user_email





