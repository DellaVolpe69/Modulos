import streamlit as st
from requests_oauthlib import OAuth2Session

# Configurações do Azure AD OAuth2
client_id = "9f155302-2159-4d3f-b2d2-54e16df7f77d"
client_secret = "~3f8Q~MBowQURXaAWcuHaeVXkWLm1BucaVivPckQ"
redirect_uri = "https://priorizanfs.streamlit.app/"
authorization_base_url = "https://login.microsoftonline.com/common/oauth2/v2.0/authorize"
token_url = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
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
    st.link_button("🔐 Clique aqui para fazer login com Microsoft", authorization_url)
    st.stop()

# Usuário autenticado
# Botão para limpar sessão se necessário
col_logout, _ = st.columns([1, 3])
with col_logout:
    if st.button("Sair e relogar"):
        st.session_state["token"] = None
        st.query_params.clear()
        st.rerun()
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
st.success(f"Bem-vindo, {user_email}!")

# Validação de domínio do e-mail
if not isinstance(user_email, str) or not user_email.lower().endswith("@dellavolpe.com.br"):
    st.error("Acesso restrito a usuários @dellavolpe.com.br. Não foi possível validar seu e-mail.")
    st.stop()