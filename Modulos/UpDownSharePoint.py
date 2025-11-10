import streamlit as st
import pandas as pd
#from requests_oauthlib import OAuth2Session
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
import io
from datetime import datetime

# Configurações do Azure AD OAuth2
client_id = "9f155302-2159-4d3f-b2d2-54e16df7f77d"
client_secret = "~3f8Q~MBowQURXaAWcuHaeVXkWLm1BucaVivPckQ"
redirect_uri = "https://cadjanelas.streamlit.app/"
authorization_base_url = "https://login.microsoftonline.com/common/oauth2/v2.0/authorize"
token_url = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
scope = ["openid", "email", "profile", "https://graph.microsoft.com/User.Read"]

# SharePoint configs
site_url = "https://dellavolpecombr.sharepoint.com/sites/DellaVolpe"
username = "alexandre.sousa@dellavolpe.com.br"
password = "TDV|10203040acs"
relative_file_url = "/sites/DellaVolpe/Documentos Compartilhados/Alexandre Sousa/St/JanelasDeEmbarque.xlsx"

# Função para ler o Excel do SharePoint (ou criar vazio com as colunas certas)
def ler_xlsx_sharepoint():
    ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))
    file = io.BytesIO()
    ctx.web.get_file_by_server_relative_url(relative_file_url).download(file).execute_query()
    file.seek(0)
    
    df = pd.read_excel(file)
    
print('Oi')

ler_xlsx_sharepoint