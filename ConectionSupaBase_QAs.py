from supabase import create_client, Client
import pandas as pd
import os

# 🔑 Suas credenciais
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

def conexao():
    # 🔗 Conectar ao Supabase
    print('Conectando ao Supabase...')
    supabase: Client = create_client(url, key)
    print('Conectando')
    return supabase
