from supabase import create_client, Client
import pandas as pd

# 🔑 Suas credenciais
url = "https://hnppkctmiqrcobkzajzl.supabase.co"
key = ""

def conexao():
    # 🔗 Conectar ao Supabase
    print('Conectando ao Supabase...')
    supabase: Client = create_client(url, key)
    print('Conectando')
    return supabase








