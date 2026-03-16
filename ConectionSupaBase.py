from supabase import create_client, Client
import pandas as pd

# 🔑 Suas credenciais
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

def conexao():
    # 🔗 Conectar ao Supabase
    print('Conectando ao Supabase...')
    if not url or not key:
        raise ValueError("SUPABASE_URL ou SUPABASE_KEY não definidos.")
    supabase: Client = create_client(url, key)
    print('Conectando')
    return supabase




