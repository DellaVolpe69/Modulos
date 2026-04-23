from supabase import create_client, Client
import pandas as pd
import os

def conexao():
    print('Conectando ao Supabase...')

    url = os.getenv("SUPABASE_URL_TDV")
    key = os.getenv("SUPABASE_KEY_TDV")

    if not url or not key:
        raise ValueError("SUPABASE_URL_TDV ou SUPABASE_KEY_TDV não definidos.")

    supabase: Client = create_client(url, key)

    print('Conectado com sucesso')
    return supabase
