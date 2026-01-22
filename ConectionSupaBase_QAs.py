from supabase import create_client, Client
import pandas as pd

# 🔑 Suas credenciais
url = "https://hnppkctmiqrcobkzajzl.supabase.co"
key = "sb_publishable_LguFtpFOOAhEouz4ToC5Bw_e9Y8t_7U"

def conexao():
    # 🔗 Conectar ao Supabase
    print('Conectando ao Supabase...')
    supabase: Client = create_client(url, key)
    print('Conectando')
    return supabase







