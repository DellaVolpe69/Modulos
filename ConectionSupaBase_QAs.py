from supabase import create_client, Client
import pandas as pd

# 🔑 Suas credenciais
url = "https://hnppkctmiqrcobkzajzl.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhucHBrY3RtaXFyY29ia3phanpsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjAwODAzMjgsImV4cCI6MjA3NTY1NjMyOH0.VR9ibC2tu4tQ6GeVoHwrNE7MOI_f3H5KYvo5l4ryZuc"  # normalmente a anon key

def conexao():
    # 🔗 Conectar ao Supabase
    print('Conectando ao Supabase...')
    supabase: Client = create_client(url, key)
    print('Conectando')
    return supabase





