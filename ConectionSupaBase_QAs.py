from supabase import create_client, Client
import pandas as pd

# 🔑 Suas credenciais
url = "https://hnppkctmiqrcobkzajzl.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhucHBrY3RtaXFyY29ia3phanpsIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA4MDMyOCwiZXhwIjoyMDc1NjU2MzI4fQ.Ve1yUZpXDrYeRW4tKnqBl73pLBc2COVch0_bcRdgkxw"

def conexao():
    # 🔗 Conectar ao Supabase
    print('Conectando ao Supabase...')
    supabase: Client = create_client(url, key)
    print('Conectando')
    return supabase









