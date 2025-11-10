from supabase import create_client, Client
import pandas as pd

# 🔑 Suas credenciais
url = "https://efougwjqwlfnkwqgtifr.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmb3Vnd2pxd2xmbmt3cWd0aWZyIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1OTc3NDIxNCwiZXhwIjoyMDc1MzUwMjE0fQ.0nbX62xtlaRAQ7LXELaOe1TwcPP7BB4buJhvSvt-hQQ"  # normalmente a anon key

def conexao():
    # 🔗 Conectar ao Supabase
    print('Conectando ao Supabase...')
    supabase: Client = create_client(url, key)
    print('Conectando')
    return supabase




