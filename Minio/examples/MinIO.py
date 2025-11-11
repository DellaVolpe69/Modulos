import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from io import BytesIO

# Adicionar o diretório pai ao path para importar minio_client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Importar o MinIOManager
from minio_client import MinIOManager, MinIOConfigError, MinIOConnectionError # type: ignore

# Carregar configurações do arquivo .env
load_dotenv()  # carrega o .env local, se existir

try:
    manager = MinIOManager(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
    )
    print(f"✅ Conectado ao MinIO: {manager.endpoint}")
except (MinIOConfigError, MinIOConnectionError) as e:
    print(f"❌ Erro de conexão: {e}")

def upload(object_name, bucket_name, sample_file):
    # 5. Upload do arquivo
    print(f"\n⬆️  Fazendo upload: {sample_file} -> {object_name}")
    try:
        result = manager.upload_file(
            file_path=sample_file,
            object_name=object_name,
            bucket_name=bucket_name,
            content_type="text/csv"
        )
        print(f"   ✅ Upload concluído:")
        print(f"      • Tamanho: {result['size']:,} bytes")
        print(f"      • ETag: {result['etag']}")
        print(f"      • Data: {result['uploaded_at']}")
    except Exception as e:
        print(f"   ❌ Erro no upload: {e}")
        return

def download(object_name, bucket_name, download_path):
    # 9. Download do arquivo
    print(f"\n⬇️  Fazendo download: {object_name} -> {download_path}")
    try:
        result = manager.download_file(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=download_path
        )
        print(f"   ✅ Download concluído:")
        print(f"      • Arquivo local: {result['local_path']}")
        print(f"      • Tamanho: {result['size']:,} bytes")
    except Exception as e:
        print(f"   ❌ Erro no download: {e}")

def read_file(object_name, bucket_name):
    try:
        response = manager.client.get_object(bucket_name, object_name)
        data = response.read()

        # Converte o conteúdo em DataFrame
        df = pd.read_parquet(BytesIO(data))
        return df
    except Exception as e:
        print(f"   ❌ Erro na leitura do arquivo: {e}")

#upload('dados/demandaFracionado.parquet', 'consultoria-fabio', r'\\tableau\Central_de_Performance\BI\Local\Bases_Tratadas\Banca_Frete_OF.parquet')
#download('dados/demandaFracionado.parquet', 'consultoria-fabio', r"C:\Users\ricardo.santos\Downloads\TESTE.parquet")

#print('Fim')
