# MinIO.py  (versão corrigida)

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from io import BytesIO

# Evite depender de sys.path quando puder. Se precisar manter:
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from Minio.minio_client import MinIOManager, MinIOConfigError, MinIOConnectionError  # type: ignore

# 1) Carrega .env (no Streamlit Cloud prefira st.secrets; .env só funciona se o arquivo existir no container)
load_dotenv()

# 2) Sempre inicialize a variável no escopo do módulo
manager = None

def _connect_manager():
    """
    Tenta criar o MinIOManager e retorna a instância.
    Lança erro com mensagem clara se falhar.
    """
    try:
        endpoint   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        secure     = os.getenv("MINIO_SECURE", "false").lower() == "true"

        m = MinIOManager(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        print(f"✅ Conectado ao MinIO: {m.endpoint}")
        return m
    except (MinIOConfigError, MinIOConnectionError) as e:
        # Propaga com contexto para facilitar debug
        raise RuntimeError(f"Falha ao conectar no MinIO: {e}")

# 3) Tenta conectar já ao importar o módulo (opcional, você pode lazy-load)
try:
    manager = _connect_manager()
except Exception as e:
    # Não deixe silencioso. Você pode optar por não elevar aqui, mas então read_file() deve abortar.
    print(f"❌ Erro de conexão: {e}")
    manager = None

def upload(object_name: str, bucket_name: str, sample_file: str, content_type: str = "application/octet-stream"):
    """
    Faz upload para o MinIO.
    content_type padrão genérico. Ajuste conforme o arquivo (ex: parquet -> 'application/octet-stream' ou 'application/parquet')
    """
    if manager is None:
        raise RuntimeError("MinIO manager não inicializado. Verifique as credenciais/variáveis de ambiente.")

    print(f"\n⬆️  Fazendo upload: {sample_file} -> {object_name}")
    try:
        result = manager.upload_file(
            file_path=sample_file,
            object_name=object_name,
            bucket_name=bucket_name,
            content_type=content_type,
        )
        print("   ✅ Upload concluído:")
        print(f"      • Tamanho: {result['size']:,} bytes")
        print(f"      • ETag: {result['etag']}")
        print(f"      • Data: {result['uploaded_at']}")
        return result
    except Exception as e:
        # Propaga para o Streamlit tratar e não seguir com estado inválido
        raise RuntimeError(f"Erro no upload de {sample_file} para {bucket_name}/{object_name}: {e}") from e

def download(object_name: str, bucket_name: str, download_path: str):
    if manager is None:
        raise RuntimeError("MinIO manager não inicializado. Verifique as credenciais/variáveis de ambiente.")

    print(f"\n⬇️  Fazendo download: {object_name} -> {download_path}")
    try:
        result = manager.download_file(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=download_path
        )
        print("   ✅ Download concluído:")
        print(f"      • Arquivo local: {result['local_path']}")
        print(f"      • Tamanho: {result['size']:,} bytes")
        return result
    except Exception as e:
        raise RuntimeError(f"Erro no download de {bucket_name}/{object_name} para {download_path}: {e}") from e

def read_file(object_name: str, bucket_name: str) -> pd.DataFrame:
    """
    Lê um arquivo Parquet do MinIO e retorna DataFrame.
    Lança exceção com causa original em caso de falha.
    """
    if manager is None:
        raise RuntimeError("MinIO manager não inicializado. Falha anterior de conexão? Verifique as credenciais.")

    response = None
    try:
        response = manager.client.get_object(bucket_name, object_name)
        data = response.read()

        # Se usar pyarrow como engine (recomendado para parquet)
        return pd.read_parquet(BytesIO(data), engine="pyarrow")
    except Exception as e:
        raise RuntimeError(f"Erro na leitura do arquivo {bucket_name}/{object_name}: {e}") from e
    finally:
        # Garanta que o stream seja fechado
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
            try:
                response.release_conn()
            except Exception:
                pass
