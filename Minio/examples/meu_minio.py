# MinIO.py
import os
from io import BytesIO
import pandas as pd

# Se este módulo for usado dentro do Streamlit, podemos importar st
try:
    import streamlit as st
    _HAS_ST = True
except Exception:
    _HAS_ST = False

from Minio.minio_client import MinIOManager, MinIOConfigError, MinIOConnectionError

manager = None
###################################################################
def _get_cfg(key: str, default=None):
    if _HAS_ST:
        val = st.secrets.get(key, None)
        if val is not None:
            return val
    return os.getenv(key, default)
###################################################################
def _connect_manager():
    try:
        endpoint   = _get_cfg("MINIO_ENDPOINT")
        access_key = _get_cfg("MINIO_ACCESS_KEY")
        secret_key = _get_cfg("MINIO_SECRET_KEY")
        secure_raw = _get_cfg("MINIO_SECURE", "false")
        secure     = str(secure_raw).lower() == "true"

        if not endpoint or not access_key or not secret_key:
            raise ValueError("Credenciais do MinIO ausentes (endpoint/access/secret).")

        # OBS: endpoint SEM https:// — só host (e porta se houver)
        m = MinIOManager(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        print(f"✅ Conectado ao MinIO: {m.endpoint} (secure={secure})")
        return m
    except (MinIOConfigError, MinIOConnectionError, Exception) as e:
        raise RuntimeError(f"Falha ao conectar no MinIO: {e}")

try:
    manager = _connect_manager()
except Exception as e:
    print(f"❌ Erro de conexão: {e}")
    manager = None
###################################################################
def upload(object_name: str, bucket_name: str, file_path: str, content_type: str = "application/octet-stream") -> dict:
    """
    Faz upload de um arquivo local para o MinIO.

    Retorna um dicionário com:
      - size
      - etag
      - uploaded_at
    """

    if manager is None:
        raise RuntimeError("MinIO manager não inicializado. Verifique as credenciais.")

    try:
        result = manager.upload_file(
            file_path=file_path,
            object_name=object_name,
            bucket_name=bucket_name,
            content_type=content_type
        )

        return result

    except Exception as e:
        raise RuntimeError(
            f"Erro ao fazer upload do arquivo '{file_path}' para "
            f"{bucket_name}/{object_name}: {e}"
        ) from e
###################################################################
def download(object_name: str, bucket_name: str, download_path: str) -> dict:
    """
    Faz download de um arquivo do MinIO para o disco local.

    Retorna um dicionário com:
      - local_path
      - size
    """

    if manager is None:
        raise RuntimeError("MinIO manager não inicializado. Verifique as credenciais.")

    try:
        result = manager.download_file(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=download_path
        )

        return result

    except Exception as e:
        raise RuntimeError(
            f"Erro ao baixar o arquivo {bucket_name}/{object_name} "
            f"para '{download_path}': {e}"
        ) from e
###################################################################
def read_file(object_name: str, bucket_name: str) -> pd.DataFrame:
    if manager is None:
        raise RuntimeError("MinIO manager não inicializado. Falha anterior de conexão? Verifique as credenciais.")
    resp = None
    try:
        resp = manager.client.get_object(bucket_name, object_name)
        data = resp.read()
        return pd.read_parquet(BytesIO(data), engine="pyarrow")
    except Exception as e:
        raise RuntimeError(f"Erro na leitura do arquivo {bucket_name}/{object_name}: {e}") from e
    finally:
        if resp is not None:
            try:
                resp.close()
            except Exception:
                pass
            try:
                resp.release_conn()
            except Exception:
                pass
###################################################################
def listar_anexos(bucket_name: str, id_registro: str) -> list[str]:
    """
    Lista todos os anexos armazenados no MinIO para um registro específico,
    usando o padrão <id_registro>_<n>.<ext>.
    """

    if manager is None:
        raise RuntimeError(
            "MinIO manager não inicializado. Falha anterior de conexão? Verifique as credenciais."
        )

    prefix = f"{id_registro}_"
    anexos = []
    objects_iter = None

    try:
        # Busca todos os objetos que começam com o prefixo
        objects_iter = manager.client.list_objects(
            bucket_name,
            prefix=prefix,
            recursive=True
        )

        for obj in objects_iter:
            anexos.append(obj.object_name)

        return anexos

    except Exception as e:
        raise RuntimeError(
            f"Erro ao listar anexos no bucket '{bucket_name}' com prefixo '{prefix}': {e}"
        ) from e


