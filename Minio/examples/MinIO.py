import os
import io
import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

# Caminho absoluto até o .env
ENV_PATH = os.path.join(os.path.dirname(__file__), "..", ".env")

# Carregar variáveis de ambiente
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    print(f"⚠️  Arquivo .env não encontrado em: {ENV_PATH}")

class MinIOConfigError(Exception):
    """Erro de configuração do MinIO."""
    pass

class MinIOConnectionError(Exception):
    """Erro de conexão com o servidor MinIO."""
    pass


class MinIOManager:
    """Gerenciador simplificado de conexão e leitura de arquivos no MinIO."""

    def __init__(self):
        self.endpoint = os.getenv("MINIO_ENDPOINT")
        self.access_key = os.getenv("MINIO_ACCESS_KEY")
        self.secret_key = os.getenv("MINIO_SECRET_KEY")
        self.bucket = os.getenv("MINIO_BUCKET")

        # Verifica se todas as variáveis estão configuradas
        if not all([self.endpoint, self.access_key, self.secret_key, self.bucket]):
            raise MinIOConfigError(
                "❌ Variáveis do .env não encontradas. Verifique o arquivo .env!"
            )

        try:
            self.client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=True  # usar HTTPS se possível
            )
            print(f"✅ Conectado ao MinIO ({self.endpoint})")
        except Exception as e:
            raise MinIOConnectionError(f"Erro ao conectar no MinIO: {e}")


    def read_file(self, object_name: str, bucket: str = None):
        """Lê um arquivo Parquet ou CSV diretamente do MinIO e devolve um DataFrame."""
        bucket = bucket or self.bucket
        try:
            response = self.client.get_object(bucket, object_name)
            data = response.read()
            if object_name.endswith(".parquet"):
                df = pd.read_parquet(io.BytesIO(data))
            elif object_name.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(data))
            else:
                raise ValueError("Tipo de arquivo não suportado. Use .parquet ou .csv")

            print(f"📥 Arquivo '{object_name}' carregado com sucesso do bucket '{bucket}'!")
            return df
        except S3Error as e:
            print(f"❌ Erro no MinIO (S3): {e}")
            return None
        except Exception as e:
            print(f"❌ Erro geral ao ler o arquivo '{object_name}': {e}")
            return None


# Instância global do gerenciador
try:
    manager = MinIOManager()
except Exception as e:
    print(f"⚠️  Não foi possível inicializar o MinIOManager: {e}")
    manager = None


# Função auxiliar pública
def read_file(object_name: str, bucket: str = None):
    """Função utilitária para ler arquivos sem precisar criar instância."""
    global manager
    if manager is None:
        print("⚠️  Manager não inicializado, tentando reconectar...")
        try:
            manager = MinIOManager()
        except Exception as e:
            print(f"❌ Erro ao reconectar ao MinIO: {e}")
            return None
    return manager.read_file(object_name, bucket)
