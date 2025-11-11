"""
Cliente MinIO para operações de ETL.

Esta classe fornece uma interface simplificada para interagir com MinIO,
focada nas necessidades de pipelines de ETL.
"""

import os
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, IO
from datetime import datetime, timedelta

from minio import Minio
from minio.error import S3Error

from .exceptions import MinIOConfigError, MinIOOperationError, MinIOConnectionError

logger = logging.getLogger(__name__)


class MinIOManager:
    """
    Gerenciador MinIO para operações de ETL.
    
    Esta classe facilita operações comuns com MinIO/S3, incluindo:
    - Gerenciamento de buckets
    - Upload/download de arquivos
    - Listagem de objetos
    - Geração de URLs pré-assinadas
    """
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = True):
        """
        Inicializa o cliente MinIO.
        
        Args:
            endpoint: Endpoint do servidor MinIO (ex: 'minio.exemplo.com:9000')
            access_key: Chave de acesso
            secret_key: Chave secreta  
            secure: Se deve usar HTTPS (padrão: True)
        
        Raises:
            MinIOConfigError: Se as credenciais estão inválidas
            MinIOConnectionError: Se não conseguir conectar ao servidor
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        
        self._validate_config()
        self._init_client()
        self._test_connection()
        
        logger.info(f"MinIOManager inicializado para endpoint: {self.endpoint}")

    def _validate_config(self):
        """Valida a configuração fornecida."""
        if not self.endpoint:
            raise MinIOConfigError("Endpoint não pode estar vazio")
        
        if not self.access_key:
            raise MinIOConfigError("Access key não pode estar vazia")
            
        if not self.secret_key:
            raise MinIOConfigError("Secret key não pode estar vazia")

    def _init_client(self):
        """Inicializa o cliente MinIO."""
        try:
            self.client = Minio(
                endpoint=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
        except Exception as e:
            raise MinIOConfigError(f"Erro ao inicializar cliente MinIO: {e}")

    def _test_connection(self):
        """Testa a conectividade com o servidor MinIO."""
        try:
            # Tenta listar buckets para validar a conexão
            list(self.client.list_buckets())
        except Exception as e:
            raise MinIOConnectionError(
                f"Falha ao conectar com MinIO: {e}", 
                endpoint=self.endpoint
            )

    def list_buckets(self) -> List[str]:
        """
        Lista todos os buckets disponíveis.
        
        Returns:
            Lista com nomes dos buckets
            
        Raises:
            MinIOOperationError: Se falhar ao listar buckets
        """
        try:
            buckets = self.client.list_buckets()
            return [bucket.name for bucket in buckets]
        except S3Error as e:
            raise MinIOOperationError(f"Erro ao listar buckets: {e}", "list_buckets")

    def create_bucket_if_not_exists(self, bucket_name: str):
        """
        Cria um bucket se ele não existir.
        
        Args:
            bucket_name: Nome do bucket a ser criado
            
        Raises:
            MinIOOperationError: Se falhar ao criar o bucket
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Bucket '{bucket_name}' criado com sucesso")
            else:
                logger.info(f"Bucket '{bucket_name}' já existe")
        except S3Error as e:
            raise MinIOOperationError(
                f"Erro ao criar/verificar bucket '{bucket_name}': {e}",
                "create_bucket"
            )

    def upload_file(self, file_path: Union[str, Path], object_name: str, bucket_name: str,
                   content_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Faz upload de um arquivo para o MinIO.
        
        Args:
            file_path: Caminho local do arquivo
            object_name: Nome do objeto no MinIO
            bucket_name: Nome do bucket de destino
            content_type: Tipo MIME do arquivo (opcional)
            
        Returns:
            Dicionário com informações do upload
            
        Raises:
            MinIOOperationError: Se falhar ao fazer upload
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise MinIOOperationError(f"Arquivo não encontrado: {file_path}", "upload_file")
        
        try:
            # Garantir que o bucket existe
            self.create_bucket_if_not_exists(bucket_name)
            
            file_size = file_path.stat().st_size
            
            result = self.client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=str(file_path),
                content_type=content_type
            )
            
            logger.info(f"Upload concluído: {object_name} ({file_size:,} bytes)")
            
            return {
                "bucket": bucket_name,
                "object_name": object_name,
                "size": file_size,
                "etag": result.etag,
                "uploaded_at": datetime.now().isoformat()
            }
            
        except S3Error as e:
            raise MinIOOperationError(f"Erro ao fazer upload de '{object_name}': {e}", "upload_file")

    def download_file(self, bucket_name: str, object_name: str, file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Baixa um arquivo do MinIO.
        
        Args:
            bucket_name: Nome do bucket
            object_name: Nome do objeto no MinIO
            file_path: Caminho local de destino
            
        Returns:
            Dicionário com informações do download
            
        Raises:
            MinIOOperationError: Se falhar ao baixar o arquivo
        """
        file_path = Path(file_path)
        
        try:
            # Criar diretório pai se não existir
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.client.fget_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=str(file_path)
            )
            
            file_size = file_path.stat().st_size
            logger.info(f"Download concluído: {object_name} ({file_size:,} bytes)")
            
            return {
                "bucket": bucket_name,
                "object_name": object_name,
                "local_path": str(file_path),
                "size": file_size,
                "downloaded_at": datetime.now().isoformat()
            }
            
        except S3Error as e:
            raise MinIOOperationError(f"Erro ao baixar '{object_name}': {e}", "download_file")

    def list_files(self, bucket_name: str, prefix: str = "", recursive: bool = True) -> List[Dict[str, Any]]:
        """
        Lista arquivos em um bucket.
        
        Args:
            bucket_name: Nome do bucket
            prefix: Prefixo para filtrar arquivos (opcional)
            recursive: Se deve listar recursivamente (padrão: True)
            
        Returns:
            Lista de dicionários com informações dos arquivos
            
        Raises:
            MinIOOperationError: Se falhar ao listar arquivos
        """
        try:
            objects = []
            for obj in self.client.list_objects(bucket_name, prefix=prefix, recursive=recursive):
                objects.append({
                    "name": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                    "etag": obj.etag,
                    "content_type": obj.content_type,
                    "bucket": bucket_name
                })
            
            logger.info(f"Encontrados {len(objects)} objetos em {bucket_name}/{prefix}")
            return objects
            
        except S3Error as e:
            raise MinIOOperationError(f"Erro ao listar arquivos em '{bucket_name}': {e}", "list_files")

    def delete_file(self, bucket_name: str, object_name: str):
        """
        Remove um arquivo do MinIO.
        
        Args:
            bucket_name: Nome do bucket
            object_name: Nome do objeto a ser removido
            
        Raises:
            MinIOOperationError: Se falhar ao remover o arquivo
        """
        try:
            self.client.remove_object(bucket_name, object_name)
            logger.info(f"Arquivo removido: {bucket_name}/{object_name}")
        except S3Error as e:
            raise MinIOOperationError(f"Erro ao remover '{object_name}': {e}", "delete_file")

    def get_file_info(self, bucket_name: str, object_name: str) -> Dict[str, Any]:
        """
        Obtém informações de um arquivo no MinIO.
        
        Args:
            bucket_name: Nome do bucket
            object_name: Nome do objeto
            
        Returns:
            Dicionário com metadados do arquivo
            
        Raises:
            MinIOOperationError: Se falhar ao obter informações
        """
        try:
            stat = self.client.stat_object(bucket_name, object_name)
            return {
                "name": stat.object_name,
                "size": stat.size,
                "last_modified": stat.last_modified.isoformat() if stat.last_modified else None,
                "etag": stat.etag,
                "content_type": stat.content_type,
                "metadata": dict(stat.metadata) if stat.metadata else {},
                "bucket": bucket_name
            }
        except S3Error as e:
            raise MinIOOperationError(f"Erro ao obter info de '{object_name}': {e}", "get_file_info")

    def generate_presigned_upload_url(self, bucket_name: str, object_name: str, 
                                    expires_hours: int = 1) -> str:
        """
        Gera URL pré-assinada para upload.
        
        Args:
            bucket_name: Nome do bucket
            object_name: Nome do objeto
            expires_hours: Horas até expirar (padrão: 1)
            
        Returns:
            URL pré-assinada para upload
            
        Raises:
            MinIOOperationError: Se falhar ao gerar URL
        """
        try:
            url = self.client.presigned_put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                expires=timedelta(hours=expires_hours)
            )
            logger.info(f"URL de upload gerada para {bucket_name}/{object_name}")
            return url
        except S3Error as e:
            raise MinIOOperationError(f"Erro ao gerar URL de upload: {e}", "generate_upload_url")

    def generate_presigned_download_url(self, bucket_name: str, object_name: str,
                                      expires_hours: int = 1) -> str:
        """
        Gera URL pré-assinada para download.
        
        Args:
            bucket_name: Nome do bucket
            object_name: Nome do objeto
            expires_hours: Horas até expirar (padrão: 1)
            
        Returns:
            URL pré-assinada para download
            
        Raises:
            MinIOOperationError: Se falhar ao gerar URL
        """
        try:
            url = self.client.presigned_get_object(
                bucket_name=bucket_name,
                object_name=object_name,
                expires=timedelta(hours=expires_hours)
            )
            logger.info(f"URL de download gerada para {bucket_name}/{object_name}")
            return url
        except S3Error as e:
            raise MinIOOperationError(f"Erro ao gerar URL de download: {e}", "generate_download_url")

    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Verifica se um bucket existe.
        
        Args:
            bucket_name: Nome do bucket
            
        Returns:
            True se o bucket existir, False caso contrário
            
        Raises:
            MinIOOperationError: Se falhar ao verificar bucket
        """
        try:
            return self.client.bucket_exists(bucket_name)
        except S3Error as e:
            raise MinIOOperationError(f"Erro ao verificar bucket '{bucket_name}': {e}", "bucket_exists")

    def __str__(self):
        return f"MinIOManager(endpoint={self.endpoint}, secure={self.secure})"
        
    def __repr__(self):
        return self.__str__()