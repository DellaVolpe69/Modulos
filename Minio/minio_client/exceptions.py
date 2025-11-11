"""
Exceções customizadas para o MinIO ETL Toolkit.
"""


class MinIOBaseError(Exception):
    """Classe base para todas as exceções do MinIO ETL Toolkit."""
    pass


class MinIOConfigError(MinIOBaseError):
    """Erro de configuração do MinIO (credenciais, endpoint, etc)."""
    
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class MinIOOperationError(MinIOBaseError):
    """Erro durante operações do MinIO (upload, download, bucket ops, etc)."""
    
    def __init__(self, message: str, operation: str = None):
        self.message = message
        self.operation = operation
        super().__init__(self.message)


class MinIOConnectionError(MinIOBaseError):  
    """Erro de conectividade com o servidor MinIO."""
    
    def __init__(self, message: str, endpoint: str = None):
        self.message = message
        self.endpoint = endpoint
        super().__init__(self.message)