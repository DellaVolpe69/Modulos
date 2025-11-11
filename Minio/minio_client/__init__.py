"""
MinIO ETL Toolkit - Cliente Python para interação com MinIO.

Este módulo fornece uma interface simples e robusta para operações
com MinIO, especialmente projetado para equipes de ETL.
"""

from .client import MinIOManager
from .exceptions import MinIOBaseError, MinIOConfigError, MinIOOperationError, MinIOConnectionError

__version__ = "1.0.0"
__author__ = "Equipe de Desenvolvimento"

__all__ = [
    "MinIOManager",
    "MinIOBaseError", 
    "MinIOConfigError",
    "MinIOOperationError",
    "MinIOConnectionError"
]