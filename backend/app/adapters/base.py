from abc import ABC, abstractmethod
from typing import Any, List, Dict


class BaseAdapter(ABC):
    @abstractmethod
    def test_connection(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def list_schemas(self) -> List[str]:
        pass

    @abstractmethod
    def list_objects(self, schema: str) -> List[Dict[str, str]]:
        pass

    @abstractmethod
    def list_columns(self, schema: str, object_name: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_ddl(self, schema: str, object_name: str) -> str:
        pass

    @abstractmethod
    def run_query(self, sql: str, limit: int = 100) -> Dict[str, Any]:
        pass