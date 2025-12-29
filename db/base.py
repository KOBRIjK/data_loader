from abc import ABC, abstractmethod
from typing import Any, Optional


class DatabaseConnector(ABC):
    """
    Абстрактный интерфейс коннектора: connect, read, write, close.
    read/write принимают SQL или другую команду в зависимости от реализации.
    """
    @abstractmethod
    def read(self, query: str, bucket: str, key: str, params: Optional[dict] = None, **kwargs) -> Any:
        """Возвращает данные по запросу. params — словарь параметров SQL (если нужен)."""
        ...

    @abstractmethod
    def write(
        self,
        sql: str,
        df: Optional[Any] = None,
        target: Optional[str] = None,
        table: Optional[str] = None
    ) -> Any:
        """Выполняет изменяющий запрос и возвращает результат/метаданные.
        df/target/table используются для операций записи DataFrame; params — опциональные SQL-параметры.
        """
        ...
