from collections import defaultdict
from typing import Any, Dict, List, Set, Optional
from utils.logger_config import logger


class KeyValueStore:
    """
    Класс для хранения in-memory key-value данных с поддержкой транзакций.
    Ключи регистронезависимые (нормализуются к нижнему регистру).
    """

    def __init__(self) -> None:
        """
        Инициализация хранилища и структуры для отслеживания значений.
        """
        self._data: List[Dict[str, Optional[str]]] = [{}]
        self._value_to_keys: List[Dict[str, Set[str]]] = [defaultdict(set)]

    def set(self, key: str, value: str) -> None:
        """
        Сохраняет значение по ключу (регистронезависимо).
        :param key: Ключ
        :param value: Значение
        """
        normalized_key = self._normalize_key(key)
        current, value_to_keys = self._current()
        old_value = self._get_from_layers(normalized_key)
        if old_value is not None:
            value_to_keys[old_value].discard(normalized_key)
        current[normalized_key] = value
        value_to_keys[value].add(normalized_key)

    def get(self, key: str) -> str:
        """
        Возвращает значение по ключу (регистронезависимо) или 'NULL', если не найдено.
        :param key: Ключ
        :return: Значение или 'NULL'
        """
        normalized_key = self._normalize_key(key)
        value = self._get_from_layers(normalized_key)
        return value if value is not None else 'NULL'

    def unset(self, key: str) -> None:
        """
        Удаляет ключ (регистронезависимо) из хранилища.
        :param key: Ключ
        """
        normalized_key = self._normalize_key(key)
        current, value_to_keys = self._current()
        old_value = self._get_from_layers(normalized_key)
        if old_value is not None:
            value_to_keys[old_value].discard(normalized_key)
        current[normalized_key] = None

    def counts(self, value: str) -> int:
        """
        Возвращает количество ключей с данным значением с учётом вложенных транзакций.
        :param value: Значение
        :return: Количество ключей
        """
        resolved = self._resolved_state()
        count = 0
        for _, resolved_value in resolved.items():
            if resolved_value == value:
                count += 1
        return count

    def find(self, value: str) -> List[str]:
        """
        Возвращает список ключей, связанных с данным значением, с учётом вложенных транзакций.
        :param value: Значение
        :return: Список ключей (в нормализованном виде)
        """
        resolved = self._resolved_state()
        result_keys: List[str] = []
        for key_name, resolved_value in resolved.items():
            if resolved_value == value:
                result_keys.append(key_name)
        return sorted(result_keys)

    def begin(self) -> bool:
        """
        Начинает новую транзакцию.
        :return: True (всегда успешный старт транзакции)
        """
        self._data.append({})
        self._value_to_keys.append(defaultdict(set))
        return True

    def rollback(self) -> bool:
        """
        Откатывает изменения текущей транзакции.
        :return: True если транзакция была откатена, False если активной транзакции нет
        """
        if len(self._data) == 1:
            return False
        self._data.pop()
        self._value_to_keys.pop()
        return True

    def commit(self) -> bool:
        """
        Применяет изменения текущей транзакции к родительской.
        :return: True если изменения применены, False если активной транзакции нет
        """
        if len(self._data) == 1:
            return False
        top_layer_changes = self._data.pop()
        self._value_to_keys.pop()
        for key_name, value_in_layer in top_layer_changes.items():
            if value_in_layer is None:
                self._data[-1][key_name] = None
                for mapped_value in self._value_to_keys[-1]:
                    self._value_to_keys[-1][mapped_value].discard(key_name)
            else:
                self._data[-1][key_name] = value_in_layer
                self._value_to_keys[-1][value_in_layer].add(key_name)
        return True

    def end(self) -> None:
        """
        Завершает выполнение программы (выход).
        """
        raise KeyboardInterrupt('Завершение работы приложения по команде END')

    def _current(self) -> Any:
        """
        Возвращает текущий слой данных и отображение значений.
        """
        return self._data[-1], self._value_to_keys[-1]

    def _get_from_layers(self, normalized_key: str) -> Optional[str]:
        """
        Ищет значение ключа во всех слоях транзакций (ключ должен быть уже нормализован).
        :param normalized_key: Нормализованный ключ (нижний регистр)
        :return: Значение или None
        """
        for layer in reversed(self._data):
            if normalized_key in layer:
                return layer[normalized_key]
        return None

    def _resolved_state(self) -> Dict[str, Optional[str]]:
        """
        Строит итоговое видимое состояние: для каждого ключа берётся первое встреченное
        сверху вниз значение (включая метку удаления None). Ключи нормализованы.
        :return: Отображение ключ -> итоговое значение (или None)
        """
        resolved: Dict[str, Optional[str]] = {}
        for layer in reversed(self._data):
            for key_name, value_in_layer in layer.items():
                if key_name not in resolved:
                    resolved[key_name] = value_in_layer
        return resolved

    def _normalize_key(self, key: str) -> str:
        """
        Нормализует ключ к нижнему регистру для регистронезависимых операций.
        :param key: Исходный ключ
        :return: Нормализованный ключ (lowercase)
        """
        return key.lower()
