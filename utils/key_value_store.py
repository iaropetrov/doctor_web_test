from collections import defaultdict
from typing import Any, Dict, List, Set, Optional
from utils.logger_config import logger


class KeyValueStore:
    """
    Класс для хранения in-memory key-value данных с поддержкой транзакций.
    """

    def __init__(self) -> None:
        """
        Инициализация хранилища и структуры для отслеживания значений.
        """
        self._data: List[Dict[str, Optional[str]]] = [{}]
        self._value_to_keys: List[Dict[str, Set[str]]] = [defaultdict(set)]

    def set(self, key: str, value: str) -> None:
        """
        Сохраняет значение по ключу.
        :param key: Ключ
        :param value: Значение
        """
        current, value_to_keys = self._current()
        old_value = self._get_from_layers(key)
        if old_value is not None:
            value_to_keys[old_value].discard(key)

        current[key] = value
        value_to_keys[value].add(key)

    def get(self, key: str) -> str:
        """
        Возвращает значение по ключу или 'NULL', если не найдено.
        :param key: Ключ
        :return: Значение или 'NULL'
        """
        value = self._get_from_layers(key)
        return value if value is not None else 'NULL'

    def unset(self, key: str) -> None:
        """
        Удаляет ключ из хранилища.
        :param key: Ключ
        """

        current, value_to_keys = self._current()
        old_value = self._get_from_layers(key)
        if old_value is not None:
            value_to_keys[old_value].discard(key)
        current[key] = None

    def counts(self, value: str) -> int:
        """
        Возвращает количество ключей с данным значением.
        :param value: Значение
        :return: Количество ключей
        """
        keys_with_target_value: Set[str] = set()
        for layer_index in range(len(self._data)):
            for key_name, value_in_layer in self._data[layer_index].items():
                if value_in_layer == value:
                    keys_with_target_value.add(key_name)
                elif value_in_layer is None and key_name in keys_with_target_value:
                    keys_with_target_value.discard(key_name)
        return len(keys_with_target_value)

    def find(self, value: str) -> List[str]:
        """
        Возвращает список ключей, связанных с данным значением.
        :param value: Значение
        :return: Список ключей
        """
        keys_with_target_value: Set[str] = set()
        for layer_index in range(len(self._data)):
            for key_name, value_in_layer in self._data[layer_index].items():
                if value_in_layer == value:
                    keys_with_target_value.add(key_name)
                elif value_in_layer is None and key_name in keys_with_target_value:
                    keys_with_target_value.discard(key_name)
        return sorted(keys_with_target_value)

    def begin(self) -> None:
        """
        Начинает новую транзакцию.
        """
        self._data.append({})
        self._value_to_keys.append(defaultdict(set))

    def rollback(self) -> None:
        """
        Откатывает изменения текущей транзакции.
        """
        if len(self._data) == 1:
            logger.info('Не запущено ни одной транзакции!')
            return
        self._data.pop()
        self._value_to_keys.pop()

    def commit(self) -> None:
        """
        Применяет изменения текущей транзакции к родительской.
        """
        if len(self._data) == 1:
            logger.info('Не запущено ни одной транзакции!')
            return
        top_layer_changes = self._data.pop()
        self._value_to_keys.pop()
        for key_name, value_in_layer in top_layer_changes.items():
            if value_in_layer is None:
                # Записываем tombstone в родительский слой, чтобы не "пробивалось" базовое значение
                self._data[-1][key_name] = None
                for mapped_value in self._value_to_keys[-1]:
                    self._value_to_keys[-1][mapped_value].discard(key_name)
            else:
                self._data[-1][key_name] = value_in_layer
                self._value_to_keys[-1][value_in_layer].add(key_name)

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

    def _get_from_layers(self, key: str) -> Optional[str]:
        """
        Ищет значение ключа во всех слоях транзакций.
        :param key: Ключ
        :return: Значение или None
        """
        for layer in reversed(self._data):
            if key in layer:
                return layer[key]
        return None
