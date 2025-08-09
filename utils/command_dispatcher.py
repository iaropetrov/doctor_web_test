from typing import Callable, Dict, List, Optional
from utils.logger_config import logger
from utils.read_command import show_help
from utils.key_value_store import KeyValueStore


class CommandDispatcher:
    """
    Класс для обработки команд key-value хранилища и транзакций.
    """

    def __init__(self, store: KeyValueStore) -> None:
        """
        Инициализация диспетчера команд.
        :param store: Экземпляр KeyValueStore
        """
        self.store = store
        self.commands: Dict[str, Callable[[List[str]], Optional[bool]]] = {
            'SET': self.cmd_set,
            'GET': self.cmd_get,
            'UNSET': self.cmd_unset,
            'COUNTS': self.cmd_counts,
            'FIND': self.cmd_find,
            'BEGIN': self.cmd_begin,
            'ROLLBACK': self.cmd_rollback,
            'COMMIT': self.cmd_commit,
            'END': self.cmd_end,
            'HELP': self.cmd_help,
        }

    def cmd_set(self, args: List[str]) -> None:
        """
        Сохраняет значение по ключу и печатает, что изменилось.
        :param args: [ключ, значение]
        """
        if len(args) != 2:
            raise ValueError('Команда SET требует 2 аргумента')
        key, value = args[0], args[1]
        old_value = self.store.get(key)
        self.store.set(key, value)
        print(f"Ключ '{key}' изменён: было '{old_value}', стало '{value}'")

    def cmd_get(self, args: List[str]) -> None:
        """
        Получает значение по ключу.
        :param args: [ключ]
        """
        if len(args) != 1:
            raise ValueError('Команда GET требует 1 аргумент')
        print(self.store.get(args[0]))

    def cmd_unset(self, args: List[str]) -> None:
        """
        Удаляет ключ и печатает, что изменилось.
        :param args: [ключ]
        """
        if len(args) != 1:
            raise ValueError('Команда UNSET требует 1 аргумент')
        key = args[0]
        old_value = self.store.get(key)
        self.store.unset(key)
        if old_value == 'NULL':
            print(f"Ключ '{key}' не существовал")
        else:
            print(f"Ключ '{key}' удалён: было '{old_value}'")

    def cmd_counts(self, args: List[str]) -> None:
        """
        Возвращает количество ключей с данным значением.
        :param args: [значение]
        """
        if len(args) != 1:
            raise ValueError('Команда COUNTS требует 1 аргумент')
        print(self.store.counts(args[0]))

    def cmd_find(self, args: List[str]) -> None:
        """
        Находит все ключи по значению.
        :param args: [значение]
        """
        if len(args) != 1:
            raise ValueError('Команда FIND требует 1 аргумент')
        keys = self.store.find(args[0])
        print(' '.join(keys) if keys else 'NULL')

    def cmd_begin(self, args: List[str]) -> None:
        """
        Начинает транзакцию.
        :param args: []
        """
        if len(args) != 0:
            raise ValueError('Команда BEGIN не принимает аргументов')
        self.store.begin()
        print('Транзакция начата')

    def cmd_rollback(self, args: List[str]) -> None:
        """
        Откатывает текущую транзакцию.
        :param args: []
        """
        if len(args) != 0:
            raise ValueError('Команда ROLLBACK не принимает аргументов')
        if self.store.rollback():
            print('Откат транзакции выполнен')
        else:
            print('Не запущено ни одной транзакции!')

    def cmd_commit(self, args: List[str]) -> None:
        """
        Применяет изменения текущей транзакции.
        :param args: []
        """
        if len(args) != 0:
            raise ValueError('Команда COMMIT не принимает аргументов')
        if self.store.commit():
            print('Транзакция применена')
        else:
            print('Не запущено ни одной транзакции!')

    def cmd_end(self, args: List[str]) -> None:
        """
        Завершает выполнение программы.
        :param args: []
        """
        if len(args) != 0:
            raise ValueError('Команда END не принимает аргументов')
        raise KeyboardInterrupt('Завершение работы приложения по команде END')

    def cmd_help(self, args: List[str]) -> None:
        """
        Показывает справку по командам.
        :param args: []
        """
        if len(args) != 0:
            raise ValueError('Команда HELP не принимает аргументов')
        show_help()

    def dispatch(self, cmd: str, args: List[str]) -> Optional[bool]:
        """
        Выполняет команду по её имени.
        :param cmd: Имя команды
        :param args: Аргументы
        """
        if cmd in self.commands:
            return self.commands[cmd](args)
        else:
            logger.info('НЕВЕРНАЯ КОМАНДА')
            return None
