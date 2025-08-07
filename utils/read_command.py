from typing import Tuple, List
from utils.logger_config import logger

COMMANDS_HELP = (
    '\nДоступные команды:\n'
    '  SET <key> <value>   - Сохранить значение по ключу\n'
    '  GET <key>           - Получить значение по ключу, или NULL если не найдено\n'
    '  UNSET <key>         - Удалить ключ\n'
    '  COUNTS <value>      - Сколько ключей имеет это значение\n'
    '  FIND <value>        - Все ключи, связанные с этим значением\n'
    '  BEGIN               - Начать транзакцию\n'
    '  ROLLBACK            - Откатить текущую транзакцию\n'
    '  COMMIT              - Применить изменения текущей транзакции\n'
    '  END                 - Завершить приложение\n'
    '  HELP                - Показать эту справку\n'
)


def show_help() -> None:
    logger.info(COMMANDS_HELP)


def read_command(show_help_flag: bool = False) -> Tuple[str, List[str]]:
    logger.info('Ожидание ввода...')
    if show_help_flag:
        show_help()

    parts = input().strip().split()
    if not parts:
        logger.debug('Нет ввода или команды.')
        raise ValueError('Нет ввода или команды.')

    cmd, *args = parts
    logger.debug(f'Разобрана команда: {cmd.upper()}, аргументы: {args}')
    return cmd.upper(), args
