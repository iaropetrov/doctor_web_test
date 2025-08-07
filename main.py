from utils.key_value_store import KeyValueStore
from utils.command_dispatcher import CommandDispatcher
from utils.logger_config import logger
from utils.read_command import read_command


def main() -> None:
    store = KeyValueStore()
    dispatcher = CommandDispatcher(store)
    first_run = True
    while True:
        try:
            cmd, args = read_command(show_help_flag=first_run)
            first_run = False
        except EOFError:
            logger.info('Получен EOF. Завершение работы приложения')
            break
        except ValueError as e:
            logger.debug(str(e))
            continue
        try:
            dispatcher.dispatch(cmd, args)
        except KeyboardInterrupt:
            logger.info('Завершение работы приложения по команде END')
            break
        except ValueError as e:
            logger.info(f'НЕВЕРНАЯ КОМАНДА: {e}')


if __name__ == '__main__':
    main()
