import pytest
from utils.key_value_store import KeyValueStore
from utils.command_dispatcher import CommandDispatcher
from utils import logger_config
from unittest.mock import patch

@pytest.fixture
def dispatcher():
    store = KeyValueStore()
    return CommandDispatcher(store)

def test_help_command(dispatcher):
    """Тест: команда HELP выводит справку"""
    with patch.object(logger_config.logger, 'info') as mock_info:
        dispatcher.dispatch('HELP', [])
        help_msgs = [call.args[0] for call in mock_info.call_args_list]
        assert any('Доступные команды' in msg or 'Available commands' in msg for msg in help_msgs)

def test_end_command(dispatcher):
    """Тест: команда END вызывает KeyboardInterrupt"""
    with pytest.raises(KeyboardInterrupt):
        dispatcher.dispatch('END', [])

def test_invalid_command(dispatcher):
    """Тест: обработка неверной команды"""
    with patch.object(logger_config.logger, 'info') as mock_info:
        dispatcher.dispatch('UNKNOWN', [])
        msgs = [call.args[0] for call in mock_info.call_args_list]
        assert any('НЕВЕРНАЯ КОМАНДА' in msg or 'INVALID' in msg for msg in msgs) 