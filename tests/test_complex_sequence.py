import pytest
from utils.key_value_store import KeyValueStore
from utils.command_dispatcher import CommandDispatcher


def last_line(capsys) -> str:
    captured = capsys.readouterr()
    out = captured.out.strip().splitlines()
    return out[-1] if out else ''


@pytest.fixture
def dispatcher():
    store = KeyValueStore()
    return CommandDispatcher(store)


def test_complex_nested_sequence(dispatcher, capsys):
    dispatcher.dispatch('SET', ['a', '1'])
    assert "было 'NULL', стало '1'" in last_line(capsys)
    dispatcher.dispatch('BEGIN', [])
    assert 'Транзакция начата' in last_line(capsys)
    dispatcher.dispatch('SET', ['a', '2'])
    assert "было '1', стало '2'" in last_line(capsys)
    dispatcher.dispatch('BEGIN', [])
    assert 'Транзакция начата' in last_line(capsys)
    dispatcher.dispatch('UNSET', ['a'])
    assert "удалён: было '2'" in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == 'NULL'
    dispatcher.dispatch('COMMIT', [])
    assert 'Транзакция применена' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == 'NULL'
    dispatcher.dispatch('ROLLBACK', [])
    assert 'Откат транзакции выполнен' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == '1'
    dispatcher.dispatch('COMMIT', [])
    assert 'Не запущено ни одной транзакции!' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == '1'
    dispatcher.dispatch('BEGIN', [])
    assert 'Транзакция начата' in last_line(capsys)
    dispatcher.dispatch('SET', ['a', '2'])
    assert "было '1', стало '2'" in last_line(capsys)
    dispatcher.dispatch('BEGIN', [])
    assert 'Транзакция начата' in last_line(capsys)
    dispatcher.dispatch('UNSET', ['a'])
    assert "удалён: было '2'" in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == 'NULL'
    dispatcher.dispatch('COMMIT', [])
    assert 'Транзакция применена' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == 'NULL'
    dispatcher.dispatch('COMMIT', [])
    assert 'Транзакция применена' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == 'NULL'
    dispatcher.dispatch('COMMIT', [])
    assert 'Не запущено ни одной транзакции!' in last_line(capsys)
    with pytest.raises(KeyboardInterrupt):
        dispatcher.dispatch('END', []) 