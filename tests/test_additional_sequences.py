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


def test_sequence_get_begin_set_rollback(dispatcher, capsys):
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == 'NULL'
    dispatcher.dispatch('BEGIN', [])
    assert 'Транзакция начата' in last_line(capsys)
    dispatcher.dispatch('SET', ['a', '2'])
    assert "было 'NULL', стало '2'" in last_line(capsys)
    dispatcher.dispatch('ROLLBACK', [])
    assert 'Откат транзакции выполнен' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == 'NULL'


def test_nested_set_then_rollback_levels(dispatcher, capsys):
    dispatcher.dispatch('BEGIN', [])
    assert 'Транзакция начата' in last_line(capsys)
    dispatcher.dispatch('SET', ['a', '2'])
    assert "было 'NULL', стало '2'" in last_line(capsys)
    dispatcher.dispatch('BEGIN', [])
    assert 'Транзакция начата' in last_line(capsys)
    dispatcher.dispatch('SET', ['a', '1'])
    assert "было '2', стало '1'" in last_line(capsys)
    dispatcher.dispatch('ROLLBACK', [])
    assert 'Откат транзакции выполнен' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == '2'
    dispatcher.dispatch('ROLLBACK', [])
    assert 'Откат транзакции выполнен' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == 'NULL'


def test_nested_set_commit_then_rollback(dispatcher, capsys):
    dispatcher.dispatch('BEGIN', [])
    assert 'Транзакция начата' in last_line(capsys)
    dispatcher.dispatch('SET', ['a', '1'])
    assert "было 'NULL', стало '1'" in last_line(capsys)
    dispatcher.dispatch('BEGIN', [])
    assert 'Транзакция начата' in last_line(capsys)
    dispatcher.dispatch('SET', ['a', '2'])
    assert "было '1', стало '2'" in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == '2'
    dispatcher.dispatch('COMMIT', [])
    assert 'Транзакция применена' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == '2'
    dispatcher.dispatch('ROLLBACK', [])
    assert 'Откат транзакции выполнен' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == 'NULL'


def test_counts_find_multi_level(dispatcher, capsys):
    dispatcher.dispatch('BEGIN', [])
    assert 'Транзакция начата' in last_line(capsys)
    dispatcher.dispatch('SET', ['a', '1'])
    dispatcher.dispatch('SET', ['b', '1'])
    dispatcher.dispatch('BEGIN', [])
    assert 'Транзакция начата' in last_line(capsys)
    dispatcher.dispatch('SET', ['a', '2'])
    dispatcher.dispatch('SET', ['b', '2'])
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == '2'
    dispatcher.dispatch('GET', ['b'])
    assert last_line(capsys) == '2'
    dispatcher.dispatch('COUNTS', ['2'])
    assert last_line(capsys) == '2'
    dispatcher.dispatch('SET', ['c', '1'])
    dispatcher.dispatch('COUNTS', ['1'])
    assert last_line(capsys) == '1'
    dispatcher.dispatch('COUNTS', ['2'])
    assert last_line(capsys) == '2'
    dispatcher.dispatch('ROLLBACK', [])
    assert 'Откат транзакции выполнен' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == '1'
    dispatcher.dispatch('GET', ['b'])
    assert last_line(capsys) == '1'
    dispatcher.dispatch('GET', ['c'])
    assert last_line(capsys) == 'NULL'
    dispatcher.dispatch('FIND', ['1'])
    assert last_line(capsys) in ('a b', 'b a')
    dispatcher.dispatch('FIND', ['2'])
    assert last_line(capsys) == 'NULL'
    dispatcher.dispatch('COUNTS', ['1'])
    assert last_line(capsys) == '2'
    dispatcher.dispatch('COUNTS', ['2'])
    assert last_line(capsys) == '0'
    dispatcher.dispatch('ROLLBACK', [])
    assert 'Откат транзакции выполнен' in last_line(capsys)
    dispatcher.dispatch('GET', ['a'])
    assert last_line(capsys) == 'NULL'
    dispatcher.dispatch('GET', ['b'])
    assert last_line(capsys) == 'NULL'
    dispatcher.dispatch('COUNTS', ['1'])
    assert last_line(capsys) == '0'
    dispatcher.dispatch('COUNTS', ['2'])
    assert last_line(capsys) == '0'
    dispatcher.dispatch('FIND', ['1'])
    assert last_line(capsys) == 'NULL'
    dispatcher.dispatch('FIND', ['2'])
    assert last_line(capsys) == 'NULL' 