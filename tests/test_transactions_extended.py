import pytest
from utils.key_value_store import KeyValueStore

@pytest.fixture
def store():
    return KeyValueStore()

def test_nested_unset_commit_persists_tombstone(store):
    """Тест: удаление в глубокой транзакции и COMMIT сохраняет tombstone (NULL) наверх"""
    store.set('a', '1')
    store.begin()
    store.set('a', '2')
    store.begin()
    store.unset('a')
    store.commit()  # после коммита внутреннего транзакция a должна быть NULL
    assert store.get('a') == 'NULL'
    store.commit()  # после финального коммита a должна оставаться NULL
    assert store.get('a') == 'NULL'

def test_nested_set_commit_overrides_parent_value(store):
    """Тест: установка значения внутри транзакции и COMMIT перезаписывает родителя"""
    store.set('a', '1')
    store.begin()
    store.set('a', '2')
    store.commit()
    assert store.get('a') == '2'

def test_rollback_restores_previous_state(store):
    """Тест: ROLLBACK возвращает прежнее значение"""
    store.set('a', '1')
    store.begin()
    store.set('a', '2')
    store.rollback()
    assert store.get('a') == '1'

def test_counts_and_find_through_transactions(store):
    """Тест: COUNTS и FIND с учетом транзакций (unset и новые ключи)"""
    store.set('a', '1')
    store.set('b', '1')
    store.set('c', '2')
    assert store.counts('1') == 2
    assert set(store.find('1')) == {'a', 'b'}

    store.begin()
    store.unset('b')
    store.set('d', '1')
    assert store.counts('1') == 2  # a и d
    assert set(store.find('1')) == {'a', 'd'}
    store.rollback()

    assert store.counts('1') == 2
    assert set(store.find('1')) == {'a', 'b'}

def test_commit_without_active_transaction_is_safe(store):
    """Тест: COMMIT без активной транзакции не меняет состояние и не бросает исключений"""
    store.set('x', 'X')
    store.commit()
    assert store.get('x') == 'X'

def test_rollback_without_active_transaction_is_safe(store):
    """Тест: ROLLBACK без активной транзакции не меняет состояние и не бросает исключений"""
    store.set('y', 'Y')
    store.rollback()
    assert store.get('y') == 'Y' 