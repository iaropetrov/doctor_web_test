import pytest
from utils.key_value_store import KeyValueStore

@pytest.fixture
def store():
    return KeyValueStore()

def test_set_and_get(store):
    """Тест: установка и получение значения по ключу"""
    store.set('ключ', 'значение')
    assert store.get('ключ') == 'значение'
    assert store.get('несуществующий') == 'NULL'

def test_unset(store):
    """Тест: удаление ключа"""
    store.set('a', '1')
    store.unset('a')
    assert store.get('a') == 'NULL'

def test_counts(store):
    """Тест: подсчет количества ключей по значению"""
    store.set('a', '1')
    store.set('b', '1')
    store.set('c', '2')
    assert store.counts('1') == 2
    assert store.counts('2') == 1
    assert store.counts('3') == 0

def test_find(store):
    """Тест: поиск ключей по значению"""
    store.set('a', '1')
    store.set('b', '1')
    store.set('c', '2')
    assert set(store.find('1')) == {'a', 'b'}
    assert store.find('2') == ['c']
    assert store.find('3') == []

def test_transactions(store):
    """Тест: вложенные транзакции, commit и rollback"""
    store.set('a', '1')
    store.begin()
    store.set('a', '2')
    assert store.get('a') == '2'
    store.rollback()
    assert store.get('a') == '1'
    store.begin()
    store.set('a', '3')
    store.commit()
    assert store.get('a') == '3' 