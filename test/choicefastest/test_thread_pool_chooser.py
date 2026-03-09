
import time
import pytest
import random
from choicefastest import ChoiceFastest, _RequestAsKey
from threading import Lock

@pytest.fixture
def test_chooser ():
  chooser = ChoiceFastest(3)
  assert chooser.closed == False
  yield chooser
  chooser.close()
  assert chooser.closed == True

def test_choice_fastest (test_chooser:ChoiceFastest):

  #基本的な操作に対する動作確認を行います

  min_value = 999

  def sample_func (*args, **kwargs):
    nonlocal min_value
    value = random.randint(0, 5)
    min_value = min(min_value, value)
    time.sleep(value)
    return value, args, kwargs

  id_ = test_chooser.put(
    sample_func, 
    (1, 2, 3), 
    {"a": 1, "b": 2, "c": 3}
  )
  assert isinstance(id_, int)
  assert test_chooser.get(id_) == (
    (min_value, (1, 2, 3), {"a": 1, "b": 2, "c": 3}),
    True
  )
  test_chooser.close()
  assert test_chooser.exceptions() == {}

def test_choice_fastest_error (test_chooser:ChoiceFastest):
  
  #関数が例外を送出した場合の動作確認です

  exception = Exception()

  def sample_func ():
    nonlocal exception
    raise exception

  id_ = test_chooser.put(sample_func)
  assert isinstance(id_, int)
  assert test_chooser.get(id_) == (None, False)
  test_chooser.close()
  assert test_chooser.exceptions() == {
    _RequestAsKey(id_, sample_func, (), ()): [exception, exception, exception]
  }

def test_choice_fastest_error2 (test_chooser:ChoiceFastest):

  #一部スレッドのみが例外を送出した場合の動作確認です

  lock = Lock()
  count_up = 0
  exception = Exception()

  def sample_func ():
    nonlocal lock
    nonlocal count_up
    nonlocal exception
    with lock:
      count_up += 1
      if count_up == 1:
        raise exception #最初に実行されたスレッドのみ例外を送出する
      else:
        return 123

  id_ = test_chooser.put(sample_func)
  assert isinstance(id_, int)
  assert test_chooser.get(id_) == (123, True)
  test_chooser.close()
  assert test_chooser.exceptions() == {
    _RequestAsKey(id_, sample_func, (), ()): [exception]
  }

def test_choice_fastest_put (test_chooser:ChoiceFastest):

  #.put の返り値がほぼ一意であることを検証します

  id1 = test_chooser.put(lambda: None)
  id2 = test_chooser.put(lambda: None)
  assert id1 != id2
