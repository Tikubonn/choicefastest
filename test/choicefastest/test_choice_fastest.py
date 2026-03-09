
import time
import pytest
import random
from choicefastest import ChoiceFastest, _RequestAsKey, Request
from threading import Lock

@pytest.fixture
def test_chooser ():
  chooser = ChoiceFastest(max_workers=3)
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

  assert test_chooser.exec(
    [
      Request(
        sample_func, 
        (1, 2, 3), 
        {"a": 1, "b": 2, "c": 3}
      )
    ] * 3
  ) == (
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

  assert test_chooser.exec([Request(sample_func)] * 3) == (None, False)
  test_chooser.close()
  assert test_chooser.exceptions() == {
    _RequestAsKey(sample_func, (), ()): [exception, exception, exception]
  }

def test_choice_fastest_error2 (test_chooser:ChoiceFastest):

  #一部スレッドのみが例外を送出した場合の動作確認です

  exception = Exception()

  def sample_func ():
    nonlocal exception
    raise exception

  assert test_chooser.exec(
    [Request(sample_func)] + [Request(lambda: 123)] * 2
  ) == (123, True)
  test_chooser.close()
  assert test_chooser.exceptions() == {
    _RequestAsKey(sample_func, (), ()): [exception]
  }
