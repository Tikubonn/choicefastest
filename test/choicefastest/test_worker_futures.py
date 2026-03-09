
import time
import pytest
from concurrent.futures import ThreadPoolExecutor
from choicefastest import _CatchedErrors, _WorkerFutures, _RequestAsKey, Request

@pytest.fixture
def test_worker_futures_ () -> "typing.Generator[choicefastest._WorkerFutures, None, None]":
  with ThreadPoolExecutor(max_workers=8) as executor:
    catched_errors = _CatchedErrors()
    worker_futures = _WorkerFutures(catched_errors, executor)
    yield worker_futures

def test_worker_futures (test_worker_futures_:_WorkerFutures):

  #関数の実行結果を取得する動作確認です

  assert test_worker_futures_.exec(
    [
      Request(
        lambda *args, **kwargs: (args, kwargs),
        (1, 2, 3),
        {"a": 1, "b": 2, "c": 3}
      )
    ] * 3
  ) == (((1, 2, 3), {"a": 1, "b": 2, "c": 3}), True)

def test_worker_futures_error (test_worker_futures_:_WorkerFutures):

  #例外が発生した場合の動作確認です

  exception = Exception()

  def test_func ():
    nonlocal exception
    raise exception

  assert test_worker_futures_.exec([Request(test_func)] * 3) == (None, False)
  test_worker_futures_.executor.shutdown()
  assert test_worker_futures_.catched_errors.as_dict() == {
    _RequestAsKey(test_func, (), ()): [exception, exception, exception]
  }

def test_worker_futures_empty (test_worker_futures_:_WorkerFutures):

  assert test_worker_futures_.exec([]) == (None, False)
