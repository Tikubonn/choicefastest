
import time
import pytest
from choicefastest import CatchedErrors, _WorkerThread

@pytest.fixture
def test_worker_thread_ () -> "typing.Generator[choicefastest._WorkerThread, None, None]":
  catched_errors = CatchedErrors()
  worker_thread = _WorkerThread(catched_errors)
  yield worker_thread
  assert worker_thread.closed == False
  worker_thread.close()
  assert worker_thread.closed == True

WAIT_SECONDS:int = 2

def test_worker_thread (test_worker_thread_:_WorkerThread):

  #関数の実行結果を取得する動作確認です

  assert test_worker_thread_.put(
    1, 
    lambda *args, **kwargs: (args, kwargs), 
    (1, 2, 3), 
    {"a": 1, "b": 2, "c": 3}
  ) == True

  time.sleep(WAIT_SECONDS) #関数の実行終了まで待機する

  assert test_worker_thread_.get(1) == (
    ((1, 2, 3), {"a": 1, "b": 2, "c": 3}),
    True,
    True,
  )

def test_worker_thread_error (test_worker_thread_:_WorkerThread):

  #例外が発生した場合の動作確認です

  def test_func ():
    raise Exception()

  assert test_worker_thread_.put(1, test_func) == True

  time.sleep(WAIT_SECONDS) #関数の実行終了まで待機する

  assert test_worker_thread_.get(1) == (None, False, True) #(None, 実行結果は失敗, 実行自体は完了) という意味になります
  assert 0 < len(test_worker_thread_.catched_errors.inner_dict) #例外が捕捉された場合、内部の .cactched_errors に当該例外が記録されます

def test_worker_thread_not_found (test_worker_thread_:_WorkerThread):

  #未実行の関数の結果を取得しようとした場合の動作確認です

  assert test_worker_thread_.get(1) == (None, False, False)
