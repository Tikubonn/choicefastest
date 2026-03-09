
import time
import queue
import logging
import traceback
from typing import NamedTuple, Callable, Hashable, Self, Any
from threading import Thread, Lock
from closeable import ICloseable, Closeable
from concurrent.futures import ThreadPoolExecutor

_LOGGER:logging.Logger = logging.getLogger(__name__)

class Request (NamedTuple):

  """ワーカースレッドに依頼する実行関数の情報がまとめられた名前付きタプルです。

  Notes
  -----
  本インスタンスは _CatchedErrors オブジェクトのキーオブジェクトとしても使用されます。

  Attributes
  ----------
  func : Callable[[...], Any]
    実行される関数オブジェクトです。
  args : tuple[Any, ...]
    実行される関数に渡される引数です。
  kwargs : dict[str, Any]
    実行される関数に渡されるキーワード引数です。
  """

  func:Callable[[...], Any]
  args:tuple[Any, ...]=()
  kwargs:dict[str, Any]={}

  def as_key (self) -> "thread_pool_chooser._RequestAsKey":
    return _RequestAsKey(
      self.func,
      self.args,
      tuple(sorted(((k, v) for k, v in self.kwargs.items())))
    )

class _RequestAsKey (NamedTuple):

  func:Callable[[...], Any]
  args:tuple[Any, ...]
  kwargs:tuple[tuple[str, Any], ...]

  def __hash__ (self) -> float:
    return hash((
      self.func if isinstance(self.func, Hashable) else None,
      tuple((a if isinstance(a, Hashable) else None for a in self.args)),
      tuple(((k, v if isinstance(v, Hashable) else None) for k, v in self.kwargs))
    ))

class _Response (NamedTuple):

  result:Any
  succeed:bool

class _CatchedErrors:

  """ワーカースレッドで送出された例外を記録する機能を提供します。
  """

  def __init__ (self):
    self.inner_dict = {}
    self.lock = Lock()

  def add (self, request:Request, exception:Exception):

    """ワーカースレッドで送出された例外を記録します。

    Parameters
    ----------
    request : Request
      例外の発生源を識別するために利用される Request オブジェクトです。
    exception : Exception
      実際に送出された例外オブジェクトです。
    """

    with self.lock:
      request_as_key = request.as_key()
      self.inner_dict.setdefault(request_as_key, [])
      self.inner_dict[request_as_key].append(exception)

  def as_dict (self) -> dict[_RequestAsKey, list[Exception]]:

    """これまでに記録された例外情報を辞書形式で返します。

    Returns
    -------
    dict[_RequestAsKey, list[Exception]]
      これまでに記録された例外の記録です。
    """

    with self.lock:
      return self.inner_dict.copy()

class _WorkerFutures:

  """依頼された関数を実行するワーカースレッドの機能を提供します。
  """

  def __init__ (self, catched_errors:_CatchedErrors, executor:ThreadPoolExecutor):
    self.catched_errors = catched_errors
    self.executor = executor
    self.response_queue = queue.Queue()

  def _worker_main (self, request:Request):
    try:
      result = request.func(*request.args, **request.kwargs)
      response = _Response(result, True)
      self.response_queue.put(response)

      _LOGGER.debug("Request succeed: {!r} -> {!r}".format(request, result)) #log.

    except Exception as exception:
      response = _Response(None, False)
      self.response_queue.put(response)
      self.catched_errors.add(request, exception)

      traceback.print_exc() #log.
      _LOGGER.debug("Request failed: {!r} -> {!r}".format(request, exception)) #log.

  def exec (self, requests:list[Request], interval:float=0.001) -> tuple[Any, bool]:

    """...

    Parameters
    ----------
    requests : list[Request]
      ...
    interval : float
      実行結果が得られなかった場合に待機する時間です。
      未指定ならば 0.001 秒が設定されます。

    Returns
    -------
    tuple[Any, bool]
      左から関数の実行結果・実行結果が存在するかを表す真偽値となります。
    """

    for request in requests:
      self.executor.submit(self._worker_main, request)
    response_count = 0
    while response_count < len(requests):
      try:
        response = self.response_queue.get(timeout=interval)
        if response.succeed:
          return response.result, True
        else:
          response_count += 1
      except queue.Empty:
        pass
    else:
      return None, False

class ChoiceFastest (ICloseable):

  """実行毎に処理時間が異なる関数を、複数スレッドで並行実行し、最も早く終了した結果を取得する機能を提供します。
  """

  def _on_close (self):
    self.executor.shutdown()

    _LOGGER.debug("Closed object: {!r}".format(self)) #log.

  def __init__ (self, max_workers:int|None=None):

    """インスタンスの初期化を行います。

    Parameters
    ----------
    max_workers : int|None
      ...
    """

    self.catched_errors = _CatchedErrors()
    self.executor = ThreadPoolExecutor(max_workers)
    self.closeable = Closeable(self._on_close)

  @property
  def closed (self) -> bool:
    return self.closeable.closed

  def close (self):
    self.closeable.close()

  def __enter__ (self) -> Self:
    return self

  def __exit__ (self, error_type, error_value, traceback):
    self.close()

  def exec (self, requests:list[Request], interval:float=0.001) -> tuple[Any, bool]:

    """...

    Parameters
    ----------
    requests : list[Request]
      ...
    interval : float
      実行結果が得られなかった場合に待機する時間です。
      未指定ならば 0.001 秒が設定されます。

    Returns
    -------
    tuple[Any, bool]
      左から関数の実行結果・実行結果が存在するかを表す真偽値となります。
    """

    worker_futures = _WorkerFutures(self.catched_errors, self.executor)
    return worker_futures.exec(requests, interval)

  def exceptions (self) -> dict[_RequestAsKey, Exception]:

    """その時点までに記録された例外情報を辞書形式で返します。

    Notes
    -----
    現在遂行中の処理を含めた例外情報を取得するには、
    一度 .close メソッドを実行し、全てのワーカースレッドが終了するまで待機する必要があります。

    Returns
    -------
    dict[_RequestAsKey, list[Exception]]
      これまでに記録された例外の記録です。
    """

    return self.catched_errors.as_dict()
