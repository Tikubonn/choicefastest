
import time
import queue
import logging
import traceback
from threading import Thread, Lock
from closeable import ICloseable, Closeable
from dataclasses import dataclass
from typing import NamedTuple, Callable, Hashable, Self, Any

_LOGGER:logging.Logger = logging.getLogger(__name__)

class Request (NamedTuple):

  """ワーカースレッドに依頼する実行関数の情報がまとめられた名前付きタプルです。

  Notes
  -----
  本インスタンスは CatchedErrors オブジェクトのキーオブジェクトとしても使用されます。

  Attributes
  ----------
  id_ : int
    ある一定期間内での一意性が確保されている識別子です。
  func : Callable[[...], Any]
    実行される関数オブジェクトです。
  args : tuple[Any, ...]
    実行される関数に渡される引数です。
  kwargs : dict[str, Any]
    実行される関数に渡されるキーワード引数です。
  """

  id_:int
  func:Callable[[...], Any]
  args:tuple[Any, ...]
  kwargs:dict[str, Any]

  def as_key (self) -> "thread_pool_chooser._RequestAsKey":
    return _RequestAsKey(
      self.id_,
      self.func,
      self.args,
      tuple(sorted(((k, v) for k, v in self.kwargs.items())))
    )

class _RequestAsKey (NamedTuple):

  id_:int
  func:Callable[[...], Any]
  args:tuple[Any, ...]
  kwargs:tuple[tuple[str, Any], ...]

  def __hash__ (self) -> float:
    return hash((
      self.id_,
      self.func if isinstance(self.func, Hashable) else None,
      tuple((a if isinstance(a, Hashable) else None for a in self.args)),
      tuple(((k, v if isinstance(v, Hashable) else None) for k, v in self.kwargs))
    ))

class _Response (NamedTuple):

  id_:int
  result:Any
  succeed:bool

class CatchedErrors:

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

class _WorkerThread (ICloseable):

  """依頼された関数を実行するワーカースレッドの機能を提供します。
  """

  def _on_close (self):
    self.should_loop = False
    self.thread.join()

    _LOGGER.debug("Closed object: {!r}".format(self)) #log.

  def _thread_main (self):
    while self.should_loop:
      if self.cur_request:
        request = self.cur_request
        try:
          try:
            result = request.func(*request.args, **request.kwargs)
            response = _Response(request.id_, result, True)
            self.result_queue.put(response)

            _LOGGER.debug("Request succeed: {!r} -> {!r}".format(self.cur_request, result)) #log.

          except Exception as exception:
            response = _Response(request.id_, None, False)
            self.result_queue.put(response)
            self.catched_errors.add(request, exception)

            traceback.print_exc() #log.
            _LOGGER.debug("Request failed: {!r} -> {!r}".format(self.cur_request, exception)) #log.

        finally:
          self.cur_request = None

  def _thread_setup (self):
    self.thread = Thread(target=self._thread_main)
    self.thread.start()

  def __init__ (self, catched_errors:CatchedErrors):

    """インスタンスの初期化を行います。

    Parameters
    ----------
    catched_errors : CatchedErrors
      例外発生時にその記録を行う CatchedErrors オブジェクトです。
    """

    self.catched_errors = catched_errors
    self.should_loop = True
    self.cur_request = None
    self.result_queue = queue.Queue()
    self.error_queue = queue.Queue()
    self.thread = None
    self.closeable = Closeable(self._on_close)
    self._thread_setup()

  @property
  def closed (self) -> bool:
    return self.closeable.closed

  def close (self):
    self.closeable.close()

  def put (self, id_:int, func:Callable[[...], Any], args:tuple[Any, ...]=(), kwargs:dict[str, Any]={}) -> bool:

    """ワーカースレッドに実行させる関数を登録します。

    Parameters
    ----------
    id_ : int
      実行依頼の識別子です。
    func : Callable[[...], Any]
      登録される実行関数です。
    args : tuple[Any, ...]
      関数実行時に渡される引数の組です。
      未指定ならば空のタプルが設定されます。
    kwargs : dict[str, Any]
      関数実行時に渡されるキーワード引数の集合です。
      未指定ならば空の辞書が設定されます。

    Returns
    -------
    bool
      遂行中の依頼がない状態ならば、実行関数の情報を登録し True を返します。
      逆に遂行中の依頼があるならば本メソッドは即座に False を返します。
    """

    if not self.cur_request:
      request = Request(id_, func, args, kwargs)
      self.cur_request = request
      return True
    else:
      return False

  def get (self, id_:int) -> tuple[Any, bool, bool]:

    """ワーカースレッドが遂行した関数の実行結果を取得します。

    Parameters
    ----------
    id_ : int
      取得する実行結果の識別子です。

    Returns
    -------
    tuple[Any, bool, bool]
      左から関数の実行結果・処理が無事に完了したかを表す真偽値・結果自体が存在するかを表す真偽値、となります。
    """

    while True:
      try:
        response = self.result_queue.get(timeout=0.0)
        if response.id_ == id_:
          return response.result, response.succeed, True
      except queue.Empty:
        return (None, False, False)

class ChoiceFastest (ICloseable):

  """実行毎に処理時間が異なる関数を、複数スレッドで並行実行し、最も早く終了した結果を取得する機能を提供します。
  """

  def _on_close (self):
    for thread in self.worker_threads:
      thread.close()

    _LOGGER.debug("Closed object: {!r}".format(self)) #log.

  def __init__ (self, exec_thread_count:int, max_thread_count:int=0, max_id:int=65536):

    """インスタンスの初期化を行います。

    Parameters
    ----------
    exec_thread_count : int
      依頼時に一度に並行処理される数です。
    max_thread_count : int
      待機状態を含めたワーカースレッドの数です。
      未指定ならば 0 が設定されます。
    max_id : int
      動的に割り当てられる依頼識別子の最大値です。
      未指定ならば 65536 が設定されます。
    """

    self.exec_thread_count = exec_thread_count
    self.max_thread_count = max(max_thread_count, exec_thread_count * 2)
    self.max_id = max(max_id, 1)
    self.cur_id = 0
    self.catched_errors = CatchedErrors()
    self.worker_threads = [
      _WorkerThread(self.catched_errors) for _ in range(self.max_thread_count)
    ]
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

  def put (self, func:Callable[[...], Any], args:tuple[Any, ...]=(), kwargs:dict[str, Any]={}, interval:float=0.001) -> int:

    """空いているワーカースレッドに対して関数の実行を依頼します。

    Notes
    -----
    依頼数が exec_thread_count を満たすまでの間、本関数は処理を待機します。
  
    Parameters
    ----------
    func : Callable[[...], Any]
      登録される実行関数です。
    args : tuple[Any, ...]
      関数実行時に渡される引数の組です。
      未指定ならば空のタプルが設定されます。
    kwargs : dict[str, Any]
      関数実行時に渡されるキーワード引数の集合です。
      未指定ならば空の辞書が設定されます。
    interval : float
      依頼数が exec_thread_count を満たさない間に、再試行までの待機する時間です。
      未指定ならば 0.001 秒が設定されます。

    Returns
    -------
    int 
      規定数の依頼が完了すると、当該依頼の識別子が返されます。
    """

    self.cur_id = (self.cur_id +1) % self.max_id
    should_sleep = False
    put_succeed_count = 0
    while put_succeed_count < self.exec_thread_count:
      if should_sleep:
        time.sleep(interval)
      else:
        should_sleep = True
      for thread in self.worker_threads:
        if put_succeed_count < self.exec_thread_count:
          put_succeed = thread.put(self.cur_id, func, args, kwargs)
          if put_succeed:
            put_succeed_count += 1
    return self.cur_id

  def get (self, id_:int, interval:float=0.001) -> tuple[Any, bool]:

    """依頼したワーカースレッドから処理結果を取得します。

    Notes
    -----
    満足いく処理結果が取得できるまでの間、本関数は処理を待機します。

    Warnings
    --------
    引数 id_ に最新の依頼識別子以外の値を与えた場合の動作は未定義です。

    Parameters
    ----------
    id_ : int
      取得する実行結果の識別子です。
    interval : float
      依頼数が exec_thread_count を満たさない間に、再試行までの待機する時間です。
      未指定ならば 0.001 秒が設定されます。
    """

    should_sleep = False
    found_count = 0
    while found_count < self.exec_thread_count:
      if should_sleep:
        time.sleep(interval)
      else:
        should_sleep = True
      for thread in self.worker_threads:
        result, succeed, found = thread.get(id_)
        if found:
          found_count += 1
          if succeed:
            return result, True
    else:
      return None, False

  def exceptions (self) -> dict[Request, Exception]:

    """その時点までに記録された例外情報を辞書形式で返します。

    Notes
    -----
    現在遂行中の処理を含めた例外情報を取得するには、
    一度 .close メソッドを実行し、全てのワーカースレッドが終了するまで待機する必要があります。

    Returns
    -------
    dict[Request, list[Exception]]
      これまでに記録された例外の記録です。
    """

    return self.catched_errors.as_dict()
