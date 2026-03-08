
import pytest
from thread_pool_chooser import Request, _RequestAsKey

def test_request_kwargs_as_key ():
  request = Request(
    id_=1,
    func=lambda *args, **kwargs: print(*args, **kwargs),
    args=(1, 2, 3),
    kwargs={"a": 1, "b": 2, "c": 3}
  )
  assert request.as_key() == _RequestAsKey(
    id_=request.id_,
    func=request.func,
    args=request.args,
    kwargs=(("a", 1), ("b", 2), ("c", 3))
  )
