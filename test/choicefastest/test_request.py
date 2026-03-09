
import pytest
from choicefastest import Request, _RequestAsKey

def test_request_kwargs_as_key ():
  request = Request(
    func=lambda *args, **kwargs: print(*args, **kwargs),
    args=(1, 2, 3),
    kwargs={"a": 1, "b": 2, "c": 3}
  )
  assert request.as_key() == _RequestAsKey(
    func=request.func,
    args=request.args,
    kwargs=(("a", 1), ("b", 2), ("c", 3))
  )
