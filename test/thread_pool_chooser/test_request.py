
import pytest
from thread_pool_chooser import Request

def test_request_kwargs_as_dict ():
  request = Request(
    id_=1,
    func=lambda *args, **kwargs: print(*args, **kwargs),
    args=(1, 2, 3),
    kwargs=(("a", 1), ("b", 2), ("c", 3))
  )
  assert request.kwargs_as_dict == {"a": 1, "b": 2, "c": 3}
