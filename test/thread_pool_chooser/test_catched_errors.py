
import pytest
from thread_pool_chooser import CatchedErrors, Request

def test_catched_errors ():
  catched_errors = CatchedErrors()
  request = Request(
    id_=1,
    func=lambda *args, **kwargs: print(*args, **kwargs),
    args=(1, 2, 3),
    kwargs=(("a", 1), ("b", 2), ("c", 3))
  )
  exception = Exception()
  catched_errors.add(request, exception)
  catched_errors.add(request, exception)
  catched_errors.add(request, exception)
  assert catched_errors.as_dict() == {
    request: [exception, exception, exception]
  }
