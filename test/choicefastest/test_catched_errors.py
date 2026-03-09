
import pytest
from choicefastest import _CatchedErrors, Request

def test_catched_errors ():
  catched_errors = _CatchedErrors()
  request = Request(
    func=lambda *args, **kwargs: print(*args, **kwargs),
    args=(1, 2, 3),
    kwargs={"a": 1, "b": 2, "c": 3}
  )
  exception = Exception()
  catched_errors.add(request, exception)
  catched_errors.add(request, exception)
  catched_errors.add(request, exception)
  assert catched_errors.as_dict() == {
    request.as_key(): [exception, exception, exception]
  }
