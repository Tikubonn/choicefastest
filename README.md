
# choicefastest

## Overview

![](https://img.shields.io/badge/Python-3.12-blue)
![](https://img.shields.io/badge/License-AGPLv3-blue)

実行毎に処理時間が異なる関数を、複数スレッドで並行実行し、最も早く終了した結果を取得する機能を提供します。

```py
import time
import random
from choicefastest import ChoiceFastest

def sample_func ():
  sleep_seconds = random.randint(0, 5)
  time.sleep(sleep_seconds)
  return sleep_seconds

with ChoiceFastest(3) as chooser:
  result, succeed = chooser.exec(sample_func)
  print(result, succeed) #0 ~ 5 の範囲内の最小値, True
```

## Install

```shell
pip install .
```

### Test

```shell
pip install .[test]
pytest .
```

### Document

```py
import choicefastest

help(choicefastest)
```

## Donation

<a href="https://buymeacoffee.com/tikubonn" target="_blank"><img src="doc/img/qr-code.png" width="3000px" height="3000px" style="width:150px;height:auto;"></a>

もし本パッケージがお役立ちになりましたら、少額の寄付で支援することができます。<br>
寄付していただいたお金は書籍の購入費用や日々の支払いに使わせていただきます。
ただし、これは寄付の多寡によって継続的な開発やサポートを保証するものではありません。ご留意ください。

If you found this package useful, you can support it with a small donation.
Donations will be used to cover book purchases and daily expenses.
However, please note that this does not guarantee ongoing development or support based on the amount donated.

## License

© 2026 tikubonn

choicefastest licensed under the [AGPLv3](./LICENSE).
