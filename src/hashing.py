import hashlib, re
from typing import Iterable, Any

_NULL = "§NULL§"
_WS_RE = re.compile(r"\s+")

def _norm(v: Any) -> str:
    if v is None or v == "":
        return _NULL
    s = str(v).strip().upper()
    s = _WS_RE.sub(" ", s)
    return s or _NULL

def dv2_hash(cols: Iterable[Any]) -> str:
    joined = "|".join(_norm(c) for c in cols)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
