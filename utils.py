# utils.py
# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ PHARM-POS AGGREGATOR
# ============================================================

import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

# ============================================================
# SROK NORMALIZATION
# ============================================================


def normalize_srok(raw: Optional[str], pattern: Optional[str] = None) -> str:
    """
    Нормализует срок годности в формат dd-mm-yyyy.

    raw:
        фактическое значение от поставщика
        (20261031, 31.10.2026, 2026-10-31 и т.п.)

    pattern:
        маска из SupplierSrokResponse.provider_srok_raw
        (yyyymmdd, ddmmyyyy, dd.mm.yyyy, yyyy-mm-dd и т.п.)
    """

    if not raw:
        return ""

    raw_str = str(raw).strip()

    # -------- STRICT BY PATTERN --------
    if pattern:
        pattern_str = pattern.strip().lower()

        pat_clean = re.sub(r"[^dmy]", "", pattern_str)
        digits = re.sub(r"\D", "", raw_str)

        if pat_clean and len(pat_clean) == len(digits):
            groups = []
            start = 0

            for i in range(1, len(pat_clean) + 1):
                if i == len(pat_clean) or pat_clean[i] != pat_clean[start]:
                    groups.append((pat_clean[start], start, i))
                    start = i

            pos = 0
            dd = mm = yyyy = None

            for ch, s, e in groups:
                length = e - s
                part = digits[pos : pos + length]
                pos += length

                if ch == "d":
                    dd = part
                elif ch == "m":
                    mm = part
                elif ch == "y":
                    yyyy = part

            if yyyy:
                if len(yyyy) == 2:
                    yyyy = "20" + yyyy
                elif len(yyyy) == 3:
                    yyyy = "2" + yyyy

            if dd and mm and yyyy:
                return f"{dd.zfill(2)}-{mm.zfill(2)}-{yyyy}"

    # -------- FALLBACK HEURISTIC --------
    cleaned = raw_str.replace("/", "-").replace(".", "-").replace(" ", "-")
    parts = [p for p in cleaned.split("-") if p]

    # YYYY-MM-DD
    if len(parts) == 3 and len(parts[0]) == 4:
        return f"{parts[2].zfill(2)}-{parts[1].zfill(2)}-{parts[0]}"

    # DD-MM-YYYY
    if len(parts) == 3 and len(parts[2]) == 4:
        return f"{parts[0].zfill(2)}-{parts[1].zfill(2)}-{parts[2]}"

    # MM-YYYY
    if len(parts) == 2 and len(parts[1]) == 4:
        return f"01-{parts[0].zfill(2)}-{parts[1]}"

    # MM-YY
    if len(parts) == 2 and len(parts[1]) == 2:
        return f"01-{parts[0].zfill(2)}-20{parts[1]}"

    # 122025
    if re.fullmatch(r"\d{6}", raw_str):
        return f"01-{raw_str[:2]}-20{raw_str[2:]}"

    return raw_str


# ============================================================
# SAFE GET
# ============================================================


def safe_get(data: Any, key: Optional[str], default=None):
    if not key or not isinstance(data, dict):
        return default
    return data.get(key, default)


# ============================================================
# NESTED GET (JSON / XML)
# ============================================================


def nested_get(data: Any, path: Optional[str], default=None):
    """
    Извлекает значение по пути через точку:
    Envelope.Body.GetPriceResponse.goods.good
    """

    if not path or not isinstance(data, dict):
        return default

    try:
        current = data
        for key in path.split("."):
            if isinstance(current, list):
                if not current:
                    return default
                current = current[0]

            if not isinstance(current, dict):
                return default

            current = current.get(key)
            if current is None:
                return default

        return current
    except Exception:
        return default


# ============================================================
# FORCE LIST
# ============================================================


def to_list(data: Any) -> List[Any]:
    if data is None:
        return []
    if isinstance(data, list):
        return data
    return [data]


# ============================================================
# BARCODE NORMALIZATION
# ============================================================


def normalize_barcode(value: Any) -> List[str]:
    """
    Приводит barcode к List[str] для JSONB
    """

    if value is None:
        return []

    if isinstance(value, dict):
        value = next(iter(value.values()), None)

    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]

    if isinstance(value, str):
        if "," in value:
            return [v.strip() for v in value.split(",") if v.strip()]
        return [value.strip()]

    return [str(value).strip()]


# ============================================================
# NUMERIC NORMALIZATION
# ============================================================


def normalize_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return None


# ============================================================
# CITY EXTRACTOR
# ============================================================


def extract_city_from_anywhere(
    *,
    query_params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, Any]] = None,
    text: Optional[str] = None,
) -> Optional[str]:

    sources: List[Any] = []

    if query_params:
        sources.append(query_params)
    if body:
        sources.append(body)
    if headers:
        sources.append(headers)
    if text:
        sources.append({"text": text})

    for source in sources:
        if isinstance(source, dict):
            for key, value in source.items():
                k = str(key).lower()
                if any(x in k for x in ("city", "town", "region")):
                    return str(value).strip()
                if k in {"code", "citycode", "regioncode"}:
                    return str(value).strip()
        elif isinstance(source, str):
            return source.strip()

    return None


# ============================================================
# CITY NORMALIZATION
# ============================================================


def normalize_city(value: Any) -> Optional[str]:
    if value is None:
        return None
    val = str(value).strip()
    return val.lower() if val else None


# ============================================================
# UNIT NORMALIZATION
# ============================================================


def clean_unit(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None

    val = str(raw).lower().strip()
    val = val.replace(".", "").replace(",", "").replace("(", "").replace(")", "")
    val = re.sub(r"\s+", " ", val)
    val = re.sub(r"\d+", "", val)
    val = val.replace("/", "").replace("\\", "")
    return val.strip()


# ============================================================
# PRODUCT NAME NORMALIZATION
# ============================================================


def normalize_name(name: Optional[str]) -> str:
    if not name:
        return ""

    s = name.lower()
    s = re.sub(r"[\s\-_,.;:]+", " ", s)

    trash = [
        "таблетки",
        "табл",
        "капсулы",
        "капс",
        "раствор",
        "спрей",
        "мазь",
        "крем",
        "гель",
        "ампулы",
        "амп",
        "фл",
        "флакон",
        "штука",
        "шт",
        "уп",
        "упаковка",
        "№",
        "n",
        "ml",
        "мл",
        "mg",
        "мг",
    ]

    for t in trash:
        s = s.replace(t, " ")

    return re.sub(r"\s+", " ", s).strip()


from typing import Iterable, List, TypeVar

T = TypeVar("T")


def chunked(iterable: Iterable[T], size: int):
    buf: List[T] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf
