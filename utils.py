# utils.py
# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ PHARM-POS AGGREGATOR
# ============================================================

from typing import Any, List, Optional, Dict, Union


# ============================================================
# SAFE GET
# ============================================================

def safe_get(data: dict, key: str, default=None):
    """
    Безопасно достаёт значение из словаря по ключу
    (если ключа нет — не падает)
    """
    if not key:
        return default

    if not isinstance(data, dict):
        return default

    return data.get(key, default)


# ============================================================
# GET VALUE BY PATH (JSON / XML SUPPORT)
# ============================================================

def nested_get(data: dict, path: str, default=None):
    """
    Достаёт вложенное значение по пути через точку.

    Пример:
    "Envelope.Body.GetPriceResponse.goods.good"
    """

    if not path or not isinstance(data, dict):
        return default

    try:
        keys = path.split(".")
        current = data

        for key in keys:
            if isinstance(current, list):
                if not current:
                    return default
                current = current[0]

            if isinstance(current, dict):
                current = current.get(key)
            else:
                return default

            if current is None:
                return default

        return current

    except Exception:
        return default


# ============================================================
# FORCE LIST
# ============================================================

def to_list(data: Any) -> List:
    """
    Приводит данные к списку:
    - список → как есть
    - объект → [объект]
    - None → []
    """
    if data is None:
        return []

    if isinstance(data, list):
        return data

    return [data]


# ============================================================
# BARCODE TO JSONB LIST
# ============================================================

def normalize_barcode(value: Any) -> List[str]:
    """
    Приводит barcode к массиву для JSONB
    Даже если приходит строкой, числом или с xml
    """

    if value is None:
        return []

    # Если это словарь (редко, но бывает в XML)
    if isinstance(value, dict):
        # попытка вытащить значение
        value = list(value.values())[0] if value else None

    # Если список
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]

    # Если строка с разделителями
    if isinstance(value, str):
        if "," in value:
            return [v.strip() for v in value.split(",") if v.strip()]
        return [value.strip()]

    # Если число или что-то еще
    return [str(value).strip()]


# ============================================================
# NUMERIC NORMALIZATION
# ============================================================

def normalize_numeric(value: Any) -> Optional[float]:
    """
    Безопасная попытка превратить в число (float)
    """

    if value is None:
        return None

    try:
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return None


# ============================================================
# CITY EXTRACTOR (AUTOMATIC)
# ============================================================

def extract_city_from_anywhere(
    *,
    query_params: Dict[str, Any] | None = None,
    body: Dict[str, Any] | None = None,
    headers: Dict[str, Any] | None = None,
    text: str | None = None,
) -> Optional[str]:
    """
    Пытается извлечь название/код города
    откуда угодно:
    - из query params
    - из body
    - из headers
    - из текста / строки / url
    """

    possible_sources = []

    if query_params:
        possible_sources.append(query_params)

    if body:
        possible_sources.append(body)

    if headers:
        possible_sources.append(headers)

    if text:
        possible_sources.append({"text": text})

    for source in possible_sources:

        if isinstance(source, dict):
            for key, value in source.items():
                key_l = str(key).lower()

                if "city" in key_l or "town" in key_l or "region" in key_l:
                    return str(value).strip()

                # иногда напрямую может быть код типа: "0100"
                if key_l in ["code", "citycode", "regioncode"]:
                    return str(value).strip()

        if isinstance(source, str):
            return source.strip()

    return None


# ============================================================
# MAP ANY CITY TO NORMAL FORM
# ============================================================

def normalize_city(value: Any) -> Optional[str]:
    """
    Приводит город к нормализованному виду
    (Example: "shymkent", "Шымкент", "0100")
    """

    if value is None:
        return None

    value = str(value).strip()

    if not value:
        return None

    # можно будет потом добавлять маппинг
    return value.lower()
