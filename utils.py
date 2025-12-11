# utils.py
# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ PHARM-POS AGGREGATOR
# ============================================================

from typing import Any, List, Optional, Dict, Union


def normalize_srok(raw: str | None, pattern: str | None = None) -> str:
    """
    Нормализует срок годности в единый формат dd-mm-yyyy.

    - raw: реальное значение из API, например "20261031" или "31.10.2026"
    - pattern: маска, которую ты записал в supplier_srok_response.provider_srok_raw,
      например "yyyymmdd", "ddmmyyyy", "dd.mm.yyyy", "yyyy-mm-dd" и т.п.

    Если pattern задана и длины совпадают — разбираем строго по маске.
    Если маска не подходит — fallback на старую эвристику.
    """

    if not raw:
        return ""

    raw_str = str(raw).strip()

    # Если есть pattern — пробуем разобрать по ней
    if pattern:
        pattern_str = pattern.strip()

        # Убираем разделители из паттерна, оставляем только d/m/y
        pat_clean = re.sub(r"[^dmyDMY]", "", pattern_str.lower())
        # Убираем всё кроме цифр из значения
        digits = re.sub(r"\D", "", raw_str)

        if pat_clean and len(pat_clean) == len(digits):
            # Разбиваем паттерн на группы (dddmmmYYYY и т.п.)
            groups = []
            start = 0
            for i in range(1, len(pat_clean) + 1):
                if i == len(pat_clean) or pat_clean[i] != pat_clean[start]:
                    groups.append((pat_clean[start], start, i))
                    start = i

            # Пробегаем по группам и вырезаем куски из digits
            pos = 0
            dd = None
            mm = None
            yyyy = None

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

            # Приводим год
            if yyyy:
                if len(yyyy) == 2:
                    yyyy = "20" + yyyy
                elif len(yyyy) == 3:
                    yyyy = "2" + yyyy  # на всякий случай

            # Если всё получилось — возвращаем dd-mm-yyyy
            if dd and mm and yyyy:
                dd = dd.zfill(2)
                mm = mm.zfill(2)
                return f"{dd}-{mm}-{yyyy}"

    # -------- Fallback: старая эвристика --------
    cleaned = raw_str.replace("/", "-").replace(".", "-").replace(" ", "-")
    parts = [p for p in cleaned.split("-") if p]

    # 1) YYYY-MM-DD
    if len(parts) == 3 and len(parts[0]) == 4:
        yyyy = parts[0]
        mm = parts[1].zfill(2)
        dd = parts[2].zfill(2)
        return f"{dd}-{mm}-{yyyy}"

    # 2) DD-MM-YYYY
    if len(parts) == 3 and len(parts[2]) == 4:
        dd = parts[0].zfill(2)
        mm = parts[1].zfill(2)
        yyyy = parts[2]
        return f"{dd}-{mm}-{yyyy}"

    # 3) MM-YYYY → ставим 01 день
    if len(parts) == 2 and len(parts[1]) == 4:
        mm = parts[0].zfill(2)
        yyyy = parts[1]
        return f"01-{mm}-{yyyy}"

    # 4) MM-YY → ставим 20YY
    if len(parts) == 2 and len(parts[1]) == 2:
        mm = parts[0].zfill(2)
        yyyy = "20" + parts[1]
        return f"01-{mm}-{yyyy}"

    # 5) "122025" и т.п.
    digits = re.fullmatch(r"\d{6}", raw_str)
    if digits:
        mm = raw_str[:2]
        yyyy = raw_str[2:]
        if len(yyyy) == 2:
            yyyy = "20" + yyyy
        return f"01-{mm}-{yyyy}"

    # 6) fallback — возвращаем как есть
    return raw_str
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


import re

def clean_unit(raw: str | None) -> str | None:
    """
    Умная очистка unit:
    - нижний регистр
    - удаляем точки, запятые, лишние пробелы
    - удаляем '1', цифры
    - убираем 'шт/уп' → 'штуп' (чтобы потом сравнивать)
    """
    if not raw:
        return None

    val = str(raw).strip().lower()

    # символы мусора
    val = val.replace(".", "").replace(",", "").replace("(", "").replace(")", "")

    # удаляем повторяющиеся пробелы
    val = re.sub(r"\s+", " ", val).strip()

    # убираем цифры "1 шт" → "шт"
    val = re.sub(r"\d+", "", val).strip()

    # заменяем разделители
    val = val.replace("/", "").replace("\\", "")

    return val


def normalize_name(name: str | None) -> str:
    if not name:
        return ""
    s = name.lower()

    # убираем лишние пробелы / спецсимволы
    s = re.sub(r"[\s\-_,.;:]+", " ", s)

    # выкидываем общие слова: табл, таблетки, капс, капсулы, №, шт, уп, мл, мг и т.п.
    trash = [
        "таблетки", "табл", "капсулы", "капс", "раствор",
        "спрей", "мазь", "крем", "гель", "ампулы", "амп",
        "фл", "флакон", "штука", "шт", "уп", "упаковка",
        "№", "n", "ml", "мл", "mg", "мг",
    ]
    for t in trash:
        s = s.replace(t, " ")

    # убираем двойные пробелы
    s = re.sub(r"\s+", " ", s).strip()
    return s

# utils.py

from datetime import datetime, date


