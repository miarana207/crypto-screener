"""
BINANCE AUTOMATED SCREENER
==========================

Objectif
--------
Scanner dynamiquement l'univers Binance Spot, appliquer une série de
critères de sélection de manière séquentielle, mesurer précisément
l'impact de chaque critère, puis analyser techniquement les survivants.

Architecture
------------
1. Découverte dynamique de l'univers Binance via exchangeInfo.
2. Récupération des statistiques 24h.
3. Récupération du carnet simplifié bid/ask pour calcul du spread.
4. Application séquentielle des critères.
5. Audit statistique de chaque critère.
6. Analyse technique des survivants :
   - EMA
   - RSI
   - MACD
   - ATR
   - ratio de volume
7. Classement technique.
8. Envoi d'un rapport détaillé par email.

Important
---------
- Le programme utilise uniquement la bibliothèque standard Python.
- Aucun requirements.txt n'est nécessaire.
- Aucun ordre n'est exécuté.
- Le programme est uniquement un scanner / outil d'analyse.
- Tous les paramètres importants sont configurables par variables
  d'environnement dans GitHub Actions.
"""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import math
import os
import smtplib
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Tuple


# ============================================================================
# CONFIGURATION
# ============================================================================

BINANCE_BASE_URL = os.getenv(
    "BINANCE_BASE_URL",
    "https://data-api.binance.vision",
).rstrip("/")

QUOTE_ASSETS = [
    x.strip().upper()
    for x in os.getenv("QUOTE_ASSETS", "USDT").split(",")
    if x.strip()
]

EXCLUDE_STABLECOINS = (
    os.getenv("EXCLUDE_STABLECOINS", "true").lower() == "true"
)

EXCLUDE_LEVERAGED_TOKENS = (
    os.getenv("EXCLUDE_LEVERAGED_TOKENS", "true").lower() == "true"
)

MIN_24H_QUOTE_VOLUME = float(
    os.getenv("MIN_24H_QUOTE_VOLUME", "5000000")
)

MIN_24H_TRADES = int(
    os.getenv("MIN_24H_TRADES", "1000")
)

MIN_24H_CHANGE_PERCENT = float(
    os.getenv("MIN_24H_CHANGE_PERCENT", "-100")
)

MAX_24H_CHANGE_PERCENT = float(
    os.getenv("MAX_24H_CHANGE_PERCENT", "100")
)

MAX_SPREAD_PERCENT = float(
    os.getenv("MAX_SPREAD_PERCENT", "0.50")
)

MIN_PRICE = float(
    os.getenv("MIN_PRICE", "0.00000001")
)

TECHNICAL_ENABLED = (
    os.getenv("TECHNICAL_ENABLED", "true").lower() == "true"
)

TECHNICAL_INTERVAL = os.getenv(
    "TECHNICAL_INTERVAL",
    "1h",
)

TECHNICAL_KLINES_LIMIT = int(
    os.getenv("TECHNICAL_KLINES_LIMIT", "250")
)

ENABLE_EMA = (
    os.getenv("ENABLE_EMA", "true").lower() == "true"
)

ENABLE_RSI = (
    os.getenv("ENABLE_RSI", "true").lower() == "true"
)

ENABLE_MACD = (
    os.getenv("ENABLE_MACD", "true").lower() == "true"
)

ENABLE_ATR = (
    os.getenv("ENABLE_ATR", "true").lower() == "true"
)

ENABLE_VOLUME_RATIO = (
    os.getenv("ENABLE_VOLUME_RATIO", "true").lower() == "true"
)

EMA_FAST_PERIOD = int(
    os.getenv("EMA_FAST_PERIOD", "20")
)

EMA_SLOW_PERIOD = int(
    os.getenv("EMA_SLOW_PERIOD", "50")
)

RSI_PERIOD = int(
    os.getenv("RSI_PERIOD", "14")
)

MACD_FAST_PERIOD = int(
    os.getenv("MACD_FAST_PERIOD", "12")
)

MACD_SLOW_PERIOD = int(
    os.getenv("MACD_SLOW_PERIOD", "26")
)

MACD_SIGNAL_PERIOD = int(
    os.getenv("MACD_SIGNAL_PERIOD", "9")
)

ATR_PERIOD = int(
    os.getenv("ATR_PERIOD", "14")
)

VOLUME_MA_PERIOD = int(
    os.getenv("VOLUME_MA_PERIOD", "20")
)

TECHNICAL_MAX_ASSETS = int(
    os.getenv("TECHNICAL_MAX_ASSETS", "300")
)

TECHNICAL_WORKERS = int(
    os.getenv("TECHNICAL_WORKERS", "8")
)

HTTP_TIMEOUT_SECONDS = int(
    os.getenv("HTTP_TIMEOUT_SECONDS", "20")
)

HTTP_RETRIES = int(
    os.getenv("HTTP_RETRIES", "3")
)

EMAIL_HOST = os.getenv(
    "EMAIL_HOST",
    "smtp.gmail.com",
)

EMAIL_PORT = int(
    os.getenv("EMAIL_PORT", "465")
)

EMAIL_USER = os.getenv("EMAIL_USER", "").strip()
EMAIL_PASS = os.getenv("EMAIL_PASS", "").strip()
EMAIL_TO = os.getenv("EMAIL_TO", "").strip()

EMAIL_TOP_RESULTS = int(
    os.getenv("EMAIL_TOP_RESULTS", "50")
)


# ============================================================================
# CONSTANTES
# ============================================================================

STABLECOIN_BASES = {
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "DAI",
    "TUSD",
    "USDP",
    "USDE",
    "USDD",
    "FRAX",
    "PYUSD",
    "EURC",
    "USD1",
    "RLUSD",
}

LEVERAGED_SUFFIXES = (
    "UP",
    "DOWN",
    "BULL",
    "BEAR",
)

LEVERAGED_PATTERNS = (
    "3L",
    "3S",
    "5L",
    "5S",
    "2L",
    "2S",
)

EPSILON = 1e-12


# ============================================================================
# OUTILS GENERAUX
# ============================================================================

def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def utc_timestamp() -> str:
    return utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_float(value: Any, default: float = float("nan")) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def fmt_number(
    value: Any,
    decimals: int = 2,
) -> str:
    number = safe_float(value)

    if math.isnan(number):
        return "N/A"

    return f"{number:,.{decimals}f}"


def fmt_percent(value: Any) -> str:
    number = safe_float(value)

    if math.isnan(number):
        return "N/A"

    return f"{number:.2f}%"


def json_dumps(data: Any) -> str:
    return json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        default=str,
    )


# ============================================================================
# HTTP BINANCE
# ============================================================================

def http_get_json(
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Appel GET JSON avec retries.

    Les erreurs 429/418/5xx sont retentées.
    """

    url = BINANCE_BASE_URL + endpoint

    if params:
        query = urllib.parse.urlencode(params)
        url = f"{url}?{query}"

    last_error: Optional[Exception] = None

    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 "
                        "Binance-Automated-Screener/1.0"
                    ),
                    "Accept": "application/json",
                },
                method="GET",
            )

            with urllib.request.urlopen(
                request,
                timeout=HTTP_TIMEOUT_SECONDS,
            ) as response:
                raw = response.read()

            return json.loads(raw.decode("utf-8"))

        except urllib.error.HTTPError as exc:
            last_error = exc

            retryable = (
                exc.code in {
                    418,
                    429,
                    500,
                    502,
                    503,
                    504,
                }
            )

            if not retryable or attempt >= HTTP_RETRIES:
                raise

            retry_after = exc.headers.get("Retry-After")

            if retry_after:
                try:
                    delay = float(retry_after)
                except ValueError:
                    delay = float(attempt * 2)
            else:
                delay = float(attempt * 2)

            time.sleep(min(delay, 15.0))

        except (
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError,
        ) as exc:
            last_error = exc

            if attempt >= HTTP_RETRIES:
                raise

            time.sleep(min(float(attempt * 2), 10.0))

        except Exception as exc:
            last_error = exc

            if attempt >= HTTP_RETRIES:
                raise

            time.sleep(min(float(attempt * 2), 10.0))

    if last_error:
        raise last_error

    raise RuntimeError("Erreur HTTP inconnue")


# ============================================================================
# RECUPERATION BINANCE
# ============================================================================

def fetch_exchange_info() -> Dict[str, Any]:
    return http_get_json("/api/v3/exchangeInfo")


def fetch_24h_tickers() -> List[Dict[str, Any]]:
    data = http_get_json("/api/v3/ticker/24hr")

    if not isinstance(data, list):
        raise RuntimeError(
            "Réponse inattendue de /api/v3/ticker/24hr"
        )

    return data


def fetch_book_tickers() -> List[Dict[str, Any]]:
    data = http_get_json("/api/v3/ticker/bookTicker")

    if not isinstance(data, list):
        raise RuntimeError(
            "Réponse inattendue de /api/v3/ticker/bookTicker"
        )

    return data


def fetch_klines(
    symbol: str,
    interval: str,
    limit: int,
) -> List[List[Any]]:
    data = http_get_json(
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        },
    )

    if not isinstance(data, list):
        raise RuntimeError(
            f"Klines invalides pour {symbol}"
        )

    return data


# ============================================================================
# DETECTION DES TOKENS SPECIAUX
# ============================================================================

def is_leveraged_token(base_asset: str) -> bool:
    """
    Détecte les tokens Binance à effet de levier.

    Exemples classiques :
    BTCUP, BTCDOWN, ETHUP, ETHDOWN, etc.
    """

    base = base_asset.upper()

    for suffix in LEVERAGED_SUFFIXES:
        if base.endswith(suffix):
            return True

    for pattern in LEVERAGED_PATTERNS:
        if base.endswith(pattern):
            return True

    return False


def is_stablecoin(base_asset: str) -> bool:
    return base_asset.upper() in STABLECOIN_BASES


# ============================================================================
# AUDIT DES CRITERES
# ============================================================================

class CriterionAudit:
    def __init__(
        self,
        name: str,
        description: str,
        before: int,
        selected: int,
        rejected: int,
    ) -> None:
        self.name = name
        self.description = description
        self.before = before
        self.selected = selected
        self.rejected = rejected

        if before > 0:
            self.retention_percent = (
                selected / before * 100.0
            )

            self.rejection_percent = (
                rejected / before * 100.0
            )
        else:
            self.retention_percent = 0.0
            self.rejection_percent = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "criterion": self.name,
            "description": self.description,
            "before": self.before,
            "selected": self.selected,
            "rejected": self.rejected,
            "retention_percent": self.retention_percent,
            "rejection_percent": self.rejection_percent,
        }


def apply_criterion(
    current: List[Dict[str, Any]],
    name: str,
    description: str,
    predicate,
    audits: List[CriterionAudit],
) -> List[Dict[str, Any]]:
    before = len(current)

    selected: List[Dict[str, Any]] = []
    rejected = 0

    for item in current:
        try:
            accepted = bool(predicate(item))
        except Exception:
            accepted = False

        if accepted:
            selected.append(item)
        else:
            rejected += 1

    audits.append(
        CriterionAudit(
            name=name,
            description=description,
            before=before,
            selected=len(selected),
            rejected=rejected,
        )
    )

    return selected


# ============================================================================
# CONSTRUCTION DE L'UNIVERS INITIAL
# ============================================================================

def build_raw_universe(
    exchange_info: Dict[str, Any],
) -> List[Dict[str, Any]]:
    symbols = exchange_info.get("symbols", [])

    if not isinstance(symbols, list):
        raise RuntimeError(
            "exchangeInfo ne contient pas une liste symbols valide"
        )

    universe: List[Dict[str, Any]] = []

    for symbol_info in symbols:
        if not isinstance(symbol_info, dict):
            continue

        symbol = str(
            symbol_info.get("symbol", "")
        ).upper()

        if not symbol:
            continue

        base_asset = str(
            symbol_info.get("baseAsset", "")
        ).upper()

        quote_asset = str(
            symbol_info.get("quoteAsset", "")
        ).upper()

        status = str(
            symbol_info.get("status", "")
        ).upper()

        permissions = symbol_info.get(
            "permissions",
            [],
        )

        if not isinstance(permissions, list):
            permissions = []

        permissions_upper = {
            str(x).upper()
            for x in permissions
        }

        universe.append(
            {
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "status": status,
                "permissions": permissions_upper,
                "raw": symbol_info,
            }
        )

    return universe


def merge_ticker_data(
    universe: List[Dict[str, Any]],
    tickers: List[Dict[str, Any]],
) -> None:
    ticker_map = {}

    for ticker in tickers:
        symbol = str(
            ticker.get("symbol", "")
        ).upper()

        if symbol:
            ticker_map[symbol] = ticker

    for item in universe:
        ticker = ticker_map.get(
            item["symbol"]
        )

        if ticker is None:
            item["ticker"] = None
            continue

        item["ticker"] = ticker

        item["price"] = safe_float(
            ticker.get("lastPrice")
        )

        item["price_change_percent"] = safe_float(
            ticker.get("priceChangePercent")
        )

        item["quote_volume"] = safe_float(
            ticker.get("quoteVolume")
        )

        item["trades"] = safe_int(
            ticker.get("count")
        )

        item["weighted_average_price"] = safe_float(
            ticker.get("weightedAvgPrice")
        )

        item["high_price"] = safe_float(
            ticker.get("highPrice")
        )

        item["low_price"] = safe_float(
            ticker.get("lowPrice")
        )

        item["volume"] = safe_float(
            ticker.get("volume")
        )

    for item in universe:
        if "ticker" not in item:
            item["ticker"] = None


def merge_book_data(
    universe: List[Dict[str, Any]],
    book_tickers: List[Dict[str, Any]],
) -> None:
    book_map = {}

    for ticker in book_tickers:
        symbol = str(
            ticker.get("symbol", "")
        ).upper()

        if symbol:
            book_map[symbol] = ticker

    for item in universe:
        book = book_map.get(
            item["symbol"]
        )

        item["book"] = book

        if book is None:
            item["bid_price"] = float("nan")
            item["ask_price"] = float("nan")
            item["spread_percent"] = float("nan")
            continue

        bid = safe_float(
            book.get("bidPrice")
        )

        ask = safe_float(
            book.get("askPrice")
        )

        item["bid_price"] = bid
        item["ask_price"] = ask

        if (
            not math.isnan(bid)
            and not math.isnan(ask)
            and bid > 0
            and ask > 0
            and ask >= bid
        ):
            midpoint = (
                bid + ask
            ) / 2.0

            item["spread_percent"] = (
                (ask - bid)
                / midpoint
                * 100.0
            )
        else:
            item["spread_percent"] = float("nan")


# ============================================================================
# APPLICATION DES FILTRES
# ============================================================================

def run_screening(
    universe: List[Dict[str, Any]],
) -> Tuple[
    List[Dict[str, Any]],
    List[CriterionAudit],
]:
    audits: List[CriterionAudit] = []

    current = universe

    # ---------------------------------------------------------------------
    # 1. STATUS TRADING
    # ---------------------------------------------------------------------

    current = apply_criterion(
        current,
        "01 — Status TRADING",
        "Le symbole doit avoir le statut Binance TRADING.",
        lambda x: x["status"] == "TRADING",
        audits,
    )

    # ---------------------------------------------------------------------
    # 2. PERMISSION SPOT
    # ---------------------------------------------------------------------

    current = apply_criterion(
        current,
        "02 — Permission SPOT",
        "Le symbole doit être autorisé pour le trading Spot.",
        lambda x: (
            "SPOT" in x["permissions"]
            or not x["permissions"]
        ),
        audits,
    )

    # ---------------------------------------------------------------------
    # 3. QUOTE ASSET
    # ---------------------------------------------------------------------

    current = apply_criterion(
        current,
        "03 — Quote asset",
        (
            "La paire doit utiliser l'un des quote assets autorisés : "
            + ", ".join(QUOTE_ASSETS)
        ),
        lambda x: (
            x["quote_asset"] in QUOTE_ASSETS
        ),
        audits,
    )

    # ---------------------------------------------------------------------
    # 4. STABLECOINS
    # ---------------------------------------------------------------------

    if EXCLUDE_STABLECOINS:
        current = apply_criterion(
            current,
            "04 — Exclusion stablecoins",
            "Exclusion des bases constituées de stablecoins.",
            lambda x: not is_stablecoin(
                x["base_asset"]
            ),
            audits,
        )
    else:
        audits.append(
            CriterionAudit(
                "04 — Exclusion stablecoins",
                "Critère désactivé.",
                len(current),
                len(current),
                0,
            )
        )

    # ---------------------------------------------------------------------
    # 5. LEVERAGED TOKENS
    # ---------------------------------------------------------------------

    if EXCLUDE_LEVERAGED_TOKENS:
        current = apply_criterion(
            current,
            "05 — Exclusion leveraged tokens",
            "Exclusion des tokens Binance à effet de levier.",
            lambda x: not is_leveraged_token(
                x["base_asset"]
            ),
            audits,
        )
    else:
        audits.append(
            CriterionAudit(
                "05 — Exclusion leveraged tokens",
                "Critère désactivé.",
                len(current),
                len(current),
                0,
            )
        )

    # ---------------------------------------------------------------------
    # 6. TICKER DISPONIBLE
    # ---------------------------------------------------------------------

    current = apply_criterion(
        current,
        "06 — Données 24h disponibles",
        "Un ticker 24h Binance doit être disponible.",
        lambda x: x.get("ticker") is not None,
        audits,
    )

    # ---------------------------------------------------------------------
    # 7. PRIX MINIMUM
    # ---------------------------------------------------------------------

    current = apply_criterion(
        current,
        "07 — Prix minimum",
        f"Prix >= {MIN_PRICE:g}.",
        lambda x: (
            not math.isnan(
                safe_float(x.get("price"))
            )
            and safe_float(x.get("price")) >= MIN_PRICE
        ),
        audits,
    )

    # ---------------------------------------------------------------------
    # 8. VOLUME 24H
    # ---------------------------------------------------------------------

    current = apply_criterion(
        current,
        "08 — Volume quote 24h",
        (
            "Volume quote 24h >= "
            f"{MIN_24H_QUOTE_VOLUME:,.0f}"
        ),
        lambda x: (
            not math.isnan(
                safe_float(x.get("quote_volume"))
            )
            and safe_float(
                x.get("quote_volume")
            ) >= MIN_24H_QUOTE_VOLUME
        ),
        audits,
    )

    # ---------------------------------------------------------------------
    # 9. NOMBRE DE TRADES
    # ---------------------------------------------------------------------

    current = apply_criterion(
        current,
        "09 — Nombre de trades 24h",
        f"Nombre de trades >= {MIN_24H_TRADES:,}.",
        lambda x: (
            safe_int(
                x.get("trades")
            ) >= MIN_24H_TRADES
        ),
        audits,
    )

    # ---------------------------------------------------------------------
    # 10. PERFORMANCE MINIMUM
    # ---------------------------------------------------------------------

    current = apply_criterion(
        current,
        "10 — Variation 24h minimum",
        (
            "Variation 24h >= "
            f"{MIN_24H_CHANGE_PERCENT:.2f}%"
        ),
        lambda x: (
            not math.isnan(
                safe_float(
                    x.get("price_change_percent")
                )
            )
            and safe_float(
                x.get("price_change_percent")
            ) >= MIN_24H_CHANGE_PERCENT
        ),
        audits,
    )

    # ---------------------------------------------------------------------
    # 11. PERFORMANCE MAXIMUM
    # ---------------------------------------------------------------------

    current = apply_criterion(
        current,
        "11 — Variation 24h maximum",
        (
            "Variation 24h <= "
            f"{MAX_24H_CHANGE_PERCENT:.2f}%"
        ),
        lambda x: (
            not math.isnan(
                safe_float(
                    x.get("price_change_percent")
                )
            )
            and safe_float(
                x.get("price_change_percent")
            ) <= MAX_24H_CHANGE_PERCENT
        ),
        audits,
    )

    # ---------------------------------------------------------------------
    # 12. SPREAD
    # ---------------------------------------------------------------------

    current = apply_criterion(
        current,
        "12 — Spread bid/ask",
        (
            "Spread bid/ask <= "
            f"{MAX_SPREAD_PERCENT:.2f}%."
        ),
        lambda x: (
            not math.isnan(
                safe_float(
                    x.get("spread_percent")
                )
            )
            and safe_float(
                x.get("spread_percent")
            ) <= MAX_SPREAD_PERCENT
        ),
        audits,
    )

    return current, audits


# ============================================================================
# INDICATEURS TECHNIQUES
# ============================================================================

def closes_from_klines(
    klines: List[List[Any]],
) -> List[float]:
    return [
        safe_float(row[4])
        for row in klines
        if len(row) >= 6
    ]


def highs_from_klines(
    klines: List[List[Any]],
) -> List[float]:
    return [
        safe_float(row[2])
        for row in klines
        if len(row) >= 6
    ]


def lows_from_klines(
    klines: List[List[Any]],
) -> List[float]:
    return [
        safe_float(row[3])
        for row in klines
        if len(row) >= 6
    ]


def volumes_from_klines(
    klines: List[List[Any]],
) -> List[float]:
    return [
        safe_float(row[5])
        for row in klines
        if len(row) >= 6
    ]


def ema(
    values: List[float],
    period: int,
) -> List[float]:
    if period <= 0:
        return []

    if len(values) < period:
        return []

    result = [float("nan")] * len(values)

    initial = sum(
        values[:period]
    ) / period

    result[period - 1] = initial

    multiplier = 2.0 / (
        period + 1.0
    )

    previous = initial

    for i in range(period, len(values)):
        current = values[i]

        if math.isnan(current):
            result[i] = previous
            continue

        previous = (
            (current - previous)
            * multiplier
            + previous
        )

        result[i] = previous

    return result


def sma(
    values: List[float],
    period: int,
) -> List[float]:
    if period <= 0:
        return []

    result = [float("nan")] * len(values)

    if len(values) < period:
        return result

    running_sum = sum(
        values[:period]
    )

    result[period - 1] = (
        running_sum / period
    )

    for i in range(period, len(values)):
        running_sum += values[i]
        running_sum -= values[i - period]

        result[i] = (
            running_sum / period
        )

    return result


def rsi(
    values: List[float],
    period: int,
) -> List[float]:
    result = [float("nan")] * len(values)

    if period <= 0 or len(values) <= period:
        return result

    gains: List[float] = []
    losses: List[float] = []

    for i in range(1, len(values)):
        delta = (
            values[i]
            - values[i - 1]
        )

        gains.append(
            max(delta, 0.0)
        )

        losses.append(
            max(-delta, 0.0)
        )

    avg_gain = sum(
        gains[:period]
    ) / period

    avg_loss = sum(
        losses[:period]
    ) / period

    index = period

    if avg_loss <= EPSILON:
        result[index] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[index] = (
            100.0
            - 100.0 / (1.0 + rs)
        )

    for i in range(period + 1, len(values)):
        gain = gains[i - 1]
        loss = losses[i - 1]

        avg_gain = (
            (
                avg_gain * (period - 1)
            )
            + gain
        ) / period

        avg_loss = (
            (
                avg_loss * (period - 1)
            )
            + loss
        ) / period

        if avg_loss <= EPSILON:
            result[i] = 100.0
        else:
            rs = avg_gain / avg_loss

            result[i] = (
                100.0
                - 100.0 / (1.0 + rs)
            )

    return result


def macd(
    values: List[float],
    fast_period: int,
    slow_period: int,
    signal_period: int,
) -> Tuple[
    List[float],
    List[float],
    List[float],
]:
    fast = ema(
        values,
        fast_period,
    )

    slow = ema(
        values,
        slow_period,
    )

    macd_line = [float("nan")] * len(values)

    for i in range(len(values)):
        if (
            i < len(fast)
            and i < len(slow)
            and not math.isnan(fast[i])
            and not math.isnan(slow[i])
        ):
            macd_line[i] = (
                fast[i] - slow[i]
            )

    valid_macd = [
        x
        for x in macd_line
        if not math.isnan(x)
    ]

    signal_valid = ema(
        valid_macd,
        signal_period,
    )

    signal_line = [float("nan")] * len(values)

    valid_index = 0

    for i in range(len(values)):
        if math.isnan(macd_line[i]):
            continue

        if valid_index < len(signal_valid):
            signal_line[i] = (
                signal_valid[valid_index]
            )

        valid_index += 1

    histogram = [float("nan")] * len(values)

    for i in range(len(values)):
        if (
            not math.isnan(macd_line[i])
            and not math.isnan(signal_line[i])
        ):
            histogram[i] = (
                macd_line[i]
                - signal_line[i]
            )

    return (
        macd_line,
        signal_line,
        histogram,
    )


def atr(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int,
) -> List[float]:
    result = [float("nan")] * len(closes)

    if (
        period <= 0
        or len(closes) < 2
        or len(highs) != len(closes)
        or len(lows) != len(closes)
    ):
        return result

    true_ranges = [float("nan")] * len(closes)

    for i in range(1, len(closes)):
        high = highs[i]
        low = lows[i]
        previous_close = closes[i - 1]

        true_ranges[i] = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close),
        )

    valid_tr = [
        x
        for x in true_ranges[1:]
        if not math.isnan(x)
    ]

    if len(valid_tr) < period:
        return result

    first_atr = (
        sum(valid_tr[:period])
        / period
    )

    source_index = period
    result[source_index] = first_atr

    previous_atr = first_atr

    for i in range(
        source_index + 1,
        len(closes),
    ):
        tr = true_ranges[i]

        if math.isnan(tr):
            result[i] = previous_atr
            continue

        previous_atr = (
            (
                previous_atr
                * (period - 1)
            )
            + tr
        ) / period

        result[i] = previous_atr

    return result


def last_valid(
    values: List[float],
) -> float:
    for value in reversed(values):
        if not math.isnan(value):
            return value

    return float("nan")


# ============================================================================
# SCORE TECHNIQUE
# ============================================================================

def technical_score(
    result: Dict[str, Any],
) -> float:
    """
    Score technique de classement.

    Ce score ne constitue PAS un signal automatique d'achat.
    Il sert uniquement à classer les actifs survivants.

    Maximum = 100 points.
    """

    score = 0.0

    close = safe_float(
        result.get("close")
    )

    ema_fast_value = safe_float(
        result.get("ema_fast")
    )

    ema_slow_value = safe_float(
        result.get("ema_slow")
    )

    rsi_value = safe_float(
        result.get("rsi")
    )

    macd_value = safe_float(
        result.get("macd")
    )

    macd_signal_value = safe_float(
        result.get("macd_signal")
    )

    macd_hist_value = safe_float(
        result.get("macd_histogram")
    )

    volume_ratio = safe_float(
        result.get("volume_ratio")
    )

    # EMA : 30 points
    if (
        not math.isnan(close)
        and not math.isnan(ema_fast_value)
        and not math.isnan(ema_slow_value)
    ):
        if (
            close > ema_fast_value
            and ema_fast_value > ema_slow_value
        ):
            score += 30.0

        elif (
            close > ema_slow_value
        ):
            score += 20.0

        elif close > ema_fast_value:
            score += 10.0

    # RSI : 20 points
    if not math.isnan(rsi_value):
        if 50.0 <= rsi_value <= 65.0:
            score += 20.0
        elif 45.0 <= rsi_value < 50.0:
            score += 10.0
        elif 65.0 < rsi_value <= 70.0:
            score += 15.0
        elif rsi_value > 70.0:
            score += 5.0

    # MACD : 25 points
    if (
        not math.isnan(macd_value)
        and not math.isnan(macd_signal_value)
        and not math.isnan(macd_hist_value)
    ):
        if (
            macd_value > macd_signal_value
            and macd_hist_value > 0
        ):
            score += 25.0

        elif macd_value > macd_signal_value:
            score += 15.0

        elif macd_hist_value > 0:
            score += 10.0

    # Volume : 15 points
    if not math.isnan(volume_ratio):
        if volume_ratio >= 2.0:
            score += 15.0
        elif volume_ratio >= 1.5:
            score += 12.0
        elif volume_ratio >= 1.0:
            score += 8.0
        elif volume_ratio >= 0.75:
            score += 4.0

    # ATR : 10 points
    atr_percent = safe_float(
        result.get("atr_percent")
    )

    if not math.isnan(atr_percent):
        if 1.0 <= atr_percent <= 6.0:
            score += 10.0
        elif 0.5 <= atr_percent < 1.0:
            score += 5.0
        elif 6.0 < atr_percent <= 10.0:
            score += 5.0

    return min(
        max(score, 0.0),
        100.0,
    )


def classify_technical_state(
    result: Dict[str, Any],
) -> str:
    score = safe_float(
        result.get("technical_score")
    )

    rsi_value = safe_float(
        result.get("rsi")
    )

    macd_hist_value = safe_float(
        result.get("macd_histogram")
    )

    close = safe_float(
        result.get("close")
    )

    ema_fast_value = safe_float(
        result.get("ema_fast")
    )

    ema_slow_value = safe_float(
        result.get("ema_slow")
    )

    bullish = 0
    bearish = 0

    if (
        not math.isnan(close)
        and not math.isnan(ema_fast_value)
        and not math.isnan(ema_slow_value)
    ):
        if (
            close > ema_fast_value
            and ema_fast_value > ema_slow_value
        ):
            bullish += 2
        elif close > ema_slow_value:
            bullish += 1

        if (
            close < ema_fast_value
            and ema_fast_value < ema_slow_value
        ):
            bearish += 2
        elif close < ema_slow_value:
            bearish += 1

    if not math.isnan(macd_hist_value):
        if macd_hist_value > 0:
            bullish += 1
        elif macd_hist_value < 0:
            bearish += 1

    if not math.isnan(rsi_value):
        if 50 <= rsi_value <= 70:
            bullish += 1
        elif rsi_value < 40:
            bearish += 1

    if score >= 75 and bullish > bearish:
        return "BULLISH FORT"

    if score >= 60 and bullish > bearish:
        return "BULLISH"

    if score <= 30 and bearish > bullish:
        return "BEARISH FORT"

    if score <= 45 and bearish > bullish:
        return "BEARISH"

    return "NEUTRE"


# ============================================================================
# ANALYSE D'UN ACTIF
# ============================================================================

def analyze_symbol(
    item: Dict[str, Any],
) -> Dict[str, Any]:
    symbol = item["symbol"]

    result: Dict[str, Any] = {
        "symbol": symbol,
        "base_asset": item.get("base_asset"),
        "quote_asset": item.get("quote_asset"),
        "price": item.get("price"),
        "price_change_percent": item.get(
            "price_change_percent"
        ),
        "quote_volume": item.get(
            "quote_volume"
        ),
        "trades": item.get("trades"),
        "spread_percent": item.get(
            "spread_percent"
        ),
        "technical_status": "ERROR",
    }

    try:
        klines = fetch_klines(
            symbol=symbol,
            interval=TECHNICAL_INTERVAL,
            limit=TECHNICAL_KLINES_LIMIT,
        )

        if len(klines) < 10:
            result["technical_status"] = (
                "INSUFFICIENT_DATA"
            )
            return result

        closes = closes_from_klines(
            klines
        )

        highs = highs_from_klines(
            klines
        )

        lows = lows_from_klines(
            klines
        )

        volumes = volumes_from_klines(
            klines
        )

        if not closes:
            result["technical_status"] = (
                "INSUFFICIENT_DATA"
            )
            return result

        close = closes[-1]

        result["close"] = close

        # ---------------------------------------------------------------
        # EMA
        # ---------------------------------------------------------------

        if ENABLE_EMA:
            ema_fast_values = ema(
                closes,
                EMA_FAST_PERIOD,
            )

            ema_slow_values = ema(
                closes,
                EMA_SLOW_PERIOD,
            )

            result["ema_fast"] = last_valid(
                ema_fast_values
            )

            result["ema_slow"] = last_valid(
                ema_slow_values
            )

        # ---------------------------------------------------------------
        # RSI
        # ---------------------------------------------------------------

        if ENABLE_RSI:
            rsi_values = rsi(
                closes,
                RSI_PERIOD,
            )

            result["rsi"] = last_valid(
                rsi_values
            )

        # ---------------------------------------------------------------
        # MACD
        # ---------------------------------------------------------------

        if ENABLE_MACD:
            (
                macd_values,
                signal_values,
                histogram_values,
            ) = macd(
                closes,
                MACD_FAST_PERIOD,
                MACD_SLOW_PERIOD,
                MACD_SIGNAL_PERIOD,
            )

            result["macd"] = last_valid(
                macd_values
            )

            result["macd_signal"] = last_valid(
                signal_values
            )

            result["macd_histogram"] = last_valid(
                histogram_values
            )

        # ---------------------------------------------------------------
        # ATR
        # ---------------------------------------------------------------

        if ENABLE_ATR:
            atr_values = atr(
                highs,
                lows,
                closes,
                ATR_PERIOD,
            )

            atr_value = last_valid(
                atr_values
            )

            result["atr"] = atr_value

            if (
                not math.isnan(atr_value)
                and close > 0
            ):
                result["atr_percent"] = (
                    atr_value
                    / close
                    * 100.0
                )

        # ---------------------------------------------------------------
        # VOLUME RATIO
        # ---------------------------------------------------------------

        if ENABLE_VOLUME_RATIO:
            volume_ma_values = sma(
                volumes,
                VOLUME_MA_PERIOD,
            )

            volume_ma = last_valid(
                volume_ma_values
            )

            result["volume_ma"] = volume_ma

            if (
                not math.isnan(volume_ma)
                and volume_ma > 0
            ):
                result["volume_ratio"] = (
                    volumes[-1]
                    / volume_ma
                )

        result["technical_score"] = (
            technical_score(result)
        )

        result["technical_state"] = (
            classify_technical_state(result)
        )

        result["technical_status"] = "OK"

        return result

    except Exception as exc:
        result["technical_status"] = "ERROR"
        result["technical_error"] = (
            f"{type(exc).__name__}: {exc}"
        )

        return result


# ============================================================================
# ANALYSE TECHNIQUE PARALLELE
# ============================================================================

def run_technical_analysis(
    assets: List[Dict[str, Any]],
) -> Tuple[
    List[Dict[str, Any]],
    List[str],
]:
    if not TECHNICAL_ENABLED:
        return [], []

    if not assets:
        return [], []

    ordered_assets = sorted(
        assets,
        key=lambda x: (
            safe_float(
                x.get("quote_volume"),
                0.0,
            )
        ),
        reverse=True,
    )

    capped = ordered_assets[
        :TECHNICAL_MAX_ASSETS
    ]

    warnings: List[str] = []

    if len(ordered_assets) > len(capped):
        warnings.append(
            (
                "Analyse technique limitée à "
                f"{len(capped)} actifs sur "
                f"{len(ordered_assets)} survivants "
                f"(TECHNICAL_MAX_ASSETS={TECHNICAL_MAX_ASSETS})."
            )
        )

    results: List[Dict[str, Any]] = []

    max_workers = max(
        1,
        min(
            TECHNICAL_WORKERS,
            len(capped),
        ),
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:

        futures = {
            executor.submit(
                analyze_symbol,
                item,
            ): item["symbol"]
            for item in capped
        }

        for future in concurrent.futures.as_completed(
            futures
        ):
            symbol = futures[future]

            try:
                result = future.result()
                results.append(result)

            except Exception as exc:
                results.append(
                    {
                        "symbol": symbol,
                        "technical_status": "ERROR",
                        "technical_error": (
                            f"{type(exc).__name__}: {exc}"
                        ),
                    }
                )

    results.sort(
        key=lambda x: (
            safe_float(
                x.get("technical_score"),
                -1.0,
            )
        ),
        reverse=True,
    )

    return results, warnings


# ============================================================================
# RAPPORT TEXTE
# ============================================================================

def configuration_lines() -> List[str]:
    return [
        "Configuration",
        "-------------",
        f"BINANCE_BASE_URL             : {BINANCE_BASE_URL}",
        f"QUOTE_ASSETS                 : {', '.join(QUOTE_ASSETS)}",
        f"EXCLUDE_STABLECOINS          : {EXCLUDE_STABLECOINS}",
        f"EXCLUDE_LEVERAGED_TOKENS     : {EXCLUDE_LEVERAGED_TOKENS}",
        (
            "MIN_24H_QUOTE_VOLUME        : "
            f"{MIN_24H_QUOTE_VOLUME:,.0f}"
        ),
        (
            "MIN_24H_TRADES              : "
            f"{MIN_24H_TRADES:,}"
        ),
        (
            "MIN_24H_CHANGE_PERCENT      : "
            f"{MIN_24H_CHANGE_PERCENT:.2f}%"
        ),
        (
            "MAX_24H_CHANGE_PERCENT      : "
            f"{MAX_24H_CHANGE_PERCENT:.2f}%"
        ),
        (
            "MAX_SPREAD_PERCENT          : "
            f"{MAX_SPREAD_PERCENT:.2f}%"
        ),
        f"MIN_PRICE                    : {MIN_PRICE:g}",
        f"TECHNICAL_ENABLED            : {TECHNICAL_ENABLED}",
        f"TECHNICAL_INTERVAL           : {TECHNICAL_INTERVAL}",
        (
            "TECHNICAL_KLINES_LIMIT      : "
            f"{TECHNICAL_KLINES_LIMIT}"
        ),
        f"ENABLE_EMA                   : {ENABLE_EMA}",
        f"ENABLE_RSI                   : {ENABLE_RSI}",
        f"ENABLE_MACD                 : {ENABLE_MACD}",
        f"ENABLE_ATR                  : {ENABLE_ATR}",
        (
            "ENABLE_VOLUME_RATIO         : "
            f"{ENABLE_VOLUME_RATIO}"
        ),
        f"EMA_FAST_PERIOD              : {EMA_FAST_PERIOD}",
        f"EMA_SLOW_PERIOD              : {EMA_SLOW_PERIOD}",
        f"RSI_PERIOD                   : {RSI_PERIOD}",
        f"MACD_FAST_PERIOD             : {MACD_FAST_PERIOD}",
        f"MACD_SLOW_PERIOD             : {MACD_SLOW_PERIOD}",
        f"MACD_SIGNAL_PERIOD           : {MACD_SIGNAL_PERIOD}",
        f"ATR_PERIOD                   : {ATR_PERIOD}",
        f"VOLUME_MA_PERIOD             : {VOLUME_MA_PERIOD}",
        (
            "TECHNICAL_MAX_ASSETS       : "
            f"{TECHNICAL_MAX_ASSETS}"
        ),
        (
            "TECHNICAL_WORKERS          : "
            f"{TECHNICAL_WORKERS}"
        ),
    ]


def build_report(
    raw_universe: List[Dict[str, Any]],
    screened_assets: List[Dict[str, Any]],
    audits: List[CriterionAudit],
    technical_results: List[Dict[str, Any]],
    warnings: List[str],
) -> str:

    lines: List[str] = []

    lines.append(
        "BINANCE AUTOMATED SCREENER"
    )
    lines.append("=" * 80)
    lines.append("")
    lines.append(
        f"Date du scan : {utc_timestamp()}"
    )
    lines.append(
        f"Endpoint     : {BINANCE_BASE_URL}"
    )
    lines.append("")

    # ---------------------------------------------------------------------
    # RESUME
    # ---------------------------------------------------------------------

    lines.append("RESUME")
    lines.append("-" * 80)

    lines.append(
        f"Univers brut Binance exchangeInfo : "
        f"{len(raw_universe):,}"
    )

    lines.append(
        f"Survivants après screening        : "
        f"{len(screened_assets):,}"
    )

    if raw_universe:
        retention = (
            len(screened_assets)
            / len(raw_universe)
            * 100.0
        )
    else:
        retention = 0.0

    lines.append(
        f"Taux de rétention global          : "
        f"{retention:.2f}%"
    )

    lines.append("")

    # ---------------------------------------------------------------------
    # CONFIGURATION
    # ---------------------------------------------------------------------

    lines.extend(
        configuration_lines()
    )

    lines.append("")

    # ---------------------------------------------------------------------
    # AUDIT DES CRITERES
    # ---------------------------------------------------------------------

    lines.append(
        "AUDIT SEQUENTIEL DES CRITERES"
    )
    lines.append("-" * 80)

    header = (
        f"{'#':<4}"
        f"{'Critère':<34}"
        f"{'Avant':>10}"
        f"{'Sélectionnés':>14}"
        f"{'Rejetés':>10}"
        f"{'Rétention':>12}"
        f"{'Rejet':>10}"
    )

    lines.append(header)
    lines.append("-" * 80)

    for index, audit in enumerate(
        audits,
        start=1,
    ):
        name = audit.name

        if len(name) > 32:
            name = name[:29] + "..."

        lines.append(
            f"{index:<4}"
            f"{name:<34}"
            f"{audit.before:>10,}"
            f"{audit.selected:>14,}"
            f"{audit.rejected:>10,}"
            f"{audit.retention_percent:>11.2f}%"
            f"{audit.rejection_percent:>9.2f}%"
        )

    lines.append("")
    lines.append(
        "Descriptions des critères :"
    )

    for audit in audits:
        lines.append(
            f"- {audit.name}: {audit.description}"
        )

    lines.append("")

    # ---------------------------------------------------------------------
    # SURVIVANTS
    # ---------------------------------------------------------------------

    lines.append(
        "SURVIVANTS DU SCREENING"
    )
    lines.append("-" * 80)

    if not screened_assets:
        lines.append(
            "Aucun actif n'a survécu à l'ensemble des critères."
        )
    else:
        sorted_screened = sorted(
            screened_assets,
            key=lambda x: safe_float(
                x.get("quote_volume"),
                0.0,
            ),
            reverse=True,
        )

        for item in sorted_screened[
            :min(len(sorted_screened), 100)
        ]:
            lines.append(
                f"{item['symbol']:<18} "
                f"Prix={fmt_number(item.get('price'), 8):>16} "
                f"24h={fmt_percent(item.get('price_change_percent')):>9} "
                f"Vol={fmt_number(item.get('quote_volume'), 0):>16} "
                f"Trades={safe_int(item.get('trades')):>9,} "
                f"Spread={fmt_percent(item.get('spread_percent')):>8}"
            )

    lines.append("")

    # ---------------------------------------------------------------------
    # TECHNIQUE
    # ---------------------------------------------------------------------

    lines.append(
        "ANALYSE TECHNIQUE"
    )
    lines.append("-" * 80)

    if not TECHNICAL_ENABLED:
        lines.append(
            "Analyse technique désactivée."
        )

    elif not screened_assets:
        lines.append(
            "Aucun actif à analyser techniquement."
        )

    else:
        successful = [
            x
            for x in technical_results
            if x.get("technical_status") == "OK"
        ]

        errors = [
            x
            for x in technical_results
            if x.get("technical_status") != "OK"
        ]

        lines.append(
            f"Actifs survivants                 : "
            f"{len(screened_assets):,}"
        )

        lines.append(
            f"Actifs analysés techniquement    : "
            f"{len(technical_results):,}"
        )

        lines.append(
            f"Analyses réussies                : "
            f"{len(successful):,}"
        )

        lines.append(
            f"Erreurs / données insuffisantes  : "
            f"{len(errors):,}"
        )

        lines.append("")

        if successful:
            lines.append(
                "TOP RESULTATS TECHNIQUES"
            )
            lines.append("-" * 80)

            tech_header = (
                f"{'#':<4}"
                f"{'Symbol':<15}"
                f"{'Score':>8}"
                f"{'Etat':<18}"
                f"{'RSI':>9}"
                f"{'EMA20':>14}"
                f"{'EMA50':>14}"
                f"{'MACD Hist':>13}"
                f"{'VolRatio':>11}"
            )

            lines.append(
                tech_header
            )

            lines.append("-" * 80)

            for index, item in enumerate(
                successful[
                    :EMAIL_TOP_RESULTS
                ],
                start=1,
            ):
                lines.append(
                    f"{index:<4}"
                    f"{item.get('symbol', ''):<15}"
                    f"{safe_float(item.get('technical_score'), 0):>7.1f}"
                    f"{item.get('technical_state', 'N/A'):<18}"
                    f"{fmt_number(item.get('rsi'), 1):>9}"
                    f"{fmt_number(item.get('ema_fast'), 8):>14}"
                    f"{fmt_number(item.get('ema_slow'), 8):>14}"
                    f"{fmt_number(item.get('macd_histogram'), 8):>13}"
                    f"{fmt_number(item.get('volume_ratio'), 2):>11}"
                )

            lines.append("")

        if errors:
            lines.append(
                "ERREURS / DONNEES INSUFFISANTES"
            )
            lines.append("-" * 80)

            for item in errors[:50]:
                lines.append(
                    f"- {item.get('symbol', 'UNKNOWN')}: "
                    f"{item.get('technical_error', item.get('technical_status', 'UNKNOWN'))}"
                )

            lines.append("")

    # ---------------------------------------------------------------------
    # WARNINGS
    # ---------------------------------------------------------------------

    if warnings:
        lines.append(
            "AVERTISSEMENTS"
        )
        lines.append("-" * 80)

        for warning in warnings:
            lines.append(
                f"- {warning}"
            )

        lines.append("")

    # ---------------------------------------------------------------------
    # FIN
    # ---------------------------------------------------------------------

    lines.append("=" * 80)
    lines.append(
        "Fin du rapport."
    )

    lines.append(
        "Ce programme ne passe aucun ordre Binance."
    )

    return "\n".join(lines)


# ============================================================================
# EMAIL
# ============================================================================

def send_email(
    report: str,
) -> None:
    if not EMAIL_USER:
        raise RuntimeError(
            "EMAIL_USER est vide."
        )

    if not EMAIL_PASS:
        raise RuntimeError(
            "EMAIL_PASS est vide."
        )

    if not EMAIL_TO:
        raise RuntimeError(
            "EMAIL_TO est vide."
        )

    recipients = [
        x.strip()
        for x in EMAIL_TO.split(",")
        if x.strip()
    ]

    if not recipients:
        raise RuntimeError(
            "EMAIL_TO ne contient aucun destinataire valide."
        )

    subject = (
        "Binance Screener — "
        + utc_now().strftime(
            "%Y-%m-%d %H:%M UTC"
        )
    )

    message = MIMEMultipart()
    message["From"] = EMAIL_USER
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject

    message.attach(
        MIMEText(
            report,
            "plain",
            "utf-8",
        )
    )

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(
        EMAIL_HOST,
        EMAIL_PORT,
        context=context,
        timeout=30,
    ) as server:
        server.login(
            EMAIL_USER,
            EMAIL_PASS,
        )

        server.sendmail(
            EMAIL_USER,
            recipients,
            message.as_string(),
        )


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    started_at = time.time()

    print("=" * 80)
    print("BINANCE AUTOMATED SCREENER")
    print("=" * 80)
    print(
        f"Début : {utc_timestamp()}"
    )
    print(
        f"Endpoint : {BINANCE_BASE_URL}"
    )
    print("")

    raw_universe: List[Dict[str, Any]] = []
    screened_assets: List[Dict[str, Any]] = []
    audits: List[CriterionAudit] = []
    technical_results: List[Dict[str, Any]] = []
    warnings: List[str] = []

    # ---------------------------------------------------------------------
    # 1. EXCHANGE INFO
    # ---------------------------------------------------------------------

    try:
        print(
            "[1/6] Récupération de exchangeInfo..."
        )

        exchange_info = fetch_exchange_info()

        raw_universe = build_raw_universe(
            exchange_info
        )

        print(
            f"      Univers brut : "
            f"{len(raw_universe):,}"
        )

    except Exception as exc:
        error_report = (
            "BINANCE AUTOMATED SCREENER\n"
            + "=" * 80
            + "\n\n"
            + f"Erreur exchangeInfo : {type(exc).__name__}: {exc}\n"
        )

        print(error_report)

        try:
            send_email(error_report)
            print(
                "Email d'erreur envoyé."
            )
        except Exception as email_exc:
            print(
                "Impossible d'envoyer l'email d'erreur : "
                f"{type(email_exc).__name__}: {email_exc}"
            )

        return 1

    # ---------------------------------------------------------------------
    # 2. TICKERS 24H
    # ---------------------------------------------------------------------

    try:
        print(
            "[2/6] Récupération des statistiques 24h..."
        )

        tickers = fetch_24h_tickers()

        merge_ticker_data(
            raw_universe,
            tickers,
        )

        print(
            f"      Tickers reçus : "
            f"{len(tickers):,}"
        )

    except Exception as exc:
        error_report = (
            "BINANCE AUTOMATED SCREENER\n"
            + "=" * 80
            + "\n\n"
            + f"Erreur ticker 24h : {type(exc).__name__}: {exc}\n"
        )

        print(error_report)

        try:
            send_email(error_report)
            print(
                "Email d'erreur envoyé."
            )
        except Exception as email_exc:
            print(
                "Impossible d'envoyer l'email d'erreur : "
                f"{type(email_exc).__name__}: {email_exc}"
            )

        return 1

    # ---------------------------------------------------------------------
    # 3. BOOK TICKER
    # ---------------------------------------------------------------------

    try:
        print(
            "[3/6] Récupération bid/ask..."
        )

        book_tickers = fetch_book_tickers()

        merge_book_data(
            raw_universe,
            book_tickers,
        )

        print(
            f"      Book tickers reçus : "
            f"{len(book_tickers):,}"
        )

    except Exception as exc:
        warnings.append(
            (
                "Impossible de récupérer bookTicker : "
                f"{type(exc).__name__}: {exc}. "
                "Les actifs seront rejetés si le spread est impossible à calculer."
            )
        )

        for item in raw_universe:
            item["book"] = None
            item["bid_price"] = float("nan")
            item["ask_price"] = float("nan")
            item["spread_percent"] = float("nan")

        print(
            "      AVERTISSEMENT : bookTicker indisponible."
        )

    # ---------------------------------------------------------------------
    # 4. SCREENING
    # ---------------------------------------------------------------------

    print(
        "[4/6] Application séquentielle des critères..."
    )

    screened_assets, audits = run_screening(
        raw_universe
    )

    print(
        f"      Survivants : "
        f"{len(screened_assets):,}"
    )

    for audit in audits:
        print(
            f"      {audit.name}: "
            f"{audit.before:,} -> "
            f"{audit.selected:,} "
            f"({audit.retention_percent:.2f}% retention)"
        )

    # ---------------------------------------------------------------------
    # 5. ANALYSE TECHNIQUE
    # ---------------------------------------------------------------------

    print(
        "[5/6] Analyse technique..."
    )

    if TECHNICAL_ENABLED:
        technical_results, tech_warnings = (
            run_technical_analysis(
                screened_assets
            )
        )

        warnings.extend(
            tech_warnings
        )

        print(
            f"      Résultats techniques : "
            f"{len(technical_results):,}"
        )
    else:
        print(
            "      Analyse technique désactivée."
        )

    # ---------------------------------------------------------------------
    # 6. RAPPORT
    # ---------------------------------------------------------------------

    print(
        "[6/6] Génération du rapport..."
    )

    report = build_report(
        raw_universe=raw_universe,
        screened_assets=screened_assets,
        audits=audits,
        technical_results=technical_results,
        warnings=warnings,
    )

    print("")
    print(
        f"Temps total : "
        f"{time.time() - started_at:.2f} secondes"
    )
    print("")

    print(
        "Envoi du rapport par email..."
    )

    try:
        send_email(report)

        print(
            "Email envoyé avec succès."
        )

    except Exception as exc:
        print(
            "ERREUR EMAIL : "
            f"{type(exc).__name__}: {exc}"
        )

        # Le scan lui-même a réussi.
        # On retourne néanmoins 1 pour que le workflow indique
        # explicitement le problème d'envoi.
        print("")
        print(
            "Le scan est terminé, mais l'email n'a pas pu être envoyé."
        )

        print("")
        print(
            "----- RAPPORT -----"
        )
        print(report)

        return 1

    print("")
    print("=" * 80)
    print("SCAN TERMINE AVEC SUCCES")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
