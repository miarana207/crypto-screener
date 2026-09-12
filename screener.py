BINANCE SCREENER
================

Objectif
--------
Scanner automatiquement l'univers Binance Spot à intervalles réguliers.

Architecture
------------
1. Récupération dynamique de l'univers Binance via exchangeInfo.
2. Récupération des statistiques 24h.
3. Application séquentielle des critères.
4. Comptabilisation des sélectionnés / rejetés à chaque étape.
5. Analyse technique des survivants.
6. Classement des résultats techniques.
7. Envoi d'un rapport détaillé par email.

Le programme utilise uniquement la bibliothèque standard Python.
Aucun requirements.txt n'est nécessaire.

Important
---------
Les critères de sélection sont volontairement configurables via
les variables d'environnement du workflow GitHub Actions.

Le programme ne constitue PAS un système d'exécution d'ordres.
Il s'agit d'un scanner / outil d'analyse.
"""

from __future__ import annotations

import concurrent.futures
import json
import math
import os
import smtplib
import ssl
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ============================================================================
# CONFIGURATION
# ============================================================================

BINANCE_BASE_URL = os.getenv(
    "BINANCE_BASE_URL",
    "https://data-api.binance.vision",
).rstrip("/")

QUOTE_ASSETS = {
    x.strip().upper()
    for x in os.getenv("QUOTE_ASSETS", "USDT").split(",")
    if x.strip()
}

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


# Technical analysis

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


# HTTP

HTTP_TIMEOUT_SECONDS = int(
    os.getenv("HTTP_TIMEOUT_SECONDS", "20")
)

HTTP_RETRIES = int(
    os.getenv("HTTP_RETRIES", "3")
)


# Email

EMAIL_HOST = os.getenv(
    "EMAIL_HOST",
    "smtp.gmail.com",
)

EMAIL_PORT = int(
    os.getenv("EMAIL_PORT", "465")
)

EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")

EMAIL_TOP_RESULTS = int(
    os.getenv("EMAIL_TOP_RESULTS", "50")
)


# ============================================================================
# UTILITAIRES
# ============================================================================

def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def pct(value: float) -> str:
    return f"{value:.2f}%"


def number(value: float) -> str:
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f} B"

    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f} M"

    if abs(value) >= 1_000:
        return f"{value / 1_000:.2f} K"

    return f"{value:.2f}"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def log(message: str) -> None:
    timestamp = utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{timestamp}] {message}", flush=True)


# ============================================================================
# HTTP BINANCE
# ============================================================================

def http_get_json(
    path: str,
    params: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    GET JSON avec retry simple.

    Aucun API key n'est nécessaire pour les endpoints publics utilisés.
    """

    url = f"{BINANCE_BASE_URL}{path}"

    if params:
        query = urllib.parse.urlencode(params)
        url = f"{url}?{query}"

    last_error: Optional[Exception] = None

    for attempt in range(1, HTTP_RETRIES + 1):

        try:
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Binance-Screener/1.0",
                    "Accept": "application/json",
                },
                method="GET",
            )

            with urllib.request.urlopen(
                request,
                timeout=HTTP_TIMEOUT_SECONDS,
            ) as response:

                status = response.status
                body = response.read()

                if status != 200:
                    raise RuntimeError(
                        f"HTTP {status}: {body[:500]!r}"
                    )

                return json.loads(body.decode("utf-8"))

        except Exception as exc:
            last_error = exc

            log(
                f"API error attempt {attempt}/"
                f"{HTTP_RETRIES}: {exc}"
            )

            if attempt < HTTP_RETRIES:
                time.sleep(2 * attempt)

    raise RuntimeError(
        f"Unable to retrieve {url}: {last_error}"
    )


# ============================================================================
# BINANCE UNIVERSE
# ============================================================================

def fetch_exchange_info() -> Dict[str, Any]:
    log("Fetching Binance exchangeInfo...")
    return http_get_json("/api/v3/exchangeInfo")


def fetch_24h_tickers() -> List[Dict[str, Any]]:
    log("Fetching Binance 24h ticker...")
    data = http_get_json("/api/v3/ticker/24hr")

    if not isinstance(data, list):
        raise RuntimeError(
            "Unexpected response from /api/v3/ticker/24hr"
        )

    return data


def fetch_book_tickers() -> List[Dict[str, Any]]:
    log("Fetching Binance book ticker...")
    data = http_get_json("/api/v3/ticker/bookTicker")

    if not isinstance(data, list):
        raise RuntimeError(
            "Unexpected response from /api/v3/ticker/bookTicker"
        )

    return data


# ============================================================================
# STABLECOINS / LEVERAGED TOKENS
# ============================================================================

STABLECOIN_BASES = {
    "USDT",
    "USDC",
    "BUSD",
    "FDUSD",
    "TUSD",
    "USDP",
    "DAI",
    "USDD",
    "PYUSD",
    "EUR",
    "GBP",
    "AUD",
    "TRY",
    "BRL",
    "UAH",
    "PLN",
    "RON",
    "ARS",
    "ZAR",
    "NGN",
}


LEVERAGED_SUFFIXES = (
    "UP",
    "DOWN",
    "BULL",
    "BEAR",
)


def is_stablecoin_base(asset: str) -> bool:
    return asset.upper() in STABLECOIN_BASES


def is_leveraged_token(asset: str) -> bool:
    asset = asset.upper()

    return any(
        asset.endswith(suffix)
        for suffix in LEVERAGED_SUFFIXES
    )


# ============================================================================
# SYMBOL REPRESENTATION
# ============================================================================

@dataclass
class SymbolRecord:
    symbol: str
    base_asset: str
    quote_asset: str

    status: str = ""
    spot_allowed: bool = False

    last_price: float = 0.0
    price_change_percent: float = 0.0
    quote_volume: float = 0.0
    trades: int = 0

    bid_price: float = 0.0
    ask_price: float = 0.0
    spread_percent: float = 0.0

    ticker_available: bool = False


# ============================================================================
# BUILD UNIVERSE
# ============================================================================

def build_universe(
    exchange_info: Dict[str, Any],
) -> List[SymbolRecord]:

    symbols = exchange_info.get("symbols", [])

    universe: List[SymbolRecord] = []

    for item in symbols:

        symbol = str(item.get("symbol", "")).upper()

        if not symbol:
            continue

        base_asset = str(
            item.get("baseAsset", "")
        ).upper()

        quote_asset = str(
            item.get("quoteAsset", "")
        ).upper()

        status = str(
            item.get("status", "")
        ).upper()

        permissions = item.get(
            "permissions",
            [],
        )

        # Binance peut exposer différentes structures
        # selon la version / configuration de l'API.
        spot_allowed = (
            "SPOT" in permissions
            or item.get("isSpotTradingAllowed") is True
        )

        universe.append(
            SymbolRecord(
                symbol=symbol,
                base_asset=base_asset,
                quote_asset=quote_asset,
                status=status,
                spot_allowed=spot_allowed,
            )
        )

    return universe


def merge_market_data(
    universe: List[SymbolRecord],
    tickers: List[Dict[str, Any]],
    book_tickers: List[Dict[str, Any]],
) -> None:

    ticker_map = {
        str(x.get("symbol", "")).upper(): x
        for x in tickers
    }

    book_map = {
        str(x.get("symbol", "")).upper(): x
        for x in book_tickers
    }

    for record in universe:

        ticker = ticker_map.get(record.symbol)

        if ticker:
            record.ticker_available = True

            record.last_price = safe_float(
                ticker.get("lastPrice")
            )

            record.price_change_percent = safe_float(
                ticker.get("priceChangePercent")
            )

            record.quote_volume = safe_float(
                ticker.get("quoteVolume")
            )

            record.trades = safe_int(
                ticker.get("count")
            )

        book = book_map.get(record.symbol)

        if book:

            record.bid_price = safe_float(
                book.get("bidPrice")
            )

            record.ask_price = safe_float(
                book.get("askPrice")
            )

            if (
                record.bid_price > 0
                and record.ask_price > 0
                and record.ask_price >= record.bid_price
            ):
                mid = (
                    record.bid_price
                    + record.ask_price
                ) / 2

                if mid > 0:
                    record.spread_percent = (
                        (
                            record.ask_price
                            - record.bid_price
                        )
                        / mid
                    ) * 100


# ============================================================================
# CRITERIA
# ============================================================================

@dataclass
class CriterionResult:
    name: str
    description: str
    before: int
    selected: int
    rejected: int
    retention_percent: float
    rejection_percent: float


def apply_criterion(
    current: List[SymbolRecord],
    name: str,
    description: str,
    predicate,
    audit: List[CriterionResult],
) -> List[SymbolRecord]:

    before = len(current)

    selected_items = [
        item
        for item in current
        if predicate(item)
    ]

    selected = len(selected_items)
    rejected = before - selected

    if before:
        retention = selected / before * 100
        rejection = rejected / before * 100
    else:
        retention = 0.0
        rejection = 0.0

    audit.append(
        CriterionResult(
            name=name,
            description=description,
            before=before,
            selected=selected,
            rejected=rejected,
            retention_percent=retention,
            rejection_percent=rejection,
        )
    )

    log(
        f"CRITERION | {name} | "
        f"before={before} | "
        f"selected={selected} | "
        f"rejected={rejected}"
    )

    return selected_items


def apply_all_criteria(
    universe: List[SymbolRecord],
) -> Tuple[List[SymbolRecord], List[CriterionResult]]:

    current = universe[:]
    audit: List[CriterionResult] = []

    current = apply_criterion(
        current,
        "STATUS_TRADING",
        "Symbol status == TRADING",
        lambda x: x.status == "TRADING",
        audit,
    )

    current = apply_criterion(
        current,
        "SPOT_ALLOWED",
        "Spot trading permission available",
        lambda x: x.spot_allowed,
        audit,
    )

    current = apply_criterion(
        current,
        "QUOTE_ASSET",
        f"Quote asset in {sorted(QUOTE_ASSETS)}",
        lambda x: x.quote_asset in QUOTE_ASSETS,
        audit,
    )

    if EXCLUDE_STABLECOINS:
        current = apply_criterion(
            current,
            "EXCLUDE_STABLECOINS",
            "Base asset is not a stablecoin",
            lambda x: not is_stablecoin_base(
                x.base_asset
            ),
            audit,
        )

    if EXCLUDE_LEVERAGED_TOKENS:
        current = apply_criterion(
            current,
            "EXCLUDE_LEVERAGED_TOKENS",
            "Base asset is not a leveraged token",
            lambda x: not is_leveraged_token(
                x.base_asset
            ),
            audit,
        )

    current = apply_criterion(
        current,
        "TICKER_AVAILABLE",
        "24h market ticker available",
        lambda x: x.ticker_available,
        audit,
    )

    current = apply_criterion(
        current,
        "MIN_PRICE",
        f"Last price >= {MIN_PRICE}",
        lambda x: x.last_price >= MIN_PRICE,
        audit,
    )

    current = apply_criterion(
        current,
        "MIN_24H_QUOTE_VOLUME",
        f"24h quote volume >= {MIN_24H_QUOTE_VOLUME:,.0f}",
        lambda x: x.quote_volume >= MIN_24H_QUOTE_VOLUME,
        audit,
    )

    current = apply_criterion(
        current,
        "MIN_24H_TRADES",
        f"24h trades >= {MIN_24H_TRADES:,}",
        lambda x: x.trades >= MIN_24H_TRADES,
        audit,
    )

    current = apply_criterion(
        current,
        "MIN_24H_CHANGE",
        f"24h change >= {MIN_24H_CHANGE_PERCENT:.2f}%",
        lambda x: (
            x.price_change_percent
            >= MIN_24H_CHANGE_PERCENT
        ),
        audit,
    )

    current = apply_criterion(
        current,
        "MAX_24H_CHANGE",
        f"24h change <= {MAX_24H_CHANGE_PERCENT:.2f}%",
        lambda x: (
            x.price_change_percent
            <= MAX_24H_CHANGE_PERCENT
        ),
        audit,
    )

    current = apply_criterion(
        current,
        "MAX_SPREAD",
        f"Spread <= {MAX_SPREAD_PERCENT:.2f}%",
        lambda x: (
            x.spread_percent > 0
            and x.spread_percent <= MAX_SPREAD_PERCENT
        ),
        audit,
    )

    return current, audit


# ============================================================================
# TECHNICAL INDICATORS
# ============================================================================

def calculate_ema(
    values: List[float],
    period: int,
) -> List[Optional[float]]:

    if period <= 0:
        return [None] * len(values)

    if len(values) < period:
        return [None] * len(values)

    result: List[Optional[float]] = [
        None
    ] * len(values)

    seed = statistics.fmean(
        values[:period]
    )

    result[period - 1] = seed

    multiplier = 2 / (period + 1)

    previous = seed

    for i in range(period, len(values)):
        previous = (
            (values[i] - previous)
            * multiplier
            + previous
        )

        result[i] = previous

    return result


def calculate_rsi(
    values: List[float],
    period: int,
) -> List[Optional[float]]:

    result: List[Optional[float]] = [
        None
    ] * len(values)

    if period <= 0 or len(values) <= period:
        return result

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]

        gains.append(
            max(change, 0.0)
        )

        losses.append(
            max(-change, 0.0)
        )

    avg_gain = statistics.fmean(
        gains[:period]
    )

    avg_loss = statistics.fmean(
        losses[:period]
    )

    def rsi_value(
        gain: float,
        loss: float,
    ) -> float:

        if loss == 0:
            return 100.0

        rs = gain / loss

        return 100.0 - (
            100.0 / (1.0 + rs)
        )

    result[period] = rsi_value(
        avg_gain,
        avg_loss,
    )

    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1))
            + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1))
            + losses[i]
        ) / period

        result[i + 1] = rsi_value(
            avg_gain,
            avg_loss,
        )

    return result


def calculate_macd(
    values: List[float],
    fast_period: int,
    slow_period: int,
    signal_period: int,
) -> Tuple[
    List[Optional[float]],
    List[Optional[float]],
    List[Optional[float]],
]:

    fast = calculate_ema(
        values,
        fast_period,
    )

    slow = calculate_ema(
        values,
        slow_period,
    )

    macd: List[Optional[float]] = [
        None
    ] * len(values)

    compact_macd: List[float] = []
    compact_indexes: List[int] = []

    for i in range(len(values)):

        if (
            fast[i] is not None
            and slow[i] is not None
        ):
            value = (
                fast[i] - slow[i]
            )

            macd[i] = value
            compact_macd.append(value)
            compact_indexes.append(i)

    signal_compact = calculate_ema(
        compact_macd,
        signal_period,
    )

    signal: List[Optional[float]] = [
        None
    ] * len(values)

    histogram: List[Optional[float]] = [
        None
    ] * len(values)

    for pos, index in enumerate(
        compact_indexes
    ):

        if signal_compact[pos] is not None:

            signal[index] = (
                signal_compact[pos]
            )

            histogram[index] = (
                macd[index]
                - signal[index]
            )

    return macd, signal, histogram


def calculate_atr(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int,
) -> List[Optional[float]]:

    result: List[Optional[float]] = [
        None
    ] * len(closes)

    if len(closes) <= period:
        return result

    true_ranges: List[float] = []

    for i in range(len(closes)):

        if i == 0:
            tr = highs[i] - lows[i]
        else:
            tr = max(
                highs[i] - lows[i],
                abs(
                    highs[i] - closes[i - 1]
                ),
                abs(
                    lows[i] - closes[i - 1]
                ),
            )

        true_ranges.append(tr)

    if len(true_ranges) < period:
        return result

    atr = statistics.fmean(
        true_ranges[:period]
    )

    result[period - 1] = atr

    for i in range(period, len(true_ranges)):

        atr = (
            (
                atr * (period - 1)
            )
            + true_ranges[i]
        ) / period

        result[i] = atr

    return result


def calculate_volume_ratio(
    volumes: List[float],
    period: int,
) -> List[Optional[float]]:

    result: List[Optional[float]] = [
        None
    ] * len(volumes)

    if period <= 0:
        return result

    for i in range(period - 1, len(volumes)):

        window = volumes[
            i - period + 1:i + 1
        ]

        average = statistics.fmean(window)

        if average > 0:
            result[i] = (
                volumes[i] / average
            )

    return result


# ============================================================================
# TECHNICAL RESULT
# ============================================================================

@dataclass
class TechnicalResult:
    symbol: str
    price: float

    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None

    rsi: Optional[float] = None

    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None

    atr: Optional[float] = None
    atr_percent: Optional[float] = None

    volume_ratio: Optional[float] = None

    ema_state: str = "N/A"
    rsi_state: str = "N/A"
    macd_state: str = "N/A"
    volume_state: str = "N/A"

    technical_score: float = 0.0

    error: str = ""


# ============================================================================
# KLINES
# ============================================================================

def fetch_klines(
    symbol: str,
) -> List[List[Any]]:

    return http_get_json(
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": TECHNICAL_INTERVAL,
            "limit": TECHNICAL_KLINES_LIMIT,
        },
    )


def extract_ohlcv(
    klines: List[List[Any]],
) -> Tuple[
    List[float],
    List[float],
    List[float],
    List[float],
]:

    opens = []
    highs = []
    lows = []
    closes = []
    volumes = []

    for candle in klines:

        if len(candle) < 6:
            continue

        opens.append(
            safe_float(candle[1])
        )

        highs.append(
            safe_float(candle[2])
        )

        lows.append(
            safe_float(candle[3])
        )

        closes.append(
            safe_float(candle[4])
        )

        volumes.append(
            safe_float(candle[5])
        )

    return (
        opens,
        highs,
        lows,
        closes,
        volumes,
    )


# ============================================================================
# TECHNICAL SCORING
# ============================================================================

def build_technical_result(
    symbol: str,
    klines: List[List[Any]],
) -> TechnicalResult:

    (
        opens,
        highs,
        lows,
        closes,
        volumes,
    ) = extract_ohlcv(klines)

    if len(closes) < 60:
        raise RuntimeError(
            f"Not enough candles: {len(closes)}"
        )

    result = TechnicalResult(
        symbol=symbol,
        price=closes[-1],
    )

    score = 0.0
    maximum = 0.0

    # ------------------------------------------------------------------------
    # EMA
    # ------------------------------------------------------------------------

    if ENABLE_EMA:

        ema_fast = calculate_ema(
            closes,
            EMA_FAST_PERIOD,
        )

        ema_slow = calculate_ema(
            closes,
            EMA_SLOW_PERIOD,
        )

        result.ema_fast = ema_fast[-1]
        result.ema_slow = ema_slow[-1]

        if (
            result.ema_fast is not None
            and result.ema_slow is not None
        ):

            if (
                result.price > result.ema_fast
                and result.ema_fast
                > result.ema_slow
            ):
                result.ema_state = "BULLISH"
                score += 25

            elif (
                result.price < result.ema_fast
                and result.ema_fast
                < result.ema_slow
            ):
                result.ema_state = "BEARISH"
                score += 0

            else:
                result.ema_state = "MIXED"
                score += 12

        maximum += 25

    # ------------------------------------------------------------------------
    # RSI
    # ------------------------------------------------------------------------

    if ENABLE_RSI:

        rsi_values = calculate_rsi(
            closes,
            RSI_PERIOD,
        )

        result.rsi = rsi_values[-1]

        if result.rsi is not None:

            if 50 <= result.rsi <= 70:
                result.rsi_state = "BULLISH"
                score += 20

            elif 30 <= result.rsi < 50:
                result.rsi_state = "WEAK"
                score += 8

            elif result.rsi > 70:
                result.rsi_state = "OVERBOUGHT"
                score += 10

            else:
                result.rsi_state = "OVERSOLD"
                score += 5

        maximum += 20

    # ------------------------------------------------------------------------
    # MACD
    # ------------------------------------------------------------------------

    if ENABLE_MACD:

        (
            macd,
            signal,
            histogram,
        ) = calculate_macd(
            closes,
            MACD_FAST_PERIOD,
            MACD_SLOW_PERIOD,
            MACD_SIGNAL_PERIOD,
        )

        result.macd = macd[-1]
        result.macd_signal = signal[-1]
        result.macd_histogram = histogram[-1]

        if (
            result.macd is not None
            and result.macd_signal is not None
            and result.macd_histogram is not None
        ):

            if (
                result.macd > result.macd_signal
                and result.macd_histogram > 0
            ):
                result.macd_state = "BULLISH"
                score += 20

            elif result.macd < result.macd_signal:
                result.macd_state = "BEARISH"
                score += 0

            else:
                result.macd_state = "MIXED"
                score += 10

        maximum += 20

    # ------------------------------------------------------------------------
    # ATR
    # ------------------------------------------------------------------------

    if ENABLE_ATR:

        atr_values = calculate_atr(
            highs,
            lows,
            closes,
            ATR_PERIOD,
        )

        result.atr = atr_values[-1]

        if (
            result.atr is not None
            and result.price > 0
        ):

            result.atr_percent = (
                result.atr
                / result.price
                * 100
            )

        # ATR n'est pas ici un signal directionnel.
        # On lui donne donc une contribution neutre
        # au score mais on le conserve comme métrique.
        maximum += 15

        if result.atr_percent is not None:

            if 0.5 <= result.atr_percent <= 5:
                score += 15

            elif result.atr_percent < 0.5:
                score += 8

            else:
                score += 5

    # ------------------------------------------------------------------------
    # VOLUME
    # ------------------------------------------------------------------------

    if ENABLE_VOLUME_RATIO:

        ratios = calculate_volume_ratio(
            volumes,
            VOLUME_MA_PERIOD,
        )

        result.volume_ratio = ratios[-1]

        if result.volume_ratio is not None:

            if result.volume_ratio >= 1.5:
                result.volume_state = "HIGH"
                score += 20

            elif result.volume_ratio >= 1.0:
                result.volume_state = "NORMAL"
                score += 12

            else:
                result.volume_state = "LOW"
                score += 5

        maximum += 20

    if maximum > 0:
        result.technical_score = (
            score / maximum * 100
        )

    return result


def analyze_symbol(
    symbol: str,
) -> TechnicalResult:

    try:

        klines = fetch_klines(symbol)

        return build_technical_result(
            symbol,
            klines,
        )

    except Exception as exc:

        return TechnicalResult(
            symbol=symbol,
            price=0.0,
            error=str(exc),
        )


def run_technical_analysis(
    symbols: List[SymbolRecord],
) -> List[TechnicalResult]:

    if not TECHNICAL_ENABLED:
        return []

    selected = symbols[:]

    capped = False

    if (
        TECHNICAL_MAX_ASSETS > 0
        and len(selected) > TECHNICAL_MAX_ASSETS
    ):
        selected = sorted(
            selected,
            key=lambda x: x.quote_volume,
            reverse=True,
        )[:TECHNICAL_MAX_ASSETS]

        capped = True

    log(
        f"Technical analysis: "
        f"{len(selected)} assets"
        + (
            " (capped)"
            if capped
            else ""
        )
    )

    results: List[TechnicalResult] = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=TECHNICAL_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                analyze_symbol,
                item.symbol,
            ): item.symbol
            for item in selected
        }

        completed = 0
        total = len(futures)

        for future in concurrent.futures.as_completed(
            futures
        ):

            symbol = futures[future]

            try:
                result = future.result()
            except Exception as exc:
                result = TechnicalResult(
                    symbol=symbol,
                    price=0.0,
                    error=str(exc),
                )

            results.append(result)

            completed += 1

            if (
                completed == total
                or completed % 25 == 0
            ):
                log(
                    f"Technical progress: "
                    f"{completed}/{total}"
                )

    results.sort(
        key=lambda x: x.technical_score,
        reverse=True,
    )

    return results


# ============================================================================
# EMAIL REPORT
# ============================================================================

def format_criterion_table(
    audit: List[CriterionResult],
) -> str:

    lines = []

    lines.append(
        "CRITERE | AVANT | SELECTIONNES | REJETES | "
        "RETENTION | REJET"
    )

    lines.append("-" * 105)

    for item in audit:

        lines.append(
            f"{item.name} | "
            f"{item.before:,} | "
            f"{item.selected:,} | "
            f"{item.rejected:,} | "
            f"{item.retention_percent:.2f}% | "
            f"{item.rejection_percent:.2f}%"
        )

    return "\n".join(lines)


def format_technical_results(
    results: List[TechnicalResult],
) -> str:

    if not results:
        return "Aucun résultat technique."

    lines = []

    lines.append(
        "RANG | SYMBOL | PRICE | SCORE | EMA | RSI | "
        "MACD | ATR% | VOL RATIO"
    )

    lines.append("-" * 125)

    rank = 0

    for result in results:

        if result.error:
            continue

        rank += 1

        if rank > EMAIL_TOP_RESULTS:
            break

        price = (
            f"{result.price:.10g}"
            if result.price
            else "-"
        )

        rsi = (
            f"{result.rsi:.2f}"
            if result.rsi is not None
            else "-"
        )

        atr = (
            f"{result.atr_percent:.2f}%"
            if result.atr_percent is not None
            else "-"
        )

        volume_ratio = (
            f"{result.volume_ratio:.2f}"
            if result.volume_ratio is not None
            else "-"
        )

        lines.append(
            f"{rank} | "
            f"{result.symbol} | "
            f"{price} | "
            f"{result.technical_score:.1f} | "
            f"{result.ema_state} | "
            f"{rsi} | "
            f"{result.macd_state} | "
            f"{atr} | "
            f"{volume_ratio}"
        )

    if rank == 0:
        return "Aucun résultat technique exploitable."

    return "\n".join(lines)


def build_email_body(
    universe: List[SymbolRecord],
    selected: List[SymbolRecord],
    audit: List[CriterionResult],
    technical_results: List[TechnicalResult],
    duration_seconds: float,
) -> str:

    technical_errors = sum(
        1
        for x in technical_results
        if x.error
    )

    successful_technical = (
        len(technical_results)
        - technical_errors
    )

    lines: List[str] = []

    lines.append(
        "BINANCE SCREENER"
    )

    lines.append("=" * 100)

    lines.append(
        f"Date UTC : "
        f"{utc_now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    lines.append(
        f"Durée : {duration_seconds:.1f} secondes"
    )

    lines.append("")

    lines.append(
        "RESUME"
    )

    lines.append("-" * 100)

    lines.append(
        f"Univers Binance Spot initial : "
        f"{len(universe):,}"
    )

    lines.append(
        f"Actifs après tous les critères : "
        f"{len(selected):,}"
    )

    if universe:
        final_retention = (
            len(selected)
            / len(universe)
            * 100
        )
    else:
        final_retention = 0.0

    lines.append(
        f"Rétention finale : "
        f"{final_retention:.2f}%"
    )

    lines.append("")

    lines.append(
        "PARAMETRES"
    )

    lines.append("-" * 100)

    lines.append(
        f"Quotes : {', '.join(sorted(QUOTE_ASSETS))}"
    )

    lines.append(
        f"Volume 24h minimum : "
        f"{MIN_24H_QUOTE_VOLUME:,.0f}"
    )

    lines.append(
        f"Trades 24h minimum : "
        f"{MIN_24H_TRADES:,}"
    )

    lines.append(
        f"Variation 24h : "
        f"{MIN_24H_CHANGE_PERCENT:.2f}% "
        f"à "
        f"{MAX_24H_CHANGE_PERCENT:.2f}%"
    )

    lines.append(
        f"Spread maximum : "
        f"{MAX_SPREAD_PERCENT:.2f}%"
    )

    lines.append(
        f"Exclusion stablecoins : "
        f"{EXCLUDE_STABLECOINS}"
    )

    lines.append(
        f"Exclusion leveraged tokens : "
        f"{EXCLUDE_LEVERAGED_TOKENS}"
    )

    lines.append("")

    lines.append(
        "AUDIT DES CRITERES"
    )

    lines.append("-" * 100)

    lines.append(
        format_criterion_table(audit)
    )

    lines.append("")

    lines.append(
        "ANALYSE TECHNIQUE"
    )

    lines.append("-" * 100)

    lines.append(
        f"Activée : {TECHNICAL_ENABLED}"
    )

    if TECHNICAL_ENABLED:

        lines.append(
            f"Timeframe : "
            f"{TECHNICAL_INTERVAL}"
        )

        lines.append(
            f"Actifs analysés : "
            f"{len(technical_results):,}"
        )

        lines.append(
            f"Analyses réussies : "
            f"{successful_technical:,}"
        )

        lines.append(
            f"Erreurs techniques : "
            f"{technical_errors:,}"
        )

        if TECHNICAL_MAX_ASSETS > 0:
            lines.append(
                f"Limite technique : "
                f"{TECHNICAL_MAX_ASSETS:,}"
            )
        else:
            lines.append(
                "Limite technique : aucune"
            )

        lines.append("")

        lines.append(
            format_technical_results(
                technical_results
            )
        )

    lines.append("")

    lines.append(
        "TOP ACTIFS APRES FILTRAGE"
    )

    lines.append("-" * 100)

    top_selected = sorted(
        selected,
        key=lambda x: x.quote_volume,
        reverse=True,
    )[:EMAIL_TOP_RESULTS]

    if not top_selected:
        lines.append(
            "Aucun actif ne satisfait tous les critères."
        )
    else:

        lines.append(
            "SYMBOL | PRICE | 24H% | VOLUME 24H | "
            "TRADES | SPREAD"
        )

        lines.append("-" * 100)

        for item in top_selected:

            lines.append(
                f"{item.symbol} | "
                f"{item.last_price:.10g} | "
                f"{item.price_change_percent:.2f}% | "
                f"{number(item.quote_volume)} | "
                f"{item.trades:,} | "
                f"{item.spread_percent:.4f}%"
            )

    lines.append("")

    lines.append(
        "FIN DU RAPPORT"
    )

    return "\n".join(lines)


def send_email(
    subject: str,
    body: str,
) -> None:

    if not EMAIL_USER:
        raise RuntimeError(
            "EMAIL_USER secret is missing."
        )

    if not EMAIL_PASS:
        raise RuntimeError(
            "EMAIL_PASS secret is missing."
        )

    if not EMAIL_TO:
        raise RuntimeError(
            "EMAIL_TO secret is missing."
        )

    recipients = [
        x.strip()
        for x in EMAIL_TO.split(",")
        if x.strip()
    ]

    if not recipients:
        raise RuntimeError(
            "EMAIL_TO does not contain a valid recipient."
        )

    message = EmailMessage()

    message["From"] = EMAIL_USER
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject

    message.set_content(body)

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

        server.send_message(message)


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:

    started = time.time()

    log("=" * 80)
    log("BINANCE SCREENER START")
    log("=" * 80)

    try:

        # --------------------------------------------------------------------
        # 1. UNIVERS
        # --------------------------------------------------------------------

        exchange_info = fetch_exchange_info()

        universe = build_universe(
            exchange_info
        )

        log(
            f"Raw Binance symbol universe: "
            f"{len(universe):,}"
        )

        # --------------------------------------------------------------------
        # 2. MARKET DATA
        # --------------------------------------------------------------------

        tickers = fetch_24h_tickers()

        book_tickers = fetch_book_tickers()

        merge_market_data(
            universe,
            tickers,
            book_tickers,
        )

        log(
            f"24h tickers received: "
            f"{len(tickers):,}"
        )

        log(
            f"Book tickers received: "
            f"{len(book_tickers):,}"
        )

        # --------------------------------------------------------------------
        # 3. CRITERES
        # --------------------------------------------------------------------

        selected, audit = apply_all_criteria(
            universe
        )

        log(
            f"Final selected universe: "
            f"{len(selected):,}"
        )

        # --------------------------------------------------------------------
        # 4. TECHNICAL ANALYSIS
        # --------------------------------------------------------------------

        technical_results = run_technical_analysis(
            selected
        )

        # --------------------------------------------------------------------
        # 5. EMAIL
        # --------------------------------------------------------------------

        duration = time.time() - started

        body = build_email_body(
            universe=universe,
            selected=selected,
            audit=audit,
            technical_results=technical_results,
            duration_seconds=duration,
        )

        subject = (
            "Binance Screener | "
            f"{len(selected)} actifs sélectionnés | "
            f"{utc_now().strftime('%Y-%m-%d %H:%M UTC')}"
        )

        log("Sending email report...")

        send_email(
            subject=subject,
            body=body,
        )

        log("Email sent successfully.")

        log("=" * 80)
        log(
            f"BINANCE SCREENER END | "
            f"{duration:.1f}s"
        )
        log("=" * 80)

        return 0

    except Exception as exc:

        duration = time.time() - started

        log("=" * 80)
        log(
            f"FATAL ERROR after "
            f"{duration:.1f}s"
        )
        log(
            f"{type(exc).__name__}: {exc}"
        )
        log("=" * 80)

        return 1


if __name__ == "__main__":
    sys.exit(main())
