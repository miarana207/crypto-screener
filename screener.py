from __future__ import annotations

import json
import logging
import math
import os
import smtplib
import time
import urllib.error
import urllib.parse
import urllib.request

from concurrent.futures import ThreadPoolExecutor, as_completed
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

LOGGER = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION BINANCE
# ============================================================

BINANCE_BASE_URL = os.getenv(
    "BINANCE_BASE_URL",
    "https://data-api.binance.vision",
).rstrip("/")

EXCHANGE_INFO_ENDPOINT = "/api/v3/exchangeInfo"
TICKER_24HR_ENDPOINT = "/api/v3/ticker/24hr"
KLINES_ENDPOINT = "/api/v3/klines"

API_TIMEOUT = int(
    os.getenv("API_TIMEOUT", "20")
)

API_RETRIES = int(
    os.getenv("API_RETRIES", "3")
)


# ============================================================
# CONFIGURATION DE L'UNIVERS
# ============================================================

QUOTE_ASSETS = {
    item.strip().upper()
    for item in os.getenv(
        "QUOTE_ASSETS",
        "USDT",
    ).split(",")
    if item.strip()
}


# ============================================================
# CRITÈRES DE SCREENING
# ============================================================

MIN_24H_QUOTE_VOLUME = float(
    os.getenv(
        "MIN_24H_QUOTE_VOLUME",
        "5000000",
    )
)

MIN_24H_TRADES = int(
    os.getenv(
        "MIN_24H_TRADES",
        "1000",
    )
)

MIN_24H_CHANGE_PERCENT = float(
    os.getenv(
        "MIN_24H_CHANGE_PERCENT",
        "-100",
    )
)

MAX_24H_CHANGE_PERCENT = float(
    os.getenv(
        "MAX_24H_CHANGE_PERCENT",
        "100",
    )
)

MAX_SPREAD_PERCENT = float(
    os.getenv(
        "MAX_SPREAD_PERCENT",
        "0.50",
    )
)

MIN_PRICE = float(
    os.getenv(
        "MIN_PRICE",
        "0.00000001",
    )
)


# ============================================================
# EXCLUSIONS
# ============================================================

EXCLUDE_STABLECOINS = (
    os.getenv(
        "EXCLUDE_STABLECOINS",
        "true",
    ).lower()
    == "true"
)

EXCLUDE_LEVERAGED_TOKENS = (
    os.getenv(
        "EXCLUDE_LEVERAGED_TOKENS",
        "true",
    ).lower()
    == "true"
)


# ============================================================
# ANALYSE TECHNIQUE
# ============================================================

TECHNICAL_ENABLED = (
    os.getenv(
        "TECHNICAL_ENABLED",
        "true",
    ).lower()
    == "true"
)

TECHNICAL_INTERVAL = os.getenv(
    "TECHNICAL_INTERVAL",
    "1h",
)

TECHNICAL_KLINES_LIMIT = int(
    os.getenv(
        "TECHNICAL_KLINES_LIMIT",
        "250",
    )
)

EMA_FAST = int(
    os.getenv(
        "EMA_FAST",
        "20",
    )
)

EMA_SLOW = int(
    os.getenv(
        "EMA_SLOW",
        "50",
    )
)

RSI_PERIOD = int(
    os.getenv(
        "RSI_PERIOD",
        "14",
    )
)

MACD_FAST = int(
    os.getenv(
        "MACD_FAST",
        "12",
    )
)

MACD_SLOW = int(
    os.getenv(
        "MACD_SLOW",
        "26",
    )
)

MACD_SIGNAL = int(
    os.getenv(
        "MACD_SIGNAL",
        "9",
    )
)

ATR_PERIOD = int(
    os.getenv(
        "ATR_PERIOD",
        "14",
    )
)

VOLUME_MA_PERIOD = int(
    os.getenv(
        "VOLUME_MA_PERIOD",
        "20",
    )
)

TECHNICAL_MAX_ASSETS = int(
    os.getenv(
        "TECHNICAL_MAX_ASSETS",
        "500",
    )
)

TECHNICAL_WORKERS = int(
    os.getenv(
        "TECHNICAL_WORKERS",
        "8",
    )
)

EMAIL_TOP_RESULTS = int(
    os.getenv(
        "EMAIL_TOP_RESULTS",
        "50",
    )
)


# ============================================================
# CONSTANTES D'EXCLUSION
# ============================================================

STABLECOINS = {
    "USDT",
    "USDC",
    "FDUSD",
    "TUSD",
    "USDP",
    "DAI",
    "USDE",
    "USDD",
    "PYUSD",
    "BUSD",
    "USD1",
}

LEVERAGED_SUFFIXES = (
    "UP",
    "DOWN",
    "BULL",
    "BEAR",
)


# ============================================================
# HTTP
# ============================================================

def http_get_json(
    endpoint: str,
    params: dict | None = None,
) -> object:
    """
    Effectue une requête GET vers Binance et retourne le JSON.

    Une logique de retry est utilisée pour les erreurs réseau
    temporaires et certaines erreurs HTTP.
    """

    url = f"{BINANCE_BASE_URL}{endpoint}"

    if params:
        query = urllib.parse.urlencode(params)
        url = f"{url}?{query}"

    last_error = None

    for attempt in range(
        1,
        API_RETRIES + 1,
    ):
        try:
            LOGGER.info(
                "Requête Binance [%d/%d] : %s",
                attempt,
                API_RETRIES,
                url,
            )

            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 "
                        "(compatible; "
                        "BinanceProgrammableScreener/1.0)"
                    ),
                    "Accept": "application/json",
                },
                method="GET",
            )

            with urllib.request.urlopen(
                request,
                timeout=API_TIMEOUT,
            ) as response:

                if response.status != 200:
                    raise RuntimeError(
                        f"HTTP {response.status}"
                    )

                payload = response.read().decode(
                    "utf-8"
                )

                return json.loads(payload)

        except urllib.error.HTTPError as exc:
            last_error = exc

            LOGGER.warning(
                "Erreur HTTP Binance %s : %s",
                exc.code,
                exc.reason,
            )

            if exc.code == 429:
                time.sleep(
                    min(
                        5 * attempt,
                        30,
                    )
                )

            elif exc.code in (
                403,
                408,
                500,
                502,
                503,
                504,
            ):
                time.sleep(
                    min(
                        2 ** attempt,
                        15,
                    )
                )

            else:
                raise

        except (
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError,
            RuntimeError,
        ) as exc:

            last_error = exc

            LOGGER.warning(
                "Erreur réseau/API : %s",
                exc,
            )

            if attempt < API_RETRIES:
                time.sleep(
                    min(
                        2 ** attempt,
                        15,
                    )
                )

        except Exception as exc:
            last_error = exc

            LOGGER.exception(
                "Erreur inattendue pendant la requête Binance."
            )

            if attempt < API_RETRIES:
                time.sleep(
                    min(
                        2 ** attempt,
                        15,
                    )
                )

    raise RuntimeError(
        f"Échec définitif de la requête Binance : "
        f"{last_error}"
    )


# ============================================================
# UNIVERS BINANCE
# ============================================================

def get_exchange_info() -> dict:
    """
    Récupère les informations de marché Binance.
    """

    LOGGER.info(
        "Récupération de exchangeInfo..."
    )

    data = http_get_json(
        EXCHANGE_INFO_ENDPOINT
    )

    if not isinstance(data, dict):
        raise RuntimeError(
            "Réponse exchangeInfo invalide."
        )

    return data


def get_24h_tickers() -> list[dict]:
    """
    Récupère les statistiques 24h de tous les tickers.
    """

    LOGGER.info(
        "Récupération des statistiques 24h..."
    )

    data = http_get_json(
        TICKER_24HR_ENDPOINT
    )

    if not isinstance(data, list):
        raise RuntimeError(
            "Réponse ticker/24hr invalide."
        )

    LOGGER.info(
        "Tickers reçus : %d",
        len(data),
    )

    return data


def build_universe(
    exchange_info: dict,
) -> list[dict]:
    """
    Construit la population initiale.

    Aucun filtre économique n'est appliqué ici.

    Cette étape sert à recenser l'univers fourni par Binance.
    """

    symbols = exchange_info.get(
        "symbols",
        [],
    )

    universe = []

    for item in symbols:

        if not isinstance(
            item,
            dict,
        ):
            continue

        symbol = str(
            item.get(
                "symbol",
                "",
            )
        ).upper()

        if not symbol:
            continue

        universe.append(
            {
                "symbol": symbol,
                "status": str(
                    item.get(
                        "status",
                        "",
                    )
                ).upper(),
                "baseAsset": str(
                    item.get(
                        "baseAsset",
                        "",
                    )
                ).upper(),
                "quoteAsset": str(
                    item.get(
                        "quoteAsset",
                        "",
                    )
                ).upper(),
                "permissions": item.get(
                    "permissions",
                    [],
                ),
                "permissionSets": item.get(
                    "permissionSets",
                    [],
                ),
            }
        )

    return universe


# ============================================================
# UTILITAIRES
# ============================================================

def to_float(
    value,
    default=0.0,
) -> float:

    try:
        return float(value)

    except (
        TypeError,
        ValueError,
    ):
        return default


def to_int(
    value,
    default=0,
) -> int:

    try:
        return int(value)

    except (
        TypeError,
        ValueError,
    ):
        return default


def is_spot_symbol(
    asset: dict,
) -> bool:
    """
    Vérifie la permission Spot lorsque Binance la fournit.

    Si aucune information de permission n'est fournie,
    l'actif n'est pas rejeté uniquement pour cette raison.
    """

    permissions = asset.get(
        "permissions",
        [],
    )

    permission_sets = asset.get(
        "permissionSets",
        [],
    )

    if isinstance(
        permissions,
        list,
    ) and permissions:

        return "SPOT" in permissions

    if isinstance(
        permission_sets,
        list,
    ) and permission_sets:

        for group in permission_sets:

            if isinstance(
                group,
                list,
            ):

                if "SPOT" in group:
                    return True

        return False

    return True


def is_leveraged_token(
    base_asset: str,
) -> bool:

    base = base_asset.upper()

    return any(
        base.endswith(suffix)
        for suffix in LEVERAGED_SUFFIXES
    )


def is_stablecoin(
    base_asset: str,
) -> bool:

    return (
        base_asset.upper()
        in STABLECOINS
    )


# ============================================================
# FUSION UNIVERS + MARKET DATA
# ============================================================

def merge_market_data(
    universe: list[dict],
    tickers: list[dict],
) -> list[dict]:
    """
    Fusionne exchangeInfo et ticker/24hr par symbole.
    """

    ticker_map = {}

    for ticker in tickers:

        if not isinstance(
            ticker,
            dict,
        ):
            continue

        symbol = str(
            ticker.get(
                "symbol",
                "",
            )
        ).upper()

        if symbol:
            ticker_map[symbol] = ticker

    records = []

    for asset in universe:

        symbol = asset["symbol"]

        ticker = ticker_map.get(
            symbol
        )

        record = dict(asset)

        record["ticker_found"] = (
            ticker is not None
        )

        if ticker is not None:

            record.update(
                {
                    "lastPrice": to_float(
                        ticker.get(
                            "lastPrice"
                        )
                    ),
                    "priceChangePercent": to_float(
                        ticker.get(
                            "priceChangePercent"
                        )
                    ),
                    "quoteVolume": to_float(
                        ticker.get(
                            "quoteVolume"
                        )
                    ),
                    "trades": to_int(
                        ticker.get(
                            "count"
                        )
                    ),
                    "highPrice": to_float(
                        ticker.get(
                            "highPrice"
                        )
                    ),
                    "lowPrice": to_float(
                        ticker.get(
                            "lowPrice"
                        )
                    ),
                    "bidPrice": to_float(
                        ticker.get(
                            "bidPrice"
                        )
                    ),
                    "askPrice": to_float(
                        ticker.get(
                            "askPrice"
                        )
                    ),
                }
            )

        else:

            record.update(
                {
                    "lastPrice": 0.0,
                    "priceChangePercent": 0.0,
                    "quoteVolume": 0.0,
                    "trades": 0,
                    "highPrice": 0.0,
                    "lowPrice": 0.0,
                    "bidPrice": 0.0,
                    "askPrice": 0.0,
                }
            )

        records.append(record)

    return records


# ============================================================
# MOTEUR DE CRITÈRES
# ============================================================

def run_criterion(
    name: str,
    records: list[dict],
    condition,
    audit: list[dict],
) -> list[dict]:
    """
    Applique un critère et mesure son impact.

    Pour chaque critère :
    - population avant ;
    - sélectionnés ;
    - rejetés ;
    - taux de rejet ;
    - taux de conservation.
    """

    before = len(records)

    selected = []
    rejected = 0

    for record in records:

        try:
            passed = bool(
                condition(record)
            )

        except Exception:
            passed = False

        if passed:
            selected.append(record)

        else:
            rejected += 1

    after = len(selected)

    rejection_rate = (
        (rejected / before) * 100
        if before
        else 0.0
    )

    retention_rate = (
        (after / before) * 100
        if before
        else 0.0
    )

    audit.append(
        {
            "criterion": name,
            "before": before,
            "selected": after,
            "rejected": rejected,
            "rejection_rate": rejection_rate,
            "retention_rate": retention_rate,
        }
    )

    LOGGER.info(
        "%s | avant=%d | sélectionnés=%d | "
        "rejetés=%d | rejet=%.2f%%",
        name,
        before,
        after,
        rejected,
        rejection_rate,
    )

    return selected


def apply_screening(
    records: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Applique séquentiellement tous les critères.

    Chaque critère est indépendant et audité.
    """

    current = records

    audit = []

    # --------------------------------------------------------
    # CRITÈRE 1 — STATUT
    # --------------------------------------------------------

    current = run_criterion(
        "01 — Statut TRADING",
        current,
        lambda x:
            x["status"] == "TRADING",
        audit,
    )

    # --------------------------------------------------------
    # CRITÈRE 2 — SPOT
    # --------------------------------------------------------

    current = run_criterion(
        "02 — Permission Spot",
        current,
        is_spot_symbol,
        audit,
    )

    # --------------------------------------------------------
    # CRITÈRE 3 — QUOTE ASSET
    # --------------------------------------------------------

    current = run_criterion(
        "03 — Quote asset autorisé",
        current,
        lambda x:
            x["quoteAsset"]
            in QUOTE_ASSETS,
        audit,
    )

    # --------------------------------------------------------
    # CRITÈRE 4 — STABLECOINS
    # --------------------------------------------------------

    if EXCLUDE_STABLECOINS:

        current = run_criterion(
            "04 — Exclusion stablecoins",
            current,
            lambda x:
                not is_stablecoin(
                    x["baseAsset"]
                ),
            audit,
        )

    else:

        audit.append(
            {
                "criterion":
                    "04 — Exclusion stablecoins "
                    "(désactivé)",
                "before":
                    len(current),
                "selected":
                    len(current),
                "rejected":
                    0,
                "rejection_rate":
                    0.0,
                "retention_rate":
                    100.0,
            }
        )

    # --------------------------------------------------------
    # CRITÈRE 5 — LEVERAGED TOKENS
    # --------------------------------------------------------

    if EXCLUDE_LEVERAGED_TOKENS:

        current = run_criterion(
            "05 — Exclusion tokens levier",
            current,
            lambda x:
                not is_leveraged_token(
                    x["baseAsset"]
                ),
            audit,
        )

    else:

        audit.append(
            {
                "criterion":
                    "05 — Exclusion tokens levier "
                    "(désactivé)",
                "before":
                    len(current),
                "selected":
                    len(current),
                "rejected":
                    0,
                "rejection_rate":
                    0.0,
                "retention_rate":
                    100.0,
            }
        )

    # --------------------------------------------------------
    # CRITÈRE 6 — TICKER
    # --------------------------------------------------------

    current = run_criterion(
        "06 — Ticker 24h disponible",
        current,
        lambda x:
            x["ticker_found"],
        audit,
    )

    # --------------------------------------------------------
    # CRITÈRE 7 — PRIX
    # --------------------------------------------------------

    current = run_criterion(
        "07 — Prix minimum",
        current,
        lambda x:
            x["lastPrice"]
            >= MIN_PRICE,
        audit,
    )

    # --------------------------------------------------------
    # CRITÈRE 8 — VOLUME
    # --------------------------------------------------------

    current = run_criterion(
        "08 — Volume 24h minimum",
        current,
        lambda x:
            x["quoteVolume"]
            >= MIN_24H_QUOTE_VOLUME,
        audit,
    )

    # --------------------------------------------------------
    # CRITÈRE 9 — TRADES
    # --------------------------------------------------------

    current = run_criterion(
        "09 — Nombre minimum de trades",
        current,
        lambda x:
            x["trades"]
            >= MIN_24H_TRADES,
        audit,
    )

    # --------------------------------------------------------
    # CRITÈRE 10 — VARIATION MINIMUM
    # --------------------------------------------------------

    current = run_criterion(
        "10 — Variation 24h minimum",
        current,
        lambda x:
            x["priceChangePercent"]
            >= MIN_24H_CHANGE_PERCENT,
        audit,
    )

    # --------------------------------------------------------
    # CRITÈRE 11 — VARIATION MAXIMUM
    # --------------------------------------------------------

    current = run_criterion(
        "11 — Variation 24h maximum",
        current,
        lambda x:
            x["priceChangePercent"]
            <= MAX_24H_CHANGE_PERCENT,
        audit,
    )

    # --------------------------------------------------------
    # CRITÈRE 12 — SPREAD
    # --------------------------------------------------------

    def spread_condition(
        x: dict,
    ) -> bool:

        bid = x["bidPrice"]
        ask = x["askPrice"]

        if bid <= 0 or ask <= 0:
            return False

        midpoint = (
            bid + ask
        ) / 2

        if midpoint <= 0:
            return False

        spread = (
            (ask - bid)
            / midpoint
        ) * 100

        return (
            spread
            <= MAX_SPREAD_PERCENT
        )

    current = run_criterion(
        "12 — Spread maximum",
        current,
        spread_condition,
        audit,
    )

    return current, audit


# ============================================================
# INDICATEURS TECHNIQUES — SMA
# ============================================================

def sma(
    values: list[float],
    period: int,
) -> list[float | None]:

    result = [
        None
        for _ in values
    ]

    if period <= 0:
        return result

    if len(values) < period:
        return result

    rolling_sum = sum(
        values[:period]
    )

    result[period - 1] = (
        rolling_sum / period
    )

    for i in range(
        period,
        len(values),
    ):

        rolling_sum += (
            values[i]
            - values[i - period]
        )

        result[i] = (
            rolling_sum / period
        )

    return result


# ============================================================
# INDICATEURS TECHNIQUES — EMA
# ============================================================

def ema(
    values: list[float],
    period: int,
) -> list[float | None]:

    result = [
        None
        for _ in values
    ]

    if period <= 0:
        return result

    if len(values) < period:
        return result

    seed = sum(
        values[:period]
    ) / period

    result[period - 1] = seed

    multiplier = (
        2.0
        / (period + 1)
    )

    previous = seed

    for i in range(
        period,
        len(values),
    ):

        current = (
            (
                values[i]
                - previous
            )
            * multiplier
            + previous
        )

        result[i] = current
        previous = current

    return result


# ============================================================
# INDICATEURS TECHNIQUES — RSI
# ============================================================

def rsi(
    closes: list[float],
    period: int,
) -> list[float | None]:

    result = [
        None
        for _ in closes
    ]

    if (
        period <= 0
        or len(closes)
        <= period
    ):
        return result

    gains = []
    losses = []

    for i in range(
        1,
        len(closes),
    ):

        change = (
            closes[i]
            - closes[i - 1]
        )

        gains.append(
            max(change, 0.0)
        )

        losses.append(
            max(-change, 0.0)
        )

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
    )

    result[period] = (
        100.0
        if avg_loss == 0
        else
        100.0
        - (
            100.0
            / (
                1.0
                + (
                    avg_gain
                    / avg_loss
                )
            )
        )
    )

    for i in range(
        period,
        len(gains),
    ):

        avg_gain = (
            (
                avg_gain
                * (period - 1)
                + gains[i]
            )
            / period
        )

        avg_loss = (
            (
                avg_loss
                * (period - 1)
                + losses[i]
            )
            / period
        )

        result[i + 1] = (
            100.0
            if avg_loss == 0
            else
            100.0
            - (
                100.0
                / (
                    1.0
                    + (
                        avg_gain
                        / avg_loss
                    )
                )
            )
        )

    return result


# ============================================================
# INDICATEURS TECHNIQUES — ATR
# ============================================================

def true_ranges(
    highs: list[float],
    lows: list[float],
    closes: list[float],
) -> list[float]:

    result = []

    for i in range(
        len(closes)
    ):

        if i == 0:

            tr = (
                highs[i]
                - lows[i]
            )

        else:

            tr = max(
                highs[i]
                - lows[i],
                abs(
                    highs[i]
                    - closes[i - 1]
                ),
                abs(
                    lows[i]
                    - closes[i - 1]
                ),
            )

        result.append(tr)

    return result


def atr(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int,
) -> list[float | None]:

    tr = true_ranges(
        highs,
        lows,
        closes,
    )

    return sma(
        tr,
        period,
    )


# ============================================================
# KLINES
# ============================================================

def get_klines(
    symbol: str,
) -> list[list]:

    data = http_get_json(
        KLINES_ENDPOINT,
        {
            "symbol": symbol,
            "interval": TECHNICAL_INTERVAL,
            "limit": TECHNICAL_KLINES_LIMIT,
        },
    )

    if not isinstance(
        data,
        list,
    ):
        raise RuntimeError(
            f"Klines invalides pour {symbol}"
        )

    return data


# ============================================================
# ANALYSE TECHNIQUE
# ============================================================

def calculate_technical_indicators(
    symbol: str,
    klines: list[list],
) -> dict:

    minimum_required = max(
        EMA_SLOW + 10,
        RSI_PERIOD + 10,
        MACD_SLOW
        + MACD_SIGNAL
        + 10,
        ATR_PERIOD + 10,
        VOLUME_MA_PERIOD + 10,
    )

    if len(klines) < minimum_required:

        raise RuntimeError(
            f"Historique insuffisant pour {symbol}: "
            f"{len(klines)} bougies disponibles, "
            f"{minimum_required} nécessaires."
        )

    highs = [
        float(row[2])
        for row in klines
    ]

    lows = [
        float(row[3])
        for row in klines
    ]

    closes = [
        float(row[4])
        for row in klines
    ]

    volumes = [
        float(row[5])
        for row in klines
    ]

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    ema_fast = ema(
        closes,
        EMA_FAST,
    )

    ema_slow = ema(
        closes,
        EMA_SLOW,
    )

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    rsi_values = rsi(
        closes,
        RSI_PERIOD,
    )

    # --------------------------------------------------------
    # MACD
    # --------------------------------------------------------

    macd_fast = ema(
        closes,
        MACD_FAST,
    )

    macd_slow = ema(
        closes,
        MACD_SLOW,
    )

    macd_line = []

    for fast, slow in zip(
        macd_fast,
        macd_slow,
    ):

        if (
            fast is None
            or slow is None
        ):

            macd_line.append(None)

        else:

            macd_line.append(
                fast - slow
            )

    macd_valid = [
        value
        for value in macd_line
        if value is not None
    ]

    signal_values = ema(
        macd_valid,
        MACD_SIGNAL,
    )

    signal_line = [
        None
        for _ in macd_line
    ]

    signal_start = (
        len(macd_line)
        - len(macd_valid)
    )

    for i, value in enumerate(
        signal_values
    ):

        signal_line[
            signal_start + i
        ] = value

    # --------------------------------------------------------
    # ATR
    # --------------------------------------------------------

    atr_values = atr(
        highs,
        lows,
        closes,
        ATR_PERIOD,
    )

    # --------------------------------------------------------
    # VOLUME MOYEN
    # --------------------------------------------------------

    volume_ma = sma(
        volumes,
        VOLUME_MA_PERIOD,
    )

    # --------------------------------------------------------
    # DERNIÈRES VALEURS
    # --------------------------------------------------------

    last_close = closes[-1]

    last_ema_fast = (
        ema_fast[-1]
    )

    last_ema_slow = (
        ema_slow[-1]
    )

    last_rsi = (
        rsi_values[-1]
    )

    last_macd = (
        macd_line[-1]
    )

    last_signal = (
        signal_line[-1]
    )

    last_atr = (
        atr_values[-1]
    )

    last_volume_ma = (
        volume_ma[-1]
    )

    last_volume = volumes[-1]

    # --------------------------------------------------------
    # TENDANCE EMA
    # --------------------------------------------------------

    if (
        last_ema_fast is not None
        and last_ema_slow is not None
    ):

        if (
            last_close > last_ema_fast
            and last_ema_fast
            > last_ema_slow
        ):

            ema_trend = "BULLISH"

        elif (
            last_close < last_ema_fast
            and last_ema_fast
            < last_ema_slow
        ):

            ema_trend = "BEARISH"

        else:

            ema_trend = "MIXED"

    else:

        ema_trend = "UNKNOWN"

    # --------------------------------------------------------
    # MACD
    # --------------------------------------------------------

    if (
        last_macd is not None
        and last_signal is not None
    ):

        if last_macd > last_signal:

            macd_state = "BULLISH"

        elif last_macd < last_signal:

            macd_state = "BEARISH"

        else:

            macd_state = "NEUTRAL"

    else:

        macd_state = "UNKNOWN"

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if last_rsi is None:

        rsi_state = "UNKNOWN"

    elif last_rsi >= 70:

        rsi_state = "OVERBOUGHT"

    elif last_rsi <= 30:

        rsi_state = "OVERSOLD"

    elif last_rsi >= 50:

        rsi_state = "BULLISH"

    else:

        rsi_state = "BEARISH"

    # --------------------------------------------------------
    # VOLUME RELATIF
    # --------------------------------------------------------

    if (
        last_volume_ma is not None
        and last_volume_ma > 0
    ):

        volume_ratio = (
            last_volume
            / last_volume_ma
        )

    else:

        volume_ratio = 0.0

    # --------------------------------------------------------
    # ATR %
    # --------------------------------------------------------

    if (
        last_atr is not None
        and last_close > 0
    ):

        atr_percent = (
            last_atr
            / last_close
        ) * 100.0

    else:

        atr_percent = 0.0

    # --------------------------------------------------------
    # SCORE TECHNIQUE
    #
    # Ce score est uniquement un classement.
    # Il ne constitue PAS un signal de trading.
    # --------------------------------------------------------

    technical_score = 0.0

    # EMA — 30 points
    if ema_trend == "BULLISH":

        technical_score += 30

    elif ema_trend == "MIXED":

        technical_score += 20

    elif ema_trend == "BEARISH":

        technical_score += 10

    # MACD — 25 points
    if macd_state == "BULLISH":

        technical_score += 25

    elif macd_state == "NEUTRAL":

        technical_score += 15

    elif macd_state == "BEARISH":

        technical_score += 5

    # RSI — 25 points
    if last_rsi is not None:

        if 50 <= last_rsi < 70:

            technical_score += 25

        elif 40 <= last_rsi < 50:

            technical_score += 15

        elif 70 <= last_rsi < 80:

            technical_score += 10

        elif 30 <= last_rsi < 40:

            technical_score += 10

        else:

            technical_score += 5

    # Volume relatif — 20 points
    if volume_ratio >= 1.5:

        technical_score += 20

    elif volume_ratio >= 1.0:

        technical_score += 15

    elif volume_ratio >= 0.75:

        technical_score += 10

    else:

        technical_score += 5

    return {
        "symbol": symbol,
        "close": last_close,
        "emaFast": last_ema_fast,
        "emaSlow": last_ema_slow,
        "rsi": last_rsi,
        "macd": last_macd,
        "macdSignal": last_signal,
        "atr": last_atr,
        "atrPercent": atr_percent,
        "volume": last_volume,
        "volumeMA": last_volume_ma,
        "volumeRatio": volume_ratio,
        "emaTrend": ema_trend,
        "rsiState": rsi_state,
        "macdState": macd_state,
        "technicalScore": round(
            technical_score,
            1,
        ),
    }


def analyze_one_asset(
    record: dict,
) -> dict:

    symbol = record["symbol"]

    try:

        klines = get_klines(
            symbol
        )

        indicators = (
            calculate_technical_indicators(
                symbol,
                klines,
            )
        )

        result = dict(record)

        result["technical"] = (
            indicators
        )

        result["technicalStatus"] = (
            "OK"
        )

        return result

    except Exception as exc:

        LOGGER.warning(
            "Analyse technique échouée "
            "pour %s : %s",
            symbol,
            exc,
        )

        result = dict(record)

        result["technical"] = {}

        result["technicalStatus"] = (
            f"ERROR: {exc}"
        )

        return result


def run_technical_analysis(
    records: list[dict],
) -> tuple[list[dict], dict]:

    if not TECHNICAL_ENABLED:

        LOGGER.info(
            "Analyse technique désactivée."
        )

        return (
            records,
            {
                "requested": 0,
                "processed": 0,
                "successful": 0,
                "failed": 0,
                "limited": False,
            },
        )

    original_count = len(records)

    limited = False

    # Protection contre un trop grand nombre
    # de requêtes klines.
    if (
        TECHNICAL_MAX_ASSETS > 0
        and len(records)
        > TECHNICAL_MAX_ASSETS
    ):

        LOGGER.warning(
            "Univers technique réduit de %d à %d actifs.",
            len(records),
            TECHNICAL_MAX_ASSETS,
        )

        records = records[
            :TECHNICAL_MAX_ASSETS
        ]

        limited = True

    LOGGER.info(
        "Analyse technique de %d actif(s)...",
        len(records),
    )

    if not records:

        return (
            [],
            {
                "requested": 0,
                "processed": 0,
                "successful": 0,
                "failed": 0,
                "limited": limited,
            },
        )

    results = []

    workers = max(
        1,
        min(
            TECHNICAL_WORKERS,
            len(records),
        ),
    )

    with ThreadPoolExecutor(
        max_workers=workers
    ) as executor:

        futures = {
            executor.submit(
                analyze_one_asset,
                record,
            ): record
            for record in records
        }

        for future in as_completed(
            futures
        ):

            try:

                results.append(
                    future.result()
                )

            except Exception as exc:

                record = dict(
                    futures[future]
                )

                record[
                    "technical"
                ] = {}

                record[
                    "technicalStatus"
                ] = (
                    f"ERROR: {exc}"
                )

                results.append(
                    record
                )

    successful = sum(
        1
        for item in results
        if item.get(
            "technicalStatus"
        ) == "OK"
    )

    failed = (
        len(results)
        - successful
    )

    results.sort(
        key=lambda x:
            x.get(
                "technical",
                {}
            ).get(
                "technicalScore",
                -1,
            ),
        reverse=True,
    )

    return (
        results,
        {
            "requested": original_count,
            "processed": len(results),
            "successful": successful,
            "failed": failed,
            "limited": limited,
        },
    )


# ============================================================
# FORMATAGE
# ============================================================

def format_number(
    value,
    decimals=2,
) -> str:

    if value is None:
        return "N/A"

    try:
        return f"{float(value):,.{decimals}f}"

    except (
        TypeError,
        ValueError,
    ):
        return "N/A"


def format_volume(
    value,
) -> str:

    try:
        value = float(value)

    except (
        TypeError,
        ValueError,
    ):
        return "N/A"

    if value >= 1_000_000_000:

        return (
            f"{value / 1_000_000_000:.2f} Md"
        )

    if value >= 1_000_000:

        return (
            f"{value / 1_000_000:.2f} M"
        )

    if value >= 1_000:

        return (
            f"{value / 1_000:.1f} K"
        )

    return f"{value:.0f}"


def format_price(
    value,
) -> str:

    try:
        value = float(value)

    except (
        TypeError,
        ValueError,
    ):
        return "N/A"

    if value >= 1000:

        return f"{value:,.2f}"

    if value >= 1:

        return f"{value:,.4f}"

    if value >= 0.01:

        return f"{value:.6f}"

    return f"{value:.10f}"


# ============================================================
# EMAIL
# ============================================================

def build_email_body(
    universe: list[dict],
    audit: list[dict],
    final_records: list[dict],
    technical_stats: dict,
) -> str:

    lines = []

    lines.append(
        "BINANCE PROGRAMMABLE SCREENER"
    )

    lines.append(
        "=" * 70
    )

    lines.append("")

    lines.append(
        "OBJECTIF"
    )

    lines.append(
        "Recenser l'univers Binance, appliquer "
        "les critères séquentiellement, mesurer "
        "l'impact de chaque critère puis analyser "
        "techniquement les actifs survivants."
    )

    lines.append("")

    # --------------------------------------------------------
    # CONFIGURATION
    # --------------------------------------------------------

    lines.append(
        "CONFIGURATION DU SCREENING"
    )

    lines.append(
        "-" * 70
    )

    lines.append(
        "Quote assets : "
        + ", ".join(
            sorted(QUOTE_ASSETS)
        )
    )

    lines.append(
        "Volume 24h minimum : "
        + format_volume(
            MIN_24H_QUOTE_VOLUME
        )
    )

    lines.append(
        f"Trades 24h minimum : "
        f"{MIN_24H_TRADES:,}"
    )

    lines.append(
        f"Variation 24h minimum : "
        f"{MIN_24H_CHANGE_PERCENT:.2f}%"
    )

    lines.append(
        f"Variation 24h maximum : "
        f"{MAX_24H_CHANGE_PERCENT:.2f}%"
    )

    lines.append(
        f"Spread maximum : "
        f"{MAX_SPREAD_PERCENT:.2f}%"
    )

    lines.append("")

    # --------------------------------------------------------
    # AUDIT DES CRITÈRES
    # --------------------------------------------------------

    lines.append(
        "AUDIT DES CRITÈRES"
    )

    lines.append(
        "-" * 70
    )

    lines.append(
        f"{'CRITÈRE':40} "
        f"{'AVANT':>8} "
        f"{'OK':>8} "
        f"{'REJET':>8} "
        f"{'REJET %':>9}"
    )

    for item in audit:

        lines.append(
            f"{item['criterion'][:40]:40} "
            f"{item['before']:>8,} "
            f"{item['selected']:>8,} "
            f"{item['rejected']:>8,} "
            f"{item['rejection_rate']:>8.2f}%"
        )

    lines.append("")

    lines.append(
        f"UNIVERS INITIAL : "
        f"{len(universe):,}"
    )

    lines.append(
        f"UNIVERS FINAL : "
        f"{len(final_records):,}"
    )

    lines.append("")

    # --------------------------------------------------------
    # ANALYSE TECHNIQUE
    # --------------------------------------------------------

    lines.append(
        "ANALYSE TECHNIQUE"
    )

    lines.append(
        "-" * 70
    )

    lines.append(
        f"Actifs sélectionnés : "
        f"{technical_stats['requested']:,}"
    )

    lines.append(
        f"Actifs analysés : "
        f"{technical_stats['processed']:,}"
    )

    lines.append(
        f"Analyses réussies : "
        f"{technical_stats['successful']:,}"
    )

    lines.append(
        f"Analyses échouées : "
        f"{technical_stats['failed']:,}"
    )

    lines.append(
        "Limitation technique : "
        + (
            "OUI"
            if technical_stats["limited"]
            else "NON"
        )
    )

    lines.append(
        f"Timeframe : "
        f"{TECHNICAL_INTERVAL}"
    )

    lines.append("")

    # --------------------------------------------------------
    # TOP RÉSULTATS
    # --------------------------------------------------------

    lines.append(
        f"TOP {EMAIL_TOP_RESULTS} — ANALYSE TECHNIQUE"
    )

    lines.append(
        "-" * 70
    )

    technical_results = [
        item
        for item in final_records
        if item.get(
            "technicalStatus"
        ) == "OK"
    ]

    technical_results.sort(
        key=lambda x:
            x.get(
                "technical",
                {}
            ).get(
                "technicalScore",
                -1,
            ),
        reverse=True,
    )

    top_results = technical_results[
        :EMAIL_TOP_RESULTS
    ]

    if not top_results:

        lines.append(
            "Aucun actif n'a atteint "
            "l'analyse technique avec succès."
        )

    else:

        for index, item in enumerate(
            top_results,
            start=1,
        ):

            tech = item[
                "technical"
            ]

            lines.append(
                f"{index}. {item['symbol']}"
            )

            lines.append(
                f"   Score technique : "
                f"{tech.get('technicalScore', 'N/A')}/100"
            )

            lines.append(
                f"   Variation 24h : "
                f"{format_number(item['priceChangePercent'])}%"
            )

            lines.append(
                f"   Volume 24h : "
                f"{format_volume(item['quoteVolume'])}"
            )

            lines.append(
                f"   Trades 24h : "
                f"{item['trades']:,}"
            )

            lines.append(
                f"   Prix : "
                f"{format_price(item['lastPrice'])}"
            )

            lines.append(
                f"   EMA {EMA_FAST} : "
                f"{format_price(tech.get('emaFast'))}"
            )

            lines.append(
                f"   EMA {EMA_SLOW} : "
                f"{format_price(tech.get('emaSlow'))}"
            )

            lines.append(
                f"   Tendance EMA : "
                f"{tech.get('emaTrend', 'N/A')}"
            )

            lines.append(
                f"   RSI {RSI_PERIOD} : "
                f"{format_number(tech.get('rsi'))}"
            )

            lines.append(
                f"   État RSI : "
                f"{tech.get('rsiState', 'N/A')}"
            )

            lines.append(
                f"   MACD : "
                f"{format_number(tech.get('macd'))}"
            )

            lines.append(
                f"   Signal MACD : "
                f"{format_number(tech.get('macdSignal'))}"
            )

            lines.append(
                f"   État MACD : "
                f"{tech.get('macdState', 'N/A')}"
            )

            lines.append(
                f"   ATR : "
                f"{format_price(tech.get('atr'))}"
            )

            lines.append(
                f"   ATR % : "
                f"{format_number(tech.get('atrPercent'))}%"
            )

            lines.append(
                f"   Volume / moyenne : "
                f"{format_number(tech.get('volumeRatio'))}x"
            )

            lines.append("")

    # --------------------------------------------------------
    # AVERTISSEMENT
    # --------------------------------------------------------

    lines.append(
        "IMPORTANT"
    )

    lines.append(
        "-" * 70
    )

    lines.append(
        "Le score technique actuel sert uniquement "
        "au classement des actifs. Il ne constitue "
        "pas un signal automatique d'achat ou de vente."
    )

    return "\n".join(lines)


def send_email(
    body: str,
    result_count: int,
) -> None:

    email_user = os.getenv(
        "EMAIL_USER"
    )

    email_pass = os.getenv(
        "EMAIL_PASS"
    )

    email_to = os.getenv(
        "EMAIL_TO"
    )

    if not email_user:
        LOGGER.warning(
            "EMAIL_USER absent."
        )
        return

    if not email_pass:
        LOGGER.warning(
            "EMAIL_PASS absent."
        )
        return

    if not email_to:
        LOGGER.warning(
            "EMAIL_TO absent."
        )
        return

    msg = MIMEMultipart()

    msg["From"] = email_user
    msg["To"] = email_to

    msg["Subject"] = (
        "Binance Screener — "
        f"{result_count} actif(s) final(aux)"
    )

    msg.attach(
        MIMEText(
            body,
            "plain",
            "utf-8",
        )
    )

    try:

        LOGGER.info(
            "Connexion SMTP Gmail..."
        )

        with smtplib.SMTP_SSL(
            "smtp.gmail.com",
            465,
            timeout=30,
        ) as server:

            server.login(
                email_user,
                email_pass,
            )

            server.send_message(
                msg
            )

        LOGGER.info(
            "Rapport email envoyé."
        )

    except Exception as exc:

        LOGGER.exception(
            "Erreur d'envoi email : %s",
            exc,
        )

        raise


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    start_time = time.time()

    LOGGER.info(
        "=" * 70
    )

    LOGGER.info(
        "BINANCE PROGRAMMABLE SCREENER"
    )

    LOGGER.info(
        "=" * 70
    )

    # --------------------------------------------------------
    # 1. UNIVERS BINANCE
    # --------------------------------------------------------

    exchange_info = (
        get_exchange_info()
    )

    universe = build_universe(
        exchange_info
    )

    LOGGER.info(
        "Univers Binance recensé : %d actifs.",
        len(universe),
    )

    if not universe:

        raise RuntimeError(
            "L'univers Binance est vide."
        )

    # --------------------------------------------------------
    # 2. MARKET DATA
    # --------------------------------------------------------

    tickers = (
        get_24h_tickers()
    )

    records = merge_market_data(
        universe,
        tickers,
    )

    # --------------------------------------------------------
    # 3. SCREENING
    # --------------------------------------------------------

    final_records, audit = (
        apply_screening(records)
    )

    LOGGER.info(
        "Univers final après screening : %d",
        len(final_records),
    )

    # --------------------------------------------------------
    # 4. ANALYSE TECHNIQUE
    # --------------------------------------------------------

    final_records, technical_stats = (
        run_technical_analysis(
            final_records
        )
    )

    # --------------------------------------------------------
    # 5. EMAIL
    # --------------------------------------------------------

    email_body = (
        build_email_body(
            universe=universe,
            audit=audit,
            final_records=final_records,
            technical_stats=technical_stats,
        )
    )

    send_email(
        body=email_body,
        result_count=len(final_records),
    )

    # --------------------------------------------------------
    # 6. LOG FINAL
    # --------------------------------------------------------

    elapsed = (
        time.time()
        - start_time
    )

    LOGGER.info(
        "=" * 70
    )

    LOGGER.info(
        "SCREENING TERMINÉ"
    )

    LOGGER.info(
        "Univers initial : %d",
        len(universe),
    )

    LOGGER.info(
        "Univers final : %d",
        len(final_records),
    )

    LOGGER.info(
        "Durée : %.1f secondes",
        elapsed,
    )

    LOGGER.info(
        "=" * 70
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
