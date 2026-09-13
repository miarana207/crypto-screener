from __future__ import annotations

import concurrent.futures
import json
import math
import os
import smtplib
import ssl
import time
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# ============================================================
# BINANCE MULTI-MARKET SCREENER V2
# Spot / Margin / USDⓈ-M / COIN-M / Options / Tokenized Stocks
# P2P intentionally not integrated: no suitable official public
# market-data API is used by this scanner.
# NO ORDERS ARE EVER EXECUTED.
# ============================================================

HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "465"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")
EMAIL_TOP_RESULTS = int(os.getenv("EMAIL_TOP_RESULTS", "50"))
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")

ENABLE_SPOT = os.getenv("ENABLE_SPOT", "true").lower() == "true"
ENABLE_MARGIN = os.getenv("ENABLE_MARGIN", "true").lower() == "true"
ENABLE_USDM = os.getenv("ENABLE_USDM_FUTURES", "true").lower() == "true"
ENABLE_COINM = os.getenv("ENABLE_COINM_FUTURES", "true").lower() == "true"
ENABLE_OPTIONS = os.getenv("ENABLE_OPTIONS", "true").lower() == "true"
ENABLE_TOKENIZED_STOCKS = os.getenv("ENABLE_TOKENIZED_STOCKS", "true").lower() == "true"

SPOT_BASE_URL = os.getenv("BINANCE_SPOT_BASE_URL", os.getenv("BINANCE_BASE_URL", "https://data-api.binance.vision"))
USDM_BASE_URL = os.getenv("BINANCE_USDM_BASE_URL", "https://fapi.binance.com")
COINM_BASE_URL = os.getenv("BINANCE_COINM_BASE_URL", "https://dapi.binance.com")
OPTIONS_BASE_URL = os.getenv("BINANCE_OPTIONS_BASE_URL", "https://eapi.binance.com")
TOKENIZED_BASE_URL = os.getenv("BINANCE_TOKENIZED_BASE_URL", "https://api.binance.com")

# Si vide, le screener construit automatiquement la liste des stablecoins
# actuellement utilisés comme quoteAsset Spot par Binance. Une valeur explicite
# dans QUOTE_ASSETS reste possible pour forcer un univers précis.
QUOTE_ASSETS = {x.strip().upper() for x in os.getenv("QUOTE_ASSETS", "").split(",") if x.strip()}
EXCLUDE_STABLECOINS = os.getenv("EXCLUDE_STABLECOINS", "true").lower() == "true"
EXCLUDE_LEVERAGED_TOKENS = os.getenv("EXCLUDE_LEVERAGED_TOKENS", "true").lower() == "true"
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "1000000"))
MAX_SPREAD_PERCENT = float(os.getenv("MAX_SPREAD_PERCENT", "0.10"))

# Régime de marché Spot : CHOP + ADX + ATR, calculés au même niveau
# après le spread. Ces indicateurs sont descriptifs et ne rejettent aucun actif.
REGIME_ENABLED = os.getenv("REGIME_ENABLED", "true").lower() == "true"
REGIME_INTERVAL = os.getenv("REGIME_INTERVAL", "1h")
CHOP_PERIOD = int(os.getenv("CHOP_PERIOD", "14"))
ADX_PERIOD = int(os.getenv("ADX_PERIOD", "14"))
ATR_REGIME_PERIOD = int(os.getenv("ATR_REGIME_PERIOD", "14"))
CHOP_RANGE_THRESHOLD = float(os.getenv("CHOP_RANGE_THRESHOLD", "55"))
CHOP_TREND_THRESHOLD = float(os.getenv("CHOP_TREND_THRESHOLD", "40"))
ADX_TREND_THRESHOLD = float(os.getenv("ADX_TREND_THRESHOLD", "25"))
ADX_STRONG_THRESHOLD = float(os.getenv("ADX_STRONG_THRESHOLD", "40"))
ATR_LOW_PERCENT = float(os.getenv("ATR_LOW_PERCENT", "0.50"))
ATR_HIGH_PERCENT = float(os.getenv("ATR_HIGH_PERCENT", "1.50"))
ATR_EXTREME_PERCENT = float(os.getenv("ATR_EXTREME_PERCENT", "2.50"))

# Order Book Depth Spot : garde-fou de liquidité réelle autour du prix médian.
# Le seuil est exprimé en valeur notionnelle quote (ex. USD/USDT).
# On mesure la profondeur cumulée des bids + asks dans une bande de ±0,25 %.
ORDER_BOOK_DEPTH_ENABLED = os.getenv("ORDER_BOOK_DEPTH_ENABLED", "true").lower() == "true"
ORDER_BOOK_DEPTH_LIMIT = int(os.getenv("ORDER_BOOK_DEPTH_LIMIT", "100"))
ORDER_BOOK_DEPTH_PCT = float(os.getenv("ORDER_BOOK_DEPTH_PCT", "0.25"))
MIN_ORDER_BOOK_DEPTH_QUOTE = float(os.getenv("MIN_ORDER_BOOK_DEPTH_QUOTE", "25000"))
ORDER_BOOK_DEPTH_WORKERS = int(os.getenv("ORDER_BOOK_DEPTH_WORKERS", "8"))

FUTURES_QUOTE_ASSETS = {x.strip().upper() for x in os.getenv("FUTURES_QUOTE_ASSETS", "USDT,USDC").split(",") if x.strip()}
MIN_FUTURES_QUOTE_VOLUME = float(os.getenv("MIN_FUTURES_QUOTE_VOLUME", "5000000"))
MIN_FUTURES_TRADES = int(os.getenv("MIN_FUTURES_TRADES", "1000"))
MAX_FUTURES_SPREAD_PERCENT = float(os.getenv("MAX_FUTURES_SPREAD_PERCENT", "0.20"))
MIN_FUNDING_RATE_PERCENT = float(os.getenv("MIN_FUNDING_RATE_PERCENT", "-1.0"))
MAX_FUNDING_RATE_PERCENT = float(os.getenv("MAX_FUNDING_RATE_PERCENT", "1.0"))
FUTURES_CONTRACT_TYPES = {x.strip().upper() for x in os.getenv("FUTURES_CONTRACT_TYPES", "PERPETUAL").split(",") if x.strip()}

OPTIONS_QUOTE_ASSETS = {x.strip().upper() for x in os.getenv("OPTIONS_QUOTE_ASSETS", "USDT").split(",") if x.strip()}
MIN_OPTIONS_QUOTE_VOLUME = float(os.getenv("MIN_OPTIONS_QUOTE_VOLUME", "0"))
MIN_OPTIONS_TRADES = int(os.getenv("MIN_OPTIONS_TRADES", "0"))
MAX_OPTIONS_SPREAD_PERCENT = float(os.getenv("MAX_OPTIONS_SPREAD_PERCENT", "5.0"))
OPTIONS_MAX_ASSETS = int(os.getenv("OPTIONS_MAX_ASSETS", "500"))

TOKENIZED_MAX_ASSETS = int(os.getenv("TOKENIZED_MAX_ASSETS", "100"))
MAX_TOKENIZED_SPREAD_PERCENT = float(os.getenv("MAX_TOKENIZED_SPREAD_PERCENT", "1.0"))

REGIME_WORKERS = int(os.getenv("REGIME_WORKERS", "8"))
REGIME_KLINES_LIMIT = int(os.getenv("REGIME_KLINES_LIMIT", "250"))

# Binance ne publie pas de champ public universel "isStablecoin" dans
# Spot /exchangeInfo. Cette base sert donc de reconnaissance de référence.
# Les actifs supprimés de Binance disparaissent automatiquement du résultat
# puisqu'ils ne sont plus présents dans exchangeInfo.
STABLECOIN_BASES = {
    "USDT", "USDC", "FDUSD", "BUSD", "DAI", "TUSD", "USDP", "USDE",
    "USDD", "FRAX", "PYUSD", "EURC", "USD1", "RLUSD", "XUSD", "EURI",
    "USDG", "USDS", "U", "AEUR", "PAXG", "USD0",
}
STABLECOIN_BASES.update({
    x.strip().upper()
    for x in os.getenv("BINANCE_STABLECOIN_QUOTES", "").split(",")
    if x.strip()
})

# Détection complémentaire prudente pour certains nouveaux tickers
# explicitement liés à USD/EUR, uniquement lorsqu'ils sont quoteAsset actifs.
STABLECOIN_QUOTE_MARKERS = (
    "USDT", "USDC", "FDUSD", "TUSD", "USDP", "USDE", "USDD",
    "PYUSD", "USD1", "RLUSD", "USDG", "USDS", "USD0", "XUSD",
    "EURC", "EURI", "AEUR",
)
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
LEVERAGED_PATTERNS = ("3L", "3S", "5L", "5S", "2L", "2S")


class BinanceHTTPError(RuntimeError):
    pass


def is_geo_restriction_error(exc: Exception) -> bool:
    """Return True when Binance explicitly blocks the request by geography/eligibility."""
    text = str(exc).lower()
    return "http 451" in text or "451" in text and "unavailable" in text


def format_market_exception(market: str, exc: Exception) -> Tuple[str, str]:
    """Classify market failures so expected Binance restrictions are not reported as code errors."""
    if is_geo_restriction_error(exc):
        return (
            f"{market} indisponible : Binance retourne HTTP 451 (restriction géographique / conditions d'éligibilité depuis le runner GitHub).",
            "unavailable",
        )
    return (f"{market}: {exc}", "error")


def http_get_json(base_url: str, path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
    url = base_url.rstrip("/") + path
    if params:
        query = urlencode({k: v for k, v in params.items() if v is not None})
        if query:
            url += "?" + query
    last_error: Optional[Exception] = None
    for attempt in range(HTTP_RETRIES + 1):
        try:
            request = Request(url, headers={"User-Agent": "BinanceMultiMarketScreener/2.0", **(headers or {})}, method="GET")
            with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            last_error = exc
            retryable = exc.code in {418, 429, 500, 502, 503, 504}
            if not retryable or attempt >= HTTP_RETRIES:
                try:
                    body = exc.read().decode("utf-8")
                except Exception:
                    body = ""
                raise BinanceHTTPError(f"HTTP {exc.code} {path}: {body[:500]}") from exc
            retry_after = exc.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else min(15.0, 2.0 ** attempt)
            except ValueError:
                delay = min(15.0, 2.0 ** attempt)
            time.sleep(delay)
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt >= HTTP_RETRIES:
                raise BinanceHTTPError(f"Request failed {path}: {exc}") from exc
            time.sleep(min(10.0, 2.0 ** attempt))
    raise BinanceHTTPError(f"Request failed {path}: {last_error}")


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_percent_spread(bid: float, ask: float) -> Optional[float]:
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / 2.0
    return ((ask - bid) / mid) * 100.0 if mid > 0 else None


def is_leveraged_symbol(base_asset: str) -> bool:
    base = base_asset.upper()
    return any(base.endswith(x) for x in LEVERAGED_SUFFIXES + LEVERAGED_PATTERNS)


class CriterionAudit:
    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []
        self.diagnostics: Dict[str, Any] = {}

    def add(self, name: str, before: int, selected: int) -> None:
        rejected = before - selected
        retention = selected / before * 100 if before else 0.0
        rejection = rejected / before * 100 if before else 0.0
        self.rows.append({"criterion": name, "before": before, "selected": selected, "rejected": rejected, "retention": retention, "rejection": rejection})


def apply_criterion(assets: List[Dict[str, Any]], audit: CriterionAudit, name: str, predicate: Callable[[Dict[str, Any]], bool]) -> List[Dict[str, Any]]:
    before = len(assets)
    selected = [asset for asset in assets if predicate(asset)]
    audit.add(name, before, len(selected))
    return selected


# ----------------------------- SPOT -----------------------------
def spot_exchange_info() -> Dict[str, Any]:
    return http_get_json(SPOT_BASE_URL, "/api/v3/exchangeInfo")


def spot_tickers() -> List[Dict[str, Any]]:
    return http_get_json(SPOT_BASE_URL, "/api/v3/ticker/24hr")


def spot_book_tickers() -> List[Dict[str, Any]]:
    return http_get_json(SPOT_BASE_URL, "/api/v3/ticker/bookTicker")


def discover_active_stablecoin_quotes(exchange_info: Dict[str, Any]) -> set[str]:
    """Découvre les stablecoins actuellement actifs comme quoteAsset Spot."""
    active_quotes = {
        str(x.get("quoteAsset", "")).upper().strip()
        for x in exchange_info.get("symbols", [])
        if isinstance(x, dict) and str(x.get("status", "")).upper() == "TRADING"
    }
    discovered = {q for q in active_quotes if q in STABLECOIN_BASES}
    for quote in active_quotes - discovered:
        if any(marker in quote for marker in STABLECOIN_QUOTE_MARKERS):
            discovered.add(quote)
    return discovered


def resolve_spot_quote_assets(exchange_info: Dict[str, Any]) -> set[str]:
    """Résout les quote assets effectifs du run courant."""
    active_quotes = {
        str(x.get("quoteAsset", "")).upper().strip()
        for x in exchange_info.get("symbols", [])
        if isinstance(x, dict)
    }
    if QUOTE_ASSETS:
        return QUOTE_ASSETS & active_quotes
    return discover_active_stablecoin_quotes(exchange_info)


def build_spot_universe(exchange_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [{"market": "SPOT", "symbol": x.get("symbol", ""), "baseAsset": x.get("baseAsset", ""), "quoteAsset": x.get("quoteAsset", ""), "status": x.get("status", ""), "permissions": x.get("permissions", [])} for x in exchange_info.get("symbols", [])]


def merge_spot_tickers(assets: List[Dict[str, Any]], tickers: List[Dict[str, Any]]) -> None:
    by_symbol = {x.get("symbol"): x for x in tickers if x.get("symbol")}
    for a in assets:
        t = by_symbol.get(a["symbol"], {})
        a.update(lastPrice=as_float(t.get("lastPrice")), priceChangePercent=as_float(t.get("priceChangePercent")), quoteVolume=as_float(t.get("quoteVolume")), trades=as_int(t.get("count")), volume=as_float(t.get("volume")), weightedAvgPrice=as_float(t.get("weightedAvgPrice")), highPrice=as_float(t.get("highPrice")), lowPrice=as_float(t.get("lowPrice")))


def merge_spot_books(assets: List[Dict[str, Any]], books: List[Dict[str, Any]]) -> None:
    by_symbol = {x.get("symbol"): x for x in books if x.get("symbol")}
    for a in assets:
        b = by_symbol.get(a["symbol"], {})
        bid, ask = as_float(b.get("bidPrice")), as_float(b.get("askPrice"))
        a.update(bidPrice=bid, askPrice=ask, spreadPercent=safe_percent_spread(bid, ask))


def spot_order_book(symbol: str) -> Dict[str, Any]:
    return http_get_json(
        SPOT_BASE_URL,
        "/api/v3/depth",
        {"symbol": symbol, "limit": ORDER_BOOK_DEPTH_LIMIT},
    )


def order_book_depth_metrics(book: Dict[str, Any], mid_price: float) -> Dict[str, float]:
    """Calcule la profondeur notionnelle cumulée autour du prix médian."""
    if mid_price <= 0:
        return {
            "depthBidQuote": 0.0,
            "depthAskQuote": 0.0,
            "depthTotalQuote": 0.0,
        }

    band = ORDER_BOOK_DEPTH_PCT / 100.0
    min_bid = mid_price * (1.0 - band)
    max_ask = mid_price * (1.0 + band)

    bid_depth = 0.0
    ask_depth = 0.0

    for level in book.get("bids", []) or []:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        price = as_float(level[0])
        quantity = as_float(level[1])
        if price >= min_bid and price > 0 and quantity > 0:
            bid_depth += price * quantity

    for level in book.get("asks", []) or []:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        price = as_float(level[0])
        quantity = as_float(level[1])
        if price <= max_ask and price > 0 and quantity > 0:
            ask_depth += price * quantity

    return {
        "depthBidQuote": bid_depth,
        "depthAskQuote": ask_depth,
        "depthTotalQuote": bid_depth + ask_depth,
    }


def merge_spot_order_book_depth(assets: List[Dict[str, Any]], warnings: Optional[List[str]] = None) -> None:
    """Récupère et calcule la profondeur Spot pour les actifs déjà validés jusqu'aux données 24h."""
    if not ORDER_BOOK_DEPTH_ENABLED or not assets:
        return

    workers = max(1, min(ORDER_BOOK_DEPTH_WORKERS, len(assets)))
    failures = 0

    def fetch(asset: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        return asset["symbol"], spot_order_book(asset["symbol"])

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(fetch, asset): asset for asset in assets}
        for future in concurrent.futures.as_completed(future_map):
            asset = future_map[future]
            try:
                symbol, book = future.result()
                bid = as_float(asset.get("bidPrice"))
                ask = as_float(asset.get("askPrice"))
                mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
                metrics = order_book_depth_metrics(book, mid)
                asset.update(metrics)
                asset["orderBookDepthPct"] = ORDER_BOOK_DEPTH_PCT
            except Exception as exc:
                failures += 1
                asset.update(
                    depthBidQuote=0.0,
                    depthAskQuote=0.0,
                    depthTotalQuote=0.0,
                    orderBookDepthPct=ORDER_BOOK_DEPTH_PCT,
                    depthError=str(exc),
                )

    if failures and warnings is not None:
        warnings.append(f"Order Book Depth Spot : {failures} échecs de récupération sur {len(assets)} paires.")


def percentile(values: List[float], fraction: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def classify_market_direction(plus_di: Optional[float], minus_di: Optional[float], adx_value: Optional[float]) -> str:
    """Donne le sens directionnel uniquement lorsque l'ADX confirme une structure.

    ADX mesure la force, pas le sens. DI+ / DI- permettent ici de distinguer
    une tendance haussière d'une tendance baissière sans modifier le filtre.
    """
    if plus_di is None or minus_di is None or adx_value is None:
        return "NON DISPONIBLE"
    if adx_value < ADX_TREND_THRESHOLD:
        return "NEUTRE"
    if plus_di > minus_di:
        return "HAUSSIÈRE"
    if minus_di > plus_di:
        return "BAISSIÈRE"
    return "NEUTRE"


def merge_spot_regime(assets: List[Dict[str, Any]], warnings: Optional[List[str]] = None) -> None:
    """Calcule CHOP, ADX et ATR% sur les survivants du spread.

    Les trois indicateurs sont au même niveau conceptuel : aucun ne sert de
    filtre d'exclusion. Le résultat est ensuite interprété en régime de marché.
    """
    if not REGIME_ENABLED or not assets:
        return

    workers = max(1, min(REGIME_WORKERS, len(assets)))
    failures = 0
    min_history = max(60, CHOP_PERIOD + 2, ADX_PERIOD * 2 + 2, ATR_REGIME_PERIOD + 2)

    def fetch(asset: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        klines = spot_klines(asset["symbol"], REGIME_INTERVAL, REGIME_KLINES_LIMIT)
        if len(klines) < min_history:
            return asset["symbol"], {"error": "Historique insuffisant"}
        highs = [as_float(row[2]) for row in klines]
        lows = [as_float(row[3]) for row in klines]
        closes = [as_float(row[4]) for row in klines]
        chop_value = choppiness_index(highs, lows, closes, CHOP_PERIOD)
        adx_value, plus_di, minus_di = adx(highs, lows, closes, ADX_PERIOD)
        atr_value = atr(highs, lows, closes, ATR_REGIME_PERIOD)
        current = closes[-1] if closes else 0.0
        atr_percent = atr_value / current * 100.0 if atr_value is not None and current > 0 else None
        regime = classify_market_regime(chop_value, adx_value, atr_percent)
        market_nature, market_structure = interpret_market_structure(chop_value, adx_value)
        market_direction = classify_market_direction(plus_di, minus_di, adx_value)
        return asset["symbol"], {
            "chop": chop_value,
            "adx": adx_value,
            "plusDI": plus_di,
            "minusDI": minus_di,
            "regimeAtrPercent": atr_percent,
            "marketNature": market_nature,
            "marketStructure": market_structure,
            "marketDirection": market_direction,
            "marketRegime": regime,
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(fetch, asset): asset for asset in assets}
        for future in concurrent.futures.as_completed(future_map):
            asset = future_map[future]
            try:
                _, values = future.result()
                asset["regimeInterval"] = REGIME_INTERVAL
                asset["chopPeriod"] = CHOP_PERIOD
                asset["adxPeriod"] = ADX_PERIOD
                asset["atrRegimePeriod"] = ATR_REGIME_PERIOD
                asset.update(values)
                if values.get("error"):
                    asset["regimeError"] = values["error"]
                    failures += 1
            except Exception as exc:
                failures += 1
                asset["regimeError"] = str(exc)
                asset.update(chop=None, adx=None, plusDI=None, minusDI=None, regimeAtrPercent=None, marketNature="NON DISPONIBLE", marketStructure="NON DISPONIBLE", marketDirection="NON DISPONIBLE", marketRegime="NON DISPONIBLE")

    if failures and warnings is not None:
        warnings.append(f"Régime CHOP/ADX/ATR Spot : {failures} échecs de calcul sur {len(assets)} paires.")


def build_regime_diagnostics(assets: List[Dict[str, Any]]) -> Dict[str, Any]:
    valid_assets = [
        a for a in assets
        if a.get("chop") is not None and a.get("adx") is not None and a.get("regimeAtrPercent") is not None
    ]
    chop_values = [as_float(a.get("chop")) for a in valid_assets]
    adx_values = [as_float(a.get("adx")) for a in valid_assets]
    atr_values = [as_float(a.get("regimeAtrPercent")) for a in valid_assets]

    regime_counts: Dict[str, int] = {}
    nature_counts: Dict[str, int] = {}
    direction_counts: Dict[str, int] = {}
    atr_intensity_counts: Dict[str, int] = {}

    chop_zones = ("TENDANCE", "TRANSITION", "RANGE")
    adx_zones = ("FAIBLE", "TENDANCE", "FORTE")
    structure_matrix = {chop: {adx: 0 for adx in adx_zones} for chop in chop_zones}

    conflicts = 0
    for asset in assets:
        regime = asset.get("marketRegime") or "NON DISPONIBLE"
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
        nature = asset.get("marketNature") or "NON DISPONIBLE"
        nature_counts[nature] = nature_counts.get(nature, 0) + 1
        direction = asset.get("marketDirection") or "NON DISPONIBLE"
        direction_counts[direction] = direction_counts.get(direction, 0) + 1

        atr_value = asset.get("regimeAtrPercent")
        if atr_value is not None:
            atr_value = as_float(atr_value)
            if atr_value < ATR_LOW_PERCENT:
                intensity = "CALME"
            elif atr_value < ATR_HIGH_PERCENT:
                intensity = "ACTIVE"
            elif atr_value < ATR_EXTREME_PERCENT:
                intensity = "VOLATILE"
            else:
                intensity = "TRÈS VOLATILE"
            atr_intensity_counts[intensity] = atr_intensity_counts.get(intensity, 0) + 1

        chop = asset.get("chop")
        adx_value = asset.get("adx")
        if chop is None or adx_value is None:
            continue
        chop = as_float(chop)
        adx_value = as_float(adx_value)
        if chop <= CHOP_TREND_THRESHOLD:
            chop_zone = "TENDANCE"
        elif chop >= CHOP_RANGE_THRESHOLD:
            chop_zone = "RANGE"
        else:
            chop_zone = "TRANSITION"
        if adx_value < ADX_TREND_THRESHOLD:
            adx_zone = "FAIBLE"
        elif adx_value < ADX_STRONG_THRESHOLD:
            adx_zone = "TENDANCE"
        else:
            adx_zone = "FORTE"
        structure_matrix[chop_zone][adx_zone] += 1
        if (chop >= CHOP_RANGE_THRESHOLD and adx_value >= ADX_TREND_THRESHOLD) or (chop <= CHOP_TREND_THRESHOLD and adx_value < ADX_TREND_THRESHOLD):
            conflicts += 1

    def stats(values: List[float]) -> Dict[str, Optional[float]]:
        return {
            "minimum": min(values) if values else None,
            "p25": percentile(values, 0.25),
            "median": percentile(values, 0.50),
            "mean": sum(values) / len(values) if values else None,
            "p75": percentile(values, 0.75),
            "maximum": max(values) if values else None,
        }

    return {
        "universe_count": len(assets),
        "valid_count": len(valid_assets),
        "missing_count": len(assets) - len(valid_assets),
        "regime_counts": dict(sorted(regime_counts.items(), key=lambda x: (-x[1], x[0]))),
        "nature_counts": dict(sorted(nature_counts.items(), key=lambda x: (-x[1], x[0]))),
        "direction_counts": dict(sorted(direction_counts.items(), key=lambda x: (-x[1], x[0]))),
        "atr_intensity_counts": dict(sorted(atr_intensity_counts.items(), key=lambda x: (-x[1], x[0]))),
        "structure_matrix": structure_matrix,
        "conflict_count": conflicts,
        "chop": stats(chop_values),
        "adx": stats(adx_values),
        "atr": stats(atr_values),
    }

def screen_spot(assets: List[Dict[str, Any]], quote_assets: Optional[set[str]] = None, warnings: Optional[List[str]] = None) -> Tuple[List[Dict[str, Any]], CriterionAudit]:
    audit = CriterionAudit()
    effective_quotes = quote_assets if quote_assets is not None else QUOTE_ASSETS
    assets = apply_criterion(assets, audit, "1. Status TRADING", lambda x: x["status"] == "TRADING")
    assets = apply_criterion(assets, audit, "2. Permission SPOT", lambda x: not x["permissions"] or "SPOT" in x["permissions"])
    assets = apply_criterion(assets, audit, "3. Quote asset autorisé", lambda x: x["quoteAsset"] in effective_quotes)
    if EXCLUDE_STABLECOINS:
        assets = apply_criterion(assets, audit, "4. Exclusion stablecoins", lambda x: x["baseAsset"].upper() not in STABLECOIN_BASES)
    if EXCLUDE_LEVERAGED_TOKENS:
        assets = apply_criterion(assets, audit, "5. Exclusion tokens à levier", lambda x: not is_leveraged_symbol(x["baseAsset"]))
    assets = apply_criterion(assets, audit, "6. Données 24h disponibles", lambda x: x["lastPrice"] > 0)

    # L'ordre demandé est volontairement : VOLUME 24h en premier, puis Order Book Depth.
    # Les deux critères occupent donc les positions 7 et 8, entre les données 24h et le prix minimum.
    # 7. Volume quote 24h minimum
    # Diagnostic indépendant du seuil actif : il permet d'évaluer le seuil de volume
    # après plusieurs runs sans modifier prématurément le filtre.
    volume_universe = [a for a in assets if a.get("quoteVolume", 0) > 0]
    audit.diagnostics["volume_distribution"] = {
        threshold: sum(1 for a in volume_universe if a.get("quoteVolume", 0) >= threshold)
        for threshold in (1_000_000, 2_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000, 100_000_000, 500_000_000, 1_000_000_000)
    }
    audit.diagnostics["volume_universe_count"] = len(volume_universe)
    assets = apply_criterion(assets, audit, "7. Volume quote 24h minimum", lambda x: x["quoteVolume"] >= MIN_24H_QUOTE_VOLUME)

    # 8. Order Book Depth : calculé uniquement après le filtre de volume,
    # afin d'éviter des appels /api/v3/depth inutiles sur les marchés à faible volume.
    if ORDER_BOOK_DEPTH_ENABLED:
        merge_spot_order_book_depth(assets, warnings)
        assets = apply_criterion(
            assets,
            audit,
            "8. Épaisseur carnet d'ordres minimum",
            lambda x: x.get("depthTotalQuote", 0.0) >= MIN_ORDER_BOOK_DEPTH_QUOTE,
        )
    else:
        audit.add("8. Épaisseur carnet d'ordres minimum", len(assets), len(assets))

    # 9. Spread maximum
    assets = apply_criterion(assets, audit, "9. Spread maximum", lambda x: x["spreadPercent"] is not None and x["spreadPercent"] <= MAX_SPREAD_PERCENT)

    # 10. Régime de marché : CHOP + ADX + ATR au même niveau.
    # Calculé sur tous les survivants du spread. Aucun des trois indicateurs
    # n'est un filtre d'exclusion à cette étape : ils servent à caractériser
    # la nature, la force directionnelle et l'amplitude de l'actif.
    if REGIME_ENABLED:
        merge_spot_regime(assets, warnings)
        audit.diagnostics["regime"] = build_regime_diagnostics(assets)
    else:
        audit.diagnostics["regime"] = build_regime_diagnostics(assets)
    audit.add("10. Interprétation CHOP + ADX + ATR", len(assets), len(assets))
    return assets, audit


# ----------------------------- MARGIN -----------------------------
def build_margin_market(spot_assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result = []
    for asset in spot_assets:
        copied = dict(asset)
        copied["market"] = "MARGIN"
        copied["marginEligibility"] = "ACCOUNT_DEPENDENT"
        result.append(copied)
    return result


# ----------------------------- FUTURES -----------------------------
def futures_exchange_info(base_url: str, path: str) -> Dict[str, Any]:
    return http_get_json(base_url, path)


def futures_tickers(base_url: str, path: str) -> List[Dict[str, Any]]:
    return http_get_json(base_url, path)


def futures_book_tickers(base_url: str, path: str) -> List[Dict[str, Any]]:
    return http_get_json(base_url, path)


def futures_premium_index(base_url: str, path: str) -> Any:
    return http_get_json(base_url, path)


def build_usdm_universe(exchange_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []
    for x in exchange_info.get("symbols", []):
        contract = str(x.get("contractType", "")).upper()
        quote = str(x.get("quoteAsset", "")).upper()
        if contract not in FUTURES_CONTRACT_TYPES or quote not in FUTURES_QUOTE_ASSETS:
            continue
        result.append({"market": "USDⓈ-M FUTURES", "symbol": x.get("symbol", ""), "pair": x.get("pair", ""), "baseAsset": x.get("baseAsset", ""), "quoteAsset": quote, "contractType": contract, "status": x.get("status", x.get("contractStatus", ""))})
    return result


def build_coinm_universe(exchange_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []
    for x in exchange_info.get("symbols", []):
        contract = str(x.get("contractType", "")).upper()
        if contract not in FUTURES_CONTRACT_TYPES:
            continue
        result.append({"market": "COIN-M FUTURES", "symbol": x.get("symbol", ""), "pair": x.get("pair", ""), "baseAsset": x.get("baseAsset", ""), "quoteAsset": x.get("quoteAsset", ""), "marginAsset": x.get("marginAsset", ""), "contractType": contract, "status": x.get("contractStatus", x.get("status", ""))})
    return result


def merge_futures_tickers(assets: List[Dict[str, Any]], tickers: List[Dict[str, Any]]) -> None:
    by_symbol = {x.get("symbol"): x for x in tickers if x.get("symbol")}
    for a in assets:
        t = by_symbol.get(a["symbol"], {})
        quote_volume = as_float(t.get("quoteVolume"))
        if quote_volume <= 0:
            quote_volume = as_float(t.get("baseVolume"))
        a.update(lastPrice=as_float(t.get("lastPrice")), priceChangePercent=as_float(t.get("priceChangePercent")), volume=as_float(t.get("volume")), quoteVolume=quote_volume, trades=as_int(t.get("count")), highPrice=as_float(t.get("highPrice")), lowPrice=as_float(t.get("lowPrice")))


def merge_futures_books(assets: List[Dict[str, Any]], books: List[Dict[str, Any]]) -> None:
    by_symbol = {x.get("symbol"): x for x in books if x.get("symbol")}
    for a in assets:
        b = by_symbol.get(a["symbol"], {})
        bid, ask = as_float(b.get("bidPrice")), as_float(b.get("askPrice"))
        a.update(bidPrice=bid, askPrice=ask, spreadPercent=safe_percent_spread(bid, ask))


def merge_funding(assets: List[Dict[str, Any]], funding_data: Any) -> None:
    if isinstance(funding_data, dict):
        funding_data = [funding_data]
    by_symbol = {x.get("symbol"): x for x in (funding_data or []) if x.get("symbol")}
    for a in assets:
        item = by_symbol.get(a["symbol"], {})
        if "lastFundingRate" in item:
            a["fundingRate"] = as_float(item.get("lastFundingRate")) * 100.0


def screen_futures(assets: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], CriterionAudit]:
    audit = CriterionAudit()
    assets = apply_criterion(assets, audit, "1. Contrat TRADING", lambda x: x["status"] == "TRADING")
    assets = apply_criterion(assets, audit, "2. Données 24h disponibles", lambda x: x["lastPrice"] > 0)
    assets = apply_criterion(assets, audit, "3. Volume quote minimum", lambda x: x["quoteVolume"] >= MIN_FUTURES_QUOTE_VOLUME)
    assets = apply_criterion(assets, audit, "4. Trades minimum", lambda x: x["trades"] >= MIN_FUTURES_TRADES)
    assets = apply_criterion(assets, audit, "5. Spread maximum", lambda x: x["spreadPercent"] is not None and x["spreadPercent"] <= MAX_FUTURES_SPREAD_PERCENT)
    assets = apply_criterion(assets, audit, "6. Funding disponible", lambda x: "fundingRate" in x)
    assets = apply_criterion(assets, audit, "7. Funding dans la plage autorisée", lambda x: MIN_FUNDING_RATE_PERCENT <= x["fundingRate"] <= MAX_FUNDING_RATE_PERCENT)
    return assets, audit


# ----------------------------- OPTIONS -----------------------------
def options_exchange_info() -> Dict[str, Any]:
    return http_get_json(OPTIONS_BASE_URL, "/eapi/v1/exchangeInfo")


def options_tickers() -> List[Dict[str, Any]]:
    return http_get_json(OPTIONS_BASE_URL, "/eapi/v1/ticker")


def build_options_universe(exchange_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []
    for x in exchange_info.get("optionSymbols", []):
        quote = str(x.get("quoteAsset", "")).upper()
        if quote not in OPTIONS_QUOTE_ASSETS:
            continue
        result.append({"market": "OPTIONS", "symbol": x.get("symbol", ""), "underlying": x.get("underlying", ""), "quoteAsset": quote, "side": x.get("side", ""), "strikePrice": as_float(x.get("strikePrice")), "expiryDate": x.get("expiryDate"), "status": x.get("status", "")})
    return result


def merge_option_tickers(assets: List[Dict[str, Any]], tickers: List[Dict[str, Any]]) -> None:
    by_symbol = {x.get("symbol"): x for x in tickers if x.get("symbol")}
    for a in assets:
        t = by_symbol.get(a["symbol"], {})
        bid, ask = as_float(t.get("bidPrice")), as_float(t.get("askPrice"))
        a.update(lastPrice=as_float(t.get("lastPrice")), priceChangePercent=as_float(t.get("priceChangePercent")), volume=as_float(t.get("volume")), quoteVolume=as_float(t.get("amount")), trades=as_int(t.get("tradeCount")), bidPrice=bid, askPrice=ask, spreadPercent=safe_percent_spread(bid, ask))


def screen_options(assets: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], CriterionAudit]:
    audit = CriterionAudit()
    assets = apply_criterion(assets, audit, "1. Option TRADING", lambda x: x["status"] == "TRADING")
    assets = apply_criterion(assets, audit, "2. Prix disponible", lambda x: x["lastPrice"] > 0)
    assets = apply_criterion(assets, audit, "3. Volume disponible", lambda x: x["quoteVolume"] >= MIN_OPTIONS_QUOTE_VOLUME)
    assets = apply_criterion(assets, audit, "4. Trades minimum", lambda x: x["trades"] >= MIN_OPTIONS_TRADES)
    assets = apply_criterion(assets, audit, "5. Spread maximum", lambda x: x["spreadPercent"] is not None and x["spreadPercent"] <= MAX_OPTIONS_SPREAD_PERCENT)
    assets.sort(key=lambda x: x["quoteVolume"], reverse=True)
    if OPTIONS_MAX_ASSETS > 0:
        assets = assets[:OPTIONS_MAX_ASSETS]
    return assets, audit


# ----------------------------- TOKENIZED STOCKS -----------------------------
def tokenized_headers() -> Dict[str, str]:
    if not BINANCE_API_KEY:
        raise BinanceHTTPError("BINANCE_API_KEY absent")
    return {"X-MBX-APIKEY": BINANCE_API_KEY}


def tokenized_exchange_info() -> Dict[str, Any]:
    return http_get_json(TOKENIZED_BASE_URL, "/sapi/v1/equity/market/exchangeInfo", headers=tokenized_headers())


def tokenized_assets_info() -> Any:
    return http_get_json(TOKENIZED_BASE_URL, "/sapi/v1/equity/market/tokenized-assets", headers=tokenized_headers())


def tokenized_quote(symbol: str) -> Dict[str, Any]:
    return http_get_json(TOKENIZED_BASE_URL, "/sapi/v1/equity/market/quote", params={"symbol": symbol}, headers=tokenized_headers())


def build_tokenized_universe(exchange_info: Dict[str, Any], tokenized_assets: Any) -> List[Dict[str, Any]]:
    token_map = {}
    if isinstance(tokenized_assets, list):
        for item in tokenized_assets:
            code = item.get("assetCode")
            if code:
                token_map[code] = item
    result = []
    for item in exchange_info.get("symbols", []):
        symbol = item.get("symbol", "")
        tradability = item.get("tradability", "NONE")
        if tradability == "NONE":
            continue
        token = token_map.get(symbol, {})
        result.append({"market": "TOKENIZED STOCKS", "symbol": symbol, "tradability": tradability, "overnightSupported": item.get("overnightSupported", False), "fractionable": item.get("fractionable", False), "underlyingEquitySymbol": token.get("underlyingEquitySymbol", ""), "assetName": token.get("assetName", "")})
    return result


def merge_tokenized_quotes(assets: List[Dict[str, Any]]) -> None:
    def fetch(asset: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        return asset["symbol"], tokenized_quote(asset["symbol"])
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, max(1, REGIME_WORKERS))) as executor:
        futures = [executor.submit(fetch, a) for a in assets]
        for future in concurrent.futures.as_completed(futures):
            try:
                symbol, quote = future.result()
                for asset in assets:
                    if asset["symbol"] == symbol:
                        bid, ask = as_float(quote.get("bidPrice")), as_float(quote.get("askPrice"))
                        asset.update(bidPrice=bid, askPrice=ask, bidSize=as_float(quote.get("bidSize")), askSize=as_float(quote.get("askSize")), spreadPercent=safe_percent_spread(bid, ask))
                        break
            except Exception:
                continue


def screen_tokenized(assets: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], CriterionAudit]:
    audit = CriterionAudit()
    assets = apply_criterion(assets, audit, "1. Tradabilité", lambda x: x["tradability"] in {"BUY_SELL", "BUY", "SELL"})
    assets = apply_criterion(assets, audit, "2. Quote disponible", lambda x: x.get("bidPrice", 0) > 0 and x.get("askPrice", 0) > 0)
    assets = apply_criterion(assets, audit, "3. Spread maximum", lambda x: x.get("spreadPercent") is not None and x["spreadPercent"] <= MAX_TOKENIZED_SPREAD_PERCENT)
    return assets, audit


# ----------------------------- ANALYSE DU RÉGIME -----------------------------
# Le screener s'arrête volontairement ici pour l'analyse technique :
# CHOP + ADX + ATR sont les trois seuls indicateurs de marché interprétés.

# ----------------------------- REPORTING -----------------------------
def format_number(value: Any, decimals: int = 2) -> str:
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return "-"


def audit_to_html(audit: CriterionAudit) -> str:
    rows = []
    for row in audit.rows:
        rows.append(f"<tr><td>{row['criterion']}</td><td>{row['before']:,}</td><td>{row['selected']:,}</td><td>{row['rejected']:,}</td><td>{row['retention']:.2f}%</td><td>{row['rejection']:.2f}%</td></tr>")
    html = "<table border='1' cellpadding='5' cellspacing='0'><tr><th>Critère</th><th>Avant</th><th>Retenus</th><th>Rejetés</th><th>Rétention</th><th>Rejet</th></tr>" + "".join(rows) + "</table>"
    distribution = audit.diagnostics.get("volume_distribution")
    if distribution:
        html += "<h3>Diagnostic de distribution du volume 24h</h3>"
        html += f"<p>Univers disponible pour ce diagnostic : <b>{audit.diagnostics.get('volume_universe_count', 0):,}</b> actifs</p>"
        html += "<table border='1' cellpadding='5' cellspacing='0'><tr><th>Seuil volume quote 24h</th><th>Actifs restant au-dessus du seuil</th></tr>"
        for threshold, count in distribution.items():
            html += f"<tr><td>${threshold:,.0f}</td><td>{count:,}</td></tr>"
        html += "</table>"

    regime = audit.diagnostics.get("regime")
    if regime is not None:
        html += "<h3>Régime de marché — CHOP + ADX + ATR (1h)</h3>"
        html += f"<p>Univers analysé après le spread : <b>{regime.get('universe_count', 0):,}</b> actifs. Aucun filtre de rejet n'est appliqué à ces trois indicateurs.</p>"
        html += "<table border='1' cellpadding='5' cellspacing='0'><tr><th>Régime interprété</th><th>Actifs</th></tr>"
        for label, count in regime.get("regime_counts", {}).items():
            html += f"<tr><td>{label}</td><td>{count:,}</td></tr>"
        html += "</table>"
        html += "<table border='1' cellpadding='5' cellspacing='0'><tr><th>Indicateur</th><th>Min</th><th>P25</th><th>Médiane</th><th>Moyenne</th><th>P75</th><th>Max</th></tr>"
        for label, key in (("CHOP", "chop"), ("ADX", "adx"), ("ATR %", "atr")):
            stats = regime.get(key, {})
            cells = []
            for stat_key in ("minimum", "p25", "median", "mean", "p75", "maximum"):
                value = stats.get(stat_key)
                cells.append(f"{value:.4f}" if isinstance(value, (int, float)) else "-")
            html += f"<tr><td>{label}</td><td>" + "</td><td>".join(cells) + "</td></tr>"
        html += "</table>"
        html += "<h4>Croisement CHOP × ADX</h4>"
        html += "<table border='1' cellpadding='5' cellspacing='0'><tr><th>CHOP × ADX</th><th>FAIBLE</th><th>TENDANCE</th><th>FORTE</th></tr>"
        matrix = regime.get("structure_matrix", {})
        for chop_zone in ("TENDANCE", "TRANSITION", "RANGE"):
            row = matrix.get(chop_zone, {})
            html += f"<tr><td>{chop_zone}</td><td>{row.get('FAIBLE', 0)}</td><td>{row.get('TENDANCE', 0)}</td><td>{row.get('FORTE', 0)}</td></tr>"
        html += "</table>"
        html += f"<p><b>Cas CHOP/ADX contradictoires :</b> {regime.get('conflict_count', 0):,}. <b>Valeurs valides :</b> {regime.get('valid_count', 0):,}/{regime.get('universe_count', 0):,}.</p>"
        html += "<p><b>Intensité ATR :</b> " + "; ".join(f"{label}: {count}" for label, count in regime.get("atr_intensity_counts", {}).items()) + "</p>"
        html += "<p><b>Direction ADX :</b> " + "; ".join(f"{label}: {count}" for label, count in regime.get("direction_counts", {}).items()) + "</p>"
    return html


def market_summary_html(market: str, assets: List[Dict[str, Any]], audit: Optional[CriterionAudit]) -> str:
    html = [f"<h2>{market}</h2>", f"<p><b>Survivants :</b> {len(assets):,}</p>"]
    if audit:
        html.append(audit_to_html(audit))
    if assets:
        html.append(f"<table border='1' cellpadding='5' cellspacing='0'><tr><th>Symbol</th><th>Volume 24h</th><th>Depth ±{ORDER_BOOK_DEPTH_PCT:.2f}%</th><th>Spread</th><th>24h</th><th>CHOP</th><th>ADX</th><th>ATR%</th><th>Nature</th><th>Structure</th><th>Direction</th><th>Interprétation</th><th>Funding</th></tr>")
        def regime_rank(asset):
            nature = asset.get("marketNature", "")
            structure = asset.get("marketStructure", "")
            intensity = asset.get("marketRegime", "")
            rank = {"TENDANCE FORTE": 5, "TENDANCE": 4, "TRANSITION": 3, "RANGE": 1, "NON DISPONIBLE": 0}.get(nature, 0)
            if "FORTE" in intensity: rank += 0.5
            if structure == "DIRECTIONNELLE": rank += 0.4
            return (rank, asset.get("adx", 0) or 0, asset.get("quoteVolume", 0) or 0)
        sorted_assets = sorted(assets, key=regime_rank, reverse=True)
        for asset in sorted_assets[:EMAIL_TOP_RESULTS]:
            funding = format_number(asset.get("fundingRate"), 4) + "%" if "fundingRate" in asset else "—"
            html.append(f"<tr><td>{asset.get('symbol', '-')}</td><td>{format_number(asset.get('quoteVolume', 0))}</td><td>{format_number(asset.get('depthTotalQuote', 0), 0)}</td><td>{format_number(asset.get('spreadPercent', 0), 4)}%</td><td>{format_number(asset.get('priceChangePercent', 0))}%</td><td>{format_number(asset.get('chop', '-'), 2)}</td><td>{format_number(asset.get('adx', '-'), 2)}</td><td>{format_number(asset.get('regimeAtrPercent', '-'), 3)}%</td><td>{asset.get('marketNature', '-')}</td><td>{asset.get('marketStructure', '-')}</td><td>{asset.get('marketDirection', '-')}</td><td>{asset.get('marketRegime', '-')}</td><td>{funding}</td></tr>")
        html.append("</table>")
    return "".join(html)


def build_report(market_results: Dict[str, List[Dict[str, Any]]], market_audits: Dict[str, CriterionAudit], errors: List[str], warnings: List[str], unavailable_markets: List[str]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Margin est une couche d'éligibilité sur le même sous-jacent Spot : on ne le
    # recompte donc pas comme un nouvel actif dans le total principal.
    spot_symbols = {a.get("symbol") for a in market_results.get("SPOT", []) if a.get("symbol")}
    unique_market_assets = set()
    for market, assets in market_results.items():
        if market == "MARGIN":
            continue
        for asset in assets:
            symbol = asset.get("symbol")
            if symbol:
                unique_market_assets.add((market, symbol))
    unique_assets_count = len(spot_symbols) if spot_symbols else len(unique_market_assets)

    parts = [
        "<html><body>",
        "<h1>Binance Multi-Market Screener</h1>",
        f"<p><b>Date :</b> {now}</p>",
        f"<p><b>Actifs uniques :</b> {unique_assets_count:,}</p>",
        "<hr>",
    ]
    for market in ["SPOT", "MARGIN", "USDⓈ-M FUTURES", "COIN-M FUTURES", "OPTIONS", "TOKENIZED STOCKS"]:
        if market in market_results:
            parts.append(market_summary_html(market, market_results[market], market_audits.get(market)))

    parts.append("<hr><h2>Marchés non intégrés</h2><ul><li><b>P2P :</b> non intégré. Aucune API publique officielle adaptée au même type de screening n'est utilisée.</li></ul>")

    if unavailable_markets:
        parts.append("<h2>Marchés indisponibles</h2><ul>" + "".join(f"<li>{m}</li>" for m in unavailable_markets) + "</ul>")

    parts.append(
        f"<h2>Paramètres principaux</h2><ul>"
        f"<li>Spot quotes : {', '.join(sorted(QUOTE_ASSETS)) if QUOTE_ASSETS else 'dynamiques — stablecoins actifs détectés depuis Binance exchangeInfo'}</li>"
        f"<li>Spot volume minimum actif : ${MIN_24H_QUOTE_VOLUME:,.0f}</li>"
         f"<li>Order Book Depth Spot : {'activé' if ORDER_BOOK_DEPTH_ENABLED else 'désactivé'} — profondeur minimale ${MIN_ORDER_BOOK_DEPTH_QUOTE:,.0f} dans ±{ORDER_BOOK_DEPTH_PCT:.2f}% (limit {ORDER_BOOK_DEPTH_LIMIT}, workers {ORDER_BOOK_DEPTH_WORKERS})</li>"
        f"<li>Spread Spot maximum : {MAX_SPREAD_PERCENT:.2f}%</li>"
        f"<li>Régime Spot : {'activé' if REGIME_ENABLED else 'désactivé'} — CHOP({CHOP_PERIOD}) + ADX({ADX_PERIOD}) + ATR({ATR_REGIME_PERIOD}) sur {REGIME_INTERVAL}, sans filtre d'exclusion</li>"
        f"<li>Zones d'interprétation : CHOP tendance ≤ {CHOP_TREND_THRESHOLD:.1f}, CHOP intermédiaire entre {CHOP_TREND_THRESHOLD:.1f} et {CHOP_RANGE_THRESHOLD:.1f}, CHOP range ≥ {CHOP_RANGE_THRESHOLD:.1f}; ADX tendance ≥ {ADX_TREND_THRESHOLD:.1f}, ADX forte ≥ {ADX_STRONG_THRESHOLD:.1f}; intensité ATR : calme &lt; {ATR_LOW_PERCENT:.2f}%, active &lt; {ATR_HIGH_PERCENT:.2f}%, volatile &lt; {ATR_EXTREME_PERCENT:.2f}%, très volatile au-dessus</li>"
        f"<li>Futures volume minimum : ${MIN_FUTURES_QUOTE_VOLUME:,.0f}</li>"
        f"</ul>"
    )
    if warnings:
        parts.append("<h2>Avertissements</h2><ul>" + "".join(f"<li>{w}</li>" for w in warnings) + "</ul>")
    if errors:
        parts.append("<h2>Erreurs techniques réelles</h2><ul>" + "".join(f"<li>{e}</li>" for e in errors) + "</ul>")
    else:
        parts.append("<h2>État technique</h2><p><b>Erreurs techniques réelles : 0</b></p>")
    parts.append("<hr><p><b>Important :</b> ce programme effectue uniquement de la collecte et de l'analyse de données de marché. Aucun ordre Binance n'est exécuté.</p></body></html>")
    return "".join(parts)


def send_email(subject: str, html: str) -> None:
    if not (EMAIL_USER and EMAIL_PASS and EMAIL_TO):
        raise RuntimeError("EMAIL_USER / EMAIL_PASS / EMAIL_TO non configurés")
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = EMAIL_USER
    message["To"] = EMAIL_TO
    message.attach(MIMEText(html, "html", "utf-8"))
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, context=context) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, [EMAIL_TO], message.as_string())


# ----------------------------- MARKET RUNNERS -----------------------------
def run_spot(warnings: List[str]) -> Tuple[List[Dict[str, Any]], CriterionAudit]:
    exchange = spot_exchange_info()
    assets = build_spot_universe(exchange)
    effective_quotes = resolve_spot_quote_assets(exchange)
    merge_spot_tickers(assets, spot_tickers())
    try:
        merge_spot_books(assets, spot_book_tickers())
    except Exception as exc:
        warnings.append(f"Spot bookTicker indisponible : {exc}")
        for asset in assets:
            asset["spreadPercent"] = None
    warnings.append(
        "Quote assets Spot dynamiques : " + ", ".join(sorted(effective_quotes))
        if effective_quotes
        else "Aucun stablecoin quote actif détecté dans Binance exchangeInfo."
    )
    return screen_spot(assets, effective_quotes, warnings)


def run_usdm(warnings: List[str]) -> Tuple[List[Dict[str, Any]], CriterionAudit]:
    assets = build_usdm_universe(futures_exchange_info(USDM_BASE_URL, "/fapi/v1/exchangeInfo"))
    merge_futures_tickers(assets, futures_tickers(USDM_BASE_URL, "/fapi/v1/ticker/24hr"))
    merge_futures_books(assets, futures_book_tickers(USDM_BASE_URL, "/fapi/v1/ticker/bookTicker"))
    merge_funding(assets, futures_premium_index(USDM_BASE_URL, "/fapi/v1/premiumIndex"))
    return screen_futures(assets)


def run_coinm(warnings: List[str]) -> Tuple[List[Dict[str, Any]], CriterionAudit]:
    assets = build_coinm_universe(futures_exchange_info(COINM_BASE_URL, "/dapi/v1/exchangeInfo"))
    merge_futures_tickers(assets, futures_tickers(COINM_BASE_URL, "/dapi/v1/ticker/24hr"))
    merge_futures_books(assets, futures_book_tickers(COINM_BASE_URL, "/dapi/v1/ticker/bookTicker"))
    merge_funding(assets, futures_premium_index(COINM_BASE_URL, "/dapi/v1/premiumIndex"))
    return screen_futures(assets)


def run_options(warnings: List[str]) -> Tuple[List[Dict[str, Any]], CriterionAudit]:
    assets = build_options_universe(options_exchange_info())
    merge_option_tickers(assets, options_tickers())
    return screen_options(assets)


def run_tokenized(warnings: List[str]) -> Tuple[List[Dict[str, Any]], CriterionAudit]:
    assets = build_tokenized_universe(tokenized_exchange_info(), tokenized_assets_info())
    if TOKENIZED_MAX_ASSETS > 0:
        assets = assets[:TOKENIZED_MAX_ASSETS]
    merge_tokenized_quotes(assets)
    return screen_tokenized(assets)


# ----------------------------- MAIN -----------------------------
def main() -> int:
    started = time.time()
    warnings: List[str] = []
    errors: List[str] = []
    unavailable_markets: List[str] = []
    market_results: Dict[str, List[Dict[str, Any]]] = {}
    market_audits: Dict[str, CriterionAudit] = {}
    print("=" * 70)
    print("BINANCE MULTI-MARKET SCREENER V2")
    print("=" * 70)

    if ENABLE_SPOT:
        print("\n[SPOT]")
        try:
            assets, audit = run_spot(warnings)
            market_results["SPOT"], market_audits["SPOT"] = assets, audit
            print(f"Spot survivors: {len(assets):,}")
            regime_diag = audit.diagnostics.get("regime", {})
            if regime_diag:
                print(
                    "Régime CHOP/ADX/ATR : "
                    f"{regime_diag.get('nature_counts', {})} | "
                    f"ATR {regime_diag.get('atr_intensity_counts', {})} | "
                    f"contradictions={regime_diag.get('conflict_count', 0)}"
                )
                print(f"Croisement CHOP×ADX : {regime_diag.get('structure_matrix', {})}")
        except Exception as exc:
            message, kind = format_market_exception("SPOT", exc)
            if kind == "unavailable":
                unavailable_markets.append(message)
            else:
                errors.append(message)

    if ENABLE_MARGIN:
        print("\n[MARGIN]")
        if "SPOT" in market_results:
            market_results["MARGIN"] = build_margin_market(market_results["SPOT"])
            print(f"Margin underlying Spot universe: {len(market_results['MARGIN']):,}")
            warnings.append("Margin : l'éligibilité réelle est dépendante du compte et n'est pas déclarée comme garantie par ce screener.")
        else:
            warnings.append("Margin ignoré car le scan Spot n'a pas abouti.")

    if ENABLE_USDM:
        print("\n[USDⓈ-M FUTURES]")
        try:
            assets, audit = run_usdm(warnings)
            market_results["USDⓈ-M FUTURES"], market_audits["USDⓈ-M FUTURES"] = assets, audit
            print(f"USDⓈ-M survivors: {len(assets):,}")
        except Exception as exc:
            message, kind = format_market_exception("USDⓈ-M FUTURES", exc)
            if kind == "unavailable":
                unavailable_markets.append(message)
            else:
                errors.append(message)

    if ENABLE_COINM:
        print("\n[COIN-M FUTURES]")
        try:
            assets, audit = run_coinm(warnings)
            market_results["COIN-M FUTURES"], market_audits["COIN-M FUTURES"] = assets, audit
            print(f"COIN-M survivors: {len(assets):,}")
        except Exception as exc:
            message, kind = format_market_exception("COIN-M FUTURES", exc)
            if kind == "unavailable":
                unavailable_markets.append(message)
            else:
                errors.append(message)

    if ENABLE_OPTIONS:
        print("\n[OPTIONS]")
        try:
            assets, audit = run_options(warnings)
            market_results["OPTIONS"], market_audits["OPTIONS"] = assets, audit
            print(f"Options survivors: {len(assets):,}")
        except Exception as exc:
            message, kind = format_market_exception("OPTIONS", exc)
            if kind == "unavailable":
                unavailable_markets.append(message)
            else:
                errors.append(message)

    if ENABLE_TOKENIZED_STOCKS:
        print("\n[TOKENIZED STOCKS]")
        if not BINANCE_API_KEY:
            warnings.append("Tokenized Stocks non exécuté : BINANCE_API_KEY absent.")
        else:
            try:
                assets, audit = run_tokenized(warnings)
                market_results["TOKENIZED STOCKS"], market_audits["TOKENIZED STOCKS"] = assets, audit
                print(f"Tokenized Stocks survivors: {len(assets):,}")
            except Exception as exc:
                errors.append(f"TOKENIZED STOCKS: {exc}")

    print("\n[REGIME INTERPRETATION]")
    if "SPOT" in market_results:
        regime_diag = market_audits.get("SPOT", CriterionAudit()).diagnostics.get("regime", {})
        print(f"Nature : {regime_diag.get('nature_counts', {})}")
        print(f"Direction : {regime_diag.get('direction_counts', {})}")
        print(f"Intensité ATR : {regime_diag.get('atr_intensity_counts', {})}")
        print(f"Croisement CHOP×ADX : {regime_diag.get('structure_matrix', {})}")
        print(f"Cas contradictoires : {regime_diag.get('conflict_count', 0)}")

    html = build_report(market_results, market_audits, errors, warnings, unavailable_markets)
    elapsed = time.time() - started
    subject = f"Binance Multi-Market Screener — {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC"
    try:
        send_email(subject, html)
        print(f"\nEmail envoyé. Durée : {elapsed:.2f}s")
    except Exception as exc:
        print("\nERREUR EMAIL:", exc)
        print(html)
        return 1
    print(f"\nDurée totale : {elapsed:.2f}s")
    if errors:
        print(f"Erreurs techniques réelles : {len(errors)}")
    if unavailable_markets:
        print(f"Marchés indisponibles : {len(unavailable_markets)}")
    if warnings:
        print(f"Avertissements : {len(warnings)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())