from __future__ import annotations

import concurrent.futures
import json
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

QUOTE_ASSETS = {x.strip().upper() for x in os.getenv("QUOTE_ASSETS", "USDT").split(",") if x.strip()}
EXCLUDE_STABLECOINS = os.getenv("EXCLUDE_STABLECOINS", "true").lower() == "true"
EXCLUDE_LEVERAGED_TOKENS = os.getenv("EXCLUDE_LEVERAGED_TOKENS", "true").lower() == "true"
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "5000000"))
MIN_24H_TRADES = int(os.getenv("MIN_24H_TRADES", "1000"))
MIN_24H_CHANGE_PERCENT = float(os.getenv("MIN_24H_CHANGE_PERCENT", "-100"))
MAX_24H_CHANGE_PERCENT = float(os.getenv("MAX_24H_CHANGE_PERCENT", "100"))
MAX_SPREAD_PERCENT = float(os.getenv("MAX_SPREAD_PERCENT", "0.50"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.00000001"))

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

TECHNICAL_ENABLED = os.getenv("TECHNICAL_ENABLED", "true").lower() == "true"
TECHNICAL_INTERVAL = os.getenv("TECHNICAL_INTERVAL", "1h")
TECHNICAL_KLINES_LIMIT = int(os.getenv("TECHNICAL_KLINES_LIMIT", "250"))
ENABLE_EMA = os.getenv("ENABLE_EMA", "true").lower() == "true"
ENABLE_RSI = os.getenv("ENABLE_RSI", "true").lower() == "true"
ENABLE_MACD = os.getenv("ENABLE_MACD", "true").lower() == "true"
ENABLE_ATR = os.getenv("ENABLE_ATR", "true").lower() == "true"
ENABLE_VOLUME_RATIO = os.getenv("ENABLE_VOLUME_RATIO", "true").lower() == "true"
EMA_FAST_PERIOD = int(os.getenv("EMA_FAST_PERIOD", "20"))
EMA_SLOW_PERIOD = int(os.getenv("EMA_SLOW_PERIOD", "50"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
MACD_FAST_PERIOD = int(os.getenv("MACD_FAST_PERIOD", "12"))
MACD_SLOW_PERIOD = int(os.getenv("MACD_SLOW_PERIOD", "26"))
MACD_SIGNAL_PERIOD = int(os.getenv("MACD_SIGNAL_PERIOD", "9"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
VOLUME_MA_PERIOD = int(os.getenv("VOLUME_MA_PERIOD", "20"))
TECHNICAL_MAX_ASSETS = int(os.getenv("TECHNICAL_MAX_ASSETS", "300"))
TECHNICAL_WORKERS = int(os.getenv("TECHNICAL_WORKERS", "8"))

STABLECOIN_BASES = {"USDT", "USDC", "FDUSD", "BUSD", "DAI", "TUSD", "USDP", "USDE", "USDD", "FRAX", "PYUSD", "EURC", "USD1", "RLUSD"}
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


def screen_spot(assets: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], CriterionAudit]:
    audit = CriterionAudit()
    assets = apply_criterion(assets, audit, "1. Status TRADING", lambda x: x["status"] == "TRADING")
    assets = apply_criterion(assets, audit, "2. Permission SPOT", lambda x: not x["permissions"] or "SPOT" in x["permissions"])
    assets = apply_criterion(assets, audit, "3. Quote asset autorisé", lambda x: x["quoteAsset"] in QUOTE_ASSETS)
    if EXCLUDE_STABLECOINS:
        assets = apply_criterion(assets, audit, "4. Exclusion stablecoins", lambda x: x["baseAsset"].upper() not in STABLECOIN_BASES)
    if EXCLUDE_LEVERAGED_TOKENS:
        assets = apply_criterion(assets, audit, "5. Exclusion tokens à levier", lambda x: not is_leveraged_symbol(x["baseAsset"]))
    assets = apply_criterion(assets, audit, "6. Données 24h disponibles", lambda x: x["lastPrice"] > 0)
    assets = apply_criterion(assets, audit, "7. Prix minimum", lambda x: x["lastPrice"] >= MIN_PRICE)

    # Diagnostic indépendant du seuil actif : il permet d'évaluer le seuil de volume
    # après plusieurs runs sans modifier prématurément le filtre.
    volume_universe = [a for a in assets if a.get("quoteVolume", 0) > 0]
    audit.diagnostics["volume_distribution"] = {
        threshold: sum(1 for a in volume_universe if a.get("quoteVolume", 0) >= threshold)
        for threshold in (1_000_000, 2_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000, 100_000_000, 500_000_000, 1_000_000_000)
    }
    audit.diagnostics["volume_universe_count"] = len(volume_universe)

    assets = apply_criterion(assets, audit, "8. Volume quote 24h minimum", lambda x: x["quoteVolume"] >= MIN_24H_QUOTE_VOLUME)
    assets = apply_criterion(assets, audit, "9. Nombre de trades minimum", lambda x: x["trades"] >= MIN_24H_TRADES)
    assets = apply_criterion(assets, audit, "10. Variation 24h minimum", lambda x: x["priceChangePercent"] >= MIN_24H_CHANGE_PERCENT)
    assets = apply_criterion(assets, audit, "11. Variation 24h maximum", lambda x: x["priceChangePercent"] <= MAX_24H_CHANGE_PERCENT)
    assets = apply_criterion(assets, audit, "12. Spread maximum", lambda x: x["spreadPercent"] is not None and x["spreadPercent"] <= MAX_SPREAD_PERCENT)
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, max(1, TECHNICAL_WORKERS))) as executor:
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


# ----------------------------- TECHNICAL -----------------------------
def spot_klines(symbol: str) -> List[List[Any]]:
    return http_get_json(SPOT_BASE_URL, "/api/v3/klines", {"symbol": symbol, "interval": TECHNICAL_INTERVAL, "limit": TECHNICAL_KLINES_LIMIT})


def usdm_klines(symbol: str) -> List[List[Any]]:
    return http_get_json(USDM_BASE_URL, "/fapi/v1/klines", {"symbol": symbol, "interval": TECHNICAL_INTERVAL, "limit": TECHNICAL_KLINES_LIMIT})


def coinm_klines(symbol: str) -> List[List[Any]]:
    return http_get_json(COINM_BASE_URL, "/dapi/v1/klines", {"symbol": symbol, "interval": TECHNICAL_INTERVAL, "limit": TECHNICAL_KLINES_LIMIT})


def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    multiplier = 2.0 / (period + 1.0)
    result = sum(values[:period]) / period
    for value in values[period:]:
        result = (value - result) * multiplier + result
    return result


def sma(values: List[float], period: int) -> Optional[float]:
    return sum(values[-period:]) / period if len(values) >= period else None


def rsi(values: List[float], period: int) -> Optional[float]:
    if len(values) <= period:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def macd(values: List[float], fast: int, slow: int, signal: int) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if len(values) < slow + signal:
        return None, None, None
    fast_values, slow_values = [], []
    for i in range(slow, len(values) + 1):
        fv, sv = ema(values[:i], fast), ema(values[:i], slow)
        if fv is not None and sv is not None:
            fast_values.append(fv)
            slow_values.append(sv)
    macd_values = [f - s for f, s in zip(fast_values, slow_values)]
    if len(macd_values) < signal:
        return None, None, None
    macd_value = macd_values[-1]
    signal_value = ema(macd_values, signal)
    if signal_value is None:
        return None, None, None
    return macd_value, signal_value, macd_value - signal_value


def atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> Optional[float]:
    if len(closes) <= period:
        return None
    trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])) for i in range(1, len(closes))]
    if len(trs) < period:
        return None
    result = sum(trs[:period]) / period
    for value in trs[period:]:
        result = (result * (period - 1) + value) / period
    return result


def classify_technical_state(score: float) -> str:
    if score >= 75:
        return "BULLISH FORT"
    if score >= 55:
        return "BULLISH"
    if score <= 25:
        return "BEARISH FORT"
    if score <= 45:
        return "BEARISH"
    return "NEUTRE"


def analyze_klines(klines: List[List[Any]]) -> Dict[str, Any]:
    closes = [as_float(r[4]) for r in klines]
    highs = [as_float(r[2]) for r in klines]
    lows = [as_float(r[3]) for r in klines]
    volumes = [as_float(r[5]) for r in klines]
    if len(closes) < 60:
        raise ValueError("Historique insuffisant")
    current = closes[-1]
    ema_fast = ema(closes, EMA_FAST_PERIOD) if ENABLE_EMA else None
    ema_slow = ema(closes, EMA_SLOW_PERIOD) if ENABLE_EMA else None
    rsi_value = rsi(closes, RSI_PERIOD) if ENABLE_RSI else None
    macd_value = macd_signal = macd_hist = None
    if ENABLE_MACD:
        macd_value, macd_signal, macd_hist = macd(closes, MACD_FAST_PERIOD, MACD_SLOW_PERIOD, MACD_SIGNAL_PERIOD)
    atr_value = atr(highs, lows, closes, ATR_PERIOD) if ENABLE_ATR else None
    volume_ma = sma(volumes, VOLUME_MA_PERIOD) if ENABLE_VOLUME_RATIO else None
    volume_ratio = volumes[-1] / volume_ma if ENABLE_VOLUME_RATIO and volume_ma and volume_ma > 0 else None
    score = 0.0
    if ENABLE_EMA and ema_fast is not None:
        if current > ema_fast:
            score += 15
        if ema_slow is not None and current > ema_slow:
            score += 15
    if ENABLE_RSI and rsi_value is not None:
        if rsi_value >= 60:
            score += 20
        elif rsi_value >= 50:
            score += 12
        elif rsi_value >= 40:
            score += 5
    if ENABLE_MACD and macd_value is not None and macd_signal is not None:
        if macd_value > macd_signal:
            score += 15
        if macd_hist is not None and macd_hist > 0:
            score += 10
    if ENABLE_VOLUME_RATIO and volume_ratio is not None:
        if volume_ratio >= 1.5:
            score += 15
        elif volume_ratio >= 1.0:
            score += 10
        elif volume_ratio >= 0.75:
            score += 5
    if ENABLE_ATR and atr_value is not None and current > 0:
        atr_percent = atr_value / current * 100
        if atr_percent >= 2.0:
            score += 10
        elif atr_percent >= 1.0:
            score += 6
        elif atr_percent >= 0.5:
            score += 3
    return {"technicalScore": round(score, 2), "technicalState": classify_technical_state(score), "emaFast": ema_fast, "emaSlow": ema_slow, "rsi": rsi_value, "macd": macd_value, "macdSignal": macd_signal, "macdHistogram": macd_hist, "atr": atr_value, "volumeRatio": volume_ratio}


def analyze_symbol(asset: Dict[str, Any]) -> Dict[str, Any]:
    market = asset["market"]
    if market == "SPOT":
        klines = spot_klines(asset["symbol"])
    elif market == "USDⓈ-M FUTURES":
        klines = usdm_klines(asset["symbol"])
    elif market == "COIN-M FUTURES":
        klines = coinm_klines(asset["symbol"])
    else:
        return asset
    output = dict(asset)
    output.update(analyze_klines(klines))
    return output


def run_technical_analysis(assets: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str], int]:
    if not TECHNICAL_ENABLED:
        return assets, [], 0

    errors: List[str] = []
    technical_markets = {"SPOT", "USDⓈ-M FUTURES", "COIN-M FUTURES"}

    # Margin n'est pas une nouvelle série de prix : ses indicateurs techniques
    # doivent être réutilisés depuis Spot. Cela évite les appels et analyses doublons.
    eligible = [a for a in assets if a["market"] in technical_markets]
    eligible.sort(key=lambda x: x.get("quoteVolume", 0), reverse=True)
    if TECHNICAL_MAX_ASSETS > 0:
        eligible = eligible[:TECHNICAL_MAX_ASSETS]

    results: List[Dict[str, Any]] = []
    analyzed_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, TECHNICAL_WORKERS)) as executor:
        future_map = {executor.submit(analyze_symbol, a): a for a in eligible}
        for future in concurrent.futures.as_completed(future_map):
            asset = future_map[future]
            try:
                results.append(future.result())
                analyzed_count += 1
            except Exception as exc:
                errors.append(f"{asset.get('market')}:{asset.get('symbol')}: {exc}")

    # Réinjecte les actifs non techniques (dont Margin) sans les compter comme
    # analyses techniques supplémentaires. Pour Margin, on copie les indicateurs
    # du Spot de même symbole lorsque disponibles.
    spot_technical = {
        a.get("symbol"): a for a in results if a.get("market") == "SPOT" and a.get("symbol")
    }
    for asset in assets:
        if asset["market"] == "MARGIN":
            copied = dict(asset)
            source = spot_technical.get(asset.get("symbol"))
            if source:
                for key in (
                    "technicalScore", "technicalState", "emaFast", "emaSlow",
                    "rsi", "macd", "macdSignal", "macdHistogram", "atr", "volumeRatio"
                ):
                    if key in source:
                        copied[key] = source[key]
            results.append(copied)
        elif asset["market"] not in technical_markets:
            results.append(asset)

    results.sort(key=lambda x: (x.get("technicalScore", -1), x.get("quoteVolume", 0)), reverse=True)
    return results, errors, analyzed_count


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
    return html


def market_summary_html(market: str, assets: List[Dict[str, Any]], audit: Optional[CriterionAudit]) -> str:
    html = [f"<h2>{market}</h2>", f"<p><b>Survivants :</b> {len(assets):,}</p>"]
    if audit:
        html.append(audit_to_html(audit))
    if assets:
        html.append("<table border='1' cellpadding='5' cellspacing='0'><tr><th>Symbol</th><th>Volume</th><th>Spread</th><th>24h</th><th>Funding</th><th>Score technique</th><th>État</th></tr>")
        sorted_assets = sorted(assets, key=lambda x: (x.get("technicalScore", -1), x.get("quoteVolume", 0)), reverse=True)
        for asset in sorted_assets[:EMAIL_TOP_RESULTS]:
            html.append(f"<tr><td>{asset.get('symbol', '-')}</td><td>{format_number(asset.get('quoteVolume', 0))}</td><td>{format_number(asset.get('spreadPercent', 0), 4)}%</td><td>{format_number(asset.get('priceChangePercent', 0))}%</td><td>{format_number(asset.get('fundingRate', 0), 4)}%</td><td>{format_number(asset.get('technicalScore', '-'))}</td><td>{asset.get('technicalState', '-')}</td></tr>")
        html.append("</table>")
    return "".join(html)


def build_report(market_results: Dict[str, List[Dict[str, Any]]], market_audits: Dict[str, CriterionAudit], errors: List[str], warnings: List[str], unavailable_markets: List[str], technical_analysis_count: int) -> str:
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
        f"<p><b>Analyses techniques uniques :</b> {technical_analysis_count:,}</p>",
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
        f"<li>Spot quotes : {', '.join(sorted(QUOTE_ASSETS))}</li>"
        f"<li>Spot volume minimum actif : ${MIN_24H_QUOTE_VOLUME:,.0f}</li>"
        f"<li>Futures volume minimum : ${MIN_FUTURES_QUOTE_VOLUME:,.0f}</li>"
        f"<li>Technical timeframe : {TECHNICAL_INTERVAL}</li>"
        f"<li>Technical max assets : {TECHNICAL_MAX_ASSETS}</li>"
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
    assets = build_spot_universe(spot_exchange_info())
    merge_spot_tickers(assets, spot_tickers())
    try:
        merge_spot_books(assets, spot_book_tickers())
    except Exception as exc:
        warnings.append(f"Spot bookTicker indisponible : {exc}")
        for asset in assets:
            asset["spreadPercent"] = None
    return screen_spot(assets)


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

    print("\n[TECHNICAL ANALYSIS]")
    all_assets = [asset for assets in market_results.values() for asset in assets]
    technical_results, technical_errors, technical_analysis_count = run_technical_analysis(all_assets)
    errors.extend(technical_errors)
    rebuilt: Dict[str, List[Dict[str, Any]]] = {}
    for asset in technical_results:
        rebuilt.setdefault(asset["market"], []).append(asset)
    market_results = rebuilt
    print(f"Technical analyses uniques: {technical_analysis_count:,}")

    html = build_report(market_results, market_audits, errors, warnings, unavailable_markets, technical_analysis_count)
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
