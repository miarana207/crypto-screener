import json
import logging
import os
import smtplib
import urllib.error
import urllib.request
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Configuration du logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Configuration API Binance
# En cas d'erreur HTTP 451 (blocage US sur GitHub Actions), utilisez "https://api.binance.us"
BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
TICKER_24HR_ENDPOINT = "/api/v3/ticker/24hr"


def get_binance_tickers():
    """Récupère les données 24h de tous les tickers depuis Binance."""
    url = f"{BINANCE_BASE_URL}{TICKER_24HR_ENDPOINT}"
    logging.info(f"Récupération des données 24h depuis {url}...")

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/115.0.0.0 Safari/537.36"
            )
        },
    )

    # Configuration éventuelle d'un proxy via variable d'environnement (ex: PROXY_URL)
    proxy_url = os.getenv("PROXY_URL")
    if proxy_url:
        handler = urllib.request.ProxyHandler(
            {"http": proxy_url, "https": proxy_url}
        )
        opener = urllib.request.build_opener(handler)
    else:
        opener = urllib.request.build_opener()

    try:
        with opener.open(req, timeout=15) as response:
            if response.status == 200:
                data = response.read().decode("utf-8")
                return json.loads(data)
            else:
                raise Exception(f"Réponse inattendue : Code {response.status}")
    except urllib.error.HTTPError as e:
        logging.error(f"Erreur lors de la requête API Binance : HTTP Error {e.code}: {e.reason}")
        raise
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des données : {e}")
        raise


def filter_tickers(tickers):
    """Filtre les paires USDT selon vos critères personnalisés."""
    filtered = []
    for ticker in tickers:
        symbol = ticker.get("symbol", "")
        # Filtrer uniquement les paires en USDT
        if symbol.endswith("USDT"):
            try:
                price_change_percent = float(
                    ticker.get("priceChangePercent", 0)
                )
                quote_volume = float(ticker.get("quoteVolume", 0))

                # Critères d'exemple : Hausse > 5% et Volume > 1M USDT
                if price_change_percent >= 5.0 and quote_volume >= 1_000_000:
                    filtered.append(
                        {
                            "symbol": symbol,
                            "priceChangePercent": price_change_percent,
                            "lastPrice": float(ticker.get("lastPrice", 0)),
                            "quoteVolume": quote_volume,
                        }
                    )
            except ValueError:
                continue

    # Tri par variation de prix décroissante
    filtered.sort(key=lambda x: x["priceChangePercent"], reverse=True)
    return filtered


def send_email_alert(results):
    """Envoie un rapport par email si des critères sont remplis."""
    email_user = os.getenv("EMAIL_USER")
    email_pass = os.getenv("EMAIL_PASS")
    email_to = os.getenv("EMAIL_TO")

    if not email_user or not email_pass or not email_to:
        logging.warning(
            "Variables d'environnement Email manquantes. Envoi ignoré."
        )
        return

    msg = MIMEMultipart()
    msg["From"] = email_user
    msg["To"] = email_to
    msg["Subject"] = f"📈 Binance Screener Alert : {len(results)} opportunités"

    body = "Voici les paires détectées :\n\n"
    for res in results:
        body += (
            f"• {res['symbol']}: {res['priceChangePercent']:.2f}% | "
            f"Prix: {res['lastPrice']} USDT | "
            f"Volume: {res['quoteVolume']:,.0f} USDT\n"
        )

    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(email_user, email_pass)
            server.send_message(msg)
        logging.info("Email d'alerte envoyé avec succès !")
    except Exception as e:
        logging.error(f"Erreur lors de l'envoi de l'email : {e}")


def main():
    logging.info("=== Démarrage du Binance Screener ===")
    try:
        tickers = get_binance_tickers()
        results = filter_tickers(tickers)

        logging.info(
            f"Screener terminé. {len(results)} résultat(s) trouvé(s)."
        )

        if results:
            send_email_alert(results)
        else:
            logging.info("Aucune paire ne correspond aux critères.")

    except Exception as e:
        logging.error(f"Erreur d'exécution globale : {e}")
        raise


if __name__ == "__main__":
    main()
