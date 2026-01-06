import yfinance as yf


def get_universe():
    """
    Returns a list of tickers to scan.
    Includes ETFs + large liquid stocks.
    """

    etfs = [
        "SPY", "QQQ", "IWM", "DIA",
        "XLK", "XLF", "XLE", "XLV",
        "SMH", "ARKK"
    ]

    mega_caps = [
        "AAPL", "MSFT", "NVDA", "AMZN",
        "GOOGL", "META", "TSLA", "BRK-B"
    ]

    liquid_names = [
        "AMD", "NFLX", "AVGO", "JPM",
        "BAC", "XOM", "CVX", "UNH",
        "COST", "WMT"
    ]

    return list(set(etfs + mega_caps + liquid_names))
