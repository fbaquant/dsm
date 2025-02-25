from enum import Enum

class Exchange(Enum):
    """Enum representing supported exchanges for market data streaming."""
    BINANCE = "binance"
    COINBASE = "coinbase"
    OKX = "okx"
    BYBIT = "bybit"