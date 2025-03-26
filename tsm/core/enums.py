from enum import Enum

class OrderSide(Enum, str):
    BUY = "BUY"
    SELL = "SELL"

class Market(Enum,str):
    COINBASE = "COINBASE"
    BYBIT = "BYBIT"
    BINANCE = "BINANCE"
    OKX = "OKX"

class Instrument(Enum, str):
    BTCUSD = "BTC-USD"
    ETHUSD = "ETH-USD"

class TimeInForce(Enum, str):
    IOC = "IOC"
    GTC = "GTC"
    DAY = "DAY"

class OrderType(Enum, str):
    LIMIT = "LIMIT"
    MARKET = "MARKET"