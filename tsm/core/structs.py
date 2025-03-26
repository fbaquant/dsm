from datetime import datetime

import csp

from tsm.core.enums import Instrument, Market, OrderSide, TimeInForce, OrderType


class TwoWayPrice(csp.Struct):
    bid_price: float
    ask_price: float
    bid_qty: float
    ask_qty: float
    time: datetime


class Order(csp.Struct):
    symbol: Instrument
    market: Market
    order_side: OrderSide
    price: float
    qty: float
    time_in_force: TimeInForce
    order_type: OrderType
    time: datetime
    # split time into timeCreated, timeSent
    # metadata
    # maxShownQuantity
