from datetime import datetime, timedelta

import csp
import numpy as np
from csp import ts, stats

st = datetime(2025, 1, 1)
prices_data = [
    (st + timedelta(seconds=1.3), 12.653),
    (st + timedelta(seconds=2.3), 14.210),
    (st + timedelta(seconds=3.8), 13.099),
    (st + timedelta(seconds=4.1), 12.892),
    (st + timedelta(seconds=4.4), 17.328),
    (st + timedelta(seconds=5.1), 18.543),
    (st + timedelta(seconds=5.3), 17.564),
    (st + timedelta(seconds=6.3), 19.023),
    (st + timedelta(seconds=8.7), 19.763),
]


@csp.node
def time_moving_average(window: int, x: ts[float]) -> ts[float]:
    with csp.start():
        assert window > 0
        csp.set_buffering_policy(x, tick_history=timedelta(seconds=window))

    if csp.ticked(x):
        buffer = csp.values_at(
            x,
            timedelta(seconds=-window),
            timedelta(seconds=0),
            csp.TimeIndexPolicy.INCLUSIVE,
            csp.TimeIndexPolicy.INCLUSIVE
        )

        return np.mean(buffer)

@csp.node
def tick_moving_average():
    pass


@csp.graph
def calculate_moving_average(window: int, price: ts[float]):
    ma_price = time_moving_average(window=window, x=price)
    stats_ma_price = stats.mean(
        price,
        interval=timedelta(seconds=window),
        min_window=timedelta(seconds=0)
    )
    csp.add_graph_output("ma_price", ma_price)
    csp.add_graph_output("stats_ma_price", stats_ma_price)


def main():
    price = csp.curve(typ=float, data=prices_data)
    results = csp.run(calculate_moving_average, 3, price, starttime=st, endtime=st + timedelta(seconds=10))
    for result in results["ma_price"]:
        print(f"time: {result[0]}, ma_price: {result[1]}")

    for result in results["stats_ma_price"]:
        print(f"time: {result[0]}, stats_ma_price: {result[1]}")


if __name__ == "__main__":
    main()
