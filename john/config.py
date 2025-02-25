CONFIG = {
    "exchanges": {
        "binance": {
            "ws_url": "wss://stream.binance.com:9443/ws",  # Updated endpoint
        },
        "coinbase": {
            "ws_url": "wss://advanced-trade-ws.coinbase.com",  # Updated Coinbase URL
            "api_key": "organizations/c4ad83b9-641e-47f6-ac58-a7d3227727e4/apiKeys/8166b9b7-9392-4e1e-bb1f-314a835e4edc",
            "secret_key": "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEICCGTg7PUtudxRspmAZvUNJiYzVKQ/Oh90w0jncEJdbYoAoGCCqGSM49\nAwEHoUQDQgAEDyOsE3MUuICtyhfCViB4K0l3a/enUjMzxpT2Kf4zCGvjUK6JGLvm\nOuNBedqMRvXxjXiFgPO9pl3qO+XXnhzotQ==\n-----END EC PRIVATE KEY-----\n",
        },
        "okx": {
            "ws_url": "wss://ws.okx.com:8443/ws/v5/public",  # OKX latest public API
        },
        "bybit": {
            "ws_url": "wss://stream.bybit.com/v5/public/spot",  # Bybit latest WebSocket
        }
    }
}

if __name__ == "__main__":
    print(CONFIG["exchanges"]["binance"]["ws_url"])
