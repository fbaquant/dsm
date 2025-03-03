# config.py

EXCHANGE = {
    "binance": {
        "ws_url": "wss://stream.binance.com:9443/ws",  # Updated Binance endpoint
        "api_key": "qFY0Ivnh1j87uWXPghAdIplKPNTsufBHiaJ2rcnLBchC2YH70NHXhi19q5e8x8cD",
        "secret_key": "MS2jm6ATPM9xp9HQ8HLhvPnk4Cvb9pXi46CXHceNMsE43z2jPMtf1O7TG8Txcuf3",
        "topic_prefix": "ORDERBOOK_BINANCE",
        "zmq_port": 5560
    },
    "coinbase": {
        "ws_url": "wss://advanced-trade-ws.coinbase.com",  # Updated Coinbase URL
        "api_key": "organizations/c4ad83b9-641e-47f6-ac58-a7d3227727e4/apiKeys/8166b9b7-9392-4e1e-bb1f-314a835e4edc",
        "secret_key": (
            "-----BEGIN EC PRIVATE KEY-----\n"
            "MHcCAQEEICCGTg7PUtudxRspmAZvUNJiYzVKQ/Oh90w0jncEJdbYoAoGCCqGSM49\n"
            "AwEHoUQDQgAEDyOsE3MUuICtyhfCViB4K0l3a/enUjMzxpT2Kf4zCGvjUK6JGLvm\n"
            "OuNBedqMRvXxjXiFgPO9pl3qO+XXnhzotQ==\n"
            "-----END EC PRIVATE KEY-----\n"
        ),
        "topic_prefix": "ORDERBOOK_COINBASE",
        "zmq_port": 5561
    },
    "okx": {
        "ws_url": "wss://ws.okx.com:8443/ws/v5/public",  # OKX latest public API
        "api_key": "777a731d-801f-4a55-97ad-2e96cb85406d",
        "secret_key": "053BA67378BCBF68E48589F53444F4E7",
        "topic_prefix": "ORDERBOOK_OKX",
        "zmq_port": 5562
    },
    "bybit": {
        "ws_url": "wss://stream.bybit.com/v5/public/spot",  # Bybit latest WebSocket
        "api_key": "EP46MrOENoWuV8QmvF",
        "secret_key": "vPOe01QSmRMMNIy28pIDUevemhHxbr8IBpld",
        "topic_prefix": "ORDERBOOK_BYBIT",
        "zmq_port": 5563
    }
}

DATABASE = {
    # For InfluxDB 3 Core settings. When you're ready to switch to InfluxDB, update these values.
    "type": "influxdb3",                  # You might also set "sqlite" here if testing with SQLite.
    "url": "http://localhost:8186",       # URL where your InfluxDB 3 Core instance is running.
    "token": "your_influxdb_api_token",   # Your InfluxDB API token, generated via the UI or CLI.
    "org": "your_organization_name",      # Your InfluxDB organization.
    "bucket": "your_bucket_name"          # The bucket where your data will be stored.
}

if __name__ == "__main__":
    print("Binance WebSocket URL:", EXCHANGE["binance"]["ws_url"])
    print("Database URL:", DATABASE["url"])
