import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8000"

def test_market_data_invalid_interval():
    print("\nTesting market data with invalid interval...")
    response = requests.post(
        f"{BASE_URL}/market-data",
        json={"symbol": "^GDAXI", "period": "1d", "interval": "1x"}
    )
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")

def test_empty_symbols_list():
    print("\nTesting signals with empty symbols list...")
    response = requests.post(
        f"{BASE_URL}/signals",
        json={"symbols": []}
    )
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")

def test_invalid_symbol_analysis():
    print("\nTesting analysis with invalid symbol...")
    response = requests.get(f"{BASE_URL}/analysis/INVALID_SYMBOL")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")

def test_invalid_notification():
    print("\nTesting notification with invalid signal type...")
    invalid_signal = {
        "signal": {
            "symbol": "^GDAXI",
            "signal_type": "INVALID",
            "close_price": 100.0,
            "confidence": 0.8,
            "timestamp": datetime.now().isoformat(),
            "reason": "Test signal"
        }
    }
    response = requests.post(f"{BASE_URL}/notify", json=invalid_signal)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")

if __name__ == "__main__":
    print("Starting server tests...")
    test_market_data_invalid_interval()
    test_empty_symbols_list()
    test_invalid_symbol_analysis()
    test_invalid_notification()
    print("\nTests completed.")
