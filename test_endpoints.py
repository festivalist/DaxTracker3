import requests
import json
from datetime import datetime
import time
import traceback

BASE_URL = "http://localhost:8000"

def print_test_result(name, response):
    print(f"\n=== Testing {name} ===")
    print(f"Status Code: {response.status_code}")
    try:
        response_json = response.json()
        print("Response:")
        print(json.dumps(response_json, indent=2))
    except Exception as e:
        print(f"Error parsing response: {str(e)}")
        print(f"Raw response: {response.text}")
    print("=" * 50)

def test_market_data_endpoint():
    try:
        print("\nTesting market data endpoint...")
        
        # Test 1: Valid request
        print("\nTest 1: Valid market data request")
        valid_data = {
            "symbol": "^GDAXI",
            "period": "1d",
            "interval": "1m"
        }
        response = requests.post(f"{BASE_URL}/market-data", json=valid_data)
        print_test_result("Market Data (Valid)", response)

        # Test 2: Invalid interval
        print("\nTest 2: Invalid interval")
        invalid_data = {
            "symbol": "^GDAXI",
            "period": "1d",
            "interval": "1x"  # Invalid interval
        }
        response = requests.post(f"{BASE_URL}/market-data", json=invalid_data)
        print_test_result("Market Data (Invalid Interval)", response)

    except Exception as e:
        print(f"Error during market data test: {str(e)}")
        traceback.print_exc()

def test_analysis_endpoint():
    try:
        print("\nTesting analysis endpoint...")
        
        # Test 1: Valid symbol
        print("\nTest 1: Valid symbol")
        response = requests.get(f"{BASE_URL}/analysis/^GDAXI")
        print_test_result("Analysis (Valid Symbol)", response)

        # Test 2: Invalid symbol
        print("\nTest 2: Invalid symbol")
        response = requests.get(f"{BASE_URL}/analysis/INVALID_SYMBOL")
        print_test_result("Analysis (Invalid Symbol)", response)

    except Exception as e:
        print(f"Error during analysis test: {str(e)}")
        traceback.print_exc()

def test_signals_endpoint():
    try:
        print("\nTesting signals endpoint...")
        
        # Test 1: Valid symbols list
        print("\nTest 1: Valid symbols list")
        valid_data = {
            "symbols": ["^GDAXI", "^DAX"]
        }
        response = requests.post(f"{BASE_URL}/signals", json=valid_data)
        print_test_result("Signals (Valid)", response)

        # Test 2: Empty symbols list
        print("\nTest 2: Empty symbols list")
        invalid_data = {
            "symbols": []
        }
        response = requests.post(f"{BASE_URL}/signals", json=invalid_data)
        print_test_result("Signals (Empty List)", response)

    except Exception as e:
        print(f"Error during signals test: {str(e)}")
        traceback.print_exc()

def test_notification_endpoint():
    try:
        print("\nTesting notification endpoint...")
        
        # Test 1: Valid notification
        print("\nTest 1: Valid notification")
        valid_data = {
            "signal": {
                "symbol": "^GDAXI",
                "signal_type": "BUY",
                "close_price": 15000.0,
                "confidence": 0.95,
                "timestamp": datetime.now().isoformat(),
                "reason": "Strong bullish pattern detected"
            }
        }
        response = requests.post(f"{BASE_URL}/notify", json=valid_data)
        print_test_result("Notification (Valid)", response)

        # Test 2: Invalid signal type
        print("\nTest 2: Invalid signal type")
        invalid_data = {
            "signal": {
                "symbol": "^GDAXI",
                "signal_type": "INVALID",
                "close_price": 15000.0,
                "confidence": 0.95,
                "timestamp": datetime.now().isoformat(),
                "reason": "Test signal"
            }
        }
        response = requests.post(f"{BASE_URL}/notify", json=invalid_data)
        print_test_result("Notification (Invalid Signal Type)", response)

    except Exception as e:
        print(f"Error during notification test: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    print("Starting endpoint tests...")
    print("Waiting for server to be ready...")
    time.sleep(2)  # Give the server a moment to start up

    try:
        test_market_data_endpoint()
        test_analysis_endpoint()
        test_signals_endpoint()
        test_notification_endpoint()
        print("\nAll tests completed!")
    except requests.exceptions.ConnectionError:
        print("\nError: Could not connect to the server. Make sure it's running on http://localhost:8000")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        traceback.print_exc()
