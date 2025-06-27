import subprocess
import sys

PYTHON = r'trading_env\Scripts\python.exe'
symbols = ['AAPL', 'RHM.DE', 'K']
start = '2024-01-01'
end = '2024-12-31'
interval = '1d'

def run_data_collector(symbol):
    print(f"Fetching data for {symbol}...")
    subprocess.run([
        PYTHON, 'data_collector.py',
        '--symbol', symbol,
        '--start', start,
        '--end', end,
        '--interval', interval
    ], check=True)

def main():
    for symbol in symbols:
        run_data_collector(symbol)
    print("Data collection complete. Please refresh your dashboard to see the results.")

if __name__ == '__main__':
    main()
