"""
Example implementation of improved rate limit handling for yfinance API calls.
This can be incorporated into your data_collector.py.
"""

import yfinance as yf
import time
import logging
import random
from requests.exceptions import HTTPError

logger = logging.getLogger('RateLimitHandler')

class RateLimitHandler:
    """
    Handler for managing API rate limits with yfinance.
    Implements exponential backoff and adaptive sleep times.
    """
    
    def __init__(self, base_sleep=2, max_retries=3, max_sleep=60):
        """
        Initialize the rate limit handler.
        
        Args:
            base_sleep (int): Base sleep time between API calls in seconds
            max_retries (int): Maximum number of retries for a failed API call
            max_sleep (int): Maximum sleep time in seconds
        """
        self.base_sleep = base_sleep
        self.max_retries = max_retries
        self.max_sleep = max_sleep
        self.consecutive_errors = 0
        self.last_call_time = 0
    
    def sleep_between_calls(self):
        """
        Sleep between API calls with adaptive timing.
        If consecutive errors occur, increases sleep time exponentially.
        """
        # Ensure minimum sleep time between calls
        current_time = time.time()
        elapsed = current_time - self.last_call_time
        
        # Calculate adaptive sleep time based on consecutive errors
        if self.consecutive_errors > 0:
            # Exponential backoff
            sleep_time = min(self.base_sleep * (2 ** self.consecutive_errors) + random.uniform(0, 1), self.max_sleep)
        else:
            sleep_time = self.base_sleep
        
        # If not enough time has elapsed since last call, sleep the remaining time
        if elapsed < sleep_time:
            time.sleep(sleep_time - elapsed)
        
        self.last_call_time = time.time()
    
    def fetch_with_rate_limit(self, symbol, fetch_function, *args, **kwargs):
        """
        Fetch data with rate limit handling and retry logic.
        
        Args:
            symbol (str): The symbol being fetched
            fetch_function (callable): The function to call to fetch data
            *args, **kwargs: Arguments to pass to the fetch function
            
        Returns:
            The result from the fetch function, or None if all retries fail
        """
        retries = 0
        
        while retries <= self.max_retries:
            try:
                # Wait before making the call
                self.sleep_between_calls()
                
                # Attempt to fetch data
                result = fetch_function(*args, **kwargs)
                
                # If successful, reset consecutive errors counter
                self.consecutive_errors = 0
                return result
                
            except HTTPError as e:
                retries += 1
                self.consecutive_errors += 1
                
                # Check for rate limit errors (HTTP 429)
                if hasattr(e, 'response') and e.response.status_code == 429:
                    logger.warning(f"Rate limit exceeded for {symbol}. Retry {retries}/{self.max_retries}")
                    # Get retry-after header if available, otherwise use exponential backoff
                    retry_after = e.response.headers.get('Retry-After', self.base_sleep * (2 ** retries))
                    time.sleep(float(retry_after))
                else:
                    logger.error(f"Error fetching data for {symbol}: {str(e)}. Retry {retries}/{self.max_retries}")
                    # Use exponential backoff for other errors
                    time.sleep(self.base_sleep * (2 ** retries))
                    
            except Exception as e:
                retries += 1
                self.consecutive_errors += 1
                logger.error(f"Unexpected error fetching data for {symbol}: {str(e)}. Retry {retries}/{self.max_retries}")
                time.sleep(self.base_sleep * (2 ** retries))
        
        # If all retries failed
        logger.error(f"Failed to fetch data for {symbol} after {self.max_retries} retries")
        return None


# Example usage:
def example_usage():
    # Initialize the rate limit handler
    rate_handler = RateLimitHandler(base_sleep=2, max_retries=3)
    
    # Example symbols
    symbols = ['AAPL', 'MSFT', 'AMZN', '^GDAXI']
    
    for symbol in symbols:
        # Use the rate handler to fetch data with rate limit handling
        def fetch_function():
            ticker = yf.Ticker(symbol)
            return ticker.history(period="1d", interval="1m")
        
        data = rate_handler.fetch_with_rate_limit(symbol, fetch_function)
        if data is not None:
            print(f"Successfully fetched data for {symbol}")
        else:
            print(f"Failed to fetch data for {symbol}")

# Example for batched collection (more efficient for large symbol sets):
def batch_collection_example(symbols, interval='1d', period='5d'):
    """
    More efficient collection using yfinance's batch download feature.
    This makes fewer API calls than individual symbol requests.
    """
    try:
        # Download data for all symbols in one call
        data = yf.download(
            tickers=symbols,
            period=period,
            interval=interval,
            group_by='ticker',
            auto_adjust=True,
            prepost=False,
            threads=True
        )
        
        # Process the data (in your case, save to database)
        for symbol in symbols:
            if symbol in data.columns.levels[0]:
                symbol_data = data[symbol]
                print(f"Got data for {symbol}, {len(symbol_data)} rows")
            else:
                print(f"No data for {symbol}")
                
        return data
    except Exception as e:
        print(f"Error in batch collection: {str(e)}")
        return None

if __name__ == "__main__":
    # Example usage
    example_usage()
    
    # Example of batch collection
    batch_collection_example(['AAPL', 'MSFT', 'AMZN', '^GDAXI'])
"""
