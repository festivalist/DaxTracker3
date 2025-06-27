# yfinance API Rate Limit Analysis for DaxTracker3

## Current Data Collection Configuration

### Collection Frequency
- Market data collection runs every 5 minutes
- News data collection runs every 60 minutes

### Symbol Coverage
- Currently collecting 11 symbols (7 stocks + 4 indices):
  - Stocks: AAPL, MSFT, AMZN, GOOGL, META, TSLA, NVDA
  - Indices: ^GSPC, ^DJI, ^IXIC, ^GDAXI
- Total symbols available in stocks.csv: approximately 121 symbols

### Request Pattern
- Sleep time between market data requests: 2 seconds
- Sleep time between news data requests: 1 second
- Each market data collection cycle: 11 API calls
- Each news collection cycle: 7 API calls

## Yahoo Finance API Rate Limits

Yahoo Finance API rate limits are not explicitly documented, but the Terms of Use states:

> "Yahoo's APIs may be subject to rate limits at Yahoo's absolute and sole discretion. These rate limits are intended to ensure the availability of the Yahoo APIs and underlying services for all of our users."

Rate limits are at Yahoo's discretion and may change at any time.

## Analysis for 16-hour Daily Operation

### API Call Volume Calculation (Current Configuration)
- **Market Data Collection:**
  - 16 hours × (60 minutes/hour ÷ 5 minutes) = 192 collection cycles per day
  - 192 cycles × 11 symbols = 2,112 API calls per day

- **News Data Collection:**
  - 16 hours × (1 call/hour) = 16 collection cycles per day
  - 16 cycles × 7 symbols = 112 API calls per day

- **Total API Calls Per Day:** 2,224 API calls

### API Call Volume Calculation (Full Symbol List)
- **Market Data Collection:**
  - 16 hours × (60 minutes/hour ÷ 5 minutes) = 192 collection cycles per day
  - 192 cycles × 121 symbols = 23,232 API calls per day

- **Total API Calls Per Day with Full Symbol List:** 23,000+ API calls

## Risk Assessment

### Current Configuration (11 Symbols)
- **Risk Level: LOW to MODERATE**
- The current setup with 11 symbols and 5-minute intervals is likely well within reasonable usage limits
- The existing 2-second pause between calls helps mitigate rate limiting issues

### Full Symbol List (121 Symbols)
- **Risk Level: HIGH**
- Expanding to all 121 symbols at the current frequency would substantially increase risk of hitting rate limits
- At 23,000+ API calls per day, rate limiting would almost certainly be triggered

## Recommendations

### Safe Usage Recommendations
1. **Continue with the current 11-symbol configuration**
   - This setup is likely safe and should not trigger rate limits

2. **If more symbols are needed:**
   - Group symbols into batches and use yfinance's batch download feature
   - Increase the interval between collections (10-15 minutes instead of 5)
   - Implement a rotating collection schedule where different symbol groups are collected at different times

3. **Technical Implementation Improvements:**
   - Add explicit handling for rate limit responses (HTTP 429)
   - Implement exponential backoff for failed requests
   - Add detailed logging of API usage patterns
   - Consider caching data to reduce API calls for frequently requested but rarely changing data
   - Use batch download functionality where possible

### Example Implementation
A sample implementation for improved rate limit handling has been provided in `improved_rate_limit_handling.py`. This includes:
- Exponential backoff for failed requests
- Adaptive sleep times between API calls
- Batch download examples for more efficient collection

## Conclusion
The current configuration with 11 symbols collected every 5 minutes is unlikely to exceed reasonable rate limits for the Yahoo Finance API. However, expanding to the full set of 121 symbols would substantially increase the risk of hitting rate limits.

By implementing the recommended improvements, particularly batch downloads and adaptive request pacing, the system can be made more robust against potential rate limiting issues.
