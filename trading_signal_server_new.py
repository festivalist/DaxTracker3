"""
Trading Signal Server - FastAPI Implementation

This module implements a RESTful API server for a trading signal system. It provides endpoints
for collecting market data, performing technical analysis, generating trading signals, and
sending notifications.

The server uses FastAPI for API implementation and Pydantic for input validation. It integrates
with several components:
- DataCollector: Fetches market data from Yahoo Finance
- TechnicalAnalyzer: Performs technical analysis on market data
- SignalGenerator: Generates trading signals based on analysis
- TelegramNotifier: Sends notifications via Telegram

Key Features:
- Input validation using Pydantic models
- Error handling with appropriate HTTP status codes
- Logging of all operations
- CORS support
- JSON serialization with datetime support

Example Usage:
    # Start the server
    uvicorn trading_signal_server_new:app --host 127.0.0.1 --port 8000

    # Access the API documentation
    # Visit http://localhost:8000/docs in your browser
"""

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
import os
import logging
from telegram.error import TelegramError

from data_collector import DataCollector
from technical_analyzer import TechnicalAnalyzer
from signal_generator import SignalGenerator
from notification_system import TelegramNotifier

class DateTimeEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that properly handles datetime objects.
    
    This encoder converts datetime objects to ISO format strings when serializing to JSON.
    It's used throughout the application for consistent datetime handling.
    
    Example:
        json.dumps(data, cls=DateTimeEncoder)
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Trading Signal Server",
    description="API for collecting market data, generating trading signals, and sending notifications",
    version="1.0.0"
)

# Exception handlers
@app.exception_handler(ValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "Validation error",
            "detail": str(exc),
            "fields": exc.errors()
        }
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "Internal server error", "detail": str(exc)}
    )

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Custom exception for data collection errors
class DataCollectionError(Exception):
    pass

# Pydantic models with strict validation
class MarketDataRequest(BaseModel):
    """
    Pydantic model for market data requests.
    
    This model validates incoming requests for market data, ensuring that:
    - Symbol is not empty
    - Period format is correct (e.g., "1d", "5m", "1mo")
    - Interval format is correct (e.g., "1m", "5m", "1h")
    
    Attributes:
        symbol (str): Trading symbol (e.g., "^GDAXI" for DAX)
        period (str): Time period to fetch (e.g., "1d" for one day)
        interval (str): Data interval (e.g., "1m" for one minute)
    
    Example:
        request = MarketDataRequest(
            symbol="^GDAXI",
            period="1d",
            interval="1m"
        )
    """
    symbol: str = Field(..., min_length=1, description="Trading symbol (e.g., ^GDAXI)")
    period: str = Field(
        default="1d",
        pattern="^[0-9]+[dmo]$",
        description="Time period (e.g., 1d, 5m, 1mo)"
    )
    interval: str = Field(
        default="1m",
        pattern="^[0-9]+[mh]$",
        description="Data interval (e.g., 1m, 5m, 1h)"
    )

    @field_validator('symbol')
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """
        Validates and normalizes the trading symbol.
        
        Args:
            v (str): The symbol to validate
            
        Returns:
            str: Normalized symbol (uppercase, stripped)
            
        Raises:
            ValueError: If symbol is empty
        """
        if not v or len(v.strip()) < 1:
            raise ValueError('Symbol cannot be empty')
        return v.strip().upper()

    @field_validator('period')
    @classmethod
    def validate_period(cls, v: str) -> str:
        """
        Validates the time period format.
        
        Period must be a number followed by:
        - 'd' for days
        - 'm' for minutes
        - 'o' for months
        
        Args:
            v (str): The period to validate
            
        Returns:
            str: Validated period
            
        Raises:
            ValueError: If period format is invalid
        """
        if not v:
            raise ValueError('Period cannot be empty')
        valid_units = ['d', 'm', 'o']
        if not any(v.endswith(unit) for unit in valid_units):
            raise ValueError(f'Period must end with one of: {", ".join(valid_units)}')
        try:
            value = int(v[:-1])
            if value <= 0:
                raise ValueError('Period value must be positive')
        except ValueError:
            raise ValueError('Period must start with a positive number')
        return v

    @field_validator('interval')
    @classmethod
    def validate_interval(cls, v: str) -> str:
        """
        Validates the data interval format.
        
        Interval must be a number followed by:
        - 'm' for minutes
        - 'h' for hours
        
        Args:
            v (str): The interval to validate
            
        Returns:
            str: Validated interval
            
        Raises:
            ValueError: If interval format is invalid
        """
        if not v:
            raise ValueError('Interval cannot be empty')
        valid_units = ['m', 'h']
        if not any(v.endswith(unit) for unit in valid_units):
            raise ValueError(f'Interval must end with one of: {", ".join(valid_units)}')
        try:
            value = int(v[:-1])
            if value <= 0:
                raise ValueError('Interval value must be positive')
        except ValueError:
            raise ValueError('Interval must start with a positive number')
        return v

class SignalRequest(BaseModel):
    """
    Pydantic model for signal generation requests.
    
    Validates requests for generating trading signals for multiple symbols.
    
    Attributes:
        symbols (List[str]): List of trading symbols to analyze
        
    Example:
        request = SignalRequest(symbols=["^GDAXI", "^DAX"])
    """
    symbols: List[str] = Field(..., min_items=1, description="List of trading symbols")

    @field_validator('symbols')
    @classmethod
    def validate_symbols(cls, v: List[str]) -> List[str]:
        """
        Validates each symbol in the list.
        
        Args:
            v (List[str]): List of symbols to validate
            
        Returns:
            List[str]: List of normalized symbols
            
        Raises:
            ValueError: If list is empty or contains invalid symbols
        """
        if not v:
            raise ValueError('Symbols list cannot be empty')
        clean_symbols = []
        for symbol in v:
            if not symbol or len(symbol.strip()) < 1:
                raise ValueError('Individual symbols cannot be empty')
            clean_symbols.append(symbol.strip().upper())
        return clean_symbols

class Signal(BaseModel):
    """
    Pydantic model representing a trading signal.
    
    This model defines the structure of a trading signal with validation rules
    for each field.
    
    Attributes:
        symbol (str): Trading symbol the signal is for
        signal_type (str): Type of signal (BUY, SELL, HOLD, NEUTRAL)
        close_price (float): Latest closing price when signal was generated
        confidence (float): Signal confidence score (0.0 to 1.0)
        timestamp (datetime): When the signal was generated
        reason (str): Explanation for the signal
        
    Example:
        signal = Signal(
            symbol="^GDAXI",
            signal_type="BUY",
            close_price=15000.0,
            confidence=0.95,
            timestamp=datetime.now(),
            reason="Strong bullish pattern"
        )
    """
    symbol: str = Field(..., min_length=1)
    signal_type: str = Field(..., pattern="^(BUY|SELL|HOLD|NEUTRAL)$")
    close_price: float = Field(..., gt=0)
    confidence: float = Field(..., ge=0.0, le=1.0)
    timestamp: datetime
    reason: str = Field(..., min_length=1)

    @field_validator('reason')
    @classmethod
    def validate_reason(cls, v: str) -> str:
        """Validates and normalizes the signal reason"""
        if not v or len(v.strip()) < 1:
            raise ValueError('Reason cannot be empty')
        return v.strip()

    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: datetime) -> datetime:
        """Ensures timestamp is not in the future"""
        if v > datetime.now():
            raise ValueError('Timestamp cannot be in the future')
        return v

class NotificationRequest(BaseModel):
    """
    Pydantic model for notification requests.
    
    Wraps a Signal model for notification purposes.
    
    Attributes:
        signal (Signal): The trading signal to send as notification
        
    Example:
        request = NotificationRequest(signal=signal_object)
    """
    signal: Signal

# Initialize components with error handling
try:
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
    DB_PATH = os.environ.get("DB_PATH", "market_data.db")

    data_collector = DataCollector(DB_PATH)
    technical_analyzer = TechnicalAnalyzer(DB_PATH)
    signal_generator = SignalGenerator(DB_PATH, confidence_threshold=0.6)  # Lower threshold for testing
    notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
except Exception as e:
    logger.error(f"Failed to initialize components: {str(e)}")
    raise

# Error handler middleware
@app.middleware("http")
async def error_handling_middleware(request, call_next):
    """
    Middleware for handling errors globally.
    
    This middleware catches unhandled exceptions in the request processing pipeline
    and returns a standardized error response.
    
    Args:
        request: The incoming request
        call_next: The next middleware or request handler in the pipeline
        
    Returns:
        Response: The response from the next handler, or an error response
    """
    try:
        return await call_next(request)
    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal server error", "detail": str(e)}
        )

@app.post("/market-data", response_model=Dict[str, Any])
async def collect_market_data(request: MarketDataRequest):
    """
    Collects market data for a given symbol from Yahoo Finance.
    
    This endpoint fetches historical market data based on the provided parameters.
    It handles data validation and various error cases.
    
    Args:
        request (MarketDataRequest): Request containing symbol and time parameters
        
    Returns:
        JSONResponse: Market data in the following format:
            {
                "success": bool,
                "symbol": str,
                "period": str,
                "interval": str,
                "data": List[dict]  # List of OHLCV data points
            }
            
    Raises:
        HTTPException: With appropriate status codes:
            - 422: Validation error
            - 404: No data found
            - 400: Invalid value
            - 500: Internal server error
            
    Example:
        POST /market-data
        {
            "symbol": "^GDAXI",
            "period": "1d",
            "interval": "1m"
        }
    """
    try:
        logger.info(f"Collecting market data for {request.symbol}")
        result = data_collector.fetch_market_data(request.symbol, request.period, request.interval)
        
        if result is None or (isinstance(result, bool) and not result):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No data found for symbol {request.symbol}"
            )
            
        # Convert result to serializable format if it's a pandas DataFrame
        if hasattr(result, 'to_dict'):
            result = result.to_dict(orient='records')
            
        return JSONResponse(content={
            "success": True,
            "symbol": request.symbol,
            "period": request.period,
            "interval": request.interval,
            "data": result
        })
    except ValidationError as ve:
        logger.error(f"Validation error: {str(ve)}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(ve)
        )
    except ValueError as ve:
        logger.error(f"Value error: {str(ve)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except Exception as e:
        logger.error(f"Error collecting market data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to collect market data: {str(e)}"
        )

@app.post("/news", response_model=Dict[str, Any])
async def collect_news(symbol: str):
    """Sammelt Nachrichtendaten für ein Symbol von Yahoo Finance"""
    try:
        logger.info(f"Collecting news for {symbol}")
        result = data_collector.fetch_news(symbol)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No news found for symbol {symbol}"
            )
        return {"success": result, "symbol": symbol}
    except Exception as e:
        logger.error(f"Error collecting news: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to collect news: {str(e)}"
        )

@app.get("/analysis/{symbol}", response_model=Dict[str, Any])
async def analyze_symbol(symbol: str):
    """Führt eine technische Analyse für ein Symbol durch"""
    try:
        if not symbol or len(symbol.strip()) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Symbol cannot be empty"
            )

        symbol = symbol.strip().upper()
        logger.info(f"Analyzing symbol {symbol}")
        
        # Check if we have data for this symbol
        if not data_collector.has_symbol_data(symbol):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No data found for symbol {symbol}"
            )
        
        # Perform technical analysis
        result = technical_analyzer.analyze_symbol(symbol)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No analysis data available for symbol {symbol}"
            )
        
        # Convert result to serializable format if needed
        if isinstance(result, (dict, list)):
            return JSONResponse(content=json.loads(json.dumps(result, cls=DateTimeEncoder)))
        else:
            return JSONResponse(content={"error": f"Unexpected analysis result type: {type(result)}"})
            
    except ValidationError as ve:
        logger.error(f"Validation error: {str(ve)}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(ve)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing symbol: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to analyze symbol: {str(e)}"
        )

@app.post("/signals", response_model=Dict[str, Any])
async def generate_trading_signals(request: SignalRequest):
    """Generates trading signals for a list of symbols
    
    This endpoint combines technical analysis and sentiment data to generate
    trading signals with confidence scores.
    """
    try:
        if not request.symbols:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Symbols list cannot be empty"
            )
        
        logger.info(f"Generating signals for {len(request.symbols)} symbols")
        signals = []
        
        # First update market data for all symbols
        for symbol in request.symbols:
            try:
                success = data_collector.collect_data(symbol, period="1d", interval="1m")
                if not success:
                    logger.warning(f"Failed to update market data for {symbol}")
            except Exception as e:
                logger.warning(f"Error updating market data for {symbol}: {str(e)}")
        
        # Then generate signals with fresh data
        for symbol in request.symbols:
            try:
                # Run technical analysis on fresh data
                technical_result = technical_analyzer.analyze_symbol(symbol)
                if not technical_result:
                    logger.warning(f"No technical analysis available for {symbol}")
                    continue
                    
                # Generate signals
                signal = signal_generator.generate_signals([symbol])
                if signal:
                    # Add technical analysis details to the signal
                    for s in signal:
                        s['technical_analysis'] = {
                            'rsi': technical_result['indicators']['rsi'],
                            'sma_20': technical_result['indicators']['sma_20'],
                            'sma_50': technical_result['indicators']['sma_50'],
                            'macd_line': technical_result['indicators']['macd_line'],
                            'signal_line': technical_result['indicators']['signal_line']
                        }
                    signals.extend(signal)
                    logger.info(f"Generated signal for {symbol}: {signal[0]['signal_type']} with confidence {signal[0]['confidence']:.2f}")
            except Exception as e:
                logger.warning(f"Failed to generate signal for {symbol}: {str(e)}")
                continue
                
        if not signals:
            return {
                "success": False,
                "signals": [],
                "count": 0,
                "message": "No valid signals could be generated for the provided symbols"
            }
            
        return {
            "success": True,
            "signals": json.loads(json.dumps(signals, cls=DateTimeEncoder)),
            "count": len(signals),
            "message": f"Generated {len(signals)} valid trading signals"
        }
        
    except ValidationError as ve:
        logger.error(f"Validation error: {str(ve)}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(ve)
        )
    except ValueError as ve:
        logger.error(f"Value error: {str(ve)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except Exception as e:
        logger.error(f"Error generating signals: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate signals: {str(e)}"
        )

@app.post("/notify", response_model=Dict[str, Any])
async def send_signal_notification(request: NotificationRequest):
    """Sendet eine Signal-Benachrichtigung über Telegram"""
    try:
        logger.info(f"Sending notification for {request.signal.symbol}")
        
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=json.loads(json.dumps({
                    "success": False,
                    "error": "Telegram notifications not configured",
                    "signal": request.signal.dict()
                }, cls=DateTimeEncoder))
            )
        
        # Convert signal to dict and encode datetime
        signal_dict = json.loads(json.dumps(request.signal.dict(), cls=DateTimeEncoder))
        success = notifier.send_signal(signal_dict)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to send notification"
            )
            
        return JSONResponse(content={
            "success": True,
            "signal": signal_dict
        })
    except ValidationError as ve:
        logger.error(f"Validation error: {str(ve)}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(ve)
        )
    except TelegramError as e:
        logger.error(f"Telegram error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to send Telegram notification: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error sending notification: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to send notification: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Trading Signal Server")
    uvicorn.run(app, host="0.0.0.0", port=8000)
