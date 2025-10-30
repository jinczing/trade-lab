"""
OKX API Client for Cryptocurrency Spot Data

This module provides functions to fetch cryptocurrency spot data from OKX API
with support for various timeframes, trading pairs, and date ranges.
"""

import os
import time
import hmac
import hashlib
import base64
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, List, Dict, Union
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class OKXAPIError(Exception):
    """Custom exception for OKX API errors"""
    pass


class OKXClient:
    """
    OKX API client for fetching cryptocurrency spot data
    """
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None, 
                 passphrase: Optional[str] = None, base_url: Optional[str] = None):
        """
        Initialize OKX API client
        
        Args:
            api_key: OKX API key (if None, will try to load from environment)
            api_secret: OKX API secret (if None, will try to load from environment)
            passphrase: OKX API passphrase (if None, will try to load from environment)
            base_url: OKX API base URL (defaults to production)
        """
        self.api_key = api_key or os.getenv('OKX_API_KEY')
        self.api_secret = api_secret or os.getenv('OKX_API_SECRET')
        self.passphrase = passphrase or os.getenv('OKX_PASSPHRASE')
        self.base_url = base_url or os.getenv('OKX_BASE_URL', 'https://www.okx.com')
        self.timeout = int(os.getenv('OKX_TIMEOUT', '30'))
        
        if not all([self.api_key, self.api_secret, self.passphrase]):
            raise ValueError(
                "API credentials not provided. Please set OKX_API_KEY, "
                "OKX_API_SECRET, and OKX_PASSPHRASE environment variables "
                "or pass them directly to the constructor."
            )
    
    def _generate_signature(self, timestamp: str, method: str, request_path: str, body: str = '') -> str:
        """
        Generate OKX API signature
        
        Args:
            timestamp: Current timestamp in ISO format
            method: HTTP method (GET, POST, etc.)
            request_path: API endpoint path
            body: Request body (empty for GET requests)
            
        Returns:
            Base64 encoded signature
        """
        message = timestamp + method + request_path + body
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()
        return base64.b64encode(signature).decode('utf-8')
    
    def _get_headers(self, method: str, request_path: str, body: str = '') -> Dict[str, str]:
        """
        Generate headers for OKX API request
        
        Args:
            method: HTTP method
            request_path: API endpoint path
            body: Request body
            
        Returns:
            Dictionary of headers
        """
        timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        signature = self._generate_signature(timestamp, method, request_path, body)
        
        return {
            'OK-ACCESS-KEY': self.api_key,
            'OK-ACCESS-SIGN': signature,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        }
    
    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None, 
                     data: Optional[Dict] = None) -> Dict:
        """
        Make authenticated request to OKX API
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            
        Returns:
            API response data
            
        Raises:
            OKXAPIError: If API request fails
        """
        url = f"{self.base_url}{endpoint}"
        body = ''
        
        if data:
            body = str(data)
        
        headers = self._get_headers(method, endpoint, body)
        
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            
            if result.get('code') != '0':
                raise OKXAPIError(f"API Error {result.get('code')}: {result.get('msg')}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            raise OKXAPIError(f"Request failed: {str(e)}")
        except ValueError as e:
            raise OKXAPIError(f"Invalid JSON response: {str(e)}")


def get_crypto_spot_data(
    symbol: str,
    interval: str = '1m',
    start_time: Optional[Union[str, datetime, int]] = None,
    end_time: Optional[Union[str, datetime, int]] = None,
    limit: int = 100,
    client: Optional[OKXClient] = None
) -> pd.DataFrame:
    """
    Fetch cryptocurrency spot data from OKX API
    
    Args:
        symbol: Trading pair symbol (e.g., 'BTC-USDT', 'ETH-USDT')
        interval: Time interval ('1m', '3m', '5m', '15m', '30m', '1H', '2H', '4H', '6H', '12H', '1D', '1W', '1M', '3M')
        start_time: Start time (ISO string, datetime object, or Unix timestamp in ms)
        end_time: End time (ISO string, datetime object, or Unix timestamp in ms)
        limit: Maximum number of candles to return (1-300, default 100)
        client: OKXClient instance (if None, will create new instance)
        
    Returns:
        pandas.DataFrame: OHLCV data with columns ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'volume_currency', 'volume_currency_pair', 'confirm']
        
    Raises:
        OKXAPIError: If API request fails
        ValueError: If parameters are invalid
    """
    if client is None:
        client = OKXClient()
    
    # Validate interval
    valid_intervals = ['1m', '3m', '5m', '15m', '30m', '1H', '2H', '4H', '6H', '12H', '1D', '1W', '1M', '3M']
    if interval not in valid_intervals:
        raise ValueError(f"Invalid interval '{interval}'. Must be one of: {valid_intervals}")
    
    # Validate limit
    if not 1 <= limit <= 300:
        raise ValueError("Limit must be between 1 and 300")
    
    # Convert time parameters to Unix timestamp in milliseconds
    def to_timestamp_ms(time_input):
        if time_input is None:
            return None
        if isinstance(time_input, int):
            return time_input
        if isinstance(time_input, str):
            dt = datetime.fromisoformat(time_input.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        if isinstance(time_input, datetime):
            return int(time_input.timestamp() * 1000)
        raise ValueError(f"Invalid time format: {type(time_input)}")
    
    start_ts = to_timestamp_ms(start_time)
    end_ts = to_timestamp_ms(end_time)
    
    # Prepare request parameters
    params = {
        'instId': symbol,
        'bar': interval,
        'limit': str(limit)
    }
    
    # OKX API logic: 
    # - 'before': get candles before this timestamp (most recent data)
    # - 'after': get candles after this timestamp (older data)
    # For recent data, we typically only use 'before' parameter
    
    # Always use 'before' if end_time is specified
    if end_ts:
        params['before'] = str(end_ts)
    
    # Only use 'after' for historical data requests (> 1 day)
    # For recent data (last 24 hours), omit 'after' to avoid conflicts
    if start_ts and start_time and end_time:
        time_diff_hours = (end_time - start_time).total_seconds() / 3600
        if time_diff_hours > 24:  # Only for requests > 24 hours
            params['after'] = str(start_ts)
    
    # Make API request
    response = client._make_request('GET', '/api/v5/market/candles', params=params)
    
    # Convert response to DataFrame
    if not response.get('data'):
        return pd.DataFrame()
    
    df = pd.DataFrame(response['data'], columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume', 
        'volume_currency', 'volume_currency_pair', 'confirm'
    ])
    
    # Convert data types
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'volume_currency', 'volume_currency_pair']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df['confirm'] = df['confirm'].astype(int)
    
    # Sort by timestamp (oldest first)
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    return df


def get_available_instruments(client: Optional[OKXClient] = None) -> pd.DataFrame:
    """
    Get list of available trading instruments from OKX
    
    Args:
        client: OKXClient instance (if None, will create new instance)
        
    Returns:
        pandas.DataFrame: Available instruments with their details
    """
    if client is None:
        client = OKXClient()
    
    response = client._make_request('GET', '/api/v5/public/instruments', params={'instType': 'SPOT'})
    
    if not response.get('data'):
        return pd.DataFrame()
    
    df = pd.DataFrame(response['data'])
    
    # Convert numeric columns
    numeric_columns = ['minSz', 'lotSz', 'tickSz', 'minPx', 'maxPx']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def get_ticker(symbol: str, client: Optional[OKXClient] = None) -> Dict:
    """
    Get current ticker data for a symbol
    
    Args:
        symbol: Trading pair symbol (e.g., 'BTC-USDT')
        client: OKXClient instance (if None, will create new instance)
        
    Returns:
        dict: Ticker data
    """
    if client is None:
        client = OKXClient()
    
    response = client._make_request('GET', '/api/v5/market/ticker', params={'instId': symbol})
    
    if response.get('data'):
        return response['data'][0]
    return {}


# Example usage and utility functions
def get_recent_data(symbol: str, hours: int = 24, interval: str = '1H', 
                   client: Optional[OKXClient] = None) -> pd.DataFrame:
    """
    Convenience function to get recent data for the last N hours
    
    Args:
        symbol: Trading pair symbol
        hours: Number of hours to look back
        interval: Time interval
        client: OKXClient instance
        
    Returns:
        pandas.DataFrame: Recent OHLCV data
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - pd.Timedelta(hours=hours)
    
    return get_crypto_spot_data(
        symbol=symbol,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        client=client
    )


if __name__ == "__main__":
    # Example usage
    try:
        # Initialize client
        client = OKXClient()
        
        # Get recent BTC-USDT data
        print("Fetching recent BTC-USDT data...")
        df = get_recent_data('BTC-USDT', hours=24, interval='1H')
        print(f"Retrieved {len(df)} data points")
        print(df.head())
        
        # Get available instruments
        print("\nFetching available instruments...")
        instruments = get_available_instruments()
        print(f"Found {len(instruments)} spot trading pairs")
        print(instruments[['instId', 'baseCcy', 'quoteCcy']].head())
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure to set up your .env file with OKX API credentials")
