# OKX Crypto Spot Data API Client

A comprehensive Python client for fetching cryptocurrency spot data from OKX API with support for various timeframes, trading pairs, and date ranges.

## Features

- ✅ **Flexible Data Fetching**: Get OHLCV data with customizable date ranges and intervals
- ✅ **Multiple Time Intervals**: Support for 1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 1W, 1M, 3M
- ✅ **Secure Authentication**: HMAC-SHA256 signature-based authentication
- ✅ **Environment Variable Support**: Secure API key management
- ✅ **Error Handling**: Comprehensive error handling with custom exceptions
- ✅ **Data Validation**: Input validation for all parameters
- ✅ **Pandas Integration**: Returns data as pandas DataFrames for easy analysis
- ✅ **Additional Endpoints**: Get available instruments, ticker data, and more

## Installation

1. Clone or download this repository
2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Setup

1. **Get OKX API Credentials**:
   - Sign up at [OKX](https://www.okx.com)
   - Create API keys in your account settings
   - Note down your API Key, Secret Key, and Passphrase

2. **Configure Environment Variables**:
   - Copy `.env.example` to `.env`
   - Fill in your actual API credentials:
   ```env
   OKX_API_KEY=your_api_key_here
   OKX_API_SECRET=your_api_secret_here
   OKX_PASSPHRASE=your_passphrase_here
   ```

3. **Run the Example**:
   ```bash
   python utils.py
   ```

## Quick Start

```python
from utils import get_crypto_spot_data, get_recent_data, OKXClient

# Get recent BTC-USDT data (last 24 hours, 1-hour intervals)
df = get_recent_data('BTC-USDT', hours=24, interval='1H')
print(df.head())

# Get data with specific date range
from datetime import datetime, timedelta

end_time = datetime.now()
start_time = end_time - timedelta(days=7)

df = get_crypto_spot_data(
    symbol='ETH-USDT',
    interval='4H',
    start_time=start_time,
    end_time=end_time,
    limit=200
)
```

## API Reference

### Main Functions

#### `get_crypto_spot_data(symbol, interval='1m', start_time=None, end_time=None, limit=100, client=None)`

Fetch cryptocurrency spot data from OKX API.

**Parameters:**
- `symbol` (str): Trading pair symbol (e.g., 'BTC-USDT', 'ETH-USDT')
- `interval` (str): Time interval ('1m', '3m', '5m', '15m', '30m', '1H', '2H', '4H', '6H', '12H', '1D', '1W', '1M', '3M')
- `start_time` (optional): Start time (ISO string, datetime object, or Unix timestamp in ms)
- `end_time` (optional): End time (ISO string, datetime object, or Unix timestamp in ms)
- `limit` (int): Maximum number of candles to return (1-300, default 100)
- `client` (optional): OKXClient instance (if None, will create new instance)

**Returns:**
- `pandas.DataFrame`: OHLCV data with columns ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'volume_currency', 'volume_currency_pair', 'confirm']

#### `get_recent_data(symbol, hours=24, interval='1H', client=None)`

Convenience function to get recent data for the last N hours.

#### `get_available_instruments(client=None)`

Get list of available trading instruments from OKX.

#### `get_ticker(symbol, client=None)`

Get current ticker data for a symbol.

### OKXClient Class

#### `OKXClient(api_key=None, api_secret=None, passphrase=None, base_url=None)`

Initialize OKX API client with authentication credentials.

## Supported Time Intervals

| Interval | Description |
|----------|-------------|
| 1m       | 1 minute    |
| 3m       | 3 minutes   |
| 5m       | 5 minutes   |
| 15m      | 15 minutes  |
| 30m      | 30 minutes  |
| 1H       | 1 hour      |
| 2H       | 2 hours     |
| 4H       | 4 hours     |
| 6H       | 6 hours     |
| 12H      | 12 hours    |
| 1D       | 1 day       |
| 1W       | 1 week      |
| 1M       | 1 month     |
| 3M       | 3 months    |

## Error Handling

The client includes comprehensive error handling:

- `OKXAPIError`: Raised for API-related errors
- `ValueError`: Raised for invalid parameters
- `requests.exceptions.RequestException`: Raised for network issues

```python
from utils import OKXAPIError

try:
    df = get_crypto_spot_data('BTC-USDT', interval='1H')
except OKXAPIError as e:
    print(f"API Error: {e}")
except ValueError as e:
    print(f"Invalid parameter: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Examples

### Basic Usage

```python
# Get recent data
df = get_recent_data('BTC-USDT', hours=24, interval='1H')

# Get data with specific parameters
df = get_crypto_spot_data(
    symbol='ETH-USDT',
    interval='4H',
    start_time='2024-01-01T00:00:00Z',
    end_time='2024-01-07T23:59:59Z',
    limit=200
)
```

### Advanced Usage

```python
# Initialize client with custom settings
client = OKXClient(
    api_key='your_key',
    api_secret='your_secret',
    passphrase='your_passphrase'
)

# Use custom client
df = get_crypto_spot_data('BTC-USDT', interval='1D', client=client)

# Get available instruments
instruments = get_available_instruments(client)
print(instruments[['instId', 'baseCcy', 'quoteCcy']].head())

# Get current ticker
ticker = get_ticker('BTC-USDT', client)
print(f"Current price: ${float(ticker['last']):,.2f}")
```

## Data Format

The returned DataFrame contains the following columns:

- `timestamp`: UTC timestamp (datetime64[ns, UTC])
- `open`: Opening price (float64)
- `high`: Highest price (float64)
- `low`: Lowest price (float64)
- `close`: Closing price (float64)
- `volume`: Trading volume (float64)
- `volume_currency`: Volume in base currency (float64)
- `volume_currency_pair`: Volume in quote currency (float64)
- `confirm`: Confirmation status (int64)

## Security Best Practices

1. **Never commit API keys**: Always use `.env` files and add them to `.gitignore`
2. **Use environment variables**: Store credentials in environment variables
3. **Rotate keys regularly**: Change your API keys periodically
4. **Limit permissions**: Only grant necessary permissions to your API keys
5. **Monitor usage**: Regularly check your API usage and logs

## Rate Limits

OKX API has rate limits. The client includes proper error handling for rate limit responses. For production use, consider implementing:

- Request throttling
- Exponential backoff
- Caching mechanisms

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve this client.

## License

This project is open source and available under the MIT License.

## Disclaimer

This software is for educational and research purposes. Always comply with OKX's terms of service and API usage policies. Cryptocurrency trading involves risk, and past performance does not guarantee future results.
