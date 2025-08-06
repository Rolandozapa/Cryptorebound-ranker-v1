import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class BitfinexService:
    """Service for fetching cryptocurrency data from Bitfinex API"""
    
    def __init__(self):
        self.base_url = "https://api-pub.bitfinex.com/v2"
        self.session = None
        self.available = True  # Public API, no authentication required
        self.rate_limit_delay = 0.7  # ~1.5 requests per second to stay under limits
        self.last_request_time = 0
        
    async def _get_session(self):
        """Get or create aiohttp session"""
        if self.session is None:
            connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300, use_dns_cache=True)
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={
                    'Accept': 'application/json',
                    'User-Agent': 'CryptoRebound/1.0'
                }
            )
        return self.session
    
    async def _rate_limited_request(self, url: str, params: dict = None) -> Optional[Any]:
        """Make rate-limited request to Bitfinex"""
        try:
            # Implement rate limiting
            current_time = asyncio.get_event_loop().time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.rate_limit_delay:
                await asyncio.sleep(self.rate_limit_delay - time_since_last)
            
            session = await self._get_session()
            async with session.get(url, params=params) as response:
                self.last_request_time = asyncio.get_event_loop().time()
                
                if response.status == 429:  # Rate limited
                    logger.warning("Bitfinex rate limited, increasing delay")
                    self.rate_limit_delay = min(self.rate_limit_delay * 2, 10.0)
                    await asyncio.sleep(self.rate_limit_delay)
                    return None
                
                if response.status == 200:
                    self.rate_limit_delay = max(self.rate_limit_delay * 0.9, 0.7)  # Reduce delay on success
                    return await response.json()
                else:
                    logger.error(f"Bitfinex error: {response.status} - {await response.text()}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error making Bitfinex request: {e}")
            return None
    
    async def get_tickers(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """Get ticker data from Bitfinex"""
        try:
            logger.info("Fetching tickers from Bitfinex")
            
            if symbols:
                # Get specific symbols
                url = f"{self.base_url}/tickers"
                symbol_params = ','.join([f't{symbol.upper()}USD' for symbol in symbols])
                params = {'symbols': symbol_params}
            else:
                # Get all tickers
                url = f"{self.base_url}/tickers?symbols=ALL"
                params = None
            
            data = await self._rate_limited_request(url, params)
            if not data or not isinstance(data, list):
                return []
            
            # Convert to our format
            crypto_data = []
            for ticker in data:
                try:
                    converted = self._convert_ticker_data(ticker)
                    if converted:
                        crypto_data.append(converted)
                except Exception as e:
                    logger.warning(f"Error converting Bitfinex ticker: {e}")
                    continue
            
            logger.info(f"Successfully processed {len(crypto_data)} tickers from Bitfinex")
            return crypto_data
            
        except Exception as e:
            logger.error(f"Error fetching Bitfinex tickers: {e}")
            return []
    
    async def get_comprehensive_data(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get comprehensive cryptocurrency data from Bitfinex"""
        try:
            logger.info(f"Fetching comprehensive data from Bitfinex (limit: {limit})")
            
            # Get all tickers first
            tickers = await self.get_tickers()
            
            if not tickers:
                logger.warning("No ticker data from Bitfinex")
                return []
            
            # Filter to USD pairs and apply limit
            usd_tickers = []
            for ticker in tickers:
                if ticker.get('symbol', '').endswith('USD') or 'USD' in ticker.get('symbol', ''):
                    usd_tickers.append(ticker)
                    if len(usd_tickers) >= limit:
                        break
            
            logger.info(f"Retrieved {len(usd_tickers)} comprehensive records from Bitfinex")
            return usd_tickers
            
        except Exception as e:
            logger.error(f"Error getting comprehensive Bitfinex data: {e}")
            return []
    
    def _convert_ticker_data(self, ticker: List) -> Optional[Dict[str, Any]]:
        """Convert Bitfinex ticker data to our standard format"""
        try:
            # Bitfinex ticker format: [SYMBOL, BID, BID_SIZE, ASK, ASK_SIZE, DAILY_CHANGE, 
            #                         DAILY_CHANGE_RELATIVE, LAST_PRICE, VOLUME, HIGH, LOW]
            if not ticker or len(ticker) < 8:
                return None
            
            symbol_full = ticker[0]  # e.g., 'tBTCUSD'
            
            # Extract symbol (remove 't' prefix and 'USD' suffix)
            if symbol_full.startswith('t') and symbol_full.endswith('USD'):
                symbol = symbol_full[1:-3]  # Remove 't' and 'USD'
            else:
                # Skip non-USD pairs for now
                return None
            
            if not symbol:
                return None
            
            last_price = float(ticker[7]) if ticker[7] else 0
            daily_change = float(ticker[5]) if ticker[5] else 0
            daily_change_perc = float(ticker[6]) * 100 if ticker[6] else 0  # Convert to percentage
            volume = float(ticker[8]) if ticker[8] else 0
            high = float(ticker[9]) if ticker[9] else 0
            low = float(ticker[10]) if ticker[10] else 0
            
            converted = {
                'symbol': symbol,
                'name': symbol,  # Bitfinex doesn't provide full names
                'price_usd': last_price,
                'market_cap_usd': None,  # Not provided by Bitfinex tickers
                'volume_24h_usd': volume * last_price if volume and last_price else 0,  # Convert volume to USD
                'percent_change_1h': None,  # Not provided
                'percent_change_24h': daily_change_perc,
                'percent_change_7d': None,  # Not provided
                'percent_change_30d': None,  # Not provided
                'source': 'bitfinex',
                'data_sources': ['bitfinex'],
                'last_updated': datetime.utcnow(),
                'api_source': 'bitfinex_tickers',
                'daily_change': daily_change,
                'high_24h': high,
                'low_24h': low,
                'volume_24h': volume,
                'bid': float(ticker[1]) if ticker[1] else None,
                'ask': float(ticker[3]) if ticker[3] else None
            }
            
            return converted
            
        except Exception as e:
            logger.error(f"Error converting Bitfinex ticker data: {e}")
            return None
    
    async def get_candles(self, symbol: str, timeframe: str = '1D', limit: int = 100) -> List[Dict[str, Any]]:
        """Get historical candle data for a symbol"""
        try:
            symbol_formatted = f't{symbol.upper()}USD'
            url = f"{self.base_url}/candles/trade:{timeframe}:{symbol_formatted}/hist"
            
            params = {
                'limit': limit,
                'sort': -1  # Most recent first
            }
            
            data = await self._rate_limited_request(url, params)
            
            if not data or not isinstance(data, list):
                return []
            
            # Convert to our format
            candles = []
            for candle in data:
                try:
                    # Candle format: [MTS, OPEN, CLOSE, HIGH, LOW, VOLUME]
                    if len(candle) >= 6:
                        candle_data = {
                            'timestamp': candle[0],
                            'open': float(candle[1]),
                            'close': float(candle[2]),
                            'high': float(candle[3]),
                            'low': float(candle[4]),
                            'volume': float(candle[5])
                        }
                        candles.append(candle_data)
                except Exception as e:
                    logger.warning(f"Error processing candle data: {e}")
                    continue
            
            return candles
            
        except Exception as e:
            logger.error(f"Error getting candles for {symbol}: {e}")
            return []
    
    async def get_symbols(self) -> List[str]:
        """Get list of available trading symbols"""
        try:
            url = f"{self.base_url}/conf/pub:list:pair:exchange"
            data = await self._rate_limited_request(url)
            
            if not data or not isinstance(data, list) or len(data) < 1:
                return []
            
            # Extract symbols from the response
            symbols = data[0] if data[0] else []
            
            # Filter for USD pairs only
            usd_symbols = []
            for symbol in symbols:
                if isinstance(symbol, str) and symbol.endswith('USD'):
                    # Remove 'USD' suffix to get base symbol
                    base_symbol = symbol[:-3]
                    if len(base_symbol) >= 2:  # Valid symbol length
                        usd_symbols.append(base_symbol)
            
            logger.info(f"Retrieved {len(usd_symbols)} USD trading pairs from Bitfinex")
            return usd_symbols
            
        except Exception as e:
            logger.error(f"Error getting Bitfinex symbols: {e}")
            return []
    
    async def get_book(self, symbol: str, precision: str = 'P0') -> Dict[str, Any]:
        """Get order book for a symbol"""
        try:
            symbol_formatted = f't{symbol.upper()}USD'
            url = f"{self.base_url}/book/{symbol_formatted}/{precision}"
            
            data = await self._rate_limited_request(url)
            
            if not data or not isinstance(data, list):
                return {}
            
            # Separate bids and asks
            bids = []
            asks = []
            
            for entry in data:
                if len(entry) >= 3:
                    price = float(entry[0])
                    amount = float(entry[2])
                    
                    if amount > 0:
                        bids.append({'price': price, 'amount': amount})
                    else:
                        asks.append({'price': price, 'amount': abs(amount)})
            
            return {
                'bids': sorted(bids, key=lambda x: x['price'], reverse=True),
                'asks': sorted(asks, key=lambda x: x['price'])
            }
            
        except Exception as e:
            logger.error(f"Error getting order book for {symbol}: {e}")
            return {}
    
    def is_available(self) -> bool:
        """Check if Bitfinex service is available"""
        return self.available
    
    async def close(self):
        """Close the session"""
        if self.session:
            await self.session.close()
            self.session = None