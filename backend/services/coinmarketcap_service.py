import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import os

logger = logging.getLogger(__name__)

class CoinMarketCapService:
    """Service for fetching cryptocurrency data from CoinMarketCap API"""
    
    def __init__(self):
        self.base_url = "https://pro-api.coinmarketcap.com/v1"
        self.api_key = os.getenv('COINMARKETCAP_API_KEY')
        self.headers = {
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json',
            'Accept-Encoding': 'deflate, gzip'
        }
        self.session = None
        self.available = bool(self.api_key)
        self.rate_limit_delay = 0.1  # CoinMarketCap has generous rate limits
        self.last_request_time = 0
        
        # Connection pool settings for better performance
        self.connector_limit = 20
        self.connector_limit_per_host = 10
        
    async def _get_session(self):
        """Get or create high-performance aiohttp session"""
        if self.session is None:
            connector = aiohttp.TCPConnector(
                limit=self.connector_limit,
                limit_per_host=self.connector_limit_per_host,
                ttl_dns_cache=300,
                use_dns_cache=True,
                keepalive_timeout=30,
                enable_cleanup_closed=True
            )
            timeout = aiohttp.ClientTimeout(total=15, connect=5)
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=self.headers
            )
        return self.session
    
    async def _rate_limited_request(self, url: str, params: dict = None) -> Optional[Dict]:
        """Make optimized rate-limited request to CoinMarketCap"""
        try:
            # Minimal rate limiting for CoinMarketCap (they have generous limits)
            current_time = asyncio.get_event_loop().time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.rate_limit_delay:
                await asyncio.sleep(self.rate_limit_delay - time_since_last)
            
            session = await self._get_session()
            async with session.get(url, params=params) as response:
                self.last_request_time = asyncio.get_event_loop().time()
                
                if response.status == 429:  # Rate limited
                    logger.warning("CoinMarketCap rate limited, waiting...")
                    await asyncio.sleep(1.0)
                    return None
                
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    logger.error(f"CoinMarketCap error: {response.status} - {error_text}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error making CoinMarketCap request: {e}")
            return None
    
    async def get_listings_latest(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get latest cryptocurrency listings from CoinMarketCap"""
        try:
            if not self.available:
                logger.warning("CoinMarketCap API key not available")
                return []
            
            logger.info(f"Fetching {limit} latest listings from CoinMarketCap")
            url = f"{self.base_url}/cryptocurrency/listings/latest"
            
            params = {
                'limit': min(limit, 5000),  # CoinMarketCap supports up to 5000
                'convert': 'USD',
                'sort': 'market_cap',
                'sort_dir': 'desc'
            }
            
            data = await self._rate_limited_request(url, params)
            if not data or 'data' not in data:
                return []
            
            # Convert to our format
            crypto_data = []
            for item in data['data']:
                try:
                    converted = self._convert_listing_data(item)
                    if converted:
                        crypto_data.append(converted)
                except Exception as e:
                    logger.warning(f"Error converting CoinMarketCap listing {item.get('symbol', 'unknown')}: {e}")
                    continue
            
            logger.info(f"Successfully fetched {len(crypto_data)} listings from CoinMarketCap")
            return crypto_data
            
        except Exception as e:
            logger.error(f"Error fetching CoinMarketCap listings: {e}")
            return []
    
    async def get_quotes_latest(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get latest quotes for specific symbols"""
        try:
            if not self.available or not symbols:
                return []
            
            # CoinMarketCap supports up to 120 symbols per request
            batch_size = 100
            all_data = []
            
            for i in range(0, len(symbols), batch_size):
                batch_symbols = symbols[i:i + batch_size]
                
                url = f"{self.base_url}/cryptocurrency/quotes/latest"
                params = {
                    'symbol': ','.join(batch_symbols),
                    'convert': 'USD'
                }
                
                data = await self._rate_limited_request(url, params)
                if data and 'data' in data:
                    for symbol, quote_data in data['data'].items():
                        try:
                            converted = self._convert_quote_data(quote_data)
                            if converted:
                                all_data.append(converted)
                        except Exception as e:
                            logger.warning(f"Error converting quote for {symbol}: {e}")
                            continue
                
                # Small delay between batches
                if i + batch_size < len(symbols):
                    await asyncio.sleep(0.1)
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error fetching CoinMarketCap quotes: {e}")
            return []
    
    async def get_comprehensive_data(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get comprehensive cryptocurrency data from CoinMarketCap"""
        try:
            if not self.available:
                logger.warning("CoinMarketCap API key not available")
                return []
            
            logger.info(f"Fetching comprehensive data from CoinMarketCap (limit: {limit})")
            
            # Use listings/latest endpoint which provides comprehensive data
            data = await self.get_listings_latest(limit)
            
            logger.info(f"Retrieved {len(data)} comprehensive records from CoinMarketCap")
            return data
            
        except Exception as e:
            logger.error(f"Error getting comprehensive CoinMarketCap data: {e}")
            return []
    
    def _convert_listing_data(self, listing: Dict) -> Optional[Dict[str, Any]]:
        """Convert CoinMarketCap listing data to our standard format"""
        try:
            symbol = listing.get('symbol', '').upper()
            name = listing.get('name', symbol)
            
            if not symbol:
                return None
            
            # Extract USD quote data
            quote_data = listing.get('quote', {}).get('USD', {})
            
            converted = {
                'symbol': symbol,
                'name': name,
                'price_usd': float(quote_data.get('price', 0)),
                'market_cap_usd': quote_data.get('market_cap'),
                'volume_24h_usd': quote_data.get('volume_24h'),
                'percent_change_1h': quote_data.get('percent_change_1h'),
                'percent_change_24h': quote_data.get('percent_change_24h'),
                'percent_change_7d': quote_data.get('percent_change_7d'),
                'percent_change_30d': quote_data.get('percent_change_30d'),
                'source': 'coinmarketcap',
                'data_sources': ['coinmarketcap'],
                'last_updated': datetime.utcnow(),
                'api_source': 'coinmarketcap_listings',
                'cmc_rank': listing.get('cmc_rank'),
                'circulating_supply': listing.get('circulating_supply'),
                'total_supply': listing.get('total_supply'),
                'max_supply': listing.get('max_supply'),
                'last_updated_cmc': listing.get('last_updated')
            }
            
            return converted
            
        except Exception as e:
            logger.error(f"Error converting CoinMarketCap listing data: {e}")
            return None
    
    def _convert_quote_data(self, quote: Dict) -> Optional[Dict[str, Any]]:
        """Convert CoinMarketCap quote data to our standard format"""
        try:
            symbol = quote.get('symbol', '').upper()
            name = quote.get('name', symbol)
            
            if not symbol:
                return None
            
            # Extract USD quote data
            quote_data = quote.get('quote', {}).get('USD', {})
            
            converted = {
                'symbol': symbol,
                'name': name,
                'price_usd': float(quote_data.get('price', 0)),
                'market_cap_usd': quote_data.get('market_cap'),
                'volume_24h_usd': quote_data.get('volume_24h'),
                'percent_change_1h': quote_data.get('percent_change_1h'),
                'percent_change_24h': quote_data.get('percent_change_24h'),
                'percent_change_7d': quote_data.get('percent_change_7d'),
                'percent_change_30d': quote_data.get('percent_change_30d'),
                'source': 'coinmarketcap',
                'data_sources': ['coinmarketcap'],
                'last_updated': datetime.utcnow(),
                'api_source': 'coinmarketcap_quotes'
            }
            
            return converted
            
        except Exception as e:
            logger.error(f"Error converting CoinMarketCap quote data: {e}")
            return None
    
    def is_available(self) -> bool:
        """Check if CoinMarketCap service is available"""
        return self.available and bool(self.api_key)
    
    async def close(self):
        """Close the session"""
        if self.session:
            await self.session.close()
            self.session = None