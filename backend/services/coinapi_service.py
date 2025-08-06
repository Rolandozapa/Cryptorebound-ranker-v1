import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import os

logger = logging.getLogger(__name__)

class CoinAPIService:
    """Service for fetching cryptocurrency data from CoinAPI.io"""
    
    def __init__(self):
        self.base_url = "https://rest.coinapi.io/v1"
        self.api_key = os.getenv('COINAPI_KEY')
        self.headers = {
            'X-CoinAPI-Key': self.api_key,
            'Accept': 'application/json'
        }
        self.session = None
        self.available = bool(self.api_key)
        self.rate_limit_delay = 1.0  # 1 second between requests for free tier
        self.last_request_time = 0
        
    async def _get_session(self):
        """Get or create aiohttp session"""
        if self.session is None:
            connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300, use_dns_cache=True)
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=self.headers
            )
        return self.session
    
    async def _rate_limited_request(self, url: str, params: dict = None) -> Optional[Dict]:
        """Make rate-limited request to CoinAPI"""
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
                    logger.warning("CoinAPI rate limited, increasing delay")
                    self.rate_limit_delay = min(self.rate_limit_delay * 2, 10.0)
                    await asyncio.sleep(self.rate_limit_delay)
                    return None
                
                if response.status == 200:
                    self.rate_limit_delay = max(self.rate_limit_delay * 0.9, 1.0)  # Reduce delay on success
                    return await response.json()
                else:
                    logger.error(f"CoinAPI error: {response.status} - {await response.text()}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error making CoinAPI request: {e}")
            return None
    
    async def get_assets_list(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get list of assets from CoinAPI"""
        try:
            if not self.available:
                logger.warning("CoinAPI key not available")
                return []
            
            logger.info(f"Fetching {limit} assets from CoinAPI")
            url = f"{self.base_url}/assets"
            
            params = {
                'filter_asset_id': '',  # No filter, get all
            }
            
            data = await self._rate_limited_request(url, params)
            if not data or not isinstance(data, list):
                return []
            
            # Convert to our format
            assets = []
            for i, asset in enumerate(data):
                if i >= limit:
                    break
                    
                try:
                    converted = self._convert_asset_data(asset)
                    if converted:
                        assets.append(converted)
                except Exception as e:
                    logger.warning(f"Error converting CoinAPI asset {asset.get('asset_id', 'unknown')}: {e}")
                    continue
            
            logger.info(f"Successfully fetched {len(assets)} assets from CoinAPI")
            return assets
            
        except Exception as e:
            logger.error(f"Error fetching CoinAPI assets: {e}")
            return []
    
    async def get_exchange_rates(self, base_symbols: List[str] = None) -> List[Dict[str, Any]]:
        """Get current exchange rates for cryptocurrencies"""
        try:
            if not self.available:
                logger.warning("CoinAPI key not available")
                return []
            
            # Use USD as quote currency
            url = f"{self.base_url}/exchangerate/USD"
            data = await self._rate_limited_request(url)
            
            if not data or 'rates' not in data:
                return []
            
            # Convert rates to our format
            crypto_data = []
            for rate in data['rates']:
                try:
                    asset_id = rate.get('asset_id_quote', '').upper()
                    rate_value = rate.get('rate', 0)
                    
                    if not asset_id or rate_value <= 0:
                        continue
                    
                    # Convert rate to price (1 USD = X crypto means price = 1/X USD per crypto)
                    price_usd = 1.0 / rate_value if rate_value > 0 else 0
                    
                    converted = {
                        'symbol': asset_id,
                        'name': asset_id,  # CoinAPI doesn't provide names in exchange rates
                        'price_usd': price_usd,
                        'source': 'coinapi',
                        'data_sources': ['coinapi'],
                        'last_updated': datetime.utcnow(),
                        'api_source': 'coinapi_exchange_rates'
                    }
                    
                    crypto_data.append(converted)
                    
                except Exception as e:
                    logger.warning(f"Error converting CoinAPI exchange rate: {e}")
                    continue
            
            logger.info(f"Fetched {len(crypto_data)} exchange rates from CoinAPI")
            return crypto_data
            
        except Exception as e:
            logger.error(f"Error fetching CoinAPI exchange rates: {e}")
            return []
    
    async def get_comprehensive_data(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get comprehensive cryptocurrency data from CoinAPI"""
        try:
            if not self.available:
                logger.warning("CoinAPI key not available")
                return []
            
            logger.info(f"Fetching comprehensive data from CoinAPI (limit: {limit})")
            
            # Get assets first
            assets = await self.get_assets_list(limit)
            
            if not assets:
                logger.warning("No assets data from CoinAPI")
                return []
            
            # For now, return assets data as comprehensive data
            # In the future, we could enhance this by fetching additional metrics
            logger.info(f"Retrieved {len(assets)} comprehensive records from CoinAPI")
            return assets
            
        except Exception as e:
            logger.error(f"Error getting comprehensive CoinAPI data: {e}")
            return []
    
    def _convert_asset_data(self, asset: Dict) -> Optional[Dict[str, Any]]:
        """Convert CoinAPI asset data to our standard format"""
        try:
            asset_id = asset.get('asset_id', '').upper()
            name = asset.get('name', asset_id)
            
            if not asset_id:
                return None
            
            # CoinAPI assets don't include price data directly
            # We would need to make additional calls for current prices
            # For now, create basic structure
            
            converted = {
                'symbol': asset_id,
                'name': name,
                'price_usd': 0.0,  # Would need separate price call
                'market_cap_usd': None,
                'volume_24h_usd': None,
                'percent_change_1h': None,
                'percent_change_24h': None,
                'percent_change_7d': None,
                'percent_change_30d': None,
                'source': 'coinapi',
                'data_sources': ['coinapi'],
                'last_updated': datetime.utcnow(),
                'api_source': 'coinapi_assets',
                'type_is_crypto': asset.get('type_is_crypto', 0) == 1,
                'data_start': asset.get('data_start'),
                'data_end': asset.get('data_end')
            }
            
            return converted
            
        except Exception as e:
            logger.error(f"Error converting CoinAPI asset data: {e}")
            return None
    
    async def get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price for a specific symbol"""
        try:
            if not self.available:
                return None
            
            url = f"{self.base_url}/exchangerate/{symbol.upper()}/USD"
            data = await self._rate_limited_request(url)
            
            if data and 'rate' in data:
                return float(data['rate'])
                
            return None
            
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}")
            return None
    
    async def get_historical_data(self, symbol: str, period_days: int = 30) -> Dict[str, Any]:
        """Get historical data for a symbol"""
        try:
            if not self.available:
                return {}
            
            # CoinAPI historical data requires specific time ranges
            # This would need more complex implementation with OHLCV endpoints
            logger.info(f"Historical data not implemented for CoinAPI yet for {symbol}")
            return {}
            
        except Exception as e:
            logger.error(f"Error getting historical data for {symbol}: {e}")
            return {}
    
    def is_available(self) -> bool:
        """Check if CoinAPI service is available"""
        return self.available and bool(self.api_key)
    
    async def close(self):
        """Close the session"""
        if self.session:
            await self.session.close()
            self.session = None