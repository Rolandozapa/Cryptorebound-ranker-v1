import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

class CoinPaprikaService:
    """Service for fetching cryptocurrency data from CoinPaprika API"""
    
    def __init__(self):
        self.base_url = "https://api.coinpaprika.com/v1"
        self.session = None
        self.available = True  # Free API, no key needed
        self.rate_limit_delay = 0.1  # 100ms between requests for free tier
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
    
    async def _rate_limited_request(self, url: str, params: dict = None) -> Optional[Dict]:
        """Make rate-limited request to CoinPaprika"""
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
                    logger.warning("CoinPaprika rate limited, increasing delay")
                    self.rate_limit_delay = min(self.rate_limit_delay * 2, 5.0)
                    await asyncio.sleep(self.rate_limit_delay)
                    return None
                
                if response.status == 200:
                    self.rate_limit_delay = max(self.rate_limit_delay * 0.9, 0.1)  # Reduce delay on success
                    return await response.json()
                else:
                    logger.error(f"CoinPaprika error: {response.status} - {await response.text()}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error making CoinPaprika request: {e}")
            return None
    
    async def get_coins_list(self) -> List[Dict[str, Any]]:
        """Get list of all coins from CoinPaprika"""
        try:
            logger.info("Fetching coins list from CoinPaprika")
            url = f"{self.base_url}/coins"
            
            data = await self._rate_limited_request(url)
            if not data or not isinstance(data, list):
                return []
            
            logger.info(f"Retrieved {len(data)} coins from CoinPaprika")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching CoinPaprika coins list: {e}")
            return []
    
    async def get_tickers(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get ticker data from CoinPaprika"""
        try:
            logger.info(f"Fetching tickers from CoinPaprika (limit: {limit})")
            url = f"{self.base_url}/tickers"
            
            params = {
                'limit': min(limit, 1000)  # CoinPaprika max limit
            }
            
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
                    logger.warning(f"Error converting CoinPaprika ticker {ticker.get('id', 'unknown')}: {e}")
                    continue
            
            logger.info(f"Successfully processed {len(crypto_data)} tickers from CoinPaprika")
            return crypto_data
            
        except Exception as e:
            logger.error(f"Error fetching CoinPaprika tickers: {e}")
            return []
    
    async def get_comprehensive_data(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get comprehensive cryptocurrency data from CoinPaprika"""
        try:
            logger.info(f"Fetching comprehensive data from CoinPaprika (limit: {limit})")
            
            # Get ticker data which includes most information we need
            tickers = await self.get_tickers(limit)
            
            if not tickers:
                logger.warning("No ticker data from CoinPaprika")
                return []
            
            logger.info(f"Retrieved {len(tickers)} comprehensive records from CoinPaprika")
            return tickers
            
        except Exception as e:
            logger.error(f"Error getting comprehensive CoinPaprika data: {e}")
            return []
    
    def _convert_ticker_data(self, ticker: Dict) -> Optional[Dict[str, Any]]:
        """Convert CoinPaprika ticker data to our standard format"""
        try:
            coin_id = ticker.get('id', '')
            symbol = ticker.get('symbol', '').upper()
            name = ticker.get('name', symbol)
            
            if not symbol:
                return None
            
            # Extract quotes (USD data)
            quotes = ticker.get('quotes', {})
            usd_data = quotes.get('USD', {})
            
            price_usd = usd_data.get('price', 0)
            if price_usd is None:
                price_usd = 0
            
            converted = {
                'symbol': symbol,
                'name': name,
                'price_usd': float(price_usd) if price_usd else 0.0,
                'market_cap_usd': usd_data.get('market_cap'),
                'volume_24h_usd': usd_data.get('volume_24h'),
                'percent_change_1h': usd_data.get('percent_change_1h'),
                'percent_change_24h': usd_data.get('percent_change_24h'),
                'percent_change_7d': usd_data.get('percent_change_7d'),
                'percent_change_30d': usd_data.get('percent_change_30d'),
                'source': 'coinpaprika',
                'data_sources': ['coinpaprika'],
                'last_updated': datetime.utcnow(),
                'api_source': 'coinpaprika_tickers',
                'coin_id': coin_id,
                'rank': ticker.get('rank'),
                'circulating_supply': ticker.get('circulating_supply'),
                'total_supply': ticker.get('total_supply'),
                'max_supply': ticker.get('max_supply'),
                'ath_price': usd_data.get('ath_price'),
                'ath_date': usd_data.get('ath_date')
            }
            
            return converted
            
        except Exception as e:
            logger.error(f"Error converting CoinPaprika ticker data: {e}")
            return None
    
    async def get_coin_details(self, coin_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific coin"""
        try:
            url = f"{self.base_url}/coins/{coin_id}"
            data = await self._rate_limited_request(url)
            
            if not data:
                return None
            
            return data
            
        except Exception as e:
            logger.error(f"Error getting coin details for {coin_id}: {e}")
            return None
    
    async def get_historical_data(self, coin_id: str, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """Get historical data for a specific coin"""
        try:
            if not start_date:
                # Default to last 30 days
                end = datetime.utcnow()
                start = end - timedelta(days=30)
                start_date = start.strftime('%Y-%m-%d')
                end_date = end.strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/coins/{coin_id}/ohlcv/historical"
            params = {
                'start': start_date,
                'end': end_date
            }
            
            data = await self._rate_limited_request(url, params)
            
            if not data or not isinstance(data, list):
                return []
            
            return data
            
        except Exception as e:
            logger.error(f"Error getting historical data for {coin_id}: {e}")
            return []
    
    async def search_coins(self, query: str) -> List[Dict[str, Any]]:
        """Search for coins by name or symbol"""
        try:
            url = f"{self.base_url}/search"
            params = {
                'q': query,
                'c': 'currencies',  # Only cryptocurrencies
                'limit': 20
            }
            
            data = await self._rate_limited_request(url, params)
            
            if not data or 'currencies' not in data:
                return []
            
            return data['currencies']
            
        except Exception as e:
            logger.error(f"Error searching coins for {query}: {e}")
            return []
    
    def is_available(self) -> bool:
        """Check if CoinPaprika service is available"""
        return self.available
    
    async def close(self):
        """Close the session"""
        if self.session:
            await self.session.close()
            self.session = None