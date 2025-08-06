import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class CryptoCompareService:
    """
    Service pour récupérer les données crypto depuis CryptoCompare API (gratuit)
    API gratuite : 100,000 requests/mois, 100 requests/minute
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key  # Optionnel, mais recommandé pour plus de limite
        self.base_url = "https://min-api.cryptocompare.com"
        self.session = None
        
        # Rate limiting (API gratuite)
        self.max_requests_per_minute = 100 if api_key else 50
        self.request_count = 0
        self.last_reset = datetime.utcnow()
        
        # Cache pour éviter les appels répétés
        self.cache = {}
        self.cache_ttl = 300  # 5 minutes
        
    async def _get_session(self):
        """Get or create aiohttp session"""
        if self.session is None:
            headers = {
                'User-Agent': 'CryptoRebound/2.0',
                'Accept': 'application/json'
            }
            
            if self.api_key:
                headers['Authorization'] = f'Apikey {self.api_key}'
            
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers=headers
            )
        return self.session
        
    async def close(self):
        """Close the session"""
        if self.session:
            await self.session.close()
    
    def _check_rate_limit(self):
        """Check and enforce rate limiting"""
        now = datetime.utcnow()
        
        # Reset counter every minute
        if (now - self.last_reset).total_seconds() >= 60:
            self.request_count = 0
            self.last_reset = now
        
        return self.request_count < self.max_requests_per_minute
    
    async def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict]:
        """Make rate-limited request to CryptoCompare API"""
        if not self._check_rate_limit():
            logger.warning("Rate limit reached, waiting...")
            await asyncio.sleep(60)
            self.request_count = 0
            self.last_reset = datetime.utcnow()
        
        try:
            session = await self._get_session()
            url = f"{self.base_url}{endpoint}"
            
            async with session.get(url, params=params) as response:
                self.request_count += 1
                
                if response.status == 200:
                    data = await response.json()
                    
                    # CryptoCompare retourne parfois des erreurs dans le JSON
                    if data.get('Response') == 'Error':
                        logger.error(f"CryptoCompare API error: {data.get('Message', 'Unknown error')}")
                        return None
                    
                    return data
                else:
                    logger.error(f"HTTP error {response.status} from CryptoCompare")
                    return None
                    
        except Exception as e:
            logger.error(f"Error making request to CryptoCompare: {e}")
            return None
    
    async def get_top_cryptocurrencies(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get top cryptocurrencies by market cap from CryptoCompare"""
        try:
            # Cache key
            cache_key = f"top_cryptos_{limit}"
            
            # Check cache
            if cache_key in self.cache:
                cached_data, timestamp = self.cache[cache_key]
                if (datetime.utcnow() - timestamp).total_seconds() < self.cache_ttl:
                    logger.info(f"Using cached CryptoCompare data for top {limit} cryptos")
                    return cached_data
            
            logger.info(f"Fetching top {limit} cryptocurrencies from CryptoCompare")
            
            # CryptoCompare limite à 100 par requête
            all_cryptos = []
            page_size = min(100, limit)
            pages_needed = (limit + page_size - 1) // page_size
            
            for page in range(pages_needed):
                offset = page * page_size
                page_limit = min(page_size, limit - offset)
                
                params = {
                    'limit': page_limit,
                    'page': page,
                    'tsym': 'USD'
                }
                
                data = await self._make_request('/data/top/mktcapfull', params)
                
                if not data or 'Data' not in data:
                    logger.error(f"No data received from CryptoCompare for page {page}")
                    continue
                
                for item in data['Data']:
                    try:
                        raw_data = item.get('RAW', {}).get('USD', {})
                        display_data = item.get('DISPLAY', {}).get('USD', {})
                        
                        if not raw_data:
                            continue
                        
                        crypto_data = {
                            'symbol': item['CoinInfo']['Name'],
                            'name': item['CoinInfo']['FullName'],
                            'price_usd': raw_data.get('PRICE', 0),
                            'market_cap_usd': raw_data.get('MKTCAP', 0),
                            'volume_24h_usd': raw_data.get('VOLUME24HOUR', 0),
                            'percent_change_24h': raw_data.get('CHANGEPCT24HOUR', 0),
                            'percent_change_7d': None,  # Pas disponible dans cette API
                            'percent_change_30d': None,  # Pas disponible dans cette API
                            'max_price_1y': None,  # Nécessite un autre appel
                            'min_price_1y': None,  # Nécessite un autre appel
                            'rank': raw_data.get('MKTCAPRANK', 0),
                            'source': 'cryptocompare'
                        }
                        
                        all_cryptos.append(crypto_data)
                        
                    except Exception as e:
                        logger.warning(f"Error processing crypto data: {e}")
                        continue
                
                # Rate limiting entre les pages
                if page < pages_needed - 1:
                    await asyncio.sleep(0.6)  # 100 req/min = 1 req per 0.6s
            
            # Cache the results
            self.cache[cache_key] = (all_cryptos, datetime.utcnow())
            
            logger.info(f"Retrieved {len(all_cryptos)} cryptocurrencies from CryptoCompare")
            return all_cryptos
            
        except Exception as e:
            logger.error(f"Error fetching top cryptocurrencies from CryptoCompare: {e}")
            return []
    
    async def get_historical_data(self, symbol: str, days: int = 365) -> Dict[str, float]:
        """Get historical price data for a cryptocurrency"""
        try:
            cache_key = f"historical_{symbol}_{days}"
            
            # Check cache
            if cache_key in self.cache:
                cached_data, timestamp = self.cache[cache_key]
                if (datetime.utcnow() - timestamp).total_seconds() < self.cache_ttl:
                    return cached_data
            
            params = {
                'fsym': symbol,
                'tsym': 'USD',
                'limit': min(days, 2000),  # CryptoCompare limite
                'aggregate': 1
            }
            
            data = await self._make_request('/data/v2/histoday', params)
            
            if not data or 'Data' not in data or 'Data' not in data['Data']:
                logger.warning(f"No historical data for {symbol}")
                return {}
            
            historical_data = data['Data']['Data']
            
            if not historical_data:
                return {}
            
            # Extraire les prix min/max sur la période
            prices = [float(day['high']) for day in historical_data if day.get('high', 0) > 0]
            
            if not prices:
                return {}
            
            result = {
                'max_price_1y': max(prices),
                'min_price_1y': min(prices),
                'current_price': prices[-1] if prices else 0
            }
            
            # Cache the results
            self.cache[cache_key] = (result, datetime.utcnow())
            
            logger.info(f"Retrieved historical data for {symbol}: max={result['max_price_1y']:.8f}, min={result['min_price_1y']:.8f}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {e}")
            return {}
    
    async def get_multiple_price_data(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get current price data for multiple cryptocurrencies"""
        try:
            if not symbols:
                return {}
            
            # CryptoCompare peut gérer jusqu'à 300 symboles par requête
            batch_size = 100  # Soyons conservateur
            all_data = {}
            
            for i in range(0, len(symbols), batch_size):
                batch_symbols = symbols[i:i + batch_size]
                fsyms = ','.join(batch_symbols)
                
                params = {
                    'fsyms': fsyms,
                    'tsyms': 'USD'
                }
                
                data = await self._make_request('/data/pricemultifull', params)
                
                if not data or 'RAW' not in data:
                    logger.error(f"No price data received for batch {i//batch_size + 1}")
                    continue
                
                for symbol, symbol_data in data['RAW'].items():
                    if 'USD' not in symbol_data:
                        continue
                    
                    usd_data = symbol_data['USD']
                    
                    all_data[symbol] = {
                        'symbol': symbol,
                        'price_usd': usd_data.get('PRICE', 0),
                        'market_cap_usd': usd_data.get('MKTCAP', 0),
                        'volume_24h_usd': usd_data.get('VOLUME24HOUR', 0),
                        'percent_change_24h': usd_data.get('CHANGEPCT24HOUR', 0),
                        'percent_change_1h': usd_data.get('CHANGEPCTHOUR', 0) if 'CHANGEPCTHOUR' in usd_data else None,
                        'source': 'cryptocompare'
                    }
                
                # Rate limiting entre les batches
                if i + batch_size < len(symbols):
                    await asyncio.sleep(0.6)
            
            logger.info(f"Retrieved price data for {len(all_data)}/{len(symbols)} symbols from CryptoCompare")
            return all_data
            
        except Exception as e:
            logger.error(f"Error fetching multiple price data: {e}")
            return {}
    
    def is_available(self) -> bool:
        """Check if the service is available"""
        # CryptoCompare est généralement très fiable
        return True
    
    async def get_comprehensive_data(self, limit: int = 500) -> List[Dict[str, Any]]:
        """Get comprehensive crypto data (price + historical) for top cryptocurrencies"""
        try:
            logger.info(f"Getting comprehensive data for top {limit} cryptocurrencies from CryptoCompare")
            
            # 1. Get basic data for top cryptos
            basic_data = await self.get_top_cryptocurrencies(limit)
            
            if not basic_data:
                return []
            
            # 2. Pour un sous-ensemble, récupérer les données historiques
            # On ne fait cela que pour les top 100 pour éviter trop d'appels API
            top_symbols = [crypto['symbol'] for crypto in basic_data[:100]]
            
            logger.info(f"Getting historical data for top {len(top_symbols)} cryptocurrencies")
            
            # 3. Récupérer les données historiques en lots
            for i, symbol in enumerate(top_symbols):
                try:
                    historical = await self.get_historical_data(symbol)
                    
                    # Trouver la crypto correspondante et ajouter les données historiques
                    for crypto in basic_data:
                        if crypto['symbol'] == symbol:
                            crypto.update(historical)
                            break
                    
                    # Rate limiting - seulement pour les données historiques
                    if i < len(top_symbols) - 1:
                        await asyncio.sleep(0.6)
                        
                except Exception as e:
                    logger.warning(f"Error getting historical data for {symbol}: {e}")
                    continue
            
            logger.info(f"Comprehensive data ready for {len(basic_data)} cryptocurrencies")
            return basic_data
            
        except Exception as e:
            logger.error(f"Error getting comprehensive data: {e}")
            return []