import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

class FallbackCryptoService:
    """Fallback service using public APIs when Binance is not available"""
    
    def __init__(self):
        self.session = None
        self.base_urls = {
            'coingecko': 'https://api.coingecko.com/api/v3',
            'coinlore': 'https://api.coinlore.net/api',
            'cryptocompare': 'https://min-api.cryptocompare.com/data'
        }
    
    async def _get_session(self):
        """Get or create aiohttp session"""
        if not self.session:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={'User-Agent': 'CryptoRebound/1.0'}
            )
        return self.session
    
    async def get_crypto_data(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get cryptocurrency data from fallback sources"""
        logger.info(f"Fetching crypto data from fallback sources (limit: {limit})")
        
        # Try multiple sources and combine results
        tasks = [
            self._get_coingecko_data(limit),
            self._get_coinlore_data(limit),
            # self._get_cryptocompare_data(limit)  # May need API key
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine and deduplicate
        all_cryptos = {}
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Fallback source failed: {result}")
                continue
            
            if isinstance(result, list):
                for crypto in result:
                    symbol = crypto.get('symbol', '').upper()
                    if symbol and symbol not in all_cryptos:
                        all_cryptos[symbol] = crypto
        
        crypto_list = list(all_cryptos.values())
        logger.info(f"Retrieved {len(crypto_list)} cryptocurrencies from fallback sources")
        return crypto_list
    
    async def _get_coingecko_data(self, limit: int) -> List[Dict[str, Any]]:
        """Get data from CoinGecko API"""
        try:
            session = await self._get_session()
            per_page = min(250, limit)  # CoinGecko limit
            pages_needed = (limit + per_page - 1) // per_page
            
            all_cryptos = []
            
            for page in range(1, min(pages_needed + 1, 5)):  # Max 5 pages (1250 cryptos)
                url = f"{self.base_urls['coingecko']}/coins/markets"
                params = {
                    'vs_currency': 'usd',
                    'order': 'market_cap_desc',
                    'per_page': per_page,
                    'page': page,
                    'sparkline': 'false',
                    'price_change_percentage': '1h,24h,7d,30d'
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        for item in data:
                            crypto_data = {
                                'symbol': item.get('symbol', '').upper(),
                                'name': item.get('name', ''),
                                'price_usd': float(item.get('current_price', 0)),  # Correction: utiliser price_usd
                                'market_cap_usd': item.get('market_cap'),  # Correction: utiliser market_cap_usd
                                'volume_24h_usd': item.get('total_volume'),  # Correction: utiliser volume_24h_usd
                                'percent_change_1h': item.get('price_change_percentage_1h_in_currency'),
                                'percent_change_24h': item.get('price_change_percentage_24h_in_currency'),
                                'percent_change_7d': item.get('price_change_percentage_7d_in_currency'),
                                'percent_change_30d': item.get('price_change_percentage_30d_in_currency'),
                                'max_price_1y': item.get('ath'),  # All-time high as proxy
                                'min_price_1y': item.get('atl'),  # All-time low as proxy
                                'rank': item.get('market_cap_rank'),
                                'source': 'coingecko'
                            }
                            all_cryptos.append(crypto_data)
                
                # Rate limiting
                await asyncio.sleep(0.5)
            
            logger.info(f"Retrieved {len(all_cryptos)} cryptocurrencies from CoinGecko")
            return all_cryptos
            
        except Exception as e:
            logger.error(f"Error fetching CoinGecko data: {e}")
            return []
    
    async def _get_coinlore_data(self, limit: int) -> List[Dict[str, Any]]:
        """Get data from Coinlore API (free, no API key needed)"""
        try:
            session = await self._get_session()
            
            # Coinlore returns 100 coins per request max
            start = 0
            all_cryptos = []
            
            while start < limit and start < 5000:  # Reasonable upper limit
                url = f"{self.base_urls['coinlore']}/tickers/"
                params = {
                    'start': start,
                    'limit': min(100, limit - start)
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if 'data' in data:
                            for item in data['data']:
                                crypto_data = {
                                    'symbol': item.get('symbol', '').upper(),
                                    'name': item.get('name', ''),
                                    'price_usd': float(item.get('price_usd', 0)),  # Correction: déjà correct
                                    'market_cap_usd': float(item.get('market_cap_usd', 0)) if item.get('market_cap_usd') else None,  # Correction: utiliser market_cap_usd
                                    'volume_24h_usd': float(item.get('volume24', 0)) if item.get('volume24') else None,  # Correction: utiliser volume_24h_usd
                                    'percent_change_1h': float(item.get('percent_change_1h', 0)) if item.get('percent_change_1h') else None,
                                    'percent_change_24h': float(item.get('percent_change_24h', 0)) if item.get('percent_change_24h') else None,
                                    'percent_change_7d': float(item.get('percent_change_7d', 0)) if item.get('percent_change_7d') else None,
                                    'rank': int(item.get('rank', 0)) if item.get('rank') else None,
                                    'source': 'coinlore'
                                }
                                all_cryptos.append(crypto_data)
                        
                        # Check if we got less than requested (end of data)
                        if not data.get('data') or len(data['data']) < params['limit']:
                            break
                    else:
                        break
                
                start += 100
                await asyncio.sleep(1)  # Rate limiting
            
            logger.info(f"Retrieved {len(all_cryptos)} cryptocurrencies from Coinlore")
            return all_cryptos
            
        except Exception as e:
            logger.error(f"Error fetching Coinlore data: {e}")
            return []
    
    async def get_historical_data(self, symbol: str, days: int = 365) -> Dict[str, Any]:
        """Get historical data for a specific cryptocurrency"""
        try:
            session = await self._get_session()
            
            # Try CoinGecko first
            url = f"{self.base_urls['coingecko']}/coins/{symbol.lower()}/market_chart"
            params = {
                'vs_currency': 'usd',
                'days': min(days, 365),  # CoinGecko free tier limit
                'interval': 'daily'
            }
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if 'prices' in data:
                        historical_prices = {}
                        prices = data['prices']
                        
                        if len(prices) > 0:
                            current_price = prices[-1][1]
                            
                            # Calculate historical prices for different periods
                            for period, days_back in [('1d', 1), ('7d', 7), ('30d', 30), ('90d', 90), ('180d', 180), ('365d', 365)]:
                                if days_back < len(prices):
                                    historical_prices[period] = prices[-days_back][1]
                                elif len(prices) > 0:
                                    historical_prices[period] = prices[0][1]  # Use oldest available
                            
                            # Calculate max/min prices
                            all_prices = [p[1] for p in prices]
                            max_price = max(all_prices) if all_prices else current_price
                            min_price = min(all_prices) if all_prices else current_price
                            
                            return {
                                'symbol': symbol.upper(),
                                'historical_prices': historical_prices,
                                'max_price_1y': max_price,
                                'min_price_1y': min_price,
                                'source': 'coingecko_historical'
                            }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting historical data for {symbol}: {e}")
            return {}
    
    def is_available(self) -> bool:
        """Always return True since these are fallback services"""
        return True
    
    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None