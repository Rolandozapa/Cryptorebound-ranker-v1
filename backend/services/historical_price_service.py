import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import aiohttp
from models import CryptoCurrency
import yfinance as yf
import pandas as pd
import requests
import time

logger = logging.getLogger(__name__)

class HistoricalPriceService:
    """Service for fetching reliable historical price data to calculate 1-year max/min"""
    
    def __init__(self):
        self.session = None
        self.coingecko_rate_limit = 0.2  # 5 requests per second max
        self.last_coingecko_call = 0
        self.yahoo_cache = {}  # Cache pour éviter les appels répétés
        self.coingecko_cache = {}
        
        # Cache TTL (Time To Live) en secondes
        self.cache_ttl = 3600  # 1 heure
        
    async def _get_session(self):
        """Get or create aiohttp session"""
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={'User-Agent': 'CryptoRebound/1.0'}
            )
        return self.session
    
    async def close(self):
        """Close the session"""
        if self.session:
            await self.session.close()
    
    def _is_cache_valid(self, cache_entry: Dict) -> bool:
        """Check if cache entry is still valid"""
        if not cache_entry:
            return False
        return (datetime.utcnow() - cache_entry['timestamp']).total_seconds() < self.cache_ttl
    
    async def get_1year_high_low(self, symbol: str, current_price: float) -> Tuple[Optional[float], Optional[float]]:
        """Get 1-year high and low prices for a cryptocurrency"""
        try:
            # Try multiple sources in order of reliability
            sources = [
                ('coingecko', self._get_coingecko_1year_data),
                ('yahoo', self._get_yahoo_1year_data),
                ('fallback', self._estimate_from_current_price)
            ]
            
            for source_name, source_func in sources:
                try:
                    logger.debug(f"Trying {source_name} for {symbol}")
                    max_price, min_price = await source_func(symbol, current_price)
                    
                    if max_price is not None and min_price is not None:
                        # Validation: max should be >= current >= min
                        if max_price >= current_price >= min_price and max_price > 0 and min_price > 0:
                            logger.info(f"Got reliable 1Y data for {symbol} from {source_name}: max={max_price:.8f}, min={min_price:.8f}")
                            return max_price, min_price
                        else:
                            logger.warning(f"Invalid price range from {source_name} for {symbol}: max={max_price}, current={current_price}, min={min_price}")
                    
                except Exception as e:
                    logger.warning(f"Failed to get data from {source_name} for {symbol}: {e}")
                    continue
            
            # If all sources failed, return None
            logger.error(f"Could not get reliable 1-year data for {symbol}")
            return None, None
            
        except Exception as e:
            logger.error(f"Error getting 1-year high/low for {symbol}: {e}")
            return None, None
    
    async def _get_coingecko_1year_data(self, symbol: str, current_price: float) -> Tuple[Optional[float], Optional[float]]:
        """Get 1-year data from CoinGecko API"""
        try:
            # Check cache first
            cache_key = f"coingecko_{symbol.lower()}"
            if cache_key in self.coingecko_cache and self._is_cache_valid(self.coingecko_cache[cache_key]):
                data = self.coingecko_cache[cache_key]['data']
                return data.get('max_price'), data.get('min_price')
            
            # Rate limiting
            now = time.time()
            if now - self.last_coingecko_call < self.coingecko_rate_limit:
                await asyncio.sleep(self.coingecko_rate_limit - (now - self.last_coingecko_call))
            
            # Get coin ID from symbol
            coin_id = await self._get_coingecko_coin_id(symbol)
            if not coin_id:
                return None, None
            
            # Get 1-year historical data
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=365)
            
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
            params = {
                'vs_currency': 'usd',
                'from': int(start_date.timestamp()),
                'to': int(end_date.timestamp())
            }
            
            session = await self._get_session()
            async with session.get(url, params=params) as response:
                self.last_coingecko_call = time.time()
                
                if response.status == 200:
                    data = await response.json()
                    prices = data.get('prices', [])
                    
                    if prices:
                        # Extract just the price values (second element of each [timestamp, price] pair)
                        price_values = [price[1] for price in prices]
                        max_price = max(price_values)
                        min_price = min(price_values)
                        
                        # Cache the result
                        self.coingecko_cache[cache_key] = {
                            'timestamp': datetime.utcnow(),
                            'data': {'max_price': max_price, 'min_price': min_price}
                        }
                        
                        return max_price, min_price
                    
        except Exception as e:
            logger.warning(f"CoinGecko API error for {symbol}: {e}")
            
        return None, None
    
    async def _get_coingecko_coin_id(self, symbol: str) -> Optional[str]:
        """Get CoinGecko coin ID from symbol"""
        try:
            # Common mappings for popular coins
            symbol_to_id_map = {
                'BTC': 'bitcoin',
                'ETH': 'ethereum', 
                'USDT': 'tether',
                'BNB': 'binancecoin',
                'SOL': 'solana',
                'USDC': 'usd-coin',
                'XRP': 'ripple',
                'DOGE': 'dogecoin',
                'TON': 'the-open-network',
                'ADA': 'cardano'
            }
            
            if symbol.upper() in symbol_to_id_map:
                return symbol_to_id_map[symbol.upper()]
            
            # For other coins, use the coins list API (cached)
            cache_key = f"coin_list"
            if cache_key not in self.coingecko_cache or not self._is_cache_valid(self.coingecko_cache[cache_key]):
                session = await self._get_session()
                async with session.get('https://api.coingecko.com/api/v3/coins/list') as response:
                    if response.status == 200:
                        coins_list = await response.json()
                        self.coingecko_cache[cache_key] = {
                            'timestamp': datetime.utcnow(),
                            'data': {coin['symbol'].upper(): coin['id'] for coin in coins_list}
                        }
            
            if cache_key in self.coingecko_cache:
                coin_map = self.coingecko_cache[cache_key]['data']
                return coin_map.get(symbol.upper())
                
        except Exception as e:
            logger.warning(f"Error getting CoinGecko coin ID for {symbol}: {e}")
            
        return None
    
    async def _get_yahoo_1year_data(self, symbol: str, current_price: float) -> Tuple[Optional[float], Optional[float]]:
        """Get 1-year data from Yahoo Finance"""
        try:
            # Check cache first
            cache_key = f"yahoo_{symbol.upper()}"
            if cache_key in self.yahoo_cache and self._is_cache_valid(self.yahoo_cache[cache_key]):
                data = self.yahoo_cache[cache_key]['data']
                return data.get('max_price'), data.get('min_price')
            
            # Try different Yahoo Finance tickers
            potential_tickers = [
                f"{symbol}-USD",
                f"{symbol}USD",
                f"{symbol}-USDT", 
                symbol
            ]
            
            for ticker_symbol in potential_tickers:
                try:
                    # Get 1-year historical data
                    ticker = yf.Ticker(ticker_symbol)
                    end_date = datetime.utcnow()
                    start_date = end_date - timedelta(days=365)
                    
                    hist = ticker.history(start=start_date, end=end_date)
                    
                    if len(hist) > 30:  # Need at least 30 days of data
                        max_price = float(hist['Close'].max())
                        min_price = float(hist['Close'].min())
                        
                        # Validation
                        if max_price > min_price > 0:
                            # Cache the result
                            self.yahoo_cache[cache_key] = {
                                'timestamp': datetime.utcnow(),
                                'data': {'max_price': max_price, 'min_price': min_price}
                            }
                            
                            return max_price, min_price
                    
                except Exception as e:
                    logger.debug(f"Yahoo ticker {ticker_symbol} failed for {symbol}: {e}")
                    continue
            
        except Exception as e:
            logger.warning(f"Yahoo Finance error for {symbol}: {e}")
        
        return None, None
    
    async def _estimate_from_current_price(self, symbol: str, current_price: float) -> Tuple[Optional[float], Optional[float]]:
        """Fallback: Estimate 1-year range from current price using market volatility assumptions"""
        try:
            # Conservative estimates based on crypto market characteristics
            volatility_factors = {
                # Major coins (lower volatility)
                'BTC': {'max_factor': 3.0, 'min_factor': 0.3},
                'ETH': {'max_factor': 4.0, 'min_factor': 0.25}, 
                'USDT': {'max_factor': 1.05, 'min_factor': 0.95},
                'USDC': {'max_factor': 1.05, 'min_factor': 0.95},
                'BNB': {'max_factor': 5.0, 'min_factor': 0.2},
                
                # Default for other coins (higher volatility)
                'default': {'max_factor': 8.0, 'min_factor': 0.1}
            }
            
            factors = volatility_factors.get(symbol.upper(), volatility_factors['default'])
            
            estimated_max = current_price * factors['max_factor']
            estimated_min = current_price * factors['min_factor']
            
            logger.info(f"Using estimated 1Y range for {symbol}: max={estimated_max:.8f}, min={estimated_min:.8f}")
            
            return estimated_max, estimated_min
            
        except Exception as e:
            logger.error(f"Error estimating price range for {symbol}: {e}")
            return None, None
    
    async def update_crypto_historical_data(self, crypto: CryptoCurrency) -> CryptoCurrency:
        """Update a single crypto with accurate 1-year high/low data"""
        try:
            if not crypto.price_usd or crypto.price_usd <= 0:
                return crypto
            
            max_price_1y, min_price_1y = await self.get_1year_high_low(crypto.symbol, crypto.price_usd)
            
            if max_price_1y is not None:
                crypto.max_price_1y = max_price_1y
            if min_price_1y is not None:
                crypto.min_price_1y = min_price_1y
                
            return crypto
            
        except Exception as e:
            logger.error(f"Error updating historical data for {crypto.symbol}: {e}")
            return crypto
    
    async def batch_update_historical_data(self, cryptos: List[CryptoCurrency]) -> List[CryptoCurrency]:
        """Update multiple cryptos with historical data - with rate limiting"""
        try:
            updated_cryptos = []
            
            for crypto in cryptos:
                try:
                    updated_crypto = await self.update_crypto_historical_data(crypto)
                    updated_cryptos.append(updated_crypto)
                    
                    # Rate limiting between requests
                    await asyncio.sleep(0.1)  # 100ms between requests
                    
                except Exception as e:
                    logger.warning(f"Failed to update historical data for {crypto.symbol}: {e}")
                    updated_cryptos.append(crypto)  # Keep original data
            
            logger.info(f"Updated historical data for {len(updated_cryptos)}/{len(cryptos)} cryptos")
            return updated_cryptos
            
        except Exception as e:
            logger.error(f"Error in batch historical data update: {e}")
            return cryptos