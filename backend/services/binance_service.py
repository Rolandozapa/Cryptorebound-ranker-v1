import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional
from binance.client import Client
from binance.exceptions import BinanceAPIException
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class BinanceService:
    def __init__(self):
        self.api_key = os.environ.get('BINANCE_API_KEY', '')
        self.api_secret = os.environ.get('BINANCE_SECRET_KEY', '')
        
        # Initialize client - for public data, no keys needed
        try:
            if self.api_key:
                self.client = Client(self.api_key, self.api_secret)
            else:
                self.client = Client()  # Public client only
        except Exception as e:
            logger.warning(f"Binance client init failed: {e}. Using public endpoints only.")
            self.client = Client()
    
    async def get_all_tickers(self) -> List[Dict[str, Any]]:
        """Get all ticker prices from Binance"""
        try:
            loop = asyncio.get_event_loop()
            tickers = await loop.run_in_executor(None, self.client.get_all_tickers)
            
            # Filter for USDT pairs primarily and other major pairs
            filtered_tickers = []
            for ticker in tickers:
                symbol = ticker['symbol']
                if (symbol.endswith('USDT') or 
                    symbol.endswith('BUSD') or 
                    symbol.endswith('BTC') and not symbol.startswith('BTC')):
                    
                    # Extract base currency
                    if symbol.endswith('USDT'):
                        base_currency = symbol[:-4]
                    elif symbol.endswith('BUSD'):
                        base_currency = symbol[:-4]
                    elif symbol.endswith('BTC'):
                        base_currency = symbol[:-3]
                    else:
                        base_currency = symbol
                    
                    filtered_tickers.append({
                        'symbol': base_currency,
                        'full_symbol': symbol,
                        'price': float(ticker['price']),
                        'source': 'binance'
                    })
            
            logger.info(f"Retrieved {len(filtered_tickers)} tickers from Binance")
            return filtered_tickers
            
        except BinanceAPIException as e:
            logger.error(f"Binance API error: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting Binance tickers: {e}")
            return []
    
    async def get_24hr_ticker_stats(self) -> List[Dict[str, Any]]:
        """Get 24hr ticker statistics"""
        try:
            loop = asyncio.get_event_loop()
            stats = await loop.run_in_executor(None, self.client.get_ticker)
            
            if not isinstance(stats, list):
                stats = [stats] if stats else []
            
            processed_stats = []
            for stat in stats:
                symbol = stat.get('symbol', '')
                if symbol.endswith('USDT') or symbol.endswith('BUSD'):
                    base_currency = symbol[:-4] if symbol.endswith('USDT') else symbol[:-4]
                    
                    processed_stats.append({
                        'symbol': base_currency,
                        'full_symbol': symbol,
                        'price': float(stat.get('lastPrice', 0)),
                        'percent_change_24h': float(stat.get('priceChangePercent', 0)),
                        'volume_24h': float(stat.get('volume', 0)),
                        'high_24h': float(stat.get('highPrice', 0)),
                        'low_24h': float(stat.get('lowPrice', 0)),
                        'source': 'binance'
                    })
            
            logger.info(f"Retrieved {len(processed_stats)} 24hr stats from Binance")
            return processed_stats
            
        except Exception as e:
            logger.error(f"Error getting Binance 24hr stats: {e}")
            return []
    
    async def get_historical_klines(self, symbol: str, interval: str = '1d', limit: int = 365) -> List[Dict]:
        """Get historical kline/candlestick data"""
        try:
            full_symbol = f"{symbol}USDT"
            loop = asyncio.get_event_loop()
            klines = await loop.run_in_executor(
                None, 
                self.client.get_historical_klines,
                full_symbol, 
                interval, 
                f"{limit} days ago UTC"
            )
            
            historical_data = []
            for kline in klines:
                historical_data.append({
                    'timestamp': kline[0],
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                })
            
            return historical_data
            
        except Exception as e:
            logger.error(f"Error getting historical data for {symbol}: {e}")
            return []
    
    def is_available(self) -> bool:
        """Check if Binance service is available"""
        try:
            # Test with a simple ping
            self.client.ping()
            return True
        except Exception:
            return False