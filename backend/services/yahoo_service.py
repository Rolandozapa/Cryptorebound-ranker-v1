import yfinance as yf
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd

logger = logging.getLogger(__name__)

class YahooFinanceService:
    def __init__(self):
        # Major crypto symbols available on Yahoo Finance
        self.crypto_symbols = [
            'BTC-USD', 'ETH-USD', 'ADA-USD', 'BNB-USD', 'XRP-USD', 
            'SOL-USD', 'DOT-USD', 'DOGE-USD', 'AVAX-USD', 'SHIB-USD',
            'MATIC-USD', 'LTC-USD', 'BCH-USD', 'LINK-USD', 'UNI-USD',
            'ATOM-USD', 'ETC-USD', 'XLM-USD', 'ALGO-USD', 'VET-USD',
            'ICP-USD', 'FIL-USD', 'TRX-USD', 'EOS-USD', 'AAVE-USD',
            'GRT-USD', 'THETA-USD', 'XTZ-USD', 'COMP-USD', 'MKR-USD'
        ]
    
    async def get_crypto_data(self, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get current crypto data from Yahoo Finance"""
        if symbols is None:
            symbols = self.crypto_symbols
        
        crypto_data = []
        
        try:
            # Process symbols in batches to avoid rate limits
            batch_size = 10
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                batch_data = await self._process_symbol_batch(batch)
                crypto_data.extend(batch_data)
                
                # Small delay between batches
                await asyncio.sleep(0.5)
            
            logger.info(f"Retrieved data for {len(crypto_data)} cryptos from Yahoo Finance")
            return crypto_data
            
        except Exception as e:
            logger.error(f"Error getting Yahoo Finance data: {e}")
            return []
    
    async def _process_symbol_batch(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Process a batch of symbols"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._fetch_batch_data, symbols)
    
    def _fetch_batch_data(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Synchronous batch data fetching"""
        batch_data = []
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info
                hist = ticker.history(period="1y")
                
                if hist.empty:
                    continue
                
                current_price = hist['Close'].iloc[-1] if len(hist) > 0 else 0
                base_symbol = symbol.replace('-USD', '')
                
                # Calculate percentage changes
                def safe_pct_change(current, previous):
                    return ((current - previous) / previous * 100) if previous > 0 else 0
                
                pct_1d = safe_pct_change(hist['Close'].iloc[-1], hist['Close'].iloc[-2]) if len(hist) >= 2 else 0
                pct_7d = safe_pct_change(hist['Close'].iloc[-1], hist['Close'].iloc[-7]) if len(hist) >= 7 else 0
                pct_30d = safe_pct_change(hist['Close'].iloc[-1], hist['Close'].iloc[-30]) if len(hist) >= 30 else 0
                
                crypto_info = {
                    'symbol': base_symbol,
                    'name': info.get('longName', base_symbol),
                    'price_usd': float(current_price),  # Correction: utiliser price_usd
                    'market_cap_usd': info.get('marketCap'),  # Correction: utiliser market_cap_usd
                    'volume_24h_usd': float(hist['Volume'].iloc[-1]) if len(hist) > 0 else None,  # Correction: utiliser volume_24h_usd
                    'percent_change_24h': float(pct_1d),
                    'percent_change_7d': float(pct_7d),
                    'percent_change_30d': float(pct_30d),
                    'max_price_1y': float(hist['Close'].max()) if len(hist) > 0 else None,
                    'min_price_1y': float(hist['Close'].min()) if len(hist) > 0 else None,
                    'historical_prices': {
                        '1d': float(hist['Close'].iloc[-1]) if len(hist) >= 1 else 0,
                        '7d': float(hist['Close'].iloc[-7]) if len(hist) >= 7 else 0,
                        '30d': float(hist['Close'].iloc[-30]) if len(hist) >= 30 else 0,
                        '90d': float(hist['Close'].iloc[-90]) if len(hist) >= 90 else 0,
                        '180d': float(hist['Close'].iloc[-180]) if len(hist) >= 180 else 0,
                        '365d': float(hist['Close'].iloc[-365]) if len(hist) >= 365 else 0
                    },
                    'source': 'yahoo_finance'
                }
                
                batch_data.append(crypto_info)
                
            except Exception as e:
                logger.warning(f"Failed to get data for {symbol}: {e}")
                continue
        
        return batch_data
    
    async def get_historical_data(self, symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
        """Get historical data for a specific symbol"""
        try:
            loop = asyncio.get_event_loop()
            ticker = yf.Ticker(f"{symbol}-USD")
            hist = await loop.run_in_executor(None, ticker.history, period)
            return hist if not hist.empty else None
            
        except Exception as e:
            logger.error(f"Error getting historical data for {symbol}: {e}")
            return None
    
    def is_available(self) -> bool:
        """Check if Yahoo Finance service is available"""
        try:
            # Test with Bitcoin
            ticker = yf.Ticker('BTC-USD')
            hist = ticker.history(period="1d")
            return not hist.empty
        except Exception:
            return False