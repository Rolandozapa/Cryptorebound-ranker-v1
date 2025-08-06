import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from models import CryptoCurrency
from services.binance_service import BinanceService
from services.yahoo_service import YahooFinanceService
from services.fallback_crypto_service import FallbackCryptoService

logger = logging.getLogger(__name__)

class DataAggregationService:
    def __init__(self):
        self.binance_service = BinanceService()
        self.yahoo_service = YahooFinanceService()
        self.fallback_service = FallbackCryptoService()
        self.last_update = None
        self.update_interval = timedelta(minutes=5)  # 5-minute minimum between updates
        
    async def get_aggregated_crypto_data(self, force_refresh: bool = False) -> List[CryptoCurrency]:
        """Get aggregated cryptocurrency data from all sources"""
        
        # Check if we need to update
        now = datetime.utcnow()
        if not force_refresh and self.last_update:
            if now - self.last_update < self.update_interval:
                logger.info("Using cached data, too soon to refresh")
                return []
        
        logger.info("Starting data aggregation from multiple sources...")
        
        # Collect data from all sources concurrently
        tasks = []
        
        # Add Binance if available
        if self.binance_service.is_available():
            tasks.append(self._get_binance_data())
            logger.info("Added Binance data source")
        else:
            logger.info("Binance not available, skipping")
        
        # Add Yahoo Finance
        tasks.append(self._get_yahoo_data())
        logger.info("Added Yahoo Finance data source")
        
        # Add fallback sources (CoinGecko, Coinlore)
        tasks.append(self._get_fallback_data())
        logger.info("Added fallback data sources")
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine and deduplicate data
        all_crypto_data = {}
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Data source {i} failed: {result}")
                continue
            
            if isinstance(result, list):
                for crypto_data in result:
                    symbol = crypto_data.get('symbol', '').upper()
                    if symbol:
                        if symbol not in all_crypto_data:
                            all_crypto_data[symbol] = crypto_data
                        else:
                            # Merge data, preferring non-null values
                            all_crypto_data[symbol] = self._merge_crypto_data(
                                all_crypto_data[symbol], 
                                crypto_data
                            )
        
        # Convert to CryptoCurrency models
        cryptos = []
        for symbol, data in all_crypto_data.items():
            try:
                crypto = self._data_to_crypto_model(data)
                if crypto:
                    cryptos.append(crypto)
            except Exception as e:
                logger.warning(f"Failed to convert {symbol} data to model: {e}")
                continue
        
        self.last_update = now
        logger.info(f"Aggregated data for {len(cryptos)} cryptocurrencies from {len([r for r in results if not isinstance(r, Exception)])} sources")
        return cryptos
    
    async def _get_binance_data(self) -> List[Dict[str, Any]]:
        """Get data from Binance"""
        try:
            if not self.binance_service.is_available():
                logger.warning("Binance service not available")
                return []
            
            # Get both ticker prices and 24hr stats
            tickers_task = self.binance_service.get_all_tickers()
            stats_task = self.binance_service.get_24hr_ticker_stats()
            
            tickers, stats = await asyncio.gather(
                tickers_task, stats_task, return_exceptions=True
            )
            
            if isinstance(tickers, Exception) or isinstance(stats, Exception):
                logger.error("Failed to get Binance data")
                return []
            
            # Merge ticker prices with 24hr stats
            stats_dict = {s.get('symbol', ''): s for s in (stats or [])}
            
            merged_data = []
            for ticker in (tickers or []):
                symbol = ticker.get('symbol', '')
                merged_item = ticker.copy()
                
                # Add 24hr stats if available
                if symbol in stats_dict:
                    merged_item.update(stats_dict[symbol])
                
                merged_data.append(merged_item)
            
            logger.info(f"Retrieved {len(merged_data)} items from Binance")
            return merged_data
            
        except Exception as e:
            logger.error(f"Error getting Binance data: {e}")
            return []
    
    async def _get_yahoo_data(self) -> List[Dict[str, Any]]:
        """Get data from Yahoo Finance"""
        try:
            if not self.yahoo_service.is_available():
                logger.warning("Yahoo Finance service not available")
                return []
            
            data = await self.yahoo_service.get_crypto_data()
            logger.info(f"Retrieved {len(data)} items from Yahoo Finance")
            return data
            
        except Exception as e:
            logger.error(f"Error getting Yahoo data: {e}")
            return []
    
    async def _get_fallback_data(self) -> List[Dict[str, Any]]:
        """Get data from fallback sources"""
        try:
            data = await self.fallback_service.get_crypto_data(limit=1500)
            logger.info(f"Retrieved {len(data)} items from fallback sources")
            return data
            
        except Exception as e:
            logger.error(f"Error getting fallback data: {e}")
            return []
    
    def _merge_crypto_data(self, existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two crypto data dictionaries, preferring non-null values"""
        merged = existing.copy()
        
        # Merge sources
        existing_sources = set(merged.get('data_sources', []))
        new_sources = set(new.get('data_sources', []))
        if 'source' in existing:
            existing_sources.add(existing['source'])
        if 'source' in new:
            new_sources.add(new['source'])
        merged['data_sources'] = list(existing_sources | new_sources)
        
        # Update with non-null values from new data
        for key, value in new.items():
            if value is not None and value != 0:
                if key not in merged or merged[key] is None or merged[key] == 0:
                    merged[key] = value
                elif key in ['price', 'market_cap', 'volume_24h'] and isinstance(value, (int, float)):
                    # For numerical values, prefer the one that seems more reasonable
                    existing_val = merged.get(key, 0)
                    if isinstance(existing_val, (int, float)) and existing_val > 0:
                        # If values are similar (within 20%), average them
                        if abs(value - existing_val) < existing_val * 0.2:
                            merged[key] = (existing_val + value) / 2
                        # Otherwise, prefer the higher-quality source
                        elif new.get('source') in ['binance', 'coingecko']:
                            merged[key] = value
                    else:
                        merged[key] = value
        
        return merged
    
    def _data_to_crypto_model(self, data: Dict[str, Any]) -> Optional[CryptoCurrency]:
        """Convert raw data dictionary to CryptoCurrency model"""
        try:
            # Required fields
            symbol = data.get('symbol', '').upper()
            price = float(data.get('price', 0))
            
            if not symbol or price <= 0:
                return None
            
            # Build CryptoCurrency object
            crypto = CryptoCurrency(
                symbol=symbol,
                name=data.get('name', symbol),
                price_usd=price,
                market_cap_usd=self._safe_float(data.get('market_cap')),
                volume_24h_usd=self._safe_float(data.get('volume_24h')),
                percent_change_1h=self._safe_float(data.get('percent_change_1h')),
                percent_change_24h=self._safe_float(data.get('percent_change_24h')),
                percent_change_7d=self._safe_float(data.get('percent_change_7d')),
                percent_change_30d=self._safe_float(data.get('percent_change_30d')),
                max_price_1y=self._safe_float(data.get('max_price_1y')),
                min_price_1y=self._safe_float(data.get('min_price_1y')),
                historical_prices=data.get('historical_prices', {}),
                data_sources=data.get('data_sources', []),
                rank=data.get('rank')
            )
            
            return crypto
            
        except Exception as e:
            logger.error(f"Error creating CryptoCurrency model: {e}")
            return None
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        try:
            if value is None or value == '':
                return None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    async def get_historical_data_for_crypto(self, symbol: str) -> Dict[str, Any]:
        """Get detailed historical data for a specific cryptocurrency"""
        try:
            tasks = []
            
            # Try Binance first for more recent data
            if self.binance_service.is_available():
                tasks.append(self.binance_service.get_historical_klines(symbol))
            
            # Get Yahoo Finance data for longer historical period
            tasks.append(self.yahoo_service.get_historical_data(symbol))
            
            # Get fallback historical data
            tasks.append(self.fallback_service.get_historical_data(symbol))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            historical_data = {}
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.warning(f"Historical data source {i} failed: {result}")
                    continue
                
                if result:
                    source_name = ['binance', 'yahoo', 'fallback'][i] if i < 3 else f'source_{i}'
                    historical_data[f'{source_name}_historical'] = result
            
            return {
                'symbol': symbol,
                'historical_data': historical_data,
                'last_updated': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Error getting historical data for {symbol}: {e}")
            return {}
    
    def is_healthy(self) -> Dict[str, bool]:
        """Check health status of all data services"""
        return {
            'binance': self.binance_service.is_available(),
            'yahoo_finance': self.yahoo_service.is_available(),
            'fallback_sources': self.fallback_service.is_available(),
            'last_update': self.last_update.isoformat() if self.last_update else None
        }
    
    async def cleanup(self):
        """Clean up resources"""
        try:
            await self.fallback_service.close()
        except Exception as e:
            logger.error(f"Error cleaning up resources: {e}")