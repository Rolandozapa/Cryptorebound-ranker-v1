import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from models import CryptoCurrency
from db_models import CryptoDataDB, DataSource
from services.binance_service import BinanceService
from services.yahoo_service import YahooFinanceService
from services.fallback_crypto_service import FallbackCryptoService
from services.database_cache_service import DatabaseCacheService
from services.data_enrichment_service import DataEnrichmentService

logger = logging.getLogger(__name__)

class DataAggregationService:
    """Service d'agrégation intelligent avec cache DB et enrichissement"""
    
    def __init__(self, db_client=None):
        # Services de données
        self.binance_service = BinanceService()
        self.yahoo_service = YahooFinanceService()
        self.fallback_service = FallbackCryptoService()
        
        # Services de cache et enrichissement
        self.db_cache = DatabaseCacheService(db_client)
        self.enrichment_service = DataEnrichmentService(self.db_cache)
        
        # Configuration
        self.last_update = None
        self.update_interval = timedelta(minutes=5)
        self.target_crypto_count = 1500  # Objectif de cryptos à maintenir
        
    def set_db_client(self, db_client):
        """Configure le client de base de données"""
        self.db_cache.set_db_client(db_client)
        
    async def get_aggregated_crypto_data(self, force_refresh: bool = False, required_fields: List[str] = None) -> List[CryptoCurrency]:
        """
        Récupère les données crypto de manière intelligente : DB first, puis API si nécessaire
        """
        try:
            logger.info("Starting intelligent data aggregation (DB-first approach)")
            
            # 1. Récupérer les données depuis la DB d'abord
            cached_cryptos = await self._get_cached_crypto_data(required_fields or [])
            logger.info(f"Retrieved {len(cached_cryptos)} cryptocurrencies from cache")
            
            # 2. Identifier les données manquantes ou obsolètes
            if force_refresh or len(cached_cryptos) < self.target_crypto_count:
                logger.info("Fetching fresh data from APIs to complement cache")
                
                # Récupérer la liste complète depuis les APIs
                api_symbols = await self._get_all_available_symbols()
                logger.info(f"Found {len(api_symbols)} total symbols from APIs")
                
                # Identifier les cryptos manquants
                cached_symbols = {crypto.symbol for crypto in cached_cryptos}
                missing_symbols = [s for s in api_symbols if s not in cached_symbols]
                
                logger.info(f"Found {len(missing_symbols)} missing cryptocurrencies")
                
                # Récupérer les données manquantes
                if missing_symbols:
                    await self._fetch_missing_crypto_data(missing_symbols[:100])  # Limiter à 100 par batch
                
                # Identifier les données obsolètes
                stale_symbols = await self.db_cache.get_stale_data_symbols(limit=50)
                if stale_symbols:
                    logger.info(f"Refreshing {len(stale_symbols)} stale cryptocurrencies")
                    await self._refresh_stale_data(stale_symbols)
                
                # Re-récupérer les données mises à jour
                cached_cryptos = await self._get_cached_crypto_data(required_fields or [])
            
            # 3. Programmer l'enrichissement en arrière-plan si nécessaire
            await self._schedule_background_enrichment()
            
            # 4. Convertir en format API
            result = await self._convert_to_api_format(cached_cryptos)
            
            self.last_update = datetime.utcnow()
            logger.info(f"Data aggregation completed: {len(result)} cryptocurrencies")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in intelligent data aggregation: {e}")
            # Fallback vers l'ancienne méthode
            return await self._fallback_aggregation()
    
    async def _get_cached_crypto_data(self, required_fields: List[str]) -> List[CryptoDataDB]:
        """Récupère les données crypto valides depuis le cache"""
        try:
            if not self.db_cache.db:
                return []
            
            # Récupérer toutes les cryptos avec qualité acceptable
            cursor = self.db_cache.db.crypto_data.find({
                "data_quality": {"$ne": "invalid"},
                "quality_score": {"$gte": 30}  # Score minimum de 30
            }).sort("quality_score", -1).limit(self.target_crypto_count)
            
            cryptos = []
            async for doc in cursor:
                try:
                    crypto_db = CryptoDataDB(**doc)
                    
                    # Vérifier la fraîcheur si des champs spécifiques sont requis
                    if required_fields:
                        if not self.db_cache._check_data_freshness(crypto_db, required_fields):
                            continue
                    
                    cryptos.append(crypto_db)
                    
                except Exception as e:
                    logger.warning(f"Error parsing cached crypto data: {e}")
                    continue
            
            return cryptos
            
        except Exception as e:
            logger.error(f"Error retrieving cached crypto data: {e}")
            return []
    
    async def _get_all_available_symbols(self) -> List[str]:
        """Récupère la liste de tous les symboles disponibles depuis les APIs"""
        try:
            all_symbols = set()
            
            # Récupérer depuis les services de fallback (plus complets)
            fallback_data = await self.fallback_service.get_crypto_data(limit=2000)
            for crypto in fallback_data:
                symbol = crypto.get('symbol', '').upper()
                if symbol and len(symbol) <= 10:  # Filtrer les symboles valides
                    all_symbols.add(symbol)
            
            # Récupérer depuis Yahoo Finance
            yahoo_data = await self.yahoo_service.get_crypto_data()
            for crypto in yahoo_data:
                symbol = crypto.get('symbol', '').upper()
                if symbol:
                    all_symbols.add(symbol)
            
            # Récupérer depuis Binance si disponible
            if self.binance_service.is_available():
                binance_data = await self.binance_service.get_all_tickers()
                for crypto in binance_data:
                    symbol = crypto.get('symbol', '').upper()
                    if symbol:
                        all_symbols.add(symbol)
            
            return list(all_symbols)
            
        except Exception as e:
            logger.error(f"Error getting available symbols: {e}")
            return []
    
    async def _fetch_missing_crypto_data(self, missing_symbols: List[str]):
        """Récupère les données pour les cryptos manquantes"""
        try:
            logger.info(f"Fetching data for {len(missing_symbols)} missing cryptocurrencies")
            
            # Récupérer par batch pour éviter les timeouts
            batch_size = 20
            for i in range(0, len(missing_symbols), batch_size):
                batch_symbols = missing_symbols[i:i + batch_size]
                
                # Récupérer depuis fallback
                fallback_data = await self.fallback_service.get_crypto_data(limit=1000)
                
                for crypto_data in fallback_data:
                    symbol = crypto_data.get('symbol', '').upper()
                    if symbol in batch_symbols:
                        # Sauvegarder en DB
                        await self.db_cache.store_crypto_data(crypto_data, validate=True)
                
                # Pause entre les batchs
                await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"Error fetching missing crypto data: {e}")
    
    async def _refresh_stale_data(self, stale_symbols: List[str]):
        """Rafraîchit les données obsolètes"""
        try:
            logger.info(f"Refreshing {len(stale_symbols)} stale cryptocurrencies")
            
            # Utiliser l'enrichissement pour ces symboles
            await self.enrichment_service.schedule_enrichment_for_symbols(stale_symbols, priority=1)
            
            # Traiter immédiatement quelques tâches critiques
            await self.enrichment_service.process_enrichment_tasks(max_tasks=10)
            
        except Exception as e:
            logger.error(f"Error refreshing stale data: {e}")
    
    async def _schedule_background_enrichment(self):
        """Programme l'enrichissement en arrière-plan"""
        try:
            # Traiter les tâches d'enrichissement en attente
            await self.enrichment_service.process_enrichment_tasks(max_tasks=5)
            
            # Nettoyer les anciennes tâches
            await self.enrichment_service.cleanup_old_tasks(days_old=3)
            
        except Exception as e:
            logger.error(f"Error in background enrichment: {e}")
    
    async def _convert_to_api_format(self, cached_cryptos: List[CryptoDataDB]) -> List[CryptoCurrency]:
        """Convertit les données DB vers le format API"""
        result = []
        
        for crypto_db in cached_cryptos:
            try:
                # Convertir vers le format API
                crypto = CryptoCurrency(
                    id=crypto_db.id,
                    symbol=crypto_db.symbol,
                    name=crypto_db.name or crypto_db.symbol,
                    price_usd=crypto_db.price_usd or 0.0,
                    market_cap_usd=crypto_db.market_cap_usd,
                    volume_24h_usd=crypto_db.volume_24h_usd,
                    percent_change_1h=crypto_db.percent_change_1h,
                    percent_change_24h=crypto_db.percent_change_24h,
                    percent_change_7d=crypto_db.percent_change_7d,
                    percent_change_30d=crypto_db.percent_change_30d,
                    historical_prices=crypto_db.historical_prices or {},
                    max_price_1y=crypto_db.max_price_1y,
                    min_price_1y=crypto_db.min_price_1y,
                    rank=crypto_db.rank,
                    last_updated=crypto_db.last_updated,
                    data_sources=[str(source) for source in crypto_db.data_sources]
                )
                
                result.append(crypto)
                
            except Exception as e:
                logger.warning(f"Error converting {crypto_db.symbol} to API format: {e}")
                continue
        
        return result
    
    async def _fallback_aggregation(self) -> List[CryptoCurrency]:
        """Méthode de fallback en cas d'erreur du cache intelligent"""
        try:
            logger.warning("Using fallback aggregation method")
            
            # Utiliser l'ancienne méthode directe
            tasks = []
            
            if self.binance_service.is_available():
                tasks.append(self._get_binance_data())
            
            tasks.append(self._get_yahoo_data())
            tasks.append(self._get_fallback_data())
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Combiner les résultats
            all_crypto_data = {}
            
            for result in results:
                if isinstance(result, Exception):
                    continue
                
                if isinstance(result, list):
                    for crypto_data in result:
                        symbol = crypto_data.get('symbol', '').upper()
                        if symbol:
                            if symbol not in all_crypto_data:
                                all_crypto_data[symbol] = crypto_data
                            else:
                                all_crypto_data[symbol] = self._merge_crypto_data(
                                    all_crypto_data[symbol], 
                                    crypto_data
                                )
            
            # Convertir en modèles CryptoCurrency
            cryptos = []
            for symbol, data in all_crypto_data.items():
                try:
                    crypto = self._data_to_crypto_model(data)
                    if crypto:
                        cryptos.append(crypto)
                except Exception as e:
                    logger.warning(f"Failed to convert {symbol} data to model: {e}")
                    continue
            
            return cryptos
            
        except Exception as e:
            logger.error(f"Error in fallback aggregation: {e}")
            return []
    
    # Méthodes existantes adaptées...
    async def _get_binance_data(self) -> List[Dict[str, Any]]:
        """Get data from Binance"""
        try:
            if not self.binance_service.is_available():
                return []
            
            tickers_task = self.binance_service.get_all_tickers()
            stats_task = self.binance_service.get_24hr_ticker_stats()
            
            tickers, stats = await asyncio.gather(
                tickers_task, stats_task, return_exceptions=True
            )
            
            if isinstance(tickers, Exception) or isinstance(stats, Exception):
                return []
            
            stats_dict = {s.get('symbol', ''): s for s in (stats or [])}
            
            merged_data = []
            for ticker in (tickers or []):
                symbol = ticker.get('symbol', '')
                merged_item = ticker.copy()
                
                if symbol in stats_dict:
                    merged_item.update(stats_dict[symbol])
                
                merged_data.append(merged_item)
            
            return merged_data
            
        except Exception as e:
            logger.error(f"Error getting Binance data: {e}")
            return []
    
    async def _get_yahoo_data(self) -> List[Dict[str, Any]]:
        """Get data from Yahoo Finance"""
        try:
            if not self.yahoo_service.is_available():
                return []
            
            data = await self.yahoo_service.get_crypto_data()
            return data
            
        except Exception as e:
            logger.error(f"Error getting Yahoo data: {e}")
            return []
    
    async def _get_fallback_data(self) -> List[Dict[str, Any]]:
        """Get data from fallback sources"""
        try:
            data = await self.fallback_service.get_crypto_data(limit=1500)
            return data
            
        except Exception as e:
            logger.error(f"Error getting fallback data: {e}")
            return []
    
    def _merge_crypto_data(self, existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two crypto data dictionaries"""
        merged = existing.copy()
        
        existing_sources = set(merged.get('data_sources', []))
        new_sources = set(new.get('data_sources', []))
        if 'source' in existing:
            existing_sources.add(existing['source'])
        if 'source' in new:
            new_sources.add(new['source'])
        merged['data_sources'] = list(existing_sources | new_sources)
        
        for key, value in new.items():
            if value is not None and value != 0:
                if key not in merged or merged[key] is None or merged[key] == 0:
                    merged[key] = value
                elif key in ['price', 'market_cap', 'volume_24h'] and isinstance(value, (int, float)):
                    existing_val = merged.get(key, 0)
                    if isinstance(existing_val, (int, float)) and existing_val > 0:
                        if abs(value - existing_val) < existing_val * 0.2:
                            merged[key] = (existing_val + value) / 2
                        elif new.get('source') in ['binance', 'coingecko']:
                            merged[key] = value
                    else:
                        merged[key] = value
        
        return merged
    
    def _data_to_crypto_model(self, data: Dict[str, Any]) -> Optional[CryptoCurrency]:
        """Convert raw data to CryptoCurrency model"""
        try:
            symbol = data.get('symbol', '').upper()
            price = float(data.get('price', 0))
            
            if not symbol or price <= 0:
                return None
            
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
        """Get historical data for a specific cryptocurrency"""
        try:
            # Essayer d'abord depuis le cache
            cached_data = await self.db_cache.get_crypto_data(symbol, required_fields=[])
            if cached_data and cached_data.historical_prices:
                return {
                    'symbol': symbol,
                    'historical_data': {
                        'cached_historical': cached_data.historical_prices
                    },
                    'last_updated': cached_data.last_updated
                }
            
            # Sinon, récupérer depuis les APIs
            tasks = []
            
            if self.binance_service.is_available():
                tasks.append(self.binance_service.get_historical_klines(symbol))
            
            tasks.append(self.yahoo_service.get_historical_data(symbol))
            tasks.append(self.fallback_service.get_historical_data(symbol))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            historical_data = {}
            for i, result in enumerate(results):
                if isinstance(result, Exception):
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
        """Check health status of all services"""
        health = {
            'binance': self.binance_service.is_available(),
            'yahoo_finance': self.yahoo_service.is_available(),
            'fallback_sources': self.fallback_service.is_available(),
            'database_cache': self.db_cache.db is not None,
            'last_update': self.last_update.isoformat() if self.last_update else None
        }
        
        # Ajouter les stats de la DB
        try:
            if self.db_cache.db:
                # Stats rapides (pas besoin d'await ici car c'est synchrone)
                health['database_available'] = True
            else:
                health['database_available'] = False
        except:
            health['database_available'] = False
        
        return health
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """Get detailed database statistics"""
        return await self.db_cache.get_database_stats()
    
    async def cleanup(self):
        """Clean up resources"""
        try:
            await self.fallback_service.close()
        except Exception as e:
            logger.error(f"Error cleaning up resources: {e}")