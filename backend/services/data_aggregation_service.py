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
from services.ranking_precompute_service import RankingPrecomputeService
import uuid

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
        self.precompute_service = RankingPrecomputeService(self.db_cache, None)  # Will set scoring_service later
        
        # Configuration
        self.last_update = None
        self.update_interval = timedelta(minutes=5)
        self.target_crypto_count = 2000  # Increased target
        self.max_analysis_limit = 5000  # Maximum cryptos that can be analyzed at once
        
        # Background refresh management
        self.background_refresh_tasks = {}  # Track active background tasks
        self.refresh_status = "idle"  # idle, running, completed, failed
        self.last_refresh_duration = None
        self.last_refresh_error = None
        
    
    async def start_background_refresh(self, force: bool = False, periods: List[str] = None) -> str:
        """Start background refresh and return task ID immediately"""
        try:
            task_id = str(uuid.uuid4())
            
            # Don't start new refresh if one is already running
            if self.refresh_status == "running":
                return None  # Refresh already in progress
            
            self.refresh_status = "running"
            self.last_refresh_error = None
            
            # Start background task
            task = asyncio.create_task(
                self._background_refresh_worker(task_id, force, periods or ['24h', '7d', '30d'])
            )
            self.background_refresh_tasks[task_id] = task
            
            logger.info(f"Started background refresh task: {task_id}")
            return task_id
            
        except Exception as e:
            logger.error(f"Error starting background refresh: {e}")
            self.refresh_status = "failed"
            self.last_refresh_error = str(e)
            return None
    
    async def _background_refresh_worker(self, task_id: str, force: bool, periods: List[str]):
        """Background worker that does the actual refresh"""
        start_time = datetime.utcnow()
        
        try:
            logger.info(f"Background refresh {task_id} starting...")
            
            # Get fresh data using parallel requests
            fresh_cryptos = await self._fetch_fresh_data_parallel()
            
            if not fresh_cryptos:
                raise Exception("No fresh data obtained from external APIs")
            
            logger.info(f"Background refresh {task_id}: Got {len(fresh_cryptos)} fresh cryptos")
            
            # Store in database with quality validation
            stored_count = 0
            for crypto_data in fresh_cryptos:
                try:
                    await self.db_cache.store_crypto_data(crypto_data, validate=True)
                    stored_count += 1
                except Exception as e:
                    logger.warning(f"Failed to store crypto data: {e}")
                    continue
            
            logger.info(f"Background refresh {task_id}: Stored {stored_count} cryptos")
            
            # Update last refresh time
            self.last_update = datetime.utcnow()
            duration = (self.last_update - start_time).total_seconds()
            self.last_refresh_duration = duration
            
            self.refresh_status = "completed"
            logger.info(f"Background refresh {task_id} completed in {duration:.1f}s")
            
        except Exception as e:
            self.refresh_status = "failed"
            self.last_refresh_error = str(e)
            logger.error(f"Background refresh {task_id} failed: {e}")
            
        finally:
            # Clean up task
            if task_id in self.background_refresh_tasks:
                del self.background_refresh_tasks[task_id]
    
    async def _fetch_fresh_data_parallel(self) -> List[Dict[str, Any]]:
        """Fetch data from all sources in parallel for better performance"""
        try:
            # Create tasks for parallel execution
            tasks = []
            
            # Binance (if available)
            if self.binance_service.is_available():
                tasks.append(self._get_binance_data())
            
            # Yahoo Finance
            tasks.append(self._get_yahoo_data())
            
            # Fallback services (CoinGecko, Coinlore)
            tasks.append(self._get_fallback_data())
            
            # Execute all tasks in parallel with timeout
            logger.info(f"Starting {len(tasks)} parallel API requests...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Combine results
            all_crypto_data = {}
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.warning(f"API source {i} failed: {result}")
                    continue
                
                if isinstance(result, list):
                    for crypto_data in result:
                        symbol = crypto_data.get('symbol', '').upper()
                        if symbol:
                            if symbol not in all_crypto_data:
                                all_crypto_data[symbol] = crypto_data
                            else:
                                # Merge data from multiple sources
                                all_crypto_data[symbol] = self._merge_crypto_data(
                                    all_crypto_data[symbol], 
                                    crypto_data
                                )
            
            result_list = list(all_crypto_data.values())
            logger.info(f"Parallel fetch completed: {len(result_list)} unique cryptocurrencies")
            
            return result_list
            
        except Exception as e:
            logger.error(f"Error in parallel data fetch: {e}")
            return []
    
    def get_refresh_status(self) -> Dict[str, Any]:
        """Get current refresh status"""
        return {
            'status': self.refresh_status,
            'active_tasks': len(self.background_refresh_tasks),
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'last_duration_seconds': self.last_refresh_duration,
            'last_error': self.last_refresh_error,
            'next_auto_refresh': (self.last_update + self.update_interval).isoformat() if self.last_update else None
        }
    
    async def get_optimized_crypto_ranking(self, period: str = '24h', limit: int = 50, offset: int = 0, force_refresh: bool = False) -> List[CryptoCurrency]:
        """
        Récupère le classement crypto de manière optimisée avec pré-calcul
        """
        try:
            logger.info(f"Getting optimized ranking for {period} (limit: {limit}, offset: {offset})")
            
            # Essayer d'abord le classement pré-calculé
            if not force_refresh and hasattr(self, 'precompute_service'):
                precomputed = await self.precompute_service.get_precomputed_ranking(period, limit, offset)
                
                if precomputed:
                    logger.info(f"Using precomputed ranking for {period}: {len(precomputed)} cryptos")
                    return precomputed
                else:
                    logger.info(f"No valid precomputed ranking for {period}, computing on demand")
            
            # Fallback : calcul à la demande mais optimisé
            return await self._compute_ranking_on_demand(period, limit, offset)
            
        except Exception as e:
            logger.error(f"Error getting optimized crypto ranking: {e}")
            return []
    
    async def _compute_ranking_on_demand(self, period: str, limit: int, offset: int) -> List[CryptoCurrency]:
        """Calcule le classement à la demande de manière optimisée"""
        try:
            logger.info(f"Computing ranking on demand for {period}")
            
            # Dynamic limit calculation based on request size
            dynamic_fetch_limit = min(
                self.max_analysis_limit,  # System maximum
                max(1000, (offset + limit) * 3)  # Smart scaling: get 3x what's needed for better ranking
            )
            
            logger.info(f"Using dynamic fetch limit: {dynamic_fetch_limit} cryptos")
            
            cached_cryptos = await self._get_cached_crypto_data_limited([], dynamic_fetch_limit)
            
            if len(cached_cryptos) < 10:
                # Pas assez de données en cache, fallback vers l'API
                logger.info("Not enough cached data, falling back to API aggregation")
                all_cryptos = await self.get_aggregated_crypto_data(force_refresh=True)
                
                # Calculer les scores si pas déjà fait
                if hasattr(self, 'precompute_service') and self.precompute_service.scoring_service:
                    scored_cryptos = await self.precompute_service._optimized_scoring(all_cryptos, period)
                else:
                    # Fallback basique si scoring service non disponible
                    scored_cryptos = sorted(all_cryptos, key=lambda x: x.market_cap_usd or 0, reverse=True)
                    for i, crypto in enumerate(scored_cryptos):
                        crypto.rank = i + 1
                
                # Appliquer la pagination
                end_index = offset + limit
                return scored_cryptos[offset:end_index]
            
            # Convertir vers le format API et calculer les scores
            api_cryptos = await self._convert_to_api_format(cached_cryptos)
            
            if hasattr(self, 'precompute_service') and self.precompute_service.scoring_service:
                scored_cryptos = await self.precompute_service._optimized_scoring(api_cryptos, period)
            else:
                # Tri simple par market cap si pas de scoring
                scored_cryptos = sorted(api_cryptos, key=lambda x: x.market_cap_usd or 0, reverse=True)
                for i, crypto in enumerate(scored_cryptos):
                    crypto.rank = i + 1
            
            # Appliquer la pagination
            end_index = offset + limit
            result = scored_cryptos[offset:end_index]
            
            logger.info(f"Computed ranking on demand for {period}: {len(result)} cryptos returned")
            return result
            
        except Exception as e:
            logger.error(f"Error computing ranking on demand: {e}")
            return []
        
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
                
                # Récupérer les données manquantes (increase batch size)
                if missing_symbols:
                    batch_size = min(200, len(missing_symbols))  # Increased from 100
                    await self._fetch_missing_crypto_data(missing_symbols[:batch_size])
                
                # Identifier les données obsolètes (increase limit)
                stale_symbols = await self.db_cache.get_stale_data_symbols(limit=100)  # Increased from 50
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
            if self.db_cache.db is None:
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
    
    async def _get_cached_crypto_data_limited(self, required_fields: List[str], limit: int = 1000) -> List:
        """Récupère une quantité limitée de cryptos depuis le cache pour optimiser les performances"""
        try:
            if self.db_cache.db is None:
                return []
            
            # Récupérer seulement le nombre nécessaire de cryptos avec qualité acceptable
            cursor = self.db_cache.db.crypto_data.find({
                "data_quality": {"$ne": "invalid"},
                "quality_score": {"$gte": 35},  # Slightly lower minimum for more results
                "price_usd": {"$gt": 0}
            }).sort([
                ("quality_score", -1),
                ("market_cap_usd", -1)
            ]).limit(limit)
            
            cryptos = []
            async for doc in cursor:
                try:
                    from db_models import CryptoDataDB
                    crypto_db = CryptoDataDB(**doc)
                    
                    # Vérifier la fraîcheur si des champs spécifiques sont requis
                    if required_fields:
                        if not self.db_cache._check_data_freshness(crypto_db, required_fields):
                            continue
                    
                    cryptos.append(crypto_db)
                    
                except Exception as e:
                    logger.warning(f"Error parsing cached crypto data: {e}")
                    continue
            
            logger.info(f"Retrieved {len(cryptos)} limited cached cryptos")
            return cryptos
            
        except Exception as e:
            logger.error(f"Error retrieving limited cached crypto data: {e}")
            return []
    
    def set_db_client(self, db_client):
        """Configure le client de base de données"""
        self.db_cache.set_db_client(db_client)
    
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