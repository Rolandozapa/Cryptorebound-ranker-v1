import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from models import CryptoCurrency
from db_models import CryptoDataDB, DataSource
from services.binance_service import BinanceService
from services.yahoo_service import YahooFinanceService
from services.fallback_crypto_service import FallbackCryptoService
from services.cryptocompare_service import CryptoCompareService
from services.coinapi_service import CoinAPIService
from services.coinpaprika_service import CoinPaprikaService
from services.bitfinex_service import BitfinexService
from services.coinmarketcap_service import CoinMarketCapService
from services.database_cache_service import DatabaseCacheService
from services.data_enrichment_service import DataEnrichmentService
from services.ranking_precompute_service import RankingPrecomputeService
from services.historical_price_service import HistoricalPriceService
import uuid

logger = logging.getLogger(__name__)

class DataAggregationService:
    """Service d'agrégation intelligent avec cache DB et enrichissement"""
    
    def __init__(self, db_client=None):
        # Services de données - OPTIMISÉS POUR LA PERFORMANCE
        self.binance_service = BinanceService()
        self.yahoo_service = YahooFinanceService()
        self.fallback_service = FallbackCryptoService()
        self.cryptocompare_service = CryptoCompareService()
        # Premium/High-performance data sources
        self.coinapi_service = CoinAPIService()
        self.coinpaprika_service = CoinPaprikaService()
        self.bitfinex_service = BitfinexService()
        self.coinmarketcap_service = CoinMarketCapService()  # NEW: CoinMarketCap for premium data
        
        # Services de cache et enrichissement
        self.db_cache = DatabaseCacheService(db_client)
        self.enrichment_service = DataEnrichmentService(self.db_cache)
        self.precompute_service = RankingPrecomputeService(self.db_cache, None)
        self.historical_price_service = HistoricalPriceService()
        
        # Configuration optimisée pour la performance
        self.last_update = None
        self.update_interval = timedelta(minutes=3)  # Plus fréquent pour moins de latence
        self.target_crypto_count = 3000  # Increased for better coverage
        self.max_analysis_limit = 8000  # Increased maximum
        
        # Configuration de parallélisme améliorée
        self.max_concurrent_requests = 15  # Plus de requêtes simultanées
        self.request_semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        # Smart caching configuration based on periods
        self.period_freshness_thresholds = {
            '24h': timedelta(minutes=3),      # Plus agressif - 3 minutes au lieu de 4.3
            '7d': timedelta(minutes=20),      # Plus agressif - 20 minutes au lieu de 30
            '30d': timedelta(hours=1.5),      # Plus agressif - 1.5h au lieu de 2.2h
            '1h': timedelta(seconds=8),       # Plus agressif - 8s au lieu de 11s
            'default': timedelta(minutes=3)   # Plus agressif
        }
        
        # Memory cache optimisé
        self.memory_cache = {}
        self.memory_cache_timestamps = {}
        self.max_memory_cache_age = timedelta(minutes=45)  # Cache plus long pour performance
        
        # Background refresh management
        self.background_refresh_tasks = {}
        self.refresh_status = "idle"
        self.last_refresh_duration = None
        self.last_refresh_error = None
        
        # Load balancing strategy with 8 APIs - OPTIMISÉ
        self.load_balancing_thresholds = {
            'small': 150,     # Augmenté pour utiliser plus d'APIs
            'medium': 700,    # Augmenté
            'large': 2000,    # Augmenté
            'xlarge': 8000    # Maximum élargi
        }
        
    
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
        """Fetch data from all 8 sources in parallel with maximum performance optimization"""
        try:
            # Create tasks for parallel execution with intelligent prioritization and concurrency control
            tasks = []
            
            # Tier 1: Premium/Most reliable sources (priorité absolue)
            if self.coinmarketcap_service.is_available():
                tasks.append(('coinmarketcap', self._get_coinmarketcap_data()))
            tasks.append(('cryptocompare', self._get_cryptocompare_data()))
            if self.coinapi_service.is_available():
                tasks.append(('coinapi', self._get_coinapi_data()))
            
            # Tier 2: High-quality free sources
            tasks.append(('coinpaprika', self._get_coinpaprika_data()))
            tasks.append(('bitfinex', self._get_bitfinex_data()))
            
            # Tier 3: Existing reliable sources
            if self.binance_service.is_available():
                tasks.append(('binance', self._get_binance_data()))
            tasks.append(('yahoo', self._get_yahoo_data()))
            
            # Tier 4: Fallback sources (only if we need more data)
            tasks.append(('fallback', self._get_fallback_data()))
            
            # Execute all tasks in parallel with controlled concurrency and timeout
            logger.info(f"Starting {len(tasks)} high-performance parallel API requests across 8 data sources")
            
            # Use semaphore to control concurrency and prevent overwhelming APIs
            async def controlled_request(task_info):
                source_name, coro = task_info
                async with self.request_semaphore:
                    try:
                        return await asyncio.wait_for(coro, timeout=20)  # 20s timeout per API
                    except asyncio.TimeoutError:
                        logger.warning(f"API source {source_name} timed out after 20s")
                        return []
                    except Exception as e:
                        logger.warning(f"API source {source_name} failed: {e}")
                        return []
            
            # Execute tasks with controlled concurrency
            start_time = asyncio.get_event_loop().time()
            task_results = await asyncio.gather(*[controlled_request(task) for task in tasks], return_exceptions=True)
            total_time = asyncio.get_event_loop().time() - start_time
            
            # Combine results with enhanced priority system
            all_crypto_data = {}
            source_priority = {
                'coinmarketcap': 1,  # Highest priority - industry standard
                'cryptocompare': 2,  # Second highest - proven reliability
                'coinapi': 3,        # Premium service
                'coinpaprika': 4,    # Comprehensive free API
                'bitfinex': 5,       # Exchange data, good for major cryptos
                'binance': 6,        # Exchange data, reliable for top cryptos
                'yahoo': 7,          # Solid mainstream source
                'fallback': 8        # CoinGecko/Coinlore as last resort
            }
            
            # Track source performance
            successful_sources = []
            failed_sources = []
            total_cryptos_fetched = 0
            
            for i, (source_name, _) in enumerate(tasks):
                result = task_results[i]
                
                if isinstance(result, Exception):
                    logger.warning(f"API source {source_name} failed with exception: {result}")
                    failed_sources.append(source_name)
                    continue
                
                if isinstance(result, list) and len(result) > 0:
                    logger.info(f"Processing {len(result)} cryptos from {source_name}")
                    successful_sources.append(f"{source_name}({len(result)})")
                    total_cryptos_fetched += len(result)
                    
                    for crypto_data in result:
                        symbol = crypto_data.get('symbol', '').upper()
                        if not symbol:
                            continue
                        
                        # Use enhanced priority system to decide which data to keep
                        current_priority = source_priority.get(source_name, 999)
                        
                        if symbol not in all_crypto_data:
                            # First time seeing this crypto
                            crypto_data['primary_source'] = source_name
                            crypto_data['source_priority'] = current_priority
                            crypto_data['fetch_time'] = total_time
                            all_crypto_data[symbol] = crypto_data
                        else:
                            # Merge data, keeping higher priority source as primary
                            existing_priority = all_crypto_data[symbol].get('source_priority', 999)
                            
                            if current_priority < existing_priority:
                                # New source has higher priority, use it as primary
                                merged_data = self._merge_crypto_data(all_crypto_data[symbol], crypto_data)
                                merged_data['primary_source'] = source_name
                                merged_data['source_priority'] = current_priority
                                merged_data['fetch_time'] = total_time
                                all_crypto_data[symbol] = merged_data
                            else:
                                # Keep existing primary, but merge useful data
                                all_crypto_data[symbol] = self._merge_crypto_data(
                                    all_crypto_data[symbol], 
                                    crypto_data
                                )
                else:
                    logger.warning(f"API source {source_name} returned no data")
                    failed_sources.append(source_name)
            
            result_list = list(all_crypto_data.values())
            
            # Sort by priority and market cap for best quality first
            result_list.sort(key=lambda x: (
                x.get('source_priority', 999),
                -(x.get('market_cap_usd', 0) or 0)
            ))
            
            # Performance metrics
            performance_grade = "Excellent" if total_time < 10 else "Good" if total_time < 20 else "Acceptable"
            
            logger.info(f"🚀 Enhanced parallel fetch completed in {total_time:.2f}s ({performance_grade})")
            logger.info(f"📊 Results: {len(result_list)} unique cryptos from {total_cryptos_fetched} total fetched")
            logger.info(f"✅ Successful sources ({len(successful_sources)}): {', '.join(successful_sources)}")
            if failed_sources:
                logger.info(f"❌ Failed sources ({len(failed_sources)}): {', '.join(failed_sources)}")
            logger.info(f"🎯 Source distribution: {self._get_source_distribution(result_list)}")
            
            return result_list
            
        except Exception as e:
            logger.error(f"Error in enhanced parallel data fetch: {e}")
            return []
    
    async def _get_coinmarketcap_data(self) -> List[Dict[str, Any]]:
        """Get comprehensive data from CoinMarketCap (highest priority premium source)"""
        try:
            logger.info("Fetching comprehensive data from CoinMarketCap")
            
            # CoinMarketCap can handle very large datasets efficiently
            limit = min(2000, self.target_crypto_count)  # Up to 2000 cryptos - très performant
            
            crypto_data = await self.coinmarketcap_service.get_comprehensive_data(limit)
            
            if crypto_data:
                logger.info(f"Retrieved {len(crypto_data)} cryptocurrencies from CoinMarketCap")
                return crypto_data
            else:
                logger.warning("No data received from CoinMarketCap")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching data from CoinMarketCap: {e}")
            return []
    
    async def _get_cryptocompare_data(self) -> List[Dict[str, Any]]:
        """Get comprehensive data from CryptoCompare (prioritized for large datasets)"""
        try:
            logger.info("Fetching comprehensive data from CryptoCompare")
            
            # CryptoCompare can handle large datasets efficiently
            limit = min(1000, self.target_crypto_count)  # Up to 1000 cryptos
            
            crypto_data = await self.cryptocompare_service.get_comprehensive_data(limit)
            
            if crypto_data:
                logger.info(f"Retrieved {len(crypto_data)} cryptocurrencies from CryptoCompare")
                return crypto_data
            else:
                logger.warning("No data received from CryptoCompare")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching data from CryptoCompare: {e}")
            return []
    
    async def _get_coinapi_data(self) -> List[Dict[str, Any]]:
        """Get comprehensive data from CoinAPI (premium service)"""
        try:
            logger.info("Fetching comprehensive data from CoinAPI")
            
            # CoinAPI has rate limits but provides high-quality data
            limit = min(500, self.target_crypto_count)  # Conservative limit due to rate limits
            
            crypto_data = await self.coinapi_service.get_comprehensive_data(limit)
            
            if crypto_data:
                logger.info(f"Retrieved {len(crypto_data)} cryptocurrencies from CoinAPI")
                return crypto_data
            else:
                logger.warning("No data received from CoinAPI")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching data from CoinAPI: {e}")
            return []
    
    async def _get_coinpaprika_data(self) -> List[Dict[str, Any]]:
        """Get comprehensive data from CoinPaprika"""
        try:
            logger.info("Fetching comprehensive data from CoinPaprika")
            
            # CoinPaprika provides good coverage for free
            limit = min(1000, self.target_crypto_count)  # Up to 1000 cryptos
            
            crypto_data = await self.coinpaprika_service.get_comprehensive_data(limit)
            
            if crypto_data:
                logger.info(f"Retrieved {len(crypto_data)} cryptocurrencies from CoinPaprika")
                return crypto_data
            else:
                logger.warning("No data received from CoinPaprika")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching data from CoinPaprika: {e}")
            return []
    
    async def _get_bitfinex_data(self) -> List[Dict[str, Any]]:
        """Get comprehensive data from Bitfinex"""
        try:
            logger.info("Fetching comprehensive data from Bitfinex")
            
            # Bitfinex provides exchange data, good for major cryptos
            limit = min(200, self.target_crypto_count)  # Conservative limit for exchange API
            
            crypto_data = await self.bitfinex_service.get_comprehensive_data(limit)
            
            if crypto_data:
                logger.info(f"Retrieved {len(crypto_data)} cryptocurrencies from Bitfinex")
                return crypto_data
            else:
                logger.warning("No data received from Bitfinex")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching data from Bitfinex: {e}")
            return []
    
    def _get_freshness_threshold_for_period(self, period: str) -> timedelta:
        """Calculate freshness threshold based on period (0.3% of period duration)"""
        return self.period_freshness_thresholds.get(period, self.period_freshness_thresholds['default'])
    
    def _is_data_fresh_for_period(self, last_updated: datetime, period: str) -> bool:
        """Check if data is fresh enough for the given period"""
        if not last_updated:
            return False
        
        threshold = self._get_freshness_threshold_for_period(period)
        time_since_update = datetime.utcnow() - last_updated
        
        is_fresh = time_since_update <= threshold
        
        if is_fresh:
            logger.info(f"Data is fresh for period {period}: {time_since_update} <= {threshold}")
        else:
            logger.info(f"Data needs refresh for period {period}: {time_since_update} > {threshold}")
        
        return is_fresh
    
    def _get_memory_cached_data(self, cache_key: str) -> Optional[List]:
        """Get data from memory cache if fresh"""
        if cache_key not in self.memory_cache:
            return None
        
        cache_timestamp = self.memory_cache_timestamps.get(cache_key)
        if not cache_timestamp:
            return None
        
        # Check if memory cache is still valid
        if datetime.utcnow() - cache_timestamp > self.max_memory_cache_age:
            # Clean up old cache
            del self.memory_cache[cache_key]
            del self.memory_cache_timestamps[cache_key]
            return None
        
        logger.info(f"Using memory cached data for {cache_key}")
        return self.memory_cache[cache_key]
    
    def _set_memory_cached_data(self, cache_key: str, data: List):
        """Store data in memory cache"""
        self.memory_cache[cache_key] = data
        self.memory_cache_timestamps[cache_key] = datetime.utcnow()
        logger.info(f"Cached {len(data) if data else 0} items in memory for {cache_key}")
    
    def _clean_memory_cache(self):
        """Clean up expired memory cache entries"""
        current_time = datetime.utcnow()
        expired_keys = []
        
        for key, timestamp in self.memory_cache_timestamps.items():
            if current_time - timestamp > self.max_memory_cache_age:
                expired_keys.append(key)
        
        for key in expired_keys:
            if key in self.memory_cache:
                del self.memory_cache[key]
            if key in self.memory_cache_timestamps:
                del self.memory_cache_timestamps[key]
        
        if expired_keys:
            logger.info(f"Cleaned up {len(expired_keys)} expired memory cache entries")

    def _get_source_distribution(self, crypto_list: List[Dict]) -> Dict[str, int]:
        """Get distribution of cryptocurrencies by primary source"""
        distribution = {}
        for crypto in crypto_list:
            source = crypto.get('primary_source', 'unknown')
            distribution[source] = distribution.get(source, 0) + 1
        return distribution
    
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
        Récupère le classement crypto de manière optimisée avec cache intelligent basé sur les périodes
        """
        try:
            logger.info(f"Getting optimized ranking for {period} (limit: {limit}, offset: {offset}, force_refresh: {force_refresh})")
            
            # Clean up expired memory cache
            self._clean_memory_cache()
            
            # Check memory cache first (highest priority)
            cache_key = f"ranking_{period}_{limit}_{offset}"
            if not force_refresh:
                memory_cached = self._get_memory_cached_data(cache_key)
                if memory_cached:
                    logger.info(f"Returning {len(memory_cached)} cryptos from memory cache")
                    return memory_cached
            
            # Check if we should skip API calls based on period freshness
            if not force_refresh and hasattr(self, 'precompute_service') and self.last_update:
                if self._is_data_fresh_for_period(self.last_update, period):
                    logger.info(f"Data is fresh enough for {period}, using precomputed ranking")
                    
                    # Try precomputed ranking with fresh data
                    precomputed = await self.precompute_service.get_precomputed_ranking(period, limit, offset)
                    if precomputed:
                        # Cache in memory for future requests
                        self._set_memory_cached_data(cache_key, precomputed)
                        logger.info(f"Using precomputed ranking for {period}: {len(precomputed)} cryptos")
                        return precomputed
            
            # If data is not fresh enough or precomputed not available, compute on demand
            # But avoid heavy API calls if data was updated recently and we're in dev/intense activity
            if (not force_refresh and 
                self.last_update and 
                (datetime.utcnow() - self.last_update) < timedelta(minutes=2)):
                
                logger.info("Recently updated data detected, preferring DB over fresh API calls")
                result = await self._compute_ranking_on_demand_fast(period, limit, offset)
            else:
                # Normal computation with potential API refresh
                result = await self._compute_ranking_on_demand(period, limit, offset)
            
            # Cache successful result in memory
            if result:
                self._set_memory_cached_data(cache_key, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting optimized crypto ranking: {e}")
            return []
    
    async def _compute_ranking_on_demand_fast(self, period: str, limit: int, offset: int) -> List[CryptoCurrency]:
        """Calcule le classement rapidement en privilégiant les données DB (mode développement/activité intense)"""
        try:
            logger.info(f"Fast ranking computation for {period} (DB-preferred mode)")
            
            # Use existing cached data with extended limit for better results
            dynamic_fetch_limit = min(
                self.max_analysis_limit,
                max(2000, (offset + limit) * 5)  # Get more data for better ranking quality
            )
            
            logger.info(f"Using fast DB fetch limit: {dynamic_fetch_limit} cryptos")
            
            cached_cryptos = await self._get_cached_crypto_data_limited([], dynamic_fetch_limit)
            
            if len(cached_cryptos) < max(50, offset + limit):
                logger.warning(f"Insufficient DB data ({len(cached_cryptos)} cryptos), falling back to minimal API refresh")
                # Fallback to normal method if really insufficient data
                return await self._compute_ranking_on_demand(period, limit, offset)
            
            # Convert to API format
            api_cryptos = await self._convert_to_api_format(cached_cryptos)
            
            # Calculate scores with existing data
            if hasattr(self, 'precompute_service') and self.precompute_service.scoring_service:
                scored_cryptos = await self.precompute_service._optimized_scoring(api_cryptos, period)
            else:
                # Simple sorting by market cap if no scoring available
                scored_cryptos = sorted(api_cryptos, key=lambda x: x.market_cap_usd or 0, reverse=True)
                for i, crypto in enumerate(scored_cryptos):
                    crypto.rank = i + 1
            
            # Apply pagination
            end_index = offset + limit
            result = scored_cryptos[offset:end_index]
            
            logger.info(f"Fast ranking completed for {period}: {len(result)} cryptos returned from DB")
            return result
            
        except Exception as e:
            logger.error(f"Error in fast ranking computation: {e}")
            # Fallback to normal method
            return await self._compute_ranking_on_demand(period, limit, offset)
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
                all_cryptos = await self.get_aggregated_crypto_data(force_refresh=True, period=period)
                
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
        
    async def get_aggregated_crypto_data(self, force_refresh: bool = False, required_fields: List[str] = None, request_size: int = None, period: str = '24h') -> List[CryptoCurrency]:
        """
        Récupère les données crypto de manière intelligente avec cache basé sur les périodes
        Enhanced with intelligent load balancing and period-based caching
        """
        try:
            logger.info(f"Starting intelligent data aggregation for {request_size or 'unknown'} cryptos (period: {period})")
            
            # Clean up memory cache
            self._clean_memory_cache()
            
            # Check if data is fresh enough for this period
            skip_api_calls = False
            if not force_refresh and self.last_update:
                if self._is_data_fresh_for_period(self.last_update, period):
                    skip_api_calls = True
                    logger.info(f"Data is fresh for {period}, preferring DB over API calls")
            
            # 1. Récupérer les données depuis la DB d'abord
            cached_cryptos = await self._get_cached_crypto_data(required_fields or [])
            logger.info(f"Retrieved {len(cached_cryptos)} cryptocurrencies from cache")
            
            # 2. Determine if we need more data
            need_more_data = (
                force_refresh or 
                len(cached_cryptos) < self.target_crypto_count or
                (request_size and len(cached_cryptos) < request_size * 1.2)  # 20% buffer
            )
            
            # 3. Only make API calls if data is not fresh or we really need more data
            if need_more_data and not skip_api_calls:
                logger.info("Making selective API calls to complement cache")
                
                # Determine load balancing strategy
                if request_size:
                    strategy = self._get_load_balancing_strategy(request_size)
                    logger.info(f"Using {strategy} strategy for {request_size} cryptos")
                else:
                    strategy = 'medium'  # Default strategy
                
                # Use strategy-specific data fetching
                fresh_data = await self._fetch_data_by_strategy(strategy, request_size)
                
                if fresh_data:
                    # Store new data in database
                    stored_count = 0
                    for crypto_data in fresh_data:
                        try:
                            await self.db_cache.store_crypto_data(crypto_data, validate=True)
                            stored_count += 1
                        except Exception as e:
                            logger.warning(f"Failed to store crypto data: {e}")
                            continue
                    
                    logger.info(f"Stored {stored_count} new/updated cryptocurrencies in database")
                    
                    # Update last refresh time
                    self.last_update = datetime.utcnow()
                
                # Re-fetch updated data from cache
                cached_cryptos = await self._get_cached_crypto_data(required_fields or [])
                
            elif skip_api_calls:
                logger.info(f"Skipping API calls - data is fresh enough for period {period}")
            
            # 4. Programme l'enrichissement en arrière-plan si nécessaire (but not in intense activity)
            if not skip_api_calls:
                await self._schedule_background_enrichment()
            
            # 5. Convertir en format API
            api_cryptos = await self._convert_to_api_format(cached_cryptos)
            
            logger.info(f"Intelligent data aggregation completed: {len(api_cryptos)} cryptocurrencies ready")
            return api_cryptos
            
        except Exception as e:
            logger.error(f"Error in intelligent data aggregation: {e}")
            return []
    
    def _get_load_balancing_strategy(self, request_size: int) -> str:
        """Determine the best load balancing strategy based on request size"""
        if request_size <= self.load_balancing_thresholds['small']:
            return 'small'
        elif request_size <= self.load_balancing_thresholds['medium']:
            return 'medium'
        elif request_size <= self.load_balancing_thresholds['large']:
            return 'large'
        else:
            return 'xlarge'
    
    async def _fetch_data_by_strategy(self, strategy: str, request_size: int = None) -> List[Dict[str, Any]]:
        """Fetch data using strategy-specific approach"""
        try:
            logger.info(f"Fetching data using {strategy} strategy")
            
            if strategy == 'small':
                # Small requests: Use lightweight fallback APIs
                return await self._fetch_small_dataset()
                
            elif strategy == 'medium':
                # Medium requests: CryptoCompare + selective others
                return await self._fetch_medium_dataset()
                
            elif strategy == 'large':
                # Large requests: CryptoCompare primary + complementary sources
                return await self._fetch_large_dataset()
                
            else:  # xlarge
                # Very large requests: Full CryptoCompare + selected high-quality sources
                return await self._fetch_xlarge_dataset()
                
        except Exception as e:
            logger.error(f"Error in strategy-based fetching: {e}")
            return []
    
    async def _fetch_small_dataset(self) -> List[Dict[str, Any]]:
        """Optimized for ≤100 cryptos: Use lightweight and reliable APIs"""
        try:
            tasks = []
            
            # Primary: Use CoinPaprika (free, comprehensive)
            tasks.append(('coinpaprika', self._get_coinpaprika_data()))
            
            # Secondary: Use fallback APIs (CoinGecko, Coinlore)
            tasks.append(('fallback', self._get_fallback_data()))
            
            # Add Binance if available (good for top cryptos)
            if self.binance_service.is_available():
                tasks.append(('binance', self._get_binance_data()))
            
            # Add Bitfinex for additional coverage
            tasks.append(('bitfinex', self._get_bitfinex_data()))
            
            results = await asyncio.gather(*[task[1] for task in tasks], return_exceptions=True)
            
            # Merge results with CoinPaprika as primary
            return await self._merge_results_with_priority(tasks, results, primary='coinpaprika')
            
        except Exception as e:
            logger.error(f"Error in small dataset fetch: {e}")
            return []
    
    async def _fetch_medium_dataset(self) -> List[Dict[str, Any]]:
        """Optimized for 101-500 cryptos: Mixed high-quality sources"""
        try:
            tasks = []
            
            # Primary: CryptoCompare (proven reliable for medium datasets)
            tasks.append(('cryptocompare', self._get_cryptocompare_data()))
            
            # Secondary: CoinPaprika (comprehensive free coverage)
            tasks.append(('coinpaprika', self._get_coinpaprika_data()))
            
            # Add CoinAPI if available (premium data)
            if self.coinapi_service.is_available():
                tasks.append(('coinapi', self._get_coinapi_data()))
            
            # Complement with Bitfinex for exchange data
            tasks.append(('bitfinex', self._get_bitfinex_data()))
            
            # Binance for high-quality top cryptos
            if self.binance_service.is_available():
                tasks.append(('binance', self._get_binance_data()))
            
            # Fallback sources
            tasks.append(('fallback', self._get_fallback_data()))
            
            results = await asyncio.gather(*[task[1] for task in tasks], return_exceptions=True)
            
            # Merge with CryptoCompare as primary
            return await self._merge_results_with_priority(tasks, results, primary='cryptocompare')
            
        except Exception as e:
            logger.error(f"Error in medium dataset fetch: {e}")
            return []
    
    async def _fetch_large_dataset(self) -> List[Dict[str, Any]]:
        """Optimized for 501-1500 cryptos: Heavy APIs with comprehensive coverage"""
        try:
            tasks = []
            
            # Tier 1: Premium/Most reliable sources
            tasks.append(('cryptocompare', self._get_cryptocompare_data()))
            if self.coinapi_service.is_available():
                tasks.append(('coinapi', self._get_coinapi_data()))
            
            # Tier 2: High-coverage free sources
            tasks.append(('coinpaprika', self._get_coinpaprika_data()))
            
            # Tier 3: Exchange data
            tasks.append(('bitfinex', self._get_bitfinex_data()))
            if self.binance_service.is_available():
                tasks.append(('binance', self._get_binance_data()))
            
            # Tier 4: Fallback for additional coverage
            tasks.append(('fallback', self._get_fallback_data()))
            
            results = await asyncio.gather(*[task[1] for task in tasks], return_exceptions=True)
            
            # Use advanced merging logic for large datasets
            all_data = {}
            
            # Process in order of preference
            priority_order = ['cryptocompare', 'coinapi', 'coinpaprika', 'bitfinex', 'binance', 'fallback']
            
            for priority_source in priority_order:
                # Find this source in tasks
                for i, (source_name, _) in enumerate(tasks):
                    if source_name == priority_source and i < len(results):
                        result = results[i]
                        if isinstance(result, list):
                            logger.info(f"Processing {len(result)} cryptos from {source_name} (large dataset)")
                            for crypto in result:
                                symbol = crypto.get('symbol', '').upper()
                                if symbol and symbol not in all_data:
                                    all_data[symbol] = crypto
                        break
            
            logger.info(f"Large dataset: {len(all_data)} unique cryptos")
            return list(all_data.values())
            
        except Exception as e:
            logger.error(f"Error in large dataset fetch: {e}")
            return []
    
    async def _fetch_xlarge_dataset(self) -> List[Dict[str, Any]]:
        """Optimized for 1500+ cryptos: All APIs with intelligent prioritization"""
        try:
            tasks = []
            
            # Use all available APIs for maximum coverage
            tasks.append(('cryptocompare', self._get_cryptocompare_data()))
            
            if self.coinapi_service.is_available():
                tasks.append(('coinapi', self._get_coinapi_data()))
            
            tasks.append(('coinpaprika', self._get_coinpaprika_data()))
            tasks.append(('bitfinex', self._get_bitfinex_data()))
            
            if self.binance_service.is_available():
                tasks.append(('binance', self._get_binance_data()))
            
            tasks.append(('yahoo', self._get_yahoo_data()))
            tasks.append(('fallback', self._get_fallback_data()))
            
            logger.info(f"XL dataset: Using all {len(tasks)} available APIs")
            
            results = await asyncio.gather(*[task[1] for task in tasks], return_exceptions=True)
            
            # Advanced merging with quality scoring
            all_data = {}
            source_quality = {
                'cryptocompare': 10,
                'coinapi': 9,
                'coinpaprika': 8,
                'bitfinex': 7,
                'binance': 6,
                'yahoo': 5,
                'fallback': 4
            }
            
            # Process results
            for i, (source_name, _) in enumerate(tasks):
                result = results[i]
                if isinstance(result, list):
                    logger.info(f"Processing {len(result)} cryptos from {source_name} (XL dataset)")
                    quality_score = source_quality.get(source_name, 1)
                    
                    for crypto in result:
                        symbol = crypto.get('symbol', '').upper()
                        if symbol:
                            if symbol not in all_data:
                                crypto['source_quality_score'] = quality_score
                                all_data[symbol] = crypto
                            else:
                                # Keep higher quality source
                                existing_quality = all_data[symbol].get('source_quality_score', 0)
                                if quality_score > existing_quality:
                                    crypto['source_quality_score'] = quality_score
                                    # Merge some data but keep new as primary
                                    merged = self._merge_crypto_data(all_data[symbol], crypto)
                                    merged['source_quality_score'] = quality_score
                                    all_data[symbol] = merged
            
            result_list = list(all_data.values())
            
            # Sort by quality and market cap
            result_list.sort(key=lambda x: (
                -x.get('source_quality_score', 0),
                -(x.get('market_cap_usd', 0) or 0)
            ))
            
            logger.info(f"XL dataset complete: {len(result_list)} unique cryptos from {len(tasks)} APIs")
            return result_list
            
        except Exception as e:
            logger.error(f"Error in XL dataset fetch: {e}")
            return []
    
    async def _merge_results_with_priority(self, tasks, results, primary: str) -> List[Dict[str, Any]]:
        """Merge results from multiple sources with priority system"""
        all_data = {}
        
        # Find primary source index
        primary_index = None
        for i, (source_name, _) in enumerate(tasks):
            if source_name == primary:
                primary_index = i
                break
        
        # Process primary source first
        if primary_index is not None and primary_index < len(results):
            primary_result = results[primary_index]
            if isinstance(primary_result, list):
                for crypto in primary_result:
                    symbol = crypto.get('symbol', '').upper()
                    if symbol:
                        all_data[symbol] = crypto
        
        # Add data from other sources for missing symbols
        for i, (source_name, _) in enumerate(tasks):
            if i == primary_index:
                continue
                
            result = results[i]
            if isinstance(result, list):
                for crypto in result:
                    symbol = crypto.get('symbol', '').upper()
                    if symbol and symbol not in all_data:
                        all_data[symbol] = crypto
        
        return list(all_data.values())
    
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
    
    async def get_enhanced_crypto_ranking(self, period: str = '24h', limit: int = 50, offset: int = 0, force_refresh: bool = False, fix_historical: bool = True) -> List[CryptoCurrency]:
        """Get crypto ranking with enhanced historical data accuracy"""
        try:
            logger.info(f"Getting enhanced crypto ranking for period {period}, limit {limit}, fix_historical={fix_historical}")
            
            # Get base ranking data
            cryptos = await self.get_optimized_crypto_ranking(period, limit, offset, force_refresh)
            
            if not cryptos or not fix_historical:
                return cryptos
            
            # Identify cryptos with missing or suspicious max_price_1y data
            cryptos_needing_fix = []
            for crypto in cryptos:
                needs_fix = (
                    not crypto.max_price_1y or 
                    crypto.max_price_1y <= 0 or
                    crypto.max_price_1y < crypto.price_usd or  # Max should be >= current
                    crypto.max_price_1y < crypto.price_usd * 1.1  # Max should be significantly higher than current
                )
                
                if needs_fix:
                    cryptos_needing_fix.append(crypto)
            
            logger.info(f"Found {len(cryptos_needing_fix)}/{len(cryptos)} cryptos with potentially incorrect historical data")
            
            if cryptos_needing_fix:
                # Update historical data for problematic cryptos
                logger.info("Updating historical price data for cryptos with missing/incorrect max_price_1y")
                updated_cryptos = await self.historical_price_service.batch_update_historical_data(cryptos_needing_fix)
                
                # Replace the updated cryptos in the original list
                crypto_map = {crypto.symbol: crypto for crypto in updated_cryptos}
                for i, crypto in enumerate(cryptos):
                    if crypto.symbol in crypto_map:
                        cryptos[i] = crypto_map[crypto.symbol]
                
                # Re-calculate scores with the corrected historical data
                logger.info("Re-calculating scores with corrected historical data")
                if hasattr(self, 'scoring_service') and self.scoring_service:
                    cryptos = self.scoring_service.calculate_scores(cryptos, period)
                
                logger.info(f"Successfully updated historical data and recalculated scores for {len(cryptos_needing_fix)} cryptos")
            
            return cryptos
            
        except Exception as e:
            logger.error(f"Error in enhanced crypto ranking: {e}")
            # Fallback to basic ranking if enhancement fails
            return await self.get_optimized_crypto_ranking(period, limit, offset, force_refresh)
    
    def set_scoring_service(self, scoring_service):
        """Configure the scoring service for precomputation"""
        if hasattr(self, 'precompute_service') and self.precompute_service:
            self.precompute_service.scoring_service = scoring_service
    
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
            'cryptocompare': self.cryptocompare_service.is_available(),
            'coinapi': self.coinapi_service.is_available(),
            'coinpaprika': self.coinpaprika_service.is_available(),
            'bitfinex': self.bitfinex_service.is_available(),
            'coinmarketcap': self.coinmarketcap_service.is_available(),
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
            await self.coinapi_service.close()
            await self.coinpaprika_service.close()
            await self.bitfinex_service.close()
            await self.coinmarketcap_service.close()
        except Exception as e:
            logger.error(f"Error cleaning up resources: {e}")