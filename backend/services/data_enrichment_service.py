import asyncio
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from db_models import CryptoDataDB, DataSource, EnrichmentTask
from services.database_cache_service import DatabaseCacheService
from services.binance_service import BinanceService
from services.yahoo_service import YahooFinanceService
from services.fallback_crypto_service import FallbackCryptoService

logger = logging.getLogger(__name__)

class DataEnrichmentService:
    """Service d'enrichissement progressif des données crypto"""
    
    def __init__(self, db_cache_service: DatabaseCacheService):
        self.db_cache = db_cache_service
        self.binance_service = BinanceService()
        self.yahoo_service = YahooFinanceService()
        self.fallback_service = FallbackCryptoService()
        
        # Configuration des sources par type de données
        self.field_source_mapping = {
            'price_usd': [DataSource.BINANCE, DataSource.COINGECKO, DataSource.YAHOO_FINANCE],
            'market_cap_usd': [DataSource.COINGECKO, DataSource.COINLORE, DataSource.YAHOO_FINANCE],
            'volume_24h_usd': [DataSource.BINANCE, DataSource.COINGECKO, DataSource.COINLORE],
            'percent_change_24h': [DataSource.BINANCE, DataSource.COINGECKO, DataSource.YAHOO_FINANCE],
            'percent_change_7d': [DataSource.COINGECKO, DataSource.YAHOO_FINANCE, DataSource.COINLORE],
            'percent_change_30d': [DataSource.COINGECKO, DataSource.YAHOO_FINANCE],
            'historical_prices': [DataSource.YAHOO_FINANCE, DataSource.COINGECKO],
            'max_price_1y': [DataSource.YAHOO_FINANCE, DataSource.COINGECKO],
            'min_price_1y': [DataSource.YAHOO_FINANCE, DataSource.COINGECKO]
        }
        
        # Limite de rate limiting
        self.rate_limits = {
            DataSource.BINANCE: 0.1,  # 100ms entre appels
            DataSource.COINGECKO: 1.0,  # 1s entre appels
            DataSource.YAHOO_FINANCE: 0.5,  # 500ms entre appels
            DataSource.COINLORE: 1.5   # 1.5s entre appels
        }
        
        self.last_api_calls = {}
    
    async def enrich_crypto_data(self, symbol: str, missing_fields: List[str] = None) -> bool:
        """Enrichit les données d'une crypto-monnaie spécifique"""
        try:
            logger.info(f"Starting enrichment for {symbol}")
            
            # Récupérer les données existantes
            existing_data = await self.db_cache.get_crypto_data(symbol, required_fields=[])
            
            if not existing_data:
                logger.info(f"No existing data for {symbol}, performing full data fetch")
                return await self._fetch_complete_data(symbol)
            
            # Déterminer quels champs enrichir
            if not missing_fields:
                missing_fields = self.db_cache.quality_service.suggest_enrichment_fields(existing_data.dict())
            
            if not missing_fields:
                logger.debug(f"No fields need enrichment for {symbol}")
                return True
            
            logger.info(f"Enriching {len(missing_fields)} fields for {symbol}: {missing_fields}")
            
            # Enrichir par groupe de champs selon les sources
            enriched_data = {}
            
            for field in missing_fields:
                field_data = await self._enrich_specific_field(symbol, field)
                if field_data:
                    enriched_data.update(field_data)
            
            if enriched_data:
                # Ajouter métadonnées d'enrichissement
                enriched_data.update({
                    'symbol': symbol,
                    'last_enrichment': datetime.utcnow(),
                    'needs_enrichment': len(missing_fields) > len(enriched_data)
                })
                
                # Sauvegarder en DB
                success = await self.db_cache.store_crypto_data(enriched_data, validate=True)
                
                if success:
                    logger.info(f"Successfully enriched {len(enriched_data)} fields for {symbol}")
                    return True
                else:
                    logger.warning(f"Failed to store enriched data for {symbol}")
                    return False
            else:
                logger.warning(f"No data could be enriched for {symbol}")
                return False
            
        except Exception as e:
            logger.error(f"Error enriching data for {symbol}: {e}")
            return False
    
    async def _enrich_specific_field(self, symbol: str, field: str) -> Dict[str, Any]:
        """Enrichit un champ spécifique d'une crypto"""
        try:
            preferred_sources = self.field_source_mapping.get(field, [DataSource.COINGECKO])
            
            for source in preferred_sources:
                try:
                    # Respecter le rate limiting
                    await self._respect_rate_limit(source)
                    
                    # Récupérer les données de la source
                    field_data = await self._fetch_field_from_source(symbol, field, source)
                    
                    if field_data and field_data.get(field) is not None:
                        # Ajouter metadata de source
                        field_data['data_sources'] = field_data.get('data_sources', [])
                        if source not in field_data['data_sources']:
                            field_data['data_sources'].append(source)
                        
                        # Timestamp du champ
                        source_timestamps = field_data.get('source_timestamps', {})
                        source_timestamps[field] = datetime.utcnow()
                        field_data['source_timestamps'] = source_timestamps
                        
                        # Mettre à jour les métriques de succès
                        await self.db_cache.update_source_metrics(source, symbol, True)
                        
                        logger.debug(f"Successfully fetched {field} for {symbol} from {source}")
                        return field_data
                
                except Exception as e:
                    logger.warning(f"Failed to fetch {field} for {symbol} from {source}: {e}")
                    await self.db_cache.update_source_metrics(source, symbol, False)
                    continue
            
            logger.warning(f"Could not enrich {field} for {symbol} from any source")
            return {}
            
        except Exception as e:
            logger.error(f"Error enriching field {field} for {symbol}: {e}")
            return {}
    
    async def _fetch_field_from_source(self, symbol: str, field: str, source: DataSource) -> Dict[str, Any]:
        """Récupère un champ spécifique d'une source donnée"""
        try:
            if source == DataSource.BINANCE and self.binance_service.is_available():
                return await self._fetch_from_binance(symbol, field)
            
            elif source == DataSource.YAHOO_FINANCE:
                return await self._fetch_from_yahoo(symbol, field)
            
            elif source == DataSource.COINGECKO or source == DataSource.COINLORE:
                return await self._fetch_from_fallback(symbol, field)
            
            else:
                logger.debug(f"Source {source} not available or not implemented")
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching {field} for {symbol} from {source}: {e}")
            return {}
    
    async def _fetch_from_binance(self, symbol: str, field: str) -> Dict[str, Any]:
        """Récupère des données depuis Binance"""
        try:
            if field in ['price_usd', 'percent_change_24h', 'volume_24h_usd']:
                # Utiliser les stats 24h
                stats = await self.binance_service.get_24hr_ticker_stats()
                
                for stat in stats:
                    if stat.get('symbol') == symbol:
                        result = {'source': DataSource.BINANCE}
                        
                        if field == 'price_usd':
                            result['price_usd'] = stat.get('price')
                        elif field == 'percent_change_24h':
                            result['percent_change_24h'] = stat.get('percent_change_24h')
                        elif field == 'volume_24h_usd':
                            result['volume_24h_usd'] = stat.get('volume_24h')
                        
                        return result
            
            elif field == 'historical_prices':
                # Récupérer les données historiques
                historical = await self.binance_service.get_historical_klines(symbol)
                if historical:
                    prices = {}
                    for i, period in enumerate(['1d', '7d', '30d', '90d']):
                        if i < len(historical):
                            prices[period] = historical[i].get('close', 0)
                    
                    return {
                        'historical_prices': prices,
                        'source': DataSource.BINANCE
                    }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error fetching from Binance: {e}")
            return {}
    
    async def _fetch_from_yahoo(self, symbol: str, field: str) -> Dict[str, Any]:
        """Récupère des données depuis Yahoo Finance"""
        try:
            # Yahoo Finance nécessite des symboles spécifiques
            yahoo_symbols = [f"{symbol}-USD"]
            
            data_list = await self.yahoo_service.get_crypto_data(yahoo_symbols)
            
            if data_list:
                data = data_list[0]
                result = {'source': DataSource.YAHOO_FINANCE}
                
                if field in data:
                    result[field] = data[field]
                elif field == 'price_usd' and 'price' in data:
                    result['price_usd'] = data['price']
                elif field == 'market_cap_usd' and 'market_cap' in data:
                    result['market_cap_usd'] = data['market_cap']
                
                return result if len(result) > 1 else {}
            
            return {}
            
        except Exception as e:
            logger.error(f"Error fetching from Yahoo Finance: {e}")
            return {}
    
    async def _fetch_from_fallback(self, symbol: str, field: str) -> Dict[str, Any]:
        """Récupère des données depuis les services de fallback"""
        try:
            # Récupérer toutes les cryptos et filtrer
            all_data = await self.fallback_service.get_crypto_data(limit=100)
            
            for data in all_data:
                if data.get('symbol', '').upper() == symbol.upper():
                    result = {'source': data.get('source', DataSource.COINGECKO)}
                    
                    # Mapper les champs
                    field_mapping = {
                        'price_usd': 'price',
                        'market_cap_usd': 'market_cap',
                        'volume_24h_usd': 'volume_24h',
                        'percent_change_24h': 'percent_change_24h',
                        'percent_change_7d': 'percent_change_7d',
                        'percent_change_30d': 'percent_change_30d'
                    }
                    
                    source_field = field_mapping.get(field, field)
                    if source_field in data:
                        result[field] = data[source_field]
                        return result
            
            return {}
            
        except Exception as e:
            logger.error(f"Error fetching from fallback services: {e}")
            return {}
    
    async def _fetch_complete_data(self, symbol: str) -> bool:
        """Récupère un set complet de données pour une crypto"""
        try:
            logger.info(f"Fetching complete data for {symbol}")
            
            # Essayer de récupérer depuis fallback (plus complet)
            fallback_data = await self.fallback_service.get_crypto_data(limit=2000)
            
            for data in fallback_data:
                if data.get('symbol', '').upper() == symbol.upper():
                    # Enrichir avec des données historiques si possible
                    if self.yahoo_service.is_available():
                        historical = await self.fallback_service.get_historical_data(symbol)
                        if historical:
                            data.update(historical)
                    
                    # Sauvegarder
                    success = await self.db_cache.store_crypto_data(data, validate=True)
                    if success:
                        logger.info(f"Successfully stored complete data for {symbol}")
                        return True
            
            logger.warning(f"Could not fetch complete data for {symbol}")
            return False
            
        except Exception as e:
            logger.error(f"Error fetching complete data for {symbol}: {e}")
            return False
    
    async def _respect_rate_limit(self, source: DataSource):
        """Respecte les limites de taux d'appel API"""
        if source in self.rate_limits:
            last_call = self.last_api_calls.get(source)
            if last_call:
                elapsed = (datetime.utcnow() - last_call).total_seconds()
                min_interval = self.rate_limits[source]
                
                if elapsed < min_interval:
                    sleep_time = min_interval - elapsed
                    await asyncio.sleep(sleep_time)
            
            self.last_api_calls[source] = datetime.utcnow()
    
    async def process_enrichment_tasks(self, max_tasks: int = 10):
        """Traite les tâches d'enrichissement en attente"""
        try:
            tasks = await self.db_cache.get_enrichment_tasks(limit=max_tasks)
            
            if not tasks:
                logger.debug("No enrichment tasks to process")
                return
            
            logger.info(f"Processing {len(tasks)} enrichment tasks")
            
            for task in tasks:
                try:
                    # Marquer comme en cours
                    if self.db_cache.db:
                        await self.db_cache.db.enrichment_tasks.update_one(
                            {"id": task.id},
                            {"$set": {"status": "in_progress", "started_at": datetime.utcnow()}}
                        )
                    
                    # Exécuter l'enrichissement
                    success = await self.enrich_crypto_data(task.symbol, task.missing_fields)
                    
                    # Mettre à jour le statut de la tâche
                    if self.db_cache.db:
                        update_data = {
                            "status": "completed" if success else "failed",
                            "completed_at": datetime.utcnow(),
                            "success": success,
                            "attempts": task.attempts + 1
                        }
                        
                        if not success:
                            update_data["error_message"] = "Failed to enrich data"
                        
                        await self.db_cache.db.enrichment_tasks.update_one(
                            {"id": task.id},
                            {"$set": update_data}
                        )
                    
                    # Petite pause entre les tâches
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error processing enrichment task {task.id}: {e}")
                    
                    # Marquer comme échoué
                    if self.db_cache.db:
                        await self.db_cache.db.enrichment_tasks.update_one(
                            {"id": task.id},
                            {"$set": {
                                "status": "failed",
                                "completed_at": datetime.utcnow(),
                                "error_message": str(e),
                                "attempts": task.attempts + 1
                            }}
                        )
            
            logger.info(f"Completed processing {len(tasks)} enrichment tasks")
            
        except Exception as e:
            logger.error(f"Error processing enrichment tasks: {e}")
    
    async def schedule_enrichment_for_symbols(self, symbols: List[str], priority: int = 2):
        """Programme des tâches d'enrichissement pour une liste de symboles"""
        try:
            if not self.db_cache.db:
                logger.error("Database not available for scheduling enrichment")
                return
            
            scheduled_count = 0
            
            for symbol in symbols:
                # Vérifier s'il y a déjà une tâche en attente
                existing_task = await self.db_cache.db.enrichment_tasks.find_one({
                    "symbol": symbol.upper(),
                    "status": {"$in": ["pending", "in_progress"]}
                })
                
                if not existing_task:
                    # Créer une nouvelle tâche
                    task = EnrichmentTask(
                        symbol=symbol.upper(),
                        priority=priority,
                        missing_fields=[],  # Sera déterminé lors de l'exécution
                        scheduled_for=datetime.utcnow() + timedelta(minutes=priority * 5)
                    )
                    
                    await self.db_cache.db.enrichment_tasks.insert_one(task.dict())
                    scheduled_count += 1
            
            logger.info(f"Scheduled enrichment for {scheduled_count} symbols")
            
        except Exception as e:
            logger.error(f"Error scheduling enrichment tasks: {e}")
    
    async def cleanup_old_tasks(self, days_old: int = 7):
        """Nettoie les anciennes tâches d'enrichissement"""
        try:
            if not self.db_cache.db:
                return
            
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)
            
            result = await self.db_cache.db.enrichment_tasks.delete_many({
                "status": {"$in": ["completed", "failed"]},
                "completed_at": {"$lt": cutoff_date}
            })
            
            if result.deleted_count > 0:
                logger.info(f"Cleaned up {result.deleted_count} old enrichment tasks")
            
        except Exception as e:
            logger.error(f"Error cleaning up old tasks: {e}")