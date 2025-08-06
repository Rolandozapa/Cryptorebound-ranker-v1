import asyncio
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from db_models import CryptoDataDB, DataQuality, DataSource, QualityMetrics, EnrichmentTask
from services.data_quality_service import DataQualityService
import os

logger = logging.getLogger(__name__)

class DatabaseCacheService:
    """Service de cache intelligent avec MongoDB"""
    
    def __init__(self, db_client: AsyncIOMotorClient = None, db_name: str = None):
        self.db_client = db_client
        self.db_name = db_name or os.environ.get('DB_NAME', 'test_database')
        self.db = None
        self.quality_service = DataQualityService()
        
        # Configuration du cache
        self.cache_config = {
            'price_usd': {'max_age_minutes': 5, 'priority': 1},
            'percent_change_24h': {'max_age_minutes': 15, 'priority': 1},
            'market_cap_usd': {'max_age_minutes': 30, 'priority': 2},
            'volume_24h_usd': {'max_age_minutes': 30, 'priority': 2},
            'historical_prices': {'max_age_minutes': 120, 'priority': 3},
            'max_price_1y': {'max_age_minutes': 1440, 'priority': 3},  # 24h
        }
        
        if self.db_client:
            self.db = self.db_client[self.db_name]
    
    def set_db_client(self, db_client: AsyncIOMotorClient):
        """Définir le client de base de données"""
        self.db_client = db_client
        self.db = db_client[self.db_name]
    
    async def get_crypto_data(self, symbol: str, required_fields: List[str] = None) -> Optional[CryptoDataDB]:
        """
        Récupère les données crypto de la DB si elles sont fraîches
        Returns: CryptoDataDB si données valides, None si données expirées/manquantes
        """
        try:
            if not self.db:
                return None
            
            # Chercher dans la collection crypto_data
            doc = await self.db.crypto_data.find_one({"symbol": symbol.upper()})
            
            if not doc:
                logger.debug(f"No cached data found for {symbol}")
                return None
            
            # Convertir en modèle Pydantic
            crypto_data = CryptoDataDB(**doc)
            
            # Vérifier la fraîcheur des données requises
            if required_fields:
                fresh_data = self._check_data_freshness(crypto_data, required_fields)
                if not fresh_data:
                    logger.debug(f"Cached data for {symbol} is stale")
                    return None
            
            # Vérifier la qualité minimale
            if crypto_data.data_quality == DataQuality.INVALID:
                logger.debug(f"Cached data for {symbol} has invalid quality")
                return None
            
            logger.info(f"Using cached data for {symbol} (quality: {crypto_data.data_quality}, score: {crypto_data.quality_score:.1f})")
            return crypto_data
            
        except Exception as e:
            logger.error(f"Error retrieving cached data for {symbol}: {e}")
            return None
    
    async def store_crypto_data(self, crypto_data: Dict[str, Any], validate: bool = True) -> bool:
        """
        Stocke les données crypto en DB après validation
        """
        try:
            if not self.db:
                logger.error("Database not initialized")
                return False
            
            symbol = crypto_data.get('symbol', '').upper()
            if not symbol:
                logger.error("Symbol is required for storing crypto data")
                return False
            
            # Validation et scoring de qualité
            if validate:
                is_valid, quality_score, quality_details = self.quality_service.validate_and_score_data(crypto_data)
                
                if not is_valid:
                    logger.warning(f"Data validation failed for {symbol}: {quality_details}")
                    return False
                
                # Ajouter les métriques de qualité
                crypto_data.update({
                    'quality_score': quality_score,
                    'data_quality': quality_details['quality_level'],
                    'completeness_score': quality_details.get('completeness', 0),
                    'freshness_score': quality_details.get('freshness', 0),
                    'consistency_score': quality_details.get('consistency', 0)
                })
            
            # Récupérer les données existantes pour merger
            existing_data = await self.get_crypto_data(symbol, required_fields=[])
            
            if existing_data:
                # Merger avec les données existantes
                merged_data = await self._merge_crypto_data(existing_data, crypto_data)
            else:
                merged_data = crypto_data
            
            # Préparer l'objet pour insertion
            merged_data.update({
                'symbol': symbol,
                'last_updated': datetime.utcnow(),
                'last_api_call': datetime.utcnow()
            })
            
            # Convertir en modèle Pydantic pour validation finale
            crypto_db_obj = CryptoDataDB(**merged_data)
            
            # Insérer ou mettre à jour dans MongoDB
            result = await self.db.crypto_data.replace_one(
                {"symbol": symbol},
                crypto_db_obj.dict(),
                upsert=True
            )
            
            # Créer une tâche d'enrichissement si nécessaire
            await self._create_enrichment_task_if_needed(crypto_db_obj)
            
            logger.info(f"Stored crypto data for {symbol} (quality: {crypto_db_obj.quality_score:.1f})")
            return True
            
        except Exception as e:
            logger.error(f"Error storing crypto data: {e}")
            return False
    
    async def get_stale_data_symbols(self, limit: int = 100) -> List[str]:
        """Récupère les symboles avec des données obsolètes"""
        try:
            if not self.db:
                return []
            
            now = datetime.utcnow()
            stale_threshold = now - timedelta(minutes=60)  # 1 heure
            
            # Chercher les données obsolètes
            cursor = self.db.crypto_data.find({
                "$or": [
                    {"last_updated": {"$lt": stale_threshold}},
                    {"data_quality": DataQuality.LOW},
                    {"needs_enrichment": True}
                ]
            }).limit(limit)
            
            symbols = []
            async for doc in cursor:
                symbols.append(doc.get('symbol'))
            
            return symbols
            
        except Exception as e:
            logger.error(f"Error getting stale data symbols: {e}")
            return []
    
    async def get_missing_cryptos(self, all_symbols: List[str]) -> List[str]:
        """Trouve les cryptos manquants dans la DB"""
        try:
            if not self.db:
                return all_symbols
            
            # Récupérer tous les symboles en DB
            cursor = self.db.crypto_data.find({}, {"symbol": 1})
            existing_symbols = set()
            
            async for doc in cursor:
                existing_symbols.add(doc.get('symbol', ''))
            
            # Trouver les manquants
            missing_symbols = [s.upper() for s in all_symbols if s.upper() not in existing_symbols]
            
            logger.info(f"Found {len(missing_symbols)} missing cryptocurrencies")
            return missing_symbols
            
        except Exception as e:
            logger.error(f"Error finding missing cryptos: {e}")
            return all_symbols
    
    async def get_enrichment_tasks(self, limit: int = 50) -> List[EnrichmentTask]:
        """Récupère les tâches d'enrichissement en attente"""
        try:
            if not self.db:
                return []
            
            cursor = self.db.enrichment_tasks.find({
                "status": "pending",
                "attempts": {"$lt": 3}
            }).sort("priority", 1).limit(limit)
            
            tasks = []
            async for doc in cursor:
                tasks.append(EnrichmentTask(**doc))
            
            return tasks
            
        except Exception as e:
            logger.error(f"Error getting enrichment tasks: {e}")
            return []
    
    async def update_source_metrics(self, source: DataSource, symbol: str, success: bool, response_time: float = 0):
        """Met à jour les métriques de qualité d'une source"""
        try:
            if not self.db:
                return
            
            now = datetime.utcnow()
            
            update_doc = {
                "updated_at": now,
                "$inc": {
                    "successful_calls" if success else "failed_calls": 1
                }
            }
            
            if success:
                update_doc["last_successful_call"] = now
            else:
                update_doc["last_failed_call"] = now
            
            await self.db.quality_metrics.update_one(
                {"symbol": symbol.upper(), "source": source},
                {"$set": update_doc},
                upsert=True
            )
            
        except Exception as e:
            logger.error(f"Error updating source metrics: {e}")
    
    async def get_best_sources_for_crypto(self, symbol: str) -> List[DataSource]:
        """Récupère les meilleures sources pour un crypto donné"""
        try:
            if not self.db:
                return [DataSource.COINGECKO, DataSource.YAHOO_FINANCE]
            
            cursor = self.db.quality_metrics.find({
                "symbol": symbol.upper()
            }).sort("successful_calls", -1)
            
            sources = []
            async for doc in cursor:
                source = doc.get('source')
                success_rate = doc.get('successful_calls', 0) / max(1, doc.get('successful_calls', 0) + doc.get('failed_calls', 0))
                
                if success_rate > 0.7:  # Taux de succès > 70%
                    sources.append(DataSource(source))
            
            # Sources par défaut si aucune métrique
            if not sources:
                sources = [DataSource.COINGECKO, DataSource.YAHOO_FINANCE, DataSource.COINLORE]
            
            return sources[:3]  # Max 3 sources
            
        except Exception as e:
            logger.error(f"Error getting best sources for {symbol}: {e}")
            return [DataSource.COINGECKO, DataSource.YAHOO_FINANCE]
    
    def _check_data_freshness(self, crypto_data: CryptoDataDB, required_fields: List[str]) -> bool:
        """Vérifie si les champs requis sont à jour"""
        now = datetime.utcnow()
        
        for field in required_fields:
            if field not in self.cache_config:
                continue
            
            max_age = self.cache_config[field]['max_age_minutes']
            
            # Vérifier la fraîcheur du champ
            field_timestamp = crypto_data.source_timestamps.get(field)
            if field_timestamp:
                if isinstance(field_timestamp, str):
                    field_timestamp = datetime.fromisoformat(field_timestamp.replace('Z', '+00:00'))
                
                age_minutes = (now - field_timestamp).total_seconds() / 60
                if age_minutes > max_age:
                    return False
            else:
                # Pas de timestamp spécifique, utiliser last_updated
                age_minutes = (now - crypto_data.last_updated).total_seconds() / 60
                if age_minutes > max_age:
                    return False
        
        return True
    
    async def _merge_crypto_data(self, existing: CryptoDataDB, new_data: Dict[str, Any]) -> Dict[str, Any]:
        """Merge intelligemment les données existantes avec les nouvelles"""
        merged = existing.dict()
        
        # Merger les sources de données
        existing_sources = set(merged.get('data_sources', []))
        new_sources = set(new_data.get('data_sources', []))
        if 'source' in new_data:
            new_sources.add(new_data['source'])
        
        merged['data_sources'] = list(existing_sources | new_sources)
        
        # Merger les timestamps des sources
        source_timestamps = merged.get('source_timestamps', {})
        if 'source_timestamps' in new_data:
            source_timestamps.update(new_data['source_timestamps'])
        merged['source_timestamps'] = source_timestamps
        
        # Mettre à jour les champs avec des données plus récentes ou de meilleure qualité
        for key, value in new_data.items():
            if value is not None and key not in ['id', 'symbol']:
                # Toujours prendre les nouvelles données pour les prix et volumes
                if key in ['price_usd', 'volume_24h_usd'] or key.startswith('percent_change_'):
                    merged[key] = value
                # Pour les autres champs, prendre si manquant ou si meilleure source
                elif key not in merged or merged[key] is None:
                    merged[key] = value
        
        # Recalculer le score de qualité
        is_valid, quality_score, quality_details = self.quality_service.validate_and_score_data(merged)
        if is_valid:
            merged.update({
                'quality_score': quality_score,
                'data_quality': quality_details['quality_level'],
                'completeness_score': quality_details.get('completeness', merged.get('completeness_score', 0)),
                'freshness_score': quality_details.get('freshness', 0),
                'consistency_score': quality_details.get('consistency', merged.get('consistency_score', 0))
            })
        
        return merged
    
    async def _create_enrichment_task_if_needed(self, crypto_data: CryptoDataDB):
        """Crée une tâche d'enrichissement si nécessaire"""
        try:
            if not self.db:
                return
            
            # Suggérer les champs à enrichir
            missing_fields = self.quality_service.suggest_enrichment_fields(crypto_data.dict())
            
            if missing_fields and crypto_data.quality_score < 80:
                # Vérifier s'il y a déjà une tâche en attente
                existing_task = await self.db.enrichment_tasks.find_one({
                    "symbol": crypto_data.symbol,
                    "status": {"$in": ["pending", "in_progress"]}
                })
                
                if not existing_task:
                    # Créer une nouvelle tâche d'enrichissement
                    task = EnrichmentTask(
                        symbol=crypto_data.symbol,
                        priority=1 if crypto_data.quality_score < 50 else 2,
                        missing_fields=missing_fields,
                        preferred_sources=await self.get_best_sources_for_crypto(crypto_data.symbol)
                    )
                    
                    await self.db.enrichment_tasks.insert_one(task.dict())
                    logger.debug(f"Created enrichment task for {crypto_data.symbol}")
        
        except Exception as e:
            logger.error(f"Error creating enrichment task: {e}")
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """Récupère les statistiques de la base de données"""
        try:
            if not self.db:
                return {}
            
            stats = {}
            
            # Statistiques générales
            total_cryptos = await self.db.crypto_data.count_documents({})
            high_quality = await self.db.crypto_data.count_documents({"data_quality": DataQuality.HIGH})
            medium_quality = await self.db.crypto_data.count_documents({"data_quality": DataQuality.MEDIUM})
            low_quality = await self.db.crypto_data.count_documents({"data_quality": DataQuality.LOW})
            
            stats['total_cryptocurrencies'] = total_cryptos
            stats['quality_distribution'] = {
                'high': high_quality,
                'medium': medium_quality,
                'low': low_quality
            }
            
            # Tâches d'enrichissement
            pending_tasks = await self.db.enrichment_tasks.count_documents({"status": "pending"})
            completed_tasks = await self.db.enrichment_tasks.count_documents({"status": "completed"})
            
            stats['enrichment_tasks'] = {
                'pending': pending_tasks,
                'completed': completed_tasks
            }
            
            # Score de qualité moyen
            pipeline = [
                {"$group": {"_id": None, "avg_quality": {"$avg": "$quality_score"}}}
            ]
            result = await self.db.crypto_data.aggregate(pipeline).to_list(1)
            
            if result:
                stats['average_quality_score'] = round(result[0]['avg_quality'], 2)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}