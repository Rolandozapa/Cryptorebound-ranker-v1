import asyncio
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from models import CryptoCurrency, CryptoRanking
from services.database_cache_service import DatabaseCacheService
from services.scoring_service import ScoringService

logger = logging.getLogger(__name__)

class RankingPrecomputeService:
    """Service de pré-calcul des classements pour optimiser les performances"""
    
    def __init__(self, db_cache_service: DatabaseCacheService, scoring_service: ScoringService):
        self.db_cache = db_cache_service
        self.scoring_service = scoring_service
        
        # Périodes à pré-calculer
        self.periods_to_precompute = ['1h', '24h', '7d', '30d', '90d', '180d', '270d', '365d']
        
        # Configuration du cache par période (durée de vie en minutes)
        self.cache_duration = {
            '1h': 5,      # 5 minutes
            '24h': 15,    # 15 minutes
            '7d': 60,     # 1 heure
            '30d': 240,   # 4 heures
            '90d': 480,   # 8 heures
            '180d': 720,  # 12 heures
            '270d': 1440, # 24 heures
            '365d': 1440  # 24 heures
        }
        
        self.is_computing = {}  # Track computing status per period
        
    async def precompute_all_rankings(self):
        """Pré-calcule tous les classements pour toutes les périodes"""
        try:
            logger.info("Starting precomputation of all rankings...")
            
            # Récupérer toutes les cryptos de qualité acceptable depuis la DB
            cached_cryptos = await self._get_quality_cryptos()
            
            if len(cached_cryptos) < 10:
                logger.warning("Not enough quality cryptos in cache for precomputation")
                return
            
            logger.info(f"Precomputing rankings for {len(cached_cryptos)} cryptocurrencies")
            
            # Pré-calculer pour chaque période en parallèle (mais limité)
            tasks = []
            semaphore = asyncio.Semaphore(3)  # Limiter à 3 calculs simultanés
            
            for period in self.periods_to_precompute:
                task = self._precompute_period_with_semaphore(semaphore, period, cached_cryptos)
                tasks.append(task)
            
            # Exécuter tous les pré-calculs
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful = len([r for r in results if not isinstance(r, Exception)])
            logger.info(f"Precomputation completed: {successful}/{len(tasks)} periods successful")
            
        except Exception as e:
            logger.error(f"Error in precompute_all_rankings: {e}")
    
    async def _precompute_period_with_semaphore(self, semaphore: asyncio.Semaphore, period: str, cached_cryptos: List):
        """Pré-calcule un classement avec limitation de concurrence"""
        async with semaphore:
            return await self._precompute_period_ranking(period, cached_cryptos)
    
    async def _precompute_period_ranking(self, period: str, cached_cryptos: List = None):
        """Pré-calcule le classement pour une période donnée"""
        try:
            if self.is_computing.get(period, False):
                logger.debug(f"Already computing ranking for {period}, skipping")
                return
            
            self.is_computing[period] = True
            logger.info(f"Precomputing ranking for period: {period}")
            
            # Vérifier si le cache est encore valide
            if await self._is_cache_valid(period):
                logger.debug(f"Cache for {period} is still valid, skipping precomputation")
                return
            
            # Récupérer les cryptos si pas fournis
            if not cached_cryptos:
                cached_cryptos = await self._get_quality_cryptos()
            
            if not cached_cryptos:
                logger.warning(f"No cryptos available for {period} precomputation")
                return
            
            # Convertir vers le format CryptoCurrency pour le scoring
            crypto_models = []
            for crypto_db in cached_cryptos:
                try:
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
                        max_price_1y=crypto_db.max_price_1y,
                        min_price_1y=crypto_db.min_price_1y,
                        historical_prices=crypto_db.historical_prices or {},
                        rank=crypto_db.rank,
                        last_updated=crypto_db.last_updated,
                        data_sources=[str(source) for source in crypto_db.data_sources]
                    )
                    crypto_models.append(crypto)
                except Exception as e:
                    logger.warning(f"Failed to convert {crypto_db.symbol} for scoring: {e}")
                    continue
            
            # Calculer les scores de manière optimisée
            start_time = datetime.utcnow()
            scored_cryptos = await self._optimized_scoring(crypto_models, period)
            
            computation_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Scoring for {period} completed in {computation_time:.2f}s for {len(scored_cryptos)} cryptos")
            
            # Sauvegarder le classement pré-calculé
            ranking = CryptoRanking(
                period=period,
                cryptos=scored_cryptos,
                total_cryptos=len(scored_cryptos),
                refresh_count=1
            )
            
            if self.db_cache.db:
                await self.db_cache.db.crypto_rankings.replace_one(
                    {"period": period},
                    ranking.dict(),
                    upsert=True
                )
                
                # Ajouter un index sur last_updated pour les performances
                await self._ensure_rankings_index()
            
            logger.info(f"Successfully precomputed ranking for {period}: {len(scored_cryptos)} cryptos")
            
        except Exception as e:
            logger.error(f"Error precomputing ranking for {period}: {e}")
        finally:
            self.is_computing[period] = False
    
    async def _optimized_scoring(self, cryptos: List[CryptoCurrency], period: str) -> List[CryptoCurrency]:
        """Version optimisée du calcul de scores"""
        try:
            # Optimisation 1: Paralléliser le calcul des scores en batches
            batch_size = 50
            scored_batches = []
            
            for i in range(0, len(cryptos), batch_size):
                batch = cryptos[i:i + batch_size]
                # Calculer les scores pour ce batch
                scored_batch = self.scoring_service.calculate_scores(batch.copy(), period)
                scored_batches.extend(scored_batch)
                
                # Petite pause pour éviter de surcharger
                if len(cryptos) > 100:
                    await asyncio.sleep(0.1)
            
            # Optimisation 2: Trier une seule fois à la fin
            scored_batches.sort(key=lambda x: x.total_score or 0, reverse=True)
            
            # Optimisation 3: Assigner les rangs
            for i, crypto in enumerate(scored_batches):
                crypto.rank = i + 1
            
            return scored_batches
            
        except Exception as e:
            logger.error(f"Error in optimized scoring for {period}: {e}")
            # Fallback vers la méthode standard
            return self.scoring_service.calculate_scores(cryptos, period)
    
    async def _get_quality_cryptos(self, min_quality_score: float = 50.0) -> List:
        """Récupère les cryptos de qualité acceptable depuis la DB"""
        try:
            if not self.db_cache.db:
                return []
            
            # Récupérer les cryptos avec un score de qualité acceptable
            cursor = self.db_cache.db.crypto_data.find({
                "$and": [
                    {"quality_score": {"$gte": min_quality_score}},
                    {"price_usd": {"$gt": 0}},
                    {"data_quality": {"$ne": "invalid"}}
                ]
            }).sort([
                ("quality_score", -1),  # Tri par qualité d'abord
                ("market_cap_usd", -1)  # Puis par market cap
            ]).limit(2000)  # Limite raisonnable
            
            cryptos = []
            async for doc in cursor:
                try:
                    from db_models import CryptoDataDB
                    crypto_db = CryptoDataDB(**doc)
                    cryptos.append(crypto_db)
                except Exception as e:
                    logger.warning(f"Failed to parse crypto data: {e}")
                    continue
            
            logger.info(f"Retrieved {len(cryptos)} quality cryptos from database")
            return cryptos
            
        except Exception as e:
            logger.error(f"Error getting quality cryptos: {e}")
            return []
    
    async def _is_cache_valid(self, period: str) -> bool:
        """Vérifie si le cache pour une période est encore valide"""
        try:
            if not self.db_cache.db:
                return False
            
            ranking_doc = await self.db_cache.db.crypto_rankings.find_one({"period": period})
            
            if not ranking_doc:
                return False
            
            last_updated = ranking_doc.get('last_updated')
            if not last_updated:
                return False
            
            if isinstance(last_updated, str):
                last_updated = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
            
            # Vérifier si le cache est expiré
            cache_duration_minutes = self.cache_duration.get(period, 60)
            expiry_time = last_updated + timedelta(minutes=cache_duration_minutes)
            
            is_valid = datetime.utcnow() < expiry_time
            
            if not is_valid:
                logger.debug(f"Cache for {period} expired (age: {datetime.utcnow() - last_updated})")
            
            return is_valid
            
        except Exception as e:
            logger.error(f"Error checking cache validity for {period}: {e}")
            return False
    
    async def get_precomputed_ranking(self, period: str, limit: int = 50, offset: int = 0) -> Optional[List[CryptoCurrency]]:
        """Récupère un classement pré-calculé depuis la DB"""
        try:
            if not self.db_cache.db:
                return None
            
            ranking_doc = await self.db_cache.db.crypto_rankings.find_one({"period": period})
            
            if not ranking_doc:
                logger.debug(f"No precomputed ranking found for {period}")
                return None
            
            # Vérifier la validité du cache
            if not await self._is_cache_valid(period):
                logger.debug(f"Precomputed ranking for {period} is expired")
                # Déclencher un recalcul en arrière-plan
                asyncio.create_task(self._precompute_period_ranking(period))
                return None
            
            # Extraire les cryptos avec pagination
            cryptos_data = ranking_doc.get('cryptos', [])
            
            # Appliquer la pagination
            end_index = offset + limit
            paginated_cryptos = cryptos_data[offset:end_index]
            
            # Convertir en modèles CryptoCurrency
            result_cryptos = []
            for crypto_data in paginated_cryptos:
                try:
                    crypto = CryptoCurrency(**crypto_data)
                    result_cryptos.append(crypto)
                except Exception as e:
                    logger.warning(f"Failed to parse cached crypto: {e}")
                    continue
            
            logger.info(f"Retrieved {len(result_cryptos)} precomputed cryptos for {period} (offset: {offset})")
            return result_cryptos
            
        except Exception as e:
            logger.error(f"Error getting precomputed ranking for {period}: {e}")
            return None
    
    async def _ensure_rankings_index(self):
        """S'assure que les index MongoDB sont présents pour les performances"""
        try:
            if not self.db_cache.db:
                return
            
            # Index pour les classements
            await self.db_cache.db.crypto_rankings.create_index([
                ("period", 1),
                ("last_updated", -1)
            ])
            
            # Index pour les cryptos
            await self.db_cache.db.crypto_data.create_index([
                ("quality_score", -1),
                ("market_cap_usd", -1)
            ])
            
            await self.db_cache.db.crypto_data.create_index([
                ("symbol", 1)
            ])
            
        except Exception as e:
            logger.debug(f"Index creation failed (probably already exists): {e}")
    
    async def schedule_background_precomputation(self):
        """Programme le pré-calcul en arrière-plan selon les priorités"""
        try:
            # Priorités : périodes courtes plus fréquemment
            high_priority_periods = ['24h', '7d']
            medium_priority_periods = ['1h', '30d', '90d']
            low_priority_periods = ['180d', '270d', '365d']
            
            # Toujours calculer les périodes haute priorité
            for period in high_priority_periods:
                if not await self._is_cache_valid(period):
                    asyncio.create_task(self._precompute_period_ranking(period))
            
            # Calculer les périodes moyenne priorité si nécessaire
            for period in medium_priority_periods:
                if not await self._is_cache_valid(period):
                    asyncio.create_task(self._precompute_period_ranking(period))
                    await asyncio.sleep(1)  # Espacement
            
            # Calculer les périodes basse priorité en dernier
            for period in low_priority_periods:
                if not await self._is_cache_valid(period):
                    asyncio.create_task(self._precompute_period_ranking(period))
                    await asyncio.sleep(2)  # Plus d'espacement
            
        except Exception as e:
            logger.error(f"Error scheduling background precomputation: {e}")
    
    def get_computation_status(self) -> Dict[str, Any]:
        """Retourne l'état des calculs en cours"""
        return {
            "periods_computing": [period for period, is_computing in self.is_computing.items() if is_computing],
            "cache_status": {
                period: "computing" if self.is_computing.get(period, False) else "ready"
                for period in self.periods_to_precompute
            }
        }