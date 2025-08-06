from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid
from enum import Enum

class DataSource(str, Enum):
    BINANCE = "binance"
    YAHOO_FINANCE = "yahoo_finance"
    COINGECKO = "coingecko"
    COINLORE = "coinlore"
    MANUAL = "manual"

class DataQuality(str, Enum):
    HIGH = "high"         # Données complètes et récentes
    MEDIUM = "medium"     # Données partielles mais utilisables
    LOW = "low"          # Données anciennes ou incomplètes
    INVALID = "invalid"   # Données corrompues ou incohérentes

class CryptoDataDB(BaseModel):
    """Modèle de données crypto pour la base de données"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    name: Optional[str] = None
    
    # Prix et données de base
    price_usd: Optional[float] = None
    market_cap_usd: Optional[float] = None
    volume_24h_usd: Optional[float] = None
    circulating_supply: Optional[float] = None
    total_supply: Optional[float] = None
    max_supply: Optional[float] = None
    
    # Changements de prix
    percent_change_1h: Optional[float] = None
    percent_change_24h: Optional[float] = None
    percent_change_7d: Optional[float] = None
    percent_change_30d: Optional[float] = None
    percent_change_90d: Optional[float] = None
    percent_change_1y: Optional[float] = None
    
    # Données historiques
    historical_prices: Optional[Dict[str, float]] = Field(default_factory=dict)
    max_price_1y: Optional[float] = None
    min_price_1y: Optional[float] = None
    ath_price: Optional[float] = None  # All-time high
    atl_price: Optional[float] = None  # All-time low
    ath_date: Optional[datetime] = None
    atl_date: Optional[datetime] = None
    
    # Métadonnées de qualité
    data_quality: DataQuality = DataQuality.LOW
    quality_score: float = 0.0  # 0-100
    completeness_score: float = 0.0  # 0-100
    freshness_score: float = 0.0  # 0-100
    consistency_score: float = 0.0  # 0-100
    
    # Sources et timestamps
    data_sources: List[DataSource] = Field(default_factory=list)
    source_timestamps: Dict[str, datetime] = Field(default_factory=dict)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    last_api_call: Optional[datetime] = None
    last_enrichment: Optional[datetime] = None
    
    # Métadonnées additionnelles
    rank: Optional[int] = None
    tags: List[str] = Field(default_factory=list)
    category: Optional[str] = None
    website: Optional[str] = None
    
    # Flags pour optimisation
    needs_enrichment: bool = True
    api_call_count: int = 0
    error_count: int = 0
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class QualityMetrics(BaseModel):
    """Métriques de qualité des données"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    source: DataSource
    
    # Scores de qualité (0-100)
    price_accuracy: float = 0.0
    data_completeness: float = 0.0
    update_frequency: float = 0.0
    consistency_with_other_sources: float = 0.0
    
    # Statistiques
    successful_calls: int = 0
    failed_calls: int = 0
    last_successful_call: Optional[datetime] = None
    last_failed_call: Optional[datetime] = None
    
    # Détection d'anomalies
    price_outliers_detected: int = 0
    inconsistent_data_count: int = 0
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class DataSourceInfo(BaseModel):
    """Informations sur les sources de données"""
    source: DataSource
    name: str
    base_url: Optional[str] = None
    api_key_required: bool = False
    rate_limit_per_minute: Optional[int] = None
    reliability_score: float = 100.0  # 0-100
    
    # Statistiques globales
    total_successful_calls: int = 0
    total_failed_calls: int = 0
    average_response_time: float = 0.0
    
    # Status
    is_available: bool = True
    last_availability_check: datetime = Field(default_factory=datetime.utcnow)
    
    # Spécialités de la source
    best_for_fields: List[str] = Field(default_factory=list)
    supported_cryptos_count: Optional[int] = None
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class EnrichmentTask(BaseModel):
    """Tâche d'enrichissement de données"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    priority: int = 1  # 1=high, 2=medium, 3=low
    
    # Ce qui doit être enrichi
    missing_fields: List[str] = Field(default_factory=list)
    outdated_fields: List[str] = Field(default_factory=list)
    preferred_sources: List[DataSource] = Field(default_factory=list)
    
    # Status
    status: str = "pending"  # pending, in_progress, completed, failed
    attempts: int = 0
    max_attempts: int = 3
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    scheduled_for: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Résultats
    success: bool = False
    fields_enriched: List[str] = Field(default_factory=list)
    error_message: Optional[str] = None

class CacheStrategy(BaseModel):
    """Stratégie de cache pour différents types de données"""
    field_name: str
    max_age_minutes: int  # Durée de vie en cache
    priority: int = 1     # Priorité pour le rafraîchissement
    sources_priority: List[DataSource] = Field(default_factory=list)
    validation_rules: Dict[str, Any] = Field(default_factory=dict)