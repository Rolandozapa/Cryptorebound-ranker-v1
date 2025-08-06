from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid

class CryptoCurrency(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    name: str
    price_usd: float
    market_cap_usd: Optional[float] = None
    volume_24h_usd: Optional[float] = None
    percent_change_1h: Optional[float] = None
    percent_change_24h: Optional[float] = None
    percent_change_7d: Optional[float] = None
    percent_change_30d: Optional[float] = None
    rank: Optional[int] = None
    
    # Historical data for scoring
    historical_prices: Optional[Dict[str, float]] = Field(default_factory=dict)
    max_price_1y: Optional[float] = None
    min_price_1y: Optional[float] = None
    
    # Calculated scores
    performance_score: Optional[float] = None
    drawdown_score: Optional[float] = None
    rebound_potential_score: Optional[float] = None
    momentum_score: Optional[float] = None
    total_score: Optional[float] = None
    
    # Additional metrics
    recovery_potential_75: Optional[str] = None
    drawdown_percentage: Optional[float] = None
    
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    data_sources: List[str] = Field(default_factory=list)

class CryptoRanking(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    period: str  # "1h", "24h", "7d", "30d", "90d", "180d", "270d", "365d"
    cryptos: List[CryptoCurrency]
    total_cryptos: int
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    refresh_count: int = 0

class RankingRequest(BaseModel):
    period: str = "24h"
    limit: int = 50
    offset: int = 0

class RefreshRequest(BaseModel):
    force: bool = False
    period: Optional[str] = None