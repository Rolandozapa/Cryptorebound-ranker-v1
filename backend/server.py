from fastapi import FastAPI, APIRouter, HTTPException, Query
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional
import uuid
from datetime import datetime, timedelta

# Import our new services and models
from models import CryptoCurrency, CryptoRanking, RankingRequest, RefreshRequest
from services.data_aggregation_service import DataAggregationService
from services.scoring_service import ScoringService

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Create the main app without a prefix
app = FastAPI(title="CryptoRebound Ranking API", version="2.0.0")

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Initialize services with database connection
data_service = DataAggregationService(db_client=client)
scoring_service = ScoringService()

# Cache for rankings
rankings_cache = {}
last_cache_update = {}

# Legacy models for backwards compatibility
class StatusCheck(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    client_name: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class StatusCheckCreate(BaseModel):
    client_name: str

# Legacy endpoints for backwards compatibility
@api_router.get("/")
async def root():
    return {"message": "CryptoRebound Ranking API v2.0 - Ready to track 1000+ cryptocurrencies!"}

@api_router.post("/status", response_model=StatusCheck)
async def create_status_check(input: StatusCheckCreate):
    status_dict = input.dict()
    status_obj = StatusCheck(**status_dict)
    _ = await db.status_checks.insert_one(status_obj.dict())
    return status_obj

@api_router.get("/status", response_model=List[StatusCheck])
async def get_status_checks():
    status_checks = await db.status_checks.find().to_list(1000)
    return [StatusCheck(**status_check) for status_check in status_checks]

# New CryptoRebound endpoints

@api_router.get("/health")
async def health_check():
    """Check the health of all data services"""
    health_status = data_service.is_healthy()
    return {
        "status": "healthy",
        "services": health_status,
        "timestamp": datetime.utcnow().isoformat()
    }

@api_router.post("/cryptos/refresh")
async def refresh_crypto_data(request: RefreshRequest = RefreshRequest()):
    """Manually refresh cryptocurrency data"""
    try:
        logger.info(f"Manual refresh requested - Force: {request.force}")
        
        # Get fresh data
        cryptos = await data_service.get_aggregated_crypto_data(force_refresh=request.force)
        
        if not cryptos:
            raise HTTPException(status_code=503, detail="No cryptocurrency data available")
        
        # Calculate scores for all periods or specific period
        periods_to_update = ['24h'] if request.period else ['1h', '24h', '7d', '30d']
        
        updated_rankings = {}
        for period in periods_to_update:
            scored_cryptos = scoring_service.calculate_scores(cryptos.copy(), period)
            
            # Cache the results
            rankings_cache[period] = scored_cryptos
            last_cache_update[period] = datetime.utcnow()
            
            # Save to database
            ranking = CryptoRanking(
                period=period,
                cryptos=scored_cryptos,
                total_cryptos=len(scored_cryptos),
                refresh_count=1
            )
            await db.crypto_rankings.replace_one(
                {"period": period},
                ranking.dict(),
                upsert=True
            )
            
            updated_rankings[period] = len(scored_cryptos)
        
        logger.info(f"Successfully refreshed crypto data: {updated_rankings}")
        
        return {
            "status": "success",
            "message": f"Refreshed cryptocurrency data",
            "updated_rankings": updated_rankings,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error refreshing crypto data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to refresh data: {str(e)}")

@api_router.get("/cryptos/ranking", response_model=List[CryptoCurrency])
async def get_crypto_ranking(
    period: str = Query("24h", description="Time period for ranking"),
    limit: int = Query(50, description="Number of results to return", le=1000),
    offset: int = Query(0, description="Offset for pagination"),
    force_refresh: bool = Query(False, description="Force refresh data")
):
    """Get cryptocurrency ranking with advanced scoring"""
    try:
        # Check cache first
        cache_key = period
        cache_age = None
        
        if cache_key in last_cache_update:
            cache_age = datetime.utcnow() - last_cache_update[cache_key]
        
        # Use cache if it's fresh (less than 10 minutes old) and no force refresh
        if (not force_refresh and 
            cache_key in rankings_cache and 
            cache_age and 
            cache_age < timedelta(minutes=10)):
            
            logger.info(f"Using cached ranking for {period} (age: {cache_age})")
            cached_cryptos = rankings_cache[cache_key]
            
            # Apply pagination
            end_index = offset + limit
            result = cached_cryptos[offset:end_index]
            
            return result
        
        # Get fresh data
        logger.info(f"Fetching fresh ranking for {period}")
        
        # Try to load from database first
        db_ranking = await db.crypto_rankings.find_one({"period": period})
        if db_ranking and not force_refresh:
            stored_cryptos = [CryptoCurrency(**crypto) for crypto in db_ranking['cryptos']]
            stored_age = datetime.utcnow() - db_ranking['last_updated']
            
            if stored_age < timedelta(minutes=15):  # Use stored data if less than 15 min old
                logger.info(f"Using stored ranking for {period} (age: {stored_age})")
                rankings_cache[cache_key] = stored_cryptos
                last_cache_update[cache_key] = db_ranking['last_updated']
                
                end_index = offset + limit
                return stored_cryptos[offset:end_index]
        
        # Need fresh data
        cryptos = await data_service.get_aggregated_crypto_data(force_refresh=True)
        
        if not cryptos:
            raise HTTPException(status_code=503, detail="No cryptocurrency data available")
        
        # Calculate scores
        scored_cryptos = scoring_service.calculate_scores(cryptos, period)
        
        # Update cache
        rankings_cache[cache_key] = scored_cryptos
        last_cache_update[cache_key] = datetime.utcnow()
        
        # Save to database
        ranking = CryptoRanking(
            period=period,
            cryptos=scored_cryptos,
            total_cryptos=len(scored_cryptos)
        )
        await db.crypto_rankings.replace_one(
            {"period": period},
            ranking.dict(),
            upsert=True
        )
        
        logger.info(f"Generated fresh ranking for {period}: {len(scored_cryptos)} cryptos")
        
        # Apply pagination
        end_index = offset + limit
        result = scored_cryptos[offset:end_index]
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting crypto ranking: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get ranking: {str(e)}")

@api_router.get("/cryptos/count")
async def get_crypto_count():
    """Get the total number of cryptocurrencies available"""
    try:
        # Check most recent cache
        max_count = 0
        for period_cryptos in rankings_cache.values():
            max_count = max(max_count, len(period_cryptos))
        
        # Also check database
        db_rankings = await db.crypto_rankings.find().to_list(10)
        for ranking in db_rankings:
            max_count = max(max_count, ranking.get('total_cryptos', 0))
        
        return {
            "total_cryptocurrencies": max_count,
            "cached_periods": list(rankings_cache.keys()),
            "last_update": max(last_cache_update.values()).isoformat() if last_cache_update else None
        }
        
    except Exception as e:
        logger.error(f"Error getting crypto count: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/cryptos/{symbol}")
async def get_crypto_details(symbol: str):
    """Get detailed information for a specific cryptocurrency"""
    try:
        symbol = symbol.upper()
        
        # Look in cache first
        for period_cryptos in rankings_cache.values():
            for crypto in period_cryptos:
                if crypto.symbol == symbol:
                    # Get additional historical data
                    historical_data = await data_service.get_historical_data_for_crypto(symbol)
                    
                    return {
                        **crypto.dict(),
                        "historical_data": historical_data
                    }
        
        # Not found in cache, try to fetch fresh data
        cryptos = await data_service.get_aggregated_crypto_data()
        for crypto in cryptos:
            if crypto.symbol == symbol:
                historical_data = await data_service.get_historical_data_for_crypto(symbol)
                return {
                    **crypto.dict(),
                    "historical_data": historical_data
                }
        
        raise HTTPException(status_code=404, detail=f"Cryptocurrency {symbol} not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting crypto details for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.on_event("startup")
async def startup_event():
    """Initialize the application on startup"""
    logger.info("CryptoRebound Ranking API starting up...")
    
    # Do an initial data refresh in the background
    try:
        cryptos = await data_service.get_aggregated_crypto_data(force_refresh=True)
        if cryptos:
            # Calculate initial rankings for main periods
            for period in ['24h', '7d']:
                scored_cryptos = scoring_service.calculate_scores(cryptos.copy(), period)
                rankings_cache[period] = scored_cryptos
                last_cache_update[period] = datetime.utcnow()
                
                # Save initial data to database
                ranking = CryptoRanking(
                    period=period,
                    cryptos=scored_cryptos,
                    total_cryptos=len(scored_cryptos)
                )
                await db.crypto_rankings.replace_one(
                    {"period": period},
                    ranking.dict(),
                    upsert=True
                )
            
            logger.info(f"Initial data loaded: {len(cryptos)} cryptocurrencies")
        else:
            logger.warning("No initial cryptocurrency data available")
            
    except Exception as e:
        logger.error(f"Error during startup data initialization: {e}")

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
