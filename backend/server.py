from fastapi import FastAPI, APIRouter, HTTPException, Query
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid
import asyncio
import psutil
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

# Configure the scoring service for precomputation
data_service.set_scoring_service(scoring_service)

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

# Models for dynamic limit system
class SystemResourcesInfo(BaseModel):
    available_memory_mb: float
    cpu_usage_percent: float
    recommended_max_cryptos: int
    performance_mode: str  # 'optimal', 'balanced', 'maximum'
    current_load: str  # 'low', 'medium', 'high'

class DynamicLimitResponse(BaseModel):
    max_recommended_limit: int
    performance_impact: str
    memory_usage_estimate: str
    system_resources: SystemResourcesInfo

# Models for background refresh system
class BackgroundRefreshResponse(BaseModel):
    status: str
    task_id: Optional[str] = None
    message: str
    estimated_duration_seconds: Optional[int] = None

class RefreshStatusResponse(BaseModel):
    status: str  # 'idle', 'running', 'completed', 'failed'
    active_tasks: int
    last_update: Optional[str] = None
    last_duration_seconds: Optional[float] = None
    last_error: Optional[str] = None
    next_auto_refresh: Optional[str] = None

# Multi-period analysis models
class MultiPeriodCrypto(BaseModel):
    symbol: str
    name: str
    price_usd: float
    market_cap_usd: Optional[float] = None
    average_score: float
    long_term_average: Optional[float] = None  # NEW: Average for 90d+180d+270d+365d
    period_scores: Dict[str, float]
    long_term_scores: Optional[Dict[str, float]] = None  # NEW: Long term period scores
    best_period: str
    worst_period: str
    consistency_score: float
    long_term_consistency: Optional[float] = None  # NEW: Consistency for long term
    trend_confirmation: Optional[str] = None  # NEW: "Strong", "Weak", "Divergent"
    rank: int

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

@api_router.post("/cryptos/refresh-async", response_model=BackgroundRefreshResponse)
async def start_background_crypto_refresh(
    force: bool = Query(False, description="Force refresh from external APIs"),
    periods: List[str] = Query([], description="Specific periods to refresh")
):
    """Start background cryptocurrency data refresh - returns immediately"""
    try:
        logger.info(f"Starting background crypto refresh: force={force}, periods={periods}")
        
        # Start background refresh
        task_id = await data_service.start_background_refresh(force=force, periods=periods)
        
        if task_id:
            return BackgroundRefreshResponse(
                status="started",
                task_id=task_id,
                message="Background refresh started successfully",
                estimated_duration_seconds=60 if force else 30
            )
        else:
            # Check current status
            refresh_status = data_service.get_refresh_status()
            if refresh_status['status'] == 'running':
                return BackgroundRefreshResponse(
                    status="already_running",
                    message="Background refresh is already in progress",
                    estimated_duration_seconds=30
                )
            else:
                return BackgroundRefreshResponse(
                    status="failed",
                    message="Failed to start background refresh",
                    estimated_duration_seconds=None
                )
        
    except Exception as e:
        logger.error(f"Error starting background refresh: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start refresh: {str(e)}")

@api_router.get("/cryptos/refresh-status", response_model=RefreshStatusResponse)
async def get_refresh_status():
    """Get the status of background refresh operations"""
    try:
        status_data = data_service.get_refresh_status()
        
        return RefreshStatusResponse(
            status=status_data['status'],
            active_tasks=status_data['active_tasks'],
            last_update=status_data['last_update'],
            last_duration_seconds=status_data['last_duration_seconds'],
            last_error=status_data['last_error'],
            next_auto_refresh=status_data['next_auto_refresh']
        )
        
    except Exception as e:
        logger.error(f"Error getting refresh status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.post("/cryptos/refresh")
async def refresh_crypto_data(request: RefreshRequest = RefreshRequest()):
    """LEGACY: Manual refresh cryptocurrency data - Now starts background refresh"""
    try:
        logger.info(f"Legacy refresh requested - redirecting to background refresh")
        
        # Start background refresh instead of blocking
        task_id = await data_service.start_background_refresh(force=request.force)
        
        if task_id:
            # Return immediately with background task info
            return {
                "status": "success",
                "message": "Background refresh started",
                "task_id": task_id,
                "check_status_endpoint": "/api/cryptos/refresh-status",
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            # Check if refresh is already running
            refresh_status = data_service.get_refresh_status()
            if refresh_status['status'] == 'running':
                return {
                    "status": "info",
                    "message": "Background refresh already in progress",
                    "check_status_endpoint": "/api/cryptos/refresh-status",
                    "timestamp": datetime.utcnow().isoformat()
                }
            else:
                raise HTTPException(status_code=500, detail="Failed to start background refresh")
        
    except Exception as e:
        logger.error(f"Error in legacy refresh endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start refresh: {str(e)}")

# New CryptoRebound endpoints

@api_router.get("/cryptos/multi-period-analysis", response_model=List[MultiPeriodCrypto])
async def get_multi_period_analysis(
    limit: int = Query(15, description="Number of top cryptos to return", ge=5, le=50),
    short_periods: List[str] = Query(['24h', '7d', '30d'], description="Short-term periods to analyze"),
    long_periods: List[str] = Query(['90d', '180d', '270d', '365d'], description="Long-term periods to analyze")
):
    """Get top cryptocurrencies analyzed across multiple periods with short/long term breakdown"""
    try:
        logger.info(f"Starting multi-period analysis: {len(short_periods)} short + {len(long_periods)} long periods, top {limit}")
        
        # Dictionary to store all crypto data with scores from different periods
        crypto_scores = {}
        
        # Get data for SHORT TERM periods first
        for period in short_periods:
            try:
                period_cryptos = await data_service.get_optimized_crypto_ranking(
                    period=period, 
                    limit=200,  # Get more data for analysis
                    offset=0, 
                    force_refresh=False
                )
                
                logger.info(f"Got {len(period_cryptos)} cryptos for SHORT period {period}")
                
                for crypto in period_cryptos:
                    symbol = crypto.symbol
                    
                    if symbol not in crypto_scores:
                        crypto_scores[symbol] = {
                            'symbol': symbol,
                            'name': crypto.name,
                            'price_usd': crypto.price_usd,
                            'market_cap_usd': crypto.market_cap_usd,
                            'short_period_scores': {},
                            'long_period_scores': {},
                            'short_total_score': 0,
                            'long_total_score': 0,
                            'short_period_count': 0,
                            'long_period_count': 0
                        }
                    
                    # Add score for this SHORT period
                    score = getattr(crypto, 'total_score', 0) or 0
                    crypto_scores[symbol]['short_period_scores'][period] = score
                    crypto_scores[symbol]['short_total_score'] += score
                    crypto_scores[symbol]['short_period_count'] += 1
                    
            except Exception as e:
                logger.warning(f"Error processing short period {period}: {e}")
                continue
        
        # Get data for LONG TERM periods 
        for period in long_periods:
            try:
                period_cryptos = await data_service.get_optimized_crypto_ranking(
                    period=period, 
                    limit=200,  # Get more data for analysis
                    offset=0, 
                    force_refresh=False
                )
                
                logger.info(f"Got {len(period_cryptos)} cryptos for LONG period {period}")
                
                for crypto in period_cryptos:
                    symbol = crypto.symbol
                    
                    # Initialize if not exists
                    if symbol not in crypto_scores:
                        crypto_scores[symbol] = {
                            'symbol': symbol,
                            'name': crypto.name,
                            'price_usd': crypto.price_usd,
                            'market_cap_usd': crypto.market_cap_usd,
                            'short_period_scores': {},
                            'long_period_scores': {},
                            'short_total_score': 0,
                            'long_total_score': 0,
                            'short_period_count': 0,
                            'long_period_count': 0
                        }
                    
                    # Add score for this LONG period
                    score = getattr(crypto, 'total_score', 0) or 0
                    crypto_scores[symbol]['long_period_scores'][period] = score
                    crypto_scores[symbol]['long_total_score'] += score
                    crypto_scores[symbol]['long_period_count'] += 1
                    
            except Exception as e:
                logger.warning(f"Error processing long period {period}: {e}")
                continue
        
        # Filter cryptos that appear in both short and long periods
        min_short_periods = max(1, len(short_periods) // 2)  # At least half the short periods
        min_long_periods = max(1, len(long_periods) // 3)    # At least 1/3 of long periods
        
        filtered_cryptos = {}
        
        for symbol, data in crypto_scores.items():
            if data['short_period_count'] >= min_short_periods:
                # Calculate SHORT TERM average score
                data['short_average_score'] = data['short_total_score'] / data['short_period_count']
                
                # Calculate LONG TERM average score if we have data
                if data['long_period_count'] >= min_long_periods:
                    data['long_average_score'] = data['long_total_score'] / data['long_period_count']
                else:
                    data['long_average_score'] = None
                
                # Calculate SHORT TERM consistency
                short_scores = list(data['short_period_scores'].values())
                if len(short_scores) > 1:
                    mean_score = sum(short_scores) / len(short_scores)
                    variance = sum((x - mean_score) ** 2 for x in short_scores) / len(short_scores)
                    std_dev = variance ** 0.5
                    data['short_consistency_score'] = max(0, 100 - (std_dev / max(mean_score, 1)) * 100)
                else:
                    data['short_consistency_score'] = 100
                
                # Calculate LONG TERM consistency
                if data['long_average_score'] is not None:
                    long_scores = list(data['long_period_scores'].values())
                    if len(long_scores) > 1:
                        mean_score = sum(long_scores) / len(long_scores)
                        variance = sum((x - mean_score) ** 2 for x in long_scores) / len(long_scores)
                        std_dev = variance ** 0.5
                        data['long_consistency_score'] = max(0, 100 - (std_dev / max(mean_score, 1)) * 100)
                    else:
                        data['long_consistency_score'] = 100
                else:
                    data['long_consistency_score'] = None
                
                # Calculate TREND CONFIRMATION
                if data['long_average_score'] is not None:
                    short_avg = data['short_average_score']
                    long_avg = data['long_average_score']
                    
                    # Compare short vs long term performance
                    if abs(short_avg - long_avg) <= 10:
                        data['trend_confirmation'] = "Strong"  # Very similar scores
                    elif short_avg > long_avg and (short_avg - long_avg) <= 20:
                        data['trend_confirmation'] = "Accelerating"  # Short term improving
                    elif long_avg > short_avg and (long_avg - short_avg) <= 20:
                        data['trend_confirmation'] = "Cooling"  # Long term was better
                    elif abs(short_avg - long_avg) > 30:
                        data['trend_confirmation'] = "Divergent"  # Very different
                    else:
                        data['trend_confirmation'] = "Weak"  # Moderate difference
                else:
                    data['trend_confirmation'] = "Unknown"  # No long term data
                
                # Find best and worst periods (combine all periods)
                all_periods = {**data['short_period_scores'], **data['long_period_scores']}
                if all_periods:
                    sorted_periods = sorted(all_periods.items(), key=lambda x: x[1], reverse=True)
                    data['best_period'] = sorted_periods[0][0]
                    data['worst_period'] = sorted_periods[-1][0]
                else:
                    data['best_period'] = short_periods[0] if short_periods else 'unknown'
                    data['worst_period'] = short_periods[0] if short_periods else 'unknown'
                
                filtered_cryptos[symbol] = data
        
        # Sort by SHORT TERM average score (with consistency bonus) - prioritize recent performance
        sorted_cryptos = []
        for symbol, data in filtered_cryptos.items():
            # Give slight bonus for SHORT TERM consistency (up to 5 points)
            consistency_bonus = (data['short_consistency_score'] / 100) * 5
            
            # Give bonus for STRONG trend confirmation (up to 3 points)
            trend_bonus = 0
            if data['trend_confirmation'] == "Strong":
                trend_bonus = 3
            elif data['trend_confirmation'] == "Accelerating":
                trend_bonus = 2
            elif data['trend_confirmation'] == "Cooling":
                trend_bonus = 1
            
            final_score = data['short_average_score'] + consistency_bonus + trend_bonus
            
            sorted_cryptos.append((symbol, data, final_score))
        
        # Sort by final score and take top N
        sorted_cryptos.sort(key=lambda x: x[2], reverse=True)
        top_cryptos = sorted_cryptos[:limit]
        
        # Convert to response format
        result = []
        for rank, (symbol, data, final_score) in enumerate(top_cryptos, 1):
            result.append(MultiPeriodCrypto(
                symbol=symbol,
                name=data['name'],
                price_usd=data['price_usd'],
                market_cap_usd=data['market_cap_usd'],
                average_score=round(data['short_average_score'], 2),  # Short term average
                long_term_average=round(data['long_average_score'], 2) if data['long_average_score'] is not None else None,
                period_scores=data['short_period_scores'],  # Short term scores
                long_term_scores=data['long_period_scores'] if data['long_period_scores'] else None,
                best_period=data['best_period'],
                worst_period=data['worst_period'],
                consistency_score=round(data['short_consistency_score'], 1),
                long_term_consistency=round(data['long_consistency_score'], 1) if data['long_consistency_score'] is not None else None,
                trend_confirmation=data['trend_confirmation'],
                rank=rank
            ))
        
        logger.info(f"Multi-period analysis completed: {len(result)} cryptos analyzed across {len(short_periods)} short + {len(long_periods)} long periods")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in multi-period analysis: {e}")
        raise HTTPException(status_code=500, detail=f"Multi-period analysis failed: {str(e)}")

@api_router.get("/system/dynamic-limit", response_model=DynamicLimitResponse)
async def get_dynamic_analysis_limit():
    """Get dynamic analysis limit based on current system resources and memory"""
    try:
        # Get system resources
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=1)
        available_memory_mb = memory.available / (1024 * 1024)
        
        # Calculate recommended limits based on available memory
        # Estimate: each crypto uses ~1KB in memory for analysis
        base_crypto_memory_kb = 1  # Base memory per crypto
        memory_safety_factor = 0.7  # Use 70% of available memory max
        
        # Memory-based calculation
        memory_based_limit = int((available_memory_mb * 1024 * memory_safety_factor) / base_crypto_memory_kb)
        
        # Performance-based recommendations
        if cpu_percent > 80:
            performance_factor = 0.5  # High CPU usage, reduce limit
            performance_mode = "optimal"
            current_load = "high"
        elif cpu_percent > 50:
            performance_factor = 0.8  # Medium CPU usage
            performance_mode = "balanced" 
            current_load = "medium"
        else:
            performance_factor = 1.0  # Low CPU usage, allow full capacity
            performance_mode = "maximum"
            current_load = "low"
        
        # Final recommended limit
        recommended_limit = min(
            int(memory_based_limit * performance_factor),
            5000  # Hard upper limit for safety
        )
        
        # Ensure minimum viable limit
        recommended_limit = max(recommended_limit, 100)
        
        # Performance impact assessment
        if recommended_limit >= 2000:
            performance_impact = "low"
            memory_usage = "< 10MB estimated"
        elif recommended_limit >= 1000:
            performance_impact = "medium"
            memory_usage = "5-10MB estimated"
        else:
            performance_impact = "high"
            memory_usage = "< 5MB estimated"
        
        system_info = SystemResourcesInfo(
            available_memory_mb=round(available_memory_mb, 2),
            cpu_usage_percent=round(cpu_percent, 2),
            recommended_max_cryptos=recommended_limit,
            performance_mode=performance_mode,
            current_load=current_load
        )
        
        logger.info(f"Dynamic limit calculated: {recommended_limit} cryptos (Memory: {available_memory_mb:.1f}MB, CPU: {cpu_percent:.1f}%)")
        
        return DynamicLimitResponse(
            max_recommended_limit=recommended_limit,
            performance_impact=performance_impact,
            memory_usage_estimate=memory_usage,
            system_resources=system_info
        )
        
    except Exception as e:
        logger.error(f"Error calculating dynamic limit: {e}")
        # Fallback to safe defaults
        return DynamicLimitResponse(
            max_recommended_limit=1000,
            performance_impact="medium", 
            memory_usage_estimate="< 5MB estimated",
            system_resources=SystemResourcesInfo(
                available_memory_mb=0.0,
                cpu_usage_percent=0.0,
                recommended_max_cryptos=1000,
                performance_mode="balanced",
                current_load="unknown"
            )
        )

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

@api_router.get("/database/stats")
async def get_database_stats():
    """Get detailed database statistics"""
    try:
        stats = await data_service.get_database_stats()
        return {
            "status": "success",
            "database_stats": stats,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.post("/database/enrich")
async def trigger_enrichment(symbols: List[str] = Query([], description="Specific symbols to enrich")):
    """Trigger data enrichment for specific symbols"""
    try:
        if not symbols:
            # Get symbols that need enrichment
            symbols = await data_service.db_cache.get_stale_data_symbols(limit=20)
        
        if symbols:
            await data_service.enrichment_service.schedule_enrichment_for_symbols(symbols, priority=1)
            await data_service.enrichment_service.process_enrichment_tasks(max_tasks=5)
            
            return {
                "status": "success",
                "message": f"Triggered enrichment for {len(symbols)} symbols",
                "symbols": symbols,
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            return {
                "status": "info",
                "message": "No symbols need enrichment",
                "timestamp": datetime.utcnow().isoformat()
            }
        
    except Exception as e:
        logger.error(f"Error triggering enrichment: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/database/quality")
async def get_data_quality_overview():
    """Get overview of data quality in the database"""
    try:
        stats = await data_service.get_database_stats()
        
        return {
            "status": "success",
            "quality_overview": {
                "total_cryptocurrencies": stats.get("total_cryptocurrencies", 0),
                "quality_distribution": stats.get("quality_distribution", {}),
                "average_quality_score": stats.get("average_quality_score", 0),
                "enrichment_tasks": stats.get("enrichment_tasks", {}),
            },
            "recommendations": await _get_quality_recommendations(stats),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting data quality overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def _get_quality_recommendations(stats: Dict[str, Any]) -> List[str]:
    """Generate recommendations based on database stats"""
    recommendations = []
    
    quality_dist = stats.get("quality_distribution", {})
    total_cryptos = stats.get("total_cryptocurrencies", 0)
    avg_quality = stats.get("average_quality_score", 0)
    
    if total_cryptos < 1000:
        recommendations.append("Consider adding more cryptocurrency data sources to reach the 1000+ target")
    
    if avg_quality < 70:
        recommendations.append("Overall data quality is below optimal. Consider running enrichment tasks")
    
    low_quality_percent = (quality_dist.get("low", 0) / max(1, total_cryptos)) * 100
    if low_quality_percent > 30:
        recommendations.append(f"{low_quality_percent:.1f}% of data has low quality. Run targeted enrichment")
    
    pending_tasks = stats.get("enrichment_tasks", {}).get("pending", 0)
    if pending_tasks > 50:
        recommendations.append(f"{pending_tasks} enrichment tasks pending. Consider processing them")
    
    if not recommendations:
        recommendations.append("Data quality looks good! System is operating optimally")
    
    return recommendations

@api_router.post("/ranking/precompute")
async def trigger_ranking_precomputation(
    periods: List[str] = Query([], description="Specific periods to precompute"),
    background: bool = Query(True, description="Run in background")
):
    """Trigger precomputation of rankings for better performance"""
    try:
        if hasattr(data_service, 'precompute_service'):
            precompute_service = data_service.precompute_service
            
            if not periods:
                periods = ['24h', '7d', '30d', '90d', '180d', '270d', '365d']
            
            if background:
                # Schedule background precomputation
                asyncio.create_task(precompute_service.precompute_all_rankings())
                
                return {
                    "status": "success", 
                    "message": f"Scheduled precomputation for {len(periods)} periods in background",
                    "periods": periods,
                    "timestamp": datetime.utcnow().isoformat()
                }
            else:
                # Compute immediately (will be slower)
                await precompute_service.precompute_all_rankings()
                
                return {
                    "status": "success",
                    "message": f"Completed precomputation for {len(periods)} periods",
                    "periods": periods,
                    "timestamp": datetime.utcnow().isoformat()
                }
        else:
            raise HTTPException(status_code=501, detail="Precomputation service not available")
            
    except Exception as e:
        logger.error(f"Error triggering ranking precomputation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/ranking/status")
async def get_ranking_computation_status():
    """Get the status of ranking computations"""
    try:
        if hasattr(data_service, 'precompute_service'):
            precompute_service = data_service.precompute_service
            status = precompute_service.get_computation_status()
            
            return {
                "status": "success",
                "computation_status": status,
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            return {
                "status": "info",
                "message": "Precomputation service not available",
                "computation_status": {"periods_computing": [], "cache_status": {}},
                "timestamp": datetime.utcnow().isoformat()
            }
            
    except Exception as e:
        logger.error(f"Error getting ranking computation status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/performance/stats")
async def get_performance_stats():
    """Get performance statistics for the application"""
    try:
        # Get database stats
        db_stats = await data_service.get_database_stats()
        
        # Get service health
        health_status = data_service.is_healthy()
        
        # Calculate performance metrics
        total_cryptos = db_stats.get("total_cryptocurrencies", 0)
        avg_quality = db_stats.get("average_quality_score", 0)
        
        performance_level = "excellent" if avg_quality >= 80 else "good" if avg_quality >= 60 else "fair"
        
        return {
            "status": "success",
            "performance_stats": {
                "total_cryptocurrencies": total_cryptos,
                "average_quality_score": avg_quality,
                "performance_level": performance_level,
                "database_stats": db_stats,
                "services_health": health_status,
                "optimization_recommendations": [
                    "Consider running precomputation for periods 90d+" if avg_quality < 70 else "Performance is optimal",
                    f"Database has {total_cryptos} cryptos - consider enrichment if below 1000" if total_cryptos < 1000 else "Good data coverage"
                ]
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting performance stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/cryptos/ranking", response_model=List[CryptoCurrency])
async def get_crypto_ranking(
    period: str = Query("24h", description="Time period for ranking"),
    limit: int = Query(50, description="Number of results to return", ge=1, le=10000),
    offset: int = Query(0, description="Offset for pagination", ge=0),
    force_refresh: bool = Query(False, description="Force refresh data"),
    fix_historical: bool = Query(True, description="Fix missing/incorrect historical price data")
):
    """Get cryptocurrency ranking with enhanced historical data accuracy"""
    try:
        logger.info(f"Getting crypto ranking: period={period}, limit={limit}, offset={offset}, force_refresh={force_refresh}")
        
        # Use optimized ranking method that leverages precomputation
        result = await data_service.get_optimized_crypto_ranking(
            period=period,
            limit=limit, 
            offset=offset,
            force_refresh=force_refresh
        )
        
        if not result:
            logger.warning(f"No ranking data available for {period}")
            # Try fallback to basic aggregation
            cryptos = await data_service.get_aggregated_crypto_data(force_refresh=True)
            
            if cryptos:
                # Apply dynamic limit based on request size and system capacity
                effective_limit = min(len(cryptos), limit + offset + 100)  # Buffer for better ranking
                limited_cryptos = cryptos[:effective_limit]
                
                # Basic scoring and pagination
                scored_cryptos = scoring_service.calculate_scores(limited_cryptos, period)
                end_index = offset + limit
                result = scored_cryptos[offset:end_index]
        
        logger.info(f"Returning {len(result)} ranked cryptocurrencies for {period}")
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
    
    # Initialize the database connection for precomputation service
    data_service.set_db_client(client)
    
    # Do a quick health check and start background tasks
    try:
        # Quick health check
        health_status = data_service.is_healthy()
        logger.info(f"Service health check: {health_status}")
        
        # Start background data loading and precomputation (non-blocking)
        asyncio.create_task(background_startup_tasks())
        
        logger.info("CryptoRebound Ranking API startup completed successfully")
        
    except Exception as e:
        logger.error(f"Error during startup initialization: {e}")
        # Don't fail the startup, just log the error

async def background_startup_tasks():
    """Background tasks that run after startup to avoid blocking server start"""
    try:
        logger.info("Starting background startup tasks...")
        
        # Start background precomputation for better performance
        if hasattr(data_service, 'precompute_service'):
            logger.info("Starting background precomputation of rankings...")
            asyncio.create_task(data_service.precompute_service.schedule_background_precomputation())
        
        # Do initial data aggregation (this can take time)
        logger.info("Loading initial cryptocurrency data...")
        cryptos = await data_service.get_aggregated_crypto_data(force_refresh=False)
        
        if cryptos:
            logger.info(f"Initial data loaded: {len(cryptos)} cryptocurrencies available")
            
            # Cache some basic rankings
            for period in ['24h', '7d']:
                try:
                    scored_cryptos = scoring_service.calculate_scores(cryptos[:100].copy(), period)  # Limit for startup
                    rankings_cache[period] = scored_cryptos
                    last_cache_update[period] = datetime.utcnow()
                    logger.info(f"Cached ranking for {period}: {len(scored_cryptos)} cryptos")
                except Exception as e:
                    logger.warning(f"Failed to cache ranking for {period}: {e}")
        else:
            logger.warning("No initial cryptocurrency data available")
            
        logger.info("Background startup tasks completed successfully")
        
    except Exception as e:
        logger.error(f"Error during background startup tasks: {e}")
        # Don't fail, just log the error

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
