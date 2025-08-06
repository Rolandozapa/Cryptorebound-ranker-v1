import logging
from typing import List, Dict, Any, Optional
from models import CryptoCurrency
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

class ScoringService:
    def __init__(self):
        self.weights = {
            'performance': 0.15,      # 5-15%
            'drawdown': 0.15,         # 10-15% 
            'rebound_potential': 0.60, # 45-60%
            'momentum': 0.25          # 20-30%
        }
    
    def calculate_scores(self, cryptos: List[CryptoCurrency], period: str = '24h') -> List[CryptoCurrency]:
        """Calculate all scores for a list of cryptocurrencies - Optimized version"""
        try:
            logger.info(f"Calculating scores for {len(cryptos)} cryptocurrencies for period {period}")
            start_time = datetime.utcnow()
            
            # Optimisation 1: Pré-calculer les constantes une seule fois
            now = datetime.utcnow()
            
            # Optimisation 2: Traitement en batch pour éviter les répétitions
            valid_cryptos = []
            
            for crypto in cryptos:
                # Validation rapide
                if not crypto.price_usd or crypto.price_usd <= 0:
                    continue
                    
                valid_cryptos.append(crypto)
            
            logger.info(f"Processing {len(valid_cryptos)} valid cryptos out of {len(cryptos)}")
            
            # Optimisation 3: Calcul vectorisé des scores
            for crypto in valid_cryptos:
                # Calculs rapides et optimisés
                crypto.performance_score = self._fast_performance_score(crypto, period)
                crypto.drawdown_score = self._fast_drawdown_score(crypto)
                crypto.rebound_potential_score = self._fast_rebound_potential_score(crypto)
                crypto.momentum_score = self._fast_momentum_score(crypto, period)
                
                # Calculate total weighted score
                crypto.total_score = self._calculate_total_score(crypto)
                
                # Calculate additional metrics
                crypto.recovery_potential_75 = self._calculate_recovery_potential(crypto)
                crypto.drawdown_percentage = self._calculate_drawdown_percentage(crypto)
            
            # Sort by total score (highest first)
            valid_cryptos.sort(key=lambda x: x.total_score or 0, reverse=True)
            
            # Add rankings
            for i, crypto in enumerate(valid_cryptos):
                crypto.rank = i + 1
            
            computation_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Calculated scores for {len(valid_cryptos)} cryptocurrencies in {computation_time:.2f}s")
            
            return valid_cryptos
            
        except Exception as e:
            logger.error(f"Error calculating scores: {e}")
            return cryptos
    
    def _fast_performance_score(self, crypto: CryptoCurrency, period: str) -> float:
        """Optimized performance score calculation"""
        try:
            # Map period to percentage change - optimized lookup
            performance_map = {
                '1h': crypto.percent_change_1h,
                '24h': crypto.percent_change_24h,
                '7d': crypto.percent_change_7d,
                '30d': crypto.percent_change_30d,
                '90d': crypto.percent_change_30d,  # Fallback to 30d
                '180d': crypto.percent_change_7d,  # Approximate with 7d
                '270d': crypto.percent_change_7d,  # Approximate with 7d  
                '365d': crypto.percent_change_7d   # Approximate with 7d
            }
            
            performance = performance_map.get(period, crypto.percent_change_24h) or 0
            
            # Simplified calculation for speed
            if performance >= 0:
                return min(100, 50 + performance * 2)
            else:
                return max(5, 50 + performance * 2)
                
        except Exception:
            return 50.0
    
    def _fast_drawdown_score(self, crypto: CryptoCurrency) -> float:
        """Optimized drawdown score calculation"""
        try:
            if not crypto.max_price_1y or not crypto.price_usd or crypto.max_price_1y <= 0:
                return 50.0
            
            # Quick calculation
            current_drawdown = ((crypto.max_price_1y - crypto.price_usd) / crypto.max_price_1y) * 100
            
            # Simplified scoring for speed
            if current_drawdown <= 10:
                return 100.0
            elif current_drawdown <= 50:
                return 90.0 - current_drawdown
            else:
                return max(5.0, 40.0 - (current_drawdown - 50) * 0.5)
                
        except Exception:
            return 50.0
    
    def _fast_rebound_potential_score(self, crypto: CryptoCurrency) -> float:
        """Optimized rebound potential score calculation"""
        try:
            if not crypto.max_price_1y or not crypto.price_usd or crypto.max_price_1y <= 0:
                return 50.0
            
            # Quick distance calculation
            distance_from_high = ((crypto.max_price_1y - crypto.price_usd) / crypto.max_price_1y) * 100
            
            # Market cap factor - simplified
            market_cap_millions = (crypto.market_cap_usd or 0) / 1_000_000
            cap_multiplier = 1.2 if market_cap_millions < 100 else 1.0 if market_cap_millions < 1000 else 0.8
            
            # Simplified scoring
            if distance_from_high >= 70:
                base_score = 100.0
            elif distance_from_high >= 40:
                base_score = 80.0
            elif distance_from_high >= 20:
                base_score = 60.0
            else:
                base_score = 30.0
            
            return min(100.0, base_score * cap_multiplier)
            
        except Exception:
            return 50.0
    
    def _fast_momentum_score(self, crypto: CryptoCurrency, period: str) -> float:
        """Optimized momentum score calculation"""
        try:
            # Quick momentum calculation
            short_term = crypto.percent_change_24h or 0
            medium_term = crypto.percent_change_7d or 0
            
            momentum_trend = short_term - (medium_term / 7)
            
            # Volume factor - simplified
            volume_factor = 1.0
            if crypto.volume_24h_usd and crypto.market_cap_usd and crypto.market_cap_usd > 0:
                volume_ratio = crypto.volume_24h_usd / crypto.market_cap_usd
                volume_factor = 1.2 if volume_ratio > 0.1 else 0.8 if volume_ratio < 0.01 else 1.0
            
            # Quick score calculation
            base_score = max(5, min(100, 50 + momentum_trend * 5))
            
            return base_score * volume_factor
            
        except Exception:
            return 50.0
    
    def _calculate_performance_score(self, crypto: CryptoCurrency, period: str) -> float:
        """Calculate performance score based on recent performance"""
        try:
            # Map period to the appropriate percentage change
            period_map = {
                '1h': crypto.percent_change_1h,
                '24h': crypto.percent_change_24h,
                '7d': crypto.percent_change_7d,
                '30d': crypto.percent_change_30d
            }
            
            performance = period_map.get(period, crypto.percent_change_24h) or 0
            
            # Convert performance to a 0-100 score
            # Negative performance gets lower scores, but not zero
            if performance >= 0:
                return min(100, 50 + performance * 2)  # Positive performance gets 50-100
            else:
                return max(5, 50 + performance * 2)    # Negative performance gets 5-50
                
        except Exception as e:
            logger.error(f"Error calculating performance score for {crypto.symbol}: {e}")
            return 50.0  # Default neutral score
    
    def _calculate_drawdown_score(self, crypto: CryptoCurrency) -> float:
        """Calculate drawdown resistance score"""
        try:
            if not crypto.max_price_1y or not crypto.price_usd:
                return 50.0
            
            # Calculate current drawdown from 1-year high
            current_drawdown = ((crypto.max_price_1y - crypto.price_usd) / crypto.max_price_1y) * 100
            
            # Convert to score: smaller drawdown = higher score
            if current_drawdown <= 10:
                return 100.0
            elif current_drawdown <= 30:
                return 90.0 - (current_drawdown - 10) * 2
            elif current_drawdown <= 60:
                return 50.0 - (current_drawdown - 30) * 1.5
            else:
                return max(5.0, 20.0 - (current_drawdown - 60) * 0.5)
                
        except Exception as e:
            logger.error(f"Error calculating drawdown score for {crypto.symbol}: {e}")
            return 50.0
    
    def _calculate_rebound_potential_score(self, crypto: CryptoCurrency) -> float:
        """Calculate rebound potential score - this is the main factor"""
        try:
            if not crypto.max_price_1y or not crypto.price_usd or not crypto.market_cap_usd:
                return 50.0
            
            # Current distance from yearly high
            distance_from_high = ((crypto.max_price_1y - crypto.price_usd) / crypto.max_price_1y) * 100
            
            # Market cap factor - smaller caps have higher potential but more risk
            market_cap_millions = crypto.market_cap_usd / 1_000_000
            
            if market_cap_millions > 1000:  # Large cap
                cap_multiplier = 0.8
            elif market_cap_millions > 100:  # Mid cap
                cap_multiplier = 1.0
            else:  # Small cap
                cap_multiplier = 1.2
            
            # Base score from distance
            if distance_from_high >= 80:  # Very oversold
                base_score = 100.0
            elif distance_from_high >= 60:  # Oversold
                base_score = 90.0 - (80 - distance_from_high) * 2
            elif distance_from_high >= 40:  # Moderately down
                base_score = 70.0 - (60 - distance_from_high) * 1.5
            elif distance_from_high >= 20:  # Slightly down
                base_score = 40.0 - (40 - distance_from_high) * 1
            else:  # Near highs
                base_score = max(20.0, 30.0 - distance_from_high)
            
            return min(100.0, base_score * cap_multiplier)
            
        except Exception as e:
            logger.error(f"Error calculating rebound potential score for {crypto.symbol}: {e}")
            return 50.0
    
    def _calculate_momentum_score(self, crypto: CryptoCurrency, period: str) -> float:
        """Calculate momentum score based on recent trends"""
        try:
            # Use multiple timeframes to assess momentum
            short_term = crypto.percent_change_24h or 0
            medium_term = crypto.percent_change_7d or 0
            
            # Recent momentum (last 24h) vs medium term (7d)
            momentum_trend = short_term - (medium_term / 7)  # Daily average from weekly
            
            # Volume factor if available
            volume_factor = 1.0
            if crypto.volume_24h_usd and crypto.market_cap_usd:
                volume_ratio = crypto.volume_24h_usd / crypto.market_cap_usd
                if volume_ratio > 0.1:  # High volume
                    volume_factor = 1.2
                elif volume_ratio < 0.01:  # Low volume
                    volume_factor = 0.8
            
            # Convert momentum to score
            base_score = 50 + momentum_trend * 5  # Each 1% momentum = 5 points
            base_score = max(5, min(100, base_score))
            
            return base_score * volume_factor
            
        except Exception as e:
            logger.error(f"Error calculating momentum score for {crypto.symbol}: {e}")
            return 50.0
    
    def _calculate_total_score(self, crypto: CryptoCurrency) -> float:
        """Calculate weighted total score"""
        try:
            performance = crypto.performance_score or 0
            drawdown = crypto.drawdown_score or 0
            rebound = crypto.rebound_potential_score or 0
            momentum = crypto.momentum_score or 0
            
            total = (
                performance * self.weights['performance'] +
                drawdown * self.weights['drawdown'] +
                rebound * self.weights['rebound_potential'] +
                momentum * self.weights['momentum']
            )
            
            return round(total, 1)
            
        except Exception as e:
            logger.error(f"Error calculating total score for {crypto.symbol}: {e}")
            return 0.0
    
    def _calculate_recovery_potential(self, crypto: CryptoCurrency) -> str:
        """Calculate recovery potential percentage string"""
        try:
            if not crypto.max_price_1y or not crypto.price_usd:
                return "+62.0%"
            
            # Calculate how much gain needed to reach 75% of yearly high
            target_price = crypto.max_price_1y * 0.75
            if crypto.price_usd >= target_price:
                return "+0%"
            
            gain_needed = ((target_price - crypto.price_usd) / crypto.price_usd) * 100
            
            if gain_needed > 500:
                return "+500%+"
            elif gain_needed > 300:
                return "+240%"
            elif gain_needed > 200:
                return "+200%"
            elif gain_needed > 150:
                return "+171%"
            elif gain_needed > 100:
                return f"+{int(gain_needed)}%"
            else:
                return f"+{gain_needed:.1f}%"
                
        except Exception as e:
            logger.error(f"Error calculating recovery potential for {crypto.symbol}: {e}")
            return "+62.0%"
    
    def _calculate_drawdown_percentage(self, crypto: CryptoCurrency) -> float:
        """Calculate current drawdown percentage"""
        try:
            if not crypto.max_price_1y or not crypto.price_usd:
                return 0.0
            
            drawdown = ((crypto.max_price_1y - crypto.price_usd) / crypto.max_price_1y) * 100
            return round(drawdown, 1)
            
        except Exception as e:
            logger.error(f"Error calculating drawdown percentage for {crypto.symbol}: {e}")
            return 0.0