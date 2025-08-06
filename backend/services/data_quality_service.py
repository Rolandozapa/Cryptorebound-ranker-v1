import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from db_models import CryptoDataDB, DataQuality, DataSource
import statistics
import re

logger = logging.getLogger(__name__)

class DataQualityService:
    """Service de validation et scoring de qualité des données crypto"""
    
    def __init__(self):
        # Règles de validation
        self.validation_rules = {
            'price_usd': {
                'min': 0.0000001,
                'max': 1000000,
                'required': True
            },
            'market_cap_usd': {
                'min': 1000,
                'max': 10_000_000_000_000,  # 10T
                'required': False
            },
            'volume_24h_usd': {
                'min': 0,
                'max': 1_000_000_000_000,  # 1T
                'required': False
            },
            'percent_change_24h': {
                'min': -99.9,
                'max': 10000,  # 10000% max change
                'required': False
            }
        }
        
        # Poids pour le calcul du score de qualité
        self.quality_weights = {
            'completeness': 0.30,
            'freshness': 0.25,
            'consistency': 0.25,
            'accuracy': 0.20
        }
        
        # Champs essentiels pour un crypto
        self.essential_fields = [
            'symbol', 'name', 'price_usd', 'market_cap_usd', 'percent_change_24h'
        ]
        
        # Champs optionnels mais importants
        self.important_fields = [
            'volume_24h_usd', 'percent_change_7d', 'percent_change_30d',
            'max_price_1y', 'min_price_1y'
        ]
    
    def validate_and_score_data(self, crypto_data: Dict[str, Any]) -> tuple[bool, float, Dict[str, Any]]:
        """
        Valide les données crypto et calcule un score de qualité
        Returns: (is_valid, quality_score, quality_details)
        """
        try:
            quality_details = {}
            
            # 1. Validation de base
            is_valid, validation_details = self._validate_basic_rules(crypto_data)
            quality_details['validation'] = validation_details
            
            if not is_valid:
                logger.warning(f"Data validation failed for {crypto_data.get('symbol', 'unknown')}: {validation_details}")
                return False, 0.0, quality_details
            
            # 2. Score de complétude
            completeness_score = self._calculate_completeness_score(crypto_data)
            quality_details['completeness'] = completeness_score
            
            # 3. Score de fraîcheur
            freshness_score = self._calculate_freshness_score(crypto_data)
            quality_details['freshness'] = freshness_score
            
            # 4. Score de cohérence
            consistency_score = self._calculate_consistency_score(crypto_data)
            quality_details['consistency'] = consistency_score
            
            # 5. Score de précision
            accuracy_score = self._calculate_accuracy_score(crypto_data)
            quality_details['accuracy'] = accuracy_score
            
            # 6. Score final pondéré
            quality_score = (
                completeness_score * self.quality_weights['completeness'] +
                freshness_score * self.quality_weights['freshness'] +
                consistency_score * self.quality_weights['consistency'] +
                accuracy_score * self.quality_weights['accuracy']
            )
            
            quality_details['final_score'] = quality_score
            quality_details['quality_level'] = self._get_quality_level(quality_score)
            
            logger.info(f"Data quality for {crypto_data.get('symbol', 'unknown')}: {quality_score:.1f}/100")
            
            return True, quality_score, quality_details
            
        except Exception as e:
            logger.error(f"Error validating data quality: {e}")
            return False, 0.0, {'error': str(e)}
    
    def _validate_basic_rules(self, crypto_data: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        """Validation des règles de base"""
        validation_results = {}
        
        # Vérifier les champs requis
        if not crypto_data.get('symbol'):
            validation_results['symbol'] = 'Symbol is required'
            return False, validation_results
        
        # Normaliser le symbol
        symbol = crypto_data['symbol'].upper().strip()
        if not re.match(r'^[A-Z0-9]{1,10}$', symbol):
            validation_results['symbol'] = f'Invalid symbol format: {symbol}'
            return False, validation_results
        
        # Valider les champs numériques
        for field, rules in self.validation_rules.items():
            value = crypto_data.get(field)
            
            if value is None:
                if rules.get('required', False):
                    validation_results[field] = f'{field} is required'
                    return False, validation_results
                continue
            
            try:
                float_value = float(value)
                
                if float_value < rules['min'] or float_value > rules['max']:
                    validation_results[field] = f'{field} value {float_value} out of range [{rules["min"]}, {rules["max"]}]'
                    return False, validation_results
                    
            except (ValueError, TypeError):
                validation_results[field] = f'{field} is not a valid number: {value}'
                return False, validation_results
        
        # Vérifier la cohérence des prix
        if crypto_data.get('price_usd') and crypto_data.get('market_cap_usd'):
            price = float(crypto_data['price_usd'])
            market_cap = float(crypto_data['market_cap_usd'])
            
            # Market cap ne peut pas être disproportionné par rapport au prix
            if market_cap / price > 1_000_000_000_000:  # Supply trop élevé
                validation_results['consistency'] = 'Market cap/price ratio too high'
                return False, validation_results
        
        validation_results['status'] = 'passed'
        return True, validation_results
    
    def _calculate_completeness_score(self, crypto_data: Dict[str, Any]) -> float:
        """Calcule le score de complétude des données"""
        total_fields = len(self.essential_fields) + len(self.important_fields)
        filled_fields = 0
        
        # Champs essentiels (poids double)
        for field in self.essential_fields:
            if crypto_data.get(field) is not None:
                filled_fields += 2
        
        # Champs importants (poids simple)
        for field in self.important_fields:
            if crypto_data.get(field) is not None:
                filled_fields += 1
        
        # Score sur 100
        max_score = len(self.essential_fields) * 2 + len(self.important_fields)
        return min(100.0, (filled_fields / max_score) * 100)
    
    def _calculate_freshness_score(self, crypto_data: Dict[str, Any]) -> float:
        """Calcule le score de fraîcheur des données"""
        now = datetime.utcnow()
        
        # Chercher la timestamp la plus récente
        timestamps = []
        
        if crypto_data.get('last_updated'):
            if isinstance(crypto_data['last_updated'], str):
                try:
                    timestamp = datetime.fromisoformat(crypto_data['last_updated'].replace('Z', '+00:00'))
                    timestamps.append(timestamp)
                except:
                    pass
            elif isinstance(crypto_data['last_updated'], datetime):
                timestamps.append(crypto_data['last_updated'])
        
        if crypto_data.get('source_timestamps'):
            for ts_str in crypto_data['source_timestamps'].values():
                try:
                    if isinstance(ts_str, str):
                        timestamp = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                        timestamps.append(timestamp)
                    elif isinstance(ts_str, datetime):
                        timestamps.append(ts_str)
                except:
                    continue
        
        if not timestamps:
            return 0.0
        
        # Prendre la timestamp la plus récente
        most_recent = max(timestamps)
        age_minutes = (now - most_recent).total_seconds() / 60
        
        # Score basé sur l'âge
        if age_minutes <= 5:
            return 100.0
        elif age_minutes <= 30:
            return 90.0
        elif age_minutes <= 60:
            return 70.0
        elif age_minutes <= 240:  # 4 heures
            return 50.0
        elif age_minutes <= 1440:  # 24 heures
            return 25.0
        else:
            return 5.0
    
    def _calculate_consistency_score(self, crypto_data: Dict[str, Any]) -> float:
        """Calcule le score de cohérence des données"""
        score = 100.0
        
        try:
            # Vérifier la cohérence prix/market cap si les deux sont présents
            if (crypto_data.get('price_usd') and 
                crypto_data.get('market_cap_usd') and 
                crypto_data.get('circulating_supply')):
                
                price = float(crypto_data['price_usd'])
                market_cap = float(crypto_data['market_cap_usd'])
                supply = float(crypto_data['circulating_supply'])
                
                expected_market_cap = price * supply
                deviation = abs(market_cap - expected_market_cap) / market_cap
                
                if deviation > 0.1:  # Plus de 10% de différence
                    score -= 20
            
            # Vérifier la cohérence des changements de prix
            changes = []
            for period in ['1h', '24h', '7d', '30d']:
                field = f'percent_change_{period}'
                if crypto_data.get(field) is not None:
                    try:
                        change = float(crypto_data[field])
                        changes.append(change)
                    except:
                        continue
            
            if len(changes) >= 2:
                # Détecter des changements incohérents (variations trop extrêmes)
                if any(abs(change) > 1000 for change in changes):  # Plus de 1000%
                    score -= 15
                
                # Vérifier la tendance générale
                if len(changes) >= 3:
                    std_dev = statistics.stdev(changes) if len(changes) > 1 else 0
                    if std_dev > 500:  # Très haute volatilité dans les données
                        score -= 10
            
            # Vérifier les prix historiques
            if (crypto_data.get('max_price_1y') and 
                crypto_data.get('min_price_1y') and 
                crypto_data.get('price_usd')):
                
                current_price = float(crypto_data['price_usd'])
                max_price = float(crypto_data['max_price_1y'])
                min_price = float(crypto_data['min_price_1y'])
                
                if current_price > max_price * 1.1:  # Prix actuel > max + 10%
                    score -= 10
                if current_price < min_price * 0.9:  # Prix actuel < min - 10%
                    score -= 10
            
        except Exception as e:
            logger.warning(f"Error calculating consistency score: {e}")
            score -= 20
        
        return max(0.0, score)
    
    def _calculate_accuracy_score(self, crypto_data: Dict[str, Any]) -> float:
        """Calcule le score de précision des données"""
        score = 100.0
        
        # Vérifier le nombre de sources
        sources = crypto_data.get('data_sources', [])
        if len(sources) >= 2:
            score += 10  # Bonus pour multiple sources
        elif len(sources) == 0:
            score -= 20
        
        # Vérifier la source de données
        high_quality_sources = [DataSource.BINANCE, DataSource.COINGECKO]
        if any(source in sources for source in high_quality_sources):
            score += 5
        
        # Pénaliser les données avec beaucoup d'erreurs
        error_count = crypto_data.get('error_count', 0)
        if error_count > 3:
            score -= error_count * 5
        
        return max(0.0, min(100.0, score))
    
    def _get_quality_level(self, score: float) -> DataQuality:
        """Détermine le niveau de qualité basé sur le score"""
        if score >= 80:
            return DataQuality.HIGH
        elif score >= 60:
            return DataQuality.MEDIUM
        elif score >= 30:
            return DataQuality.LOW
        else:
            return DataQuality.INVALID
    
    def compare_data_sources(self, data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compare les données de différentes sources pour détecter les incohérences"""
        if len(data_list) < 2:
            return {'status': 'insufficient_data'}
        
        comparison_results = {
            'price_consistency': {},
            'outliers': [],
            'best_source': None,
            'confidence': 0.0
        }
        
        try:
            # Comparer les prix
            prices = []
            for data in data_list:
                if data.get('price_usd'):
                    price = float(data['price_usd'])
                    source = data.get('source', 'unknown')
                    prices.append({'price': price, 'source': source})
            
            if len(prices) >= 2:
                price_values = [p['price'] for p in prices]
                avg_price = statistics.mean(price_values)
                std_dev = statistics.stdev(price_values) if len(price_values) > 1 else 0
                
                # Détecter les outliers (plus de 2 écarts-types)
                for price_info in prices:
                    deviation = abs(price_info['price'] - avg_price)
                    if std_dev > 0 and deviation > 2 * std_dev:
                        comparison_results['outliers'].append({
                            'source': price_info['source'],
                            'price': price_info['price'],
                            'deviation': deviation
                        })
                
                # Calculer la cohérence
                if std_dev / avg_price < 0.05:  # Moins de 5% de variation
                    comparison_results['price_consistency']['level'] = 'high'
                    comparison_results['confidence'] = 0.9
                elif std_dev / avg_price < 0.15:  # Moins de 15% de variation
                    comparison_results['price_consistency']['level'] = 'medium'
                    comparison_results['confidence'] = 0.7
                else:
                    comparison_results['price_consistency']['level'] = 'low'
                    comparison_results['confidence'] = 0.3
                
                comparison_results['price_consistency']['average'] = avg_price
                comparison_results['price_consistency']['std_dev'] = std_dev
            
            return comparison_results
            
        except Exception as e:
            logger.error(f"Error comparing data sources: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def suggest_enrichment_fields(self, crypto_data: Dict[str, Any]) -> List[str]:
        """Suggère les champs qui ont besoin d'être enrichis"""
        missing_fields = []
        
        # Vérifier les champs essentiels
        for field in self.essential_fields:
            if not crypto_data.get(field):
                missing_fields.append(field)
        
        # Vérifier les champs importants
        for field in self.important_fields:
            if not crypto_data.get(field):
                missing_fields.append(field)
        
        # Vérifier la fraîcheur des données
        now = datetime.utcnow()
        last_updated = crypto_data.get('last_updated')
        if last_updated:
            try:
                if isinstance(last_updated, str):
                    last_updated = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                
                if (now - last_updated).total_seconds() > 3600:  # Plus d'1 heure
                    missing_fields.extend(['price_usd', 'percent_change_24h', 'volume_24h_usd'])
            except:
                pass
        
        return list(set(missing_fields))  # Supprimer les doublons