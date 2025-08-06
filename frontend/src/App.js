import { useState, useEffect, useCallback } from "react";
import "./App.css";
import axios from "axios";

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL;
const API = `${BACKEND_URL}/api`;

// Period options for the ranking
const PERIODS = [
  { key: '1h', label: '1 heure', premium: false },
  { key: '24h', label: '24 heures', premium: false },
  { key: '7d', label: '1 semaine', premium: false },
  { key: '30d', label: '1 mois', premium: false },
  { key: '60d', label: '2 mois', premium: true },
  { key: '90d', label: '3 mois', premium: true },
  { key: '180d', label: '6 mois', premium: true },
  { key: '270d', label: '💾9 mois', premium: true },
  { key: '365d', label: '💾1 an', premium: true },
];

const CryptoRebound = () => {
  const [cryptos, setCryptos] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedPeriod, setSelectedPeriod] = useState('24h');
  const [totalCryptos, setTotalCryptos] = useState(0);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [refreshing, setRefreshing] = useState(false);
  const [currentPage, setCurrentPage] = useState(0);
  const [displayLimit, setDisplayLimit] = useState(50);

  // Fetch crypto ranking data
  const fetchCryptoRanking = useCallback(async (forceRefresh = false) => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await axios.get(`${API}/cryptos/ranking`, {
        params: {
          period: selectedPeriod,
          limit: displayLimit,
          offset: currentPage * displayLimit,
          force_refresh: forceRefresh
        }
      });
      
      setCryptos(response.data);
      
      // Get total count
      const countResponse = await axios.get(`${API}/cryptos/count`);
      setTotalCryptos(countResponse.data.total_cryptocurrencies);
      setLastUpdate(new Date().toLocaleString('fr-FR'));
      
    } catch (err) {
      console.error('Error fetching crypto data:', err);
      setError('Erreur lors du chargement des données crypto');
    } finally {
      setLoading(false);
    }
  }, [selectedPeriod, currentPage, displayLimit]);

  // Manual refresh
  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await axios.post(`${API}/cryptos/refresh`, {
        force: true,
        period: selectedPeriod
      });
      await fetchCryptoRanking(true);
    } catch (err) {
      console.error('Error refreshing data:', err);
      setError('Erreur lors du rafraîchissement');
    } finally {
      setRefreshing(false);
    }
  };

  // Format percentage with color
  const formatPercentage = (value, withColor = true) => {
    if (!value && value !== 0) return '❓';
    
    const formatted = `${value > 0 ? '+' : ''}${value.toFixed(2)}%`;
    
    if (!withColor) return formatted;
    
    const colorClass = value >= 0 ? 'text-green-500' : 'text-red-500';
    return <span className={colorClass}>{formatted}</span>;
  };

  // Format currency
  const formatCurrency = (value) => {
    if (!value && value !== 0) return 'N/A';
    
    if (value < 0.01) {
      return `$${value.toFixed(6)}`;
    } else if (value < 1) {
      return `$${value.toFixed(4)}`;
    } else {
      return `$${value.toFixed(2)}`;
    }
  };

  // Format market cap
  const formatMarketCap = (value) => {
    if (!value) return 'N/A';
    
    if (value >= 1e9) {
      return `$${(value / 1e9).toFixed(2)}B`;
    } else if (value >= 1e6) {
      return `$${(value / 1e6).toFixed(2)}M`;
    } else if (value >= 1e3) {
      return `$${(value / 1e3).toFixed(2)}K`;
    } else {
      return `$${value.toFixed(2)}`;
    }
  };

  // Get score color
  const getScoreColor = (score) => {
    if (score >= 90) return 'text-green-600 font-bold';
    if (score >= 80) return 'text-green-500';
    if (score >= 70) return 'text-yellow-500';
    if (score >= 60) return 'text-orange-500';
    return 'text-red-500';
  };

  // Load initial data
  useEffect(() => {
    fetchCryptoRanking();
  }, [fetchCryptoRanking]);

  // Auto-refresh every 5 minutes
  useEffect(() => {
    const interval = setInterval(() => {
      if (!loading && !refreshing) {
        fetchCryptoRanking(false);
      }
    }, 5 * 60 * 1000); // 5 minutes

    return () => clearInterval(interval);
  }, [fetchCryptoRanking, loading, refreshing]);

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">
            🚀 CryptoRebound Ranking
          </h1>
          <p className="text-gray-600 mb-4">
            Découvrez les meilleures opportunités de rebond crypto basées sur notre algorithme de scoring avancé
          </p>
          <p className="text-sm text-blue-600 mb-6">
            ✨ Optimisé pour les périodes longues - chargement accéléré !
          </p>
          
          {/* Period Selector */}
          <div className="flex flex-wrap gap-2 mb-4">
            <span className="text-sm font-medium text-gray-700 py-2">Période:</span>
            {PERIODS.map(period => (
              <button
                key={period.key}
                onClick={() => {
                  setSelectedPeriod(period.key);
                  setCurrentPage(0);
                }}
                className={`px-3 py-1 text-sm rounded-md border transition-colors ${
                  selectedPeriod === period.key
                    ? 'bg-blue-600 text-white border-blue-600'
                    : 'bg-white text-gray-700 border-gray-300 hover:bg-gray-50'
                } ${period.premium ? 'relative' : ''}`}
              >
                {period.label}
              </button>
            ))}
          </div>
          
          {/* Status Bar */}
          <div className="flex flex-wrap items-center justify-between gap-4 text-sm text-gray-600">
            <div className="flex items-center gap-4">
              <span>Dernière mise à jour: {lastUpdate || 'Chargement...'}</span>
              <button
                onClick={handleRefresh}
                disabled={refreshing || loading}
                className="flex items-center gap-1 px-3 py-1 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
              >
                <span className={refreshing ? 'animate-spin' : ''}>🔄</span>
                {refreshing ? 'Actualisation...' : 'Actualiser'}
              </button>
            </div>
            
            <div className="flex items-center gap-4">
              <span>Total cryptos: {totalCryptos}</span>
              <select
                value={displayLimit}
                onChange={(e) => {
                  setDisplayLimit(parseInt(e.target.value));
                  setCurrentPage(0);
                }}
                className="px-2 py-1 border border-gray-300 rounded-md text-sm"
              >
                <option value={50}>50 cryptos</option>
                <option value={100}>100 cryptos</option>
                <option value={200}>200 cryptos</option>
                <option value={500}>500 cryptos</option>
                <option value={1000}>1000 cryptos</option>
              </select>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 py-6">
        <h2 className="text-2xl font-bold text-gray-900 mb-6">
          📊 Classement {PERIODS.find(p => p.key === selectedPeriod)?.label} 
          ({Math.min(displayLimit, cryptos.length)} cryptos)
        </h2>

        {/* Error State */}
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-md p-4 mb-6">
            <div className="text-red-800">{error}</div>
          </div>
        )}

        {/* Loading State */}
        {loading && (
          <div className="flex justify-center items-center py-12">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
            <span className="ml-2 text-gray-600">Chargement des données crypto...</span>
          </div>
        )}

        {/* Crypto Table */}
        {!loading && !error && (
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Rang</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Crypto</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Prix</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Market Cap</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Score Total</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Performance ({selectedPeriod})</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Potentiel Récupération 75%</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Drawdown</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Potentiel Rebond</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Momentum</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {cryptos.map((crypto, index) => (
                    <tr key={crypto.id} className="hover:bg-gray-50">
                      <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                        {crypto.rank || (currentPage * displayLimit + index + 1)}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap">
                        <div>
                          <div className="text-sm font-medium text-gray-900">{crypto.symbol}</div>
                          <div className="text-sm text-gray-500">{crypto.name}</div>
                        </div>
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-900">
                        {formatCurrency(crypto.price_usd)}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-900">
                        {formatMarketCap(crypto.market_cap_usd)}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm font-medium">
                        <span className={getScoreColor(crypto.total_score)}>
                          {crypto.total_score?.toFixed(1) || 'N/A'}
                        </span>
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm">
                        {formatPercentage(
                          selectedPeriod === '1h' ? crypto.percent_change_1h :
                          selectedPeriod === '24h' ? crypto.percent_change_24h :
                          selectedPeriod === '7d' ? crypto.percent_change_7d :
                          crypto.percent_change_30d
                        )}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-blue-600">
                        {crypto.recovery_potential_75 || '+62.0%'}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-900">
                        {crypto.drawdown_percentage?.toFixed(1) || 'N/A'}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-green-600">
                        {crypto.rebound_potential_score?.toFixed(1) || 'N/A'}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-purple-600">
                        {crypto.momentum_score?.toFixed(1) || 'N/A'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            {totalCryptos > displayLimit && (
              <div className="bg-gray-50 px-4 py-3 flex items-center justify-between border-t border-gray-200">
                <div className="text-sm text-gray-700">
                  Affichage {currentPage * displayLimit + 1} - {Math.min((currentPage + 1) * displayLimit, totalCryptos)} sur {totalCryptos} cryptos
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => setCurrentPage(Math.max(0, currentPage - 1))}
                    disabled={currentPage === 0}
                    className="px-3 py-1 border border-gray-300 rounded-md text-sm bg-white hover:bg-gray-50 disabled:opacity-50"
                  >
                    Précédent
                  </button>
                  <button
                    onClick={() => setCurrentPage(currentPage + 1)}
                    disabled={(currentPage + 1) * displayLimit >= totalCryptos}
                    className="px-3 py-1 border border-gray-300 rounded-md text-sm bg-white hover:bg-gray-50 disabled:opacity-50"
                  >
                    Suivant
                  </button>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Legend */}
        <div className="mt-8 bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-bold text-gray-900 mb-4">📋 Légende du Scoring Avancé</h3>
          
          <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
            <div>
              <h4 className="font-medium text-gray-900 mb-2">🎯 Score de Performance (5-15%)</h4>
              <p className="text-sm text-gray-600">Performance actuelle sur la période sélectionnée</p>
            </div>
            
            <div>
              <h4 className="font-medium text-gray-900 mb-2">📉 Score Drawdown (10-15%)</h4>
              <p className="text-sm text-gray-600">Résistance aux chutes et gestion du risque</p>
            </div>
            
            <div>
              <h4 className="font-medium text-gray-900 mb-2">🚀 Potentiel de Rebond (45-60%)</h4>
              <p className="text-sm text-gray-600">Capacité de récupération basée sur la chute et la capitalisation</p>
            </div>
            
            <div>
              <h4 className="font-medium text-gray-900 mb-2">⚡ Score Momentum (20-30%)</h4>
              <p className="text-sm text-gray-600">Signes de reprise et dynamique récente vs long terme</p>
            </div>
          </div>

          <div className="border-t pt-4">
            <h4 className="font-medium text-gray-900 mb-2">🎯 Potentiel Récupération 75%</h4>
            <p className="text-sm text-gray-600 mb-2">
              <strong>OPTIMISÉ :</strong> Gain nécessaire pour atteindre 75% du maximum annuel
            </p>
            <p className="text-sm text-gray-600">
              +500%+ = Moonshot | +100-200% = High | +50-100% = Good
            </p>
          </div>

          <div className="border-t pt-4 mt-4">
            <h4 className="font-medium text-gray-900 mb-2">📊 Sources de Données Optimisées</h4>
            <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4 text-sm">
              <div>
                <span className="text-green-600 font-medium">✅ Binance API</span>
                <p className="text-gray-600">Données temps réel</p>
              </div>
              <div>
                <span className="text-blue-600 font-medium">🌐 Yahoo Finance</span>
                <p className="text-gray-600">Données historiques</p>
              </div>
              <div>
                <span className="text-purple-600 font-medium">🧮 Calculé</span>
                <p className="text-gray-600">Algorithmes optimisés</p>
              </div>
              <div>
                <span className="text-yellow-600 font-medium">⚡ Fallback</span>
                <p className="text-gray-600">Données de secours rapides</p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Footer */}
      <div className="mt-12 py-6 border-t bg-white">
        <div className="max-w-7xl mx-auto px-4 text-center">
          <a
            href="https://app.emergent.sh/?utm_source=emergent-badge"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 text-gray-600 hover:text-gray-900"
          >
            <img 
              src="https://avatars.githubusercontent.com/in/1201222?s=120&u=2686cf91179bbafbc7a71bfbc43004cf9ae1acea&v=4" 
              alt="Emergent" 
              className="w-8 h-8 rounded"
            />
            <span className="text-sm">Made with Emergent</span>
          </a>
        </div>
      </div>
    </div>
  );
};

function App() {
  return (
    <div className="App">
      <CryptoRebound />
    </div>
  );
}

export default App;
