"use client";

import { useEffect, useState } from "react";
import { Card } from "@/components/Card";
import { VerdictBadge } from "@/components/VerdictBadge";
import { fetchApi, Position } from "@/lib/api";

export default function PortfolioPage() {
  const [portfolio, setPortfolio] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchApi<any>("/api/portfolio")
      .then(setPortfolio)
      .catch(() => setPortfolio(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p className="text-slate-400">Loading portfolio...</p>;

  const positions: Position[] = portfolio?.positions || [];

  return (
    <div className="space-y-6">
      <div className="flex items-baseline justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">My Stocks</h2>
          <p className="text-sm text-slate-400 mt-1">Your portfolio with real-time P&L</p>
        </div>
        <button className="px-4 py-2 text-sm font-medium bg-accent-blue text-white rounded-lg hover:bg-blue-600 transition-colors">
          + Add Position
        </button>
      </div>

      {/* Summary */}
      {portfolio && positions.length > 0 && (
        <div className="grid grid-cols-4 gap-4">
          <Card>
            <p className="text-xs text-slate-400">Portfolio Value</p>
            <p className="text-lg font-bold text-white">${portfolio.total_value?.toLocaleString()}</p>
          </Card>
          <Card>
            <p className="text-xs text-slate-400">Cost Basis</p>
            <p className="text-lg font-bold text-white">${portfolio.total_cost?.toLocaleString()}</p>
          </Card>
          <Card>
            <p className="text-xs text-slate-400">Total P&L</p>
            <p className={`text-lg font-bold ${portfolio.total_pnl >= 0 ? "text-accent-green" : "text-accent-red"}`}>
              {portfolio.total_pnl >= 0 ? "+" : ""}${portfolio.total_pnl?.toLocaleString()}
            </p>
          </Card>
          <Card>
            <p className="text-xs text-slate-400">Total Return</p>
            <p className={`text-lg font-bold ${portfolio.total_pnl_pct >= 0 ? "text-accent-green" : "text-accent-red"}`}>
              {portfolio.total_pnl_pct >= 0 ? "+" : ""}{portfolio.total_pnl_pct?.toFixed(2)}%
            </p>
          </Card>
        </div>
      )}

      {/* Positions Table */}
      <Card>
        {positions.length === 0 ? (
          <p className="text-slate-400 text-sm">
            No positions yet. Add stocks you own to track your portfolio P&L.
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-slate-400 border-b border-slate-700/50">
                  <th className="py-2 text-left">Symbol</th>
                  <th className="py-2 text-right">Shares</th>
                  <th className="py-2 text-right">Avg Cost</th>
                  <th className="py-2 text-right">Current</th>
                  <th className="py-2 text-right">P&L</th>
                  <th className="py-2 text-right">Return</th>
                  <th className="py-2 text-center">AI Score</th>
                  <th className="py-2 text-center">Verdict</th>
                </tr>
              </thead>
              <tbody>
                {positions.map((pos) => (
                  <tr
                    key={pos.symbol}
                    className="border-b border-slate-700/30 hover:bg-bg-hover cursor-pointer"
                    onClick={() => (window.location.href = `/stock/${pos.symbol}`)}
                  >
                    <td className="py-3 font-semibold text-white">{pos.symbol}</td>
                    <td className="py-3 text-right">{pos.shares}</td>
                    <td className="py-3 text-right">${pos.avg_cost?.toFixed(2)}</td>
                    <td className="py-3 text-right">${pos.current_price?.toFixed(2)}</td>
                    <td className={`py-3 text-right font-medium ${pos.pnl >= 0 ? "text-accent-green" : "text-accent-red"}`}>
                      {pos.pnl >= 0 ? "+" : ""}${pos.pnl?.toFixed(0)}
                    </td>
                    <td className={`py-3 text-right ${pos.pnl_pct >= 0 ? "text-accent-green" : "text-accent-red"}`}>
                      {pos.pnl_pct >= 0 ? "+" : ""}{pos.pnl_pct?.toFixed(1)}%
                    </td>
                    <td className="py-3 text-center text-slate-300">{pos.signal_score || "—"}</td>
                    <td className="py-3 text-center">
                      {pos.verdict ? <VerdictBadge verdict={pos.verdict} /> : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}
