"use client";

import { useEffect, useState } from "react";
import { Card } from "@/components/Card";
import { VerdictBadge } from "@/components/VerdictBadge";
import { ScoreBar } from "@/components/ScoreBar";
import { fetchApi, Opportunity } from "@/lib/api";

export default function OpportunitiesPage() {
  const [opportunities, setOpportunities] = useState<Opportunity[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<string>("all");

  useEffect(() => {
    const params = filter === "all" ? "" : `?verdict=${filter}`;
    fetchApi<{ opportunities: Opportunity[] }>(`/api/signals/opportunities${params}&limit=50`)
      .then((data) => setOpportunities(data.opportunities))
      .catch(() => setOpportunities([]))
      .finally(() => setLoading(false));
  }, [filter]);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-white">Opportunities</h2>
        <p className="text-sm text-slate-400 mt-1">
          Stocks with multiple stacked buy signals — highest probability of upside
        </p>
      </div>

      {/* Filters */}
      <div className="flex gap-2">
        {["all", "BUY", "WATCH"].map((v) => (
          <button
            key={v}
            onClick={() => setFilter(v)}
            className={`px-3 py-1.5 text-xs font-medium rounded-lg transition-colors ${
              filter === v
                ? "bg-accent-blue text-white"
                : "bg-bg-card text-slate-300 hover:bg-bg-hover"
            }`}
          >
            {v === "all" ? "All" : v}
          </button>
        ))}
      </div>

      {/* Results */}
      <Card>
        {loading ? (
          <p className="text-slate-400">Loading...</p>
        ) : opportunities.length === 0 ? (
          <p className="text-slate-400">No opportunities found. Signal detection needs to run first.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-slate-400 border-b border-slate-700/50">
                  <th className="py-2 text-left">Symbol</th>
                  <th className="py-2 text-left">Sector</th>
                  <th className="py-2 text-right">Price</th>
                  <th className="py-2 text-right">PE</th>
                  <th className="py-2 text-center">Signals</th>
                  <th className="py-2 text-left w-40">Score</th>
                  <th className="py-2 text-center">Verdict</th>
                  <th className="py-2 text-left">Top Signals</th>
                </tr>
              </thead>
              <tbody>
                {opportunities.map((opp) => (
                  <tr
                    key={opp.symbol}
                    className="border-b border-slate-700/30 hover:bg-bg-hover cursor-pointer"
                    onClick={() => (window.location.href = `/stock/${opp.symbol}`)}
                  >
                    <td className="py-3 font-semibold text-white">{opp.symbol}</td>
                    <td className="py-3 text-slate-400">{opp.sector || "—"}</td>
                    <td className="py-3 text-right">${opp.current_price?.toFixed(2) || "—"}</td>
                    <td className="py-3 text-right">{opp.pe_ratio?.toFixed(1) || "—"}</td>
                    <td className="py-3 text-center">{opp.signals_fired}</td>
                    <td className="py-3">
                      <ScoreBar score={opp.composite_score} />
                    </td>
                    <td className="py-3 text-center">
                      <VerdictBadge verdict={opp.verdict} />
                    </td>
                    <td className="py-3 text-xs text-slate-400 max-w-xs truncate">
                      {opp.top_signals}
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
