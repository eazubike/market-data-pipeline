"use client";

import { useEffect, useState } from "react";
import { Card } from "@/components/Card";
import { VerdictBadge } from "@/components/VerdictBadge";
import { ScoreBar } from "@/components/ScoreBar";
import { fetchApi } from "@/lib/api";

interface ScreenerResult {
  symbol: string;
  exchange: string;
  pe_ratio: number;
  dividend_yield: number;
  revenue_ttm: number;
  composite_score: number;
  verdict: string;
  signals_fired: number;
  sector: string;
  beat_rate: number;
}

export default function ScreenerPage() {
  const [results, setResults] = useState<ScreenerResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [maxPe, setMaxPe] = useState<string>("");
  const [minScore, setMinScore] = useState<string>("");
  const [minBeatRate, setMinBeatRate] = useState<string>("");

  const runScreener = () => {
    setLoading(true);
    const params = new URLSearchParams();
    if (maxPe) params.set("max_pe", maxPe);
    if (minScore) params.set("min_signal_score", minScore);
    if (minBeatRate) params.set("min_beat_rate", minBeatRate);

    fetchApi<{ results: ScreenerResult[] }>(`/api/screener?${params.toString()}`)
      .then((data) => setResults(data.results))
      .catch(() => setResults([]))
      .finally(() => setLoading(false));
  };

  const loadPreset = (params: Record<string, any>) => {
    setMaxPe(params.max_pe?.toString() || "");
    setMinScore(params.min_signal_score?.toString() || "");
    setMinBeatRate(params.min_beat_rate?.toString() || "");
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-white">Screener</h2>
        <p className="text-sm text-slate-400 mt-1">Filter stocks by multiple criteria</p>
      </div>

      {/* Presets */}
      <div className="flex gap-2 flex-wrap">
        <button onClick={() => loadPreset({ max_pe: 15, min_signal_score: 50 })} className="px-3 py-1.5 text-xs bg-bg-card rounded-lg text-slate-300 hover:bg-bg-hover">
          Value + Signals
        </button>
        <button onClick={() => loadPreset({ min_beat_rate: 0.75, min_signal_score: 40 })} className="px-3 py-1.5 text-xs bg-bg-card rounded-lg text-slate-300 hover:bg-bg-hover">
          Earnings Beaters
        </button>
        <button onClick={() => loadPreset({ max_pe: 20, min_signal_score: 60 })} className="px-3 py-1.5 text-xs bg-bg-card rounded-lg text-slate-300 hover:bg-bg-hover">
          Undervalued Growth
        </button>
        <button onClick={() => loadPreset({ min_signal_score: 75 })} className="px-3 py-1.5 text-xs bg-bg-card rounded-lg text-slate-300 hover:bg-bg-hover">
          High Conviction BUY
        </button>
      </div>

      {/* Filters */}
      <Card>
        <div className="flex items-end gap-4 flex-wrap">
          <div>
            <label className="text-xs text-slate-400 block mb-1">Max PE</label>
            <input
              type="number"
              value={maxPe}
              onChange={(e) => setMaxPe(e.target.value)}
              placeholder="e.g. 25"
              className="bg-bg-primary border border-slate-700 rounded px-3 py-1.5 text-sm w-24 text-white"
            />
          </div>
          <div>
            <label className="text-xs text-slate-400 block mb-1">Min Signal Score</label>
            <input
              type="number"
              value={minScore}
              onChange={(e) => setMinScore(e.target.value)}
              placeholder="e.g. 60"
              className="bg-bg-primary border border-slate-700 rounded px-3 py-1.5 text-sm w-24 text-white"
            />
          </div>
          <div>
            <label className="text-xs text-slate-400 block mb-1">Min Beat Rate</label>
            <input
              type="number"
              step="0.1"
              value={minBeatRate}
              onChange={(e) => setMinBeatRate(e.target.value)}
              placeholder="e.g. 0.75"
              className="bg-bg-primary border border-slate-700 rounded px-3 py-1.5 text-sm w-24 text-white"
            />
          </div>
          <button
            onClick={runScreener}
            className="px-4 py-1.5 bg-accent-blue text-white text-sm font-medium rounded-lg hover:bg-blue-600"
          >
            Apply Filters
          </button>
        </div>
      </Card>

      {/* Results */}
      <Card title={results.length > 0 ? `${results.length} results` : undefined}>
        {loading ? (
          <p className="text-slate-400">Querying...</p>
        ) : results.length === 0 ? (
          <p className="text-slate-400 text-sm">Click &ldquo;Apply Filters&rdquo; to screen stocks.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-slate-400 border-b border-slate-700/50">
                  <th className="py-2 text-left">Symbol</th>
                  <th className="py-2 text-left">Sector</th>
                  <th className="py-2 text-right">PE</th>
                  <th className="py-2 text-right">Beat Rate</th>
                  <th className="py-2 text-left w-32">Score</th>
                  <th className="py-2 text-center">Verdict</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r) => (
                  <tr
                    key={r.symbol}
                    className="border-b border-slate-700/30 hover:bg-bg-hover cursor-pointer"
                    onClick={() => (window.location.href = `/stock/${r.symbol}`)}
                  >
                    <td className="py-3 font-semibold text-white">{r.symbol}</td>
                    <td className="py-3 text-slate-400">{r.sector || "—"}</td>
                    <td className="py-3 text-right">{r.pe_ratio?.toFixed(1) || "—"}</td>
                    <td className="py-3 text-right">{r.beat_rate ? `${(r.beat_rate * 100).toFixed(0)}%` : "—"}</td>
                    <td className="py-3">
                      {r.composite_score ? <ScoreBar score={r.composite_score} /> : "—"}
                    </td>
                    <td className="py-3 text-center">
                      {r.verdict ? <VerdictBadge verdict={r.verdict} /> : "—"}
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
