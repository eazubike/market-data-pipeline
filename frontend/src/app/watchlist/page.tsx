"use client";

import { useEffect, useState } from "react";
import { Card } from "@/components/Card";
import { VerdictBadge } from "@/components/VerdictBadge";
import { fetchApi } from "@/lib/api";

interface WatchlistItem {
  symbol: string;
  target_price: number | null;
  notes: string;
  current_price: number;
  signal_score: number | null;
  verdict: string | null;
  top_signals: string | null;
}

export default function WatchlistPage() {
  const [items, setItems] = useState<WatchlistItem[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchApi<{ watchlist: WatchlistItem[] }>("/api/portfolio/watchlist")
      .then((data) => setItems(data.watchlist))
      .catch(() => setItems([]))
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex items-baseline justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">Watchlist</h2>
          <p className="text-sm text-slate-400 mt-1">Stocks you are tracking</p>
        </div>
        <button className="px-4 py-2 text-sm font-medium bg-accent-blue text-white rounded-lg hover:bg-blue-600 transition-colors">
          + Add Stock
        </button>
      </div>

      <Card>
        {loading ? (
          <p className="text-slate-400">Loading...</p>
        ) : items.length === 0 ? (
          <p className="text-slate-400 text-sm">
            Your watchlist is empty. Add stocks to track their signals and get alerted when conditions are right.
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-slate-400 border-b border-slate-700/50">
                  <th className="py-2 text-left">Symbol</th>
                  <th className="py-2 text-right">Price</th>
                  <th className="py-2 text-right">Target</th>
                  <th className="py-2 text-center">Signal</th>
                  <th className="py-2 text-center">Verdict</th>
                  <th className="py-2 text-left">Notes</th>
                </tr>
              </thead>
              <tbody>
                {items.map((item) => (
                  <tr
                    key={item.symbol}
                    className="border-b border-slate-700/30 hover:bg-bg-hover cursor-pointer"
                    onClick={() => (window.location.href = `/stock/${item.symbol}`)}
                  >
                    <td className="py-3 font-semibold text-white">{item.symbol}</td>
                    <td className="py-3 text-right">${item.current_price?.toFixed(2) || "—"}</td>
                    <td className="py-3 text-right">{item.target_price ? `$${item.target_price.toFixed(2)}` : "—"}</td>
                    <td className="py-3 text-center">{item.signal_score || "—"}</td>
                    <td className="py-3 text-center">
                      {item.verdict ? <VerdictBadge verdict={item.verdict} /> : "—"}
                    </td>
                    <td className="py-3 text-slate-400 text-xs max-w-xs truncate">{item.notes || "—"}</td>
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
