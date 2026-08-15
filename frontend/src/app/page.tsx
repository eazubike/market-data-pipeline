"use client";

import { useEffect, useState } from "react";
import { Card } from "@/components/Card";
import { VerdictBadge } from "@/components/VerdictBadge";
import { ScoreBar } from "@/components/ScoreBar";
import { fetchApi, Opportunity } from "@/lib/api";

export default function Dashboard() {
  const [opportunities, setOpportunities] = useState<Opportunity[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchApi<{ opportunities: Opportunity[] }>("/api/signals/opportunities?limit=10")
      .then((data) => setOpportunities(data.opportunities))
      .catch(() => setOpportunities([]))
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-white">Dashboard</h2>
        <p className="text-sm text-slate-400 mt-1">AI-powered market intelligence</p>
      </div>

      {/* Morning Brief */}
      <Card title="AI Morning Brief">
        <p className="text-slate-300 text-sm leading-relaxed">
          Morning brief will be generated daily by AI, summarizing overnight market
          activity, top signals, and key events. Requires Bedrock Claude integration.
        </p>
      </Card>

      {/* Top Opportunities */}
      <Card title="Top Opportunities Today">
        {loading ? (
          <p className="text-slate-400 text-sm">Loading signals...</p>
        ) : opportunities.length === 0 ? (
          <p className="text-slate-400 text-sm">No signals detected yet. Run the signal detection pipeline first.</p>
        ) : (
          <div className="space-y-3">
            {opportunities.map((opp) => (
              <a
                key={opp.symbol}
                href={`/stock/${opp.symbol}`}
                className="flex items-center justify-between p-3 rounded-lg bg-bg-hover hover:bg-slate-700/50 transition-colors"
              >
                <div className="flex items-center gap-4">
                  <div>
                    <span className="font-semibold text-white">{opp.symbol}</span>
                    <span className="text-xs text-slate-400 ml-2">{opp.sector}</span>
                  </div>
                  <span className="text-xs text-slate-400">{opp.signals_fired} signals</span>
                </div>
                <div className="flex items-center gap-4">
                  <div className="w-32">
                    <ScoreBar score={opp.composite_score} />
                  </div>
                  <VerdictBadge verdict={opp.verdict} />
                </div>
              </a>
            ))}
          </div>
        )}
      </Card>

      {/* Sector Performance Placeholder */}
      <Card title="Sector Performance">
        <p className="text-slate-400 text-sm">Sector heatmap coming soon — requires 2+ weeks of price data.</p>
      </Card>

      {/* Earnings This Week */}
      <Card title="Earnings This Week">
        <p className="text-slate-400 text-sm">Upcoming earnings calendar will populate from earnings_dates data.</p>
      </Card>
    </div>
  );
}
