const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export async function fetchApi<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

// ── Types ────────────────────────────────────────────────────────────────────

export interface Signal {
  symbol: string;
  exchange: string;
  signal_type: string;
  score: number;
  confidence: number;
  description: string;
  evidence: string;
}

export interface Opportunity {
  symbol: string;
  exchange: string;
  composite_score: number;
  verdict: string;
  signals_fired: number;
  top_signals: string;
  sector: string;
  current_price: number;
  pe_ratio: number;
  earnings_date: string;
  beat_rate: number;
  insider_net_30d: number;
  sentiment_score: number;
  analyst_upside_pct: number;
}

export interface Position {
  symbol: string;
  shares: number;
  avg_cost: number;
  buy_date: string;
  notes: string;
  current_price: number;
  market_value: number;
  cost_basis: number;
  pnl: number;
  pnl_pct: number;
  signal_score: number | null;
  verdict: string | null;
}

export interface StockOverview {
  symbol: string;
  price: {
    price: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    currency: string;
  } | null;
  fundamentals: Record<string, any> | null;
  signals: Signal[];
}
