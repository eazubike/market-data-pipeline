"use client";

interface VerdictBadgeProps {
  verdict: string;
}

export function VerdictBadge({ verdict }: VerdictBadgeProps) {
  const styles: Record<string, string> = {
    BUY: "bg-green-500/20 text-green-400 border-green-500/30",
    WATCH: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30",
    NEUTRAL: "bg-slate-500/20 text-slate-400 border-slate-500/30",
    SELL: "bg-red-500/20 text-red-400 border-red-500/30",
  };

  const style = styles[verdict] || styles.NEUTRAL;

  return (
    <span className={`px-2 py-0.5 text-xs font-semibold rounded border ${style}`}>
      {verdict}
    </span>
  );
}
