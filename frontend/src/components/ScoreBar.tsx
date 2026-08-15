"use client";

interface ScoreBarProps {
  score: number;
  maxScore?: number;
}

export function ScoreBar({ score, maxScore = 100 }: ScoreBarProps) {
  const pct = Math.min((score / maxScore) * 100, 100);
  const color =
    score >= 75 ? "bg-accent-green" : score >= 50 ? "bg-accent-yellow" : "bg-accent-red";

  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-2 bg-slate-700 rounded-full overflow-hidden">
        <div className={`h-full ${color} rounded-full`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs font-medium w-8 text-right">{score.toFixed(0)}</span>
    </div>
  );
}
