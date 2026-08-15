"use client";

interface CardProps {
  title?: string;
  children: React.ReactNode;
  className?: string;
}

export function Card({ title, children, className = "" }: CardProps) {
  return (
    <div className={`bg-bg-card rounded-xl border border-slate-700/50 p-5 ${className}`}>
      {title && <h3 className="text-sm font-semibold text-slate-300 mb-3">{title}</h3>}
      {children}
    </div>
  );
}
