import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "MarketPulse — AI Stock Intelligence",
  description: "AI-powered stock analytics, signal detection, and portfolio management",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-bg-primary text-slate-200 antialiased">
        <div className="flex">
          <Sidebar />
          <main className="flex-1 p-6 ml-64">{children}</main>
        </div>
      </body>
    </html>
  );
}

function Sidebar() {
  const links = [
    { href: "/", label: "Dashboard", icon: "📊" },
    { href: "/opportunities", label: "Opportunities", icon: "🚀" },
    { href: "/screener", label: "Screener", icon: "🔍" },
    { href: "/portfolio", label: "My Stocks", icon: "💼" },
    { href: "/watchlist", label: "Watchlist", icon: "⭐" },
  ];

  return (
    <aside className="fixed left-0 top-0 h-screen w-64 bg-bg-secondary border-r border-slate-700/50 p-4 flex flex-col">
      <div className="mb-8">
        <h1 className="text-xl font-bold text-white">MarketPulse</h1>
        <p className="text-xs text-slate-400 mt-1">AI Stock Intelligence</p>
      </div>
      <nav className="flex-1 space-y-1">
        {links.map((link) => (
          <a
            key={link.href}
            href={link.href}
            className="flex items-center gap-3 px-3 py-2.5 rounded-lg text-slate-300 hover:bg-bg-hover hover:text-white transition-colors"
          >
            <span>{link.icon}</span>
            <span className="text-sm font-medium">{link.label}</span>
          </a>
        ))}
      </nav>
      <div className="text-xs text-slate-500 pt-4 border-t border-slate-700/50">
        Data updates hourly
      </div>
    </aside>
  );
}
