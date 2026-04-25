"use client";

import { usePathname, useRouter } from "next/navigation";
import { Menu, PanelLeftClose, PanelLeftOpen, Search, Bell } from "lucide-react";
import { useCallback, useState } from "react";
import { cn } from "@/lib/utils";
import { capitalize } from "@/lib/format";

interface TopBarProps {
  sidebarCollapsed: boolean;
  onToggleSidebar:  () => void;
  onOpenMobileNav:  () => void;
}

export function TopBar({ sidebarCollapsed, onToggleSidebar, onOpenMobileNav }: TopBarProps) {
  const pathname  = usePathname();
  const router    = useRouter();
  const [q, setQ] = useState("");
  const [focused, setFocused] = useState(false);

  const segments = pathname.split("/").filter(Boolean);
  const crumbs   = segments.map((s, i) => ({
    label: capitalize(s.replace(/-/g, " ")),
    href:  "/" + segments.slice(0, i + 1).join("/"),
  }));

  const handleSearch = useCallback((e: React.FormEvent) => {
    e.preventDefault();
    if (q.trim()) { router.push(`/search?q=${encodeURIComponent(q.trim())}`); setQ(""); }
  }, [q, router]);

  return (
    <header className={cn(
      "h-16 flex-shrink-0 flex items-center gap-4 px-6 z-20",
      "glass-topbar text-slate-900"
    )}>

      {/* Mobile hamburger */}
      <button onClick={onOpenMobileNav}
        className="md:hidden p-2 rounded-lg text-slate-500 hover:text-slate-900 hover:bg-slate-100 transition-colors"
        aria-label="Open navigation">
        <Menu className="w-5 h-5" />
      </button>

      {/* Sidebar toggle */}
      <button onClick={onToggleSidebar}
        className="hidden md:flex p-2 rounded-lg text-slate-500 hover:text-slate-900 hover:bg-slate-100 transition-colors"
        aria-label="Toggle sidebar">
        {sidebarCollapsed
          ? <PanelLeftOpen className="w-5 h-5" />
          : <PanelLeftClose className="w-5 h-5" />}
      </button>

      {/* Breadcrumb */}
      {crumbs.length > 0 && (
        <nav className="hidden sm:flex items-center gap-2 text-sm font-medium text-slate-500 mr-2">
          <span className="text-slate-700 font-semibold">TenderRadar</span>
          {crumbs.map((c, i) => (
            <span key={c.href} className="flex items-center gap-2">
              <span className="text-slate-300 mx-0.5">/</span>
              <span className={cn(
                i === crumbs.length - 1 ? "text-slate-900" : "hover:text-slate-700 transition-colors cursor-pointer"
              )}>
                {c.label}
              </span>
            </span>
          ))}
        </nav>
      )}

      {/* Search — hidden on /search (own search bar) and /chat (own input) */}
      {!pathname.startsWith("/search") && !pathname.startsWith("/chat") && (
        <form onSubmit={handleSearch} className="flex-1 max-w-sm ml-auto">
          <div className={cn(
            "relative flex items-center rounded-full transition-all duration-200 border",
            focused
              ? "border-slate-400 ring-2 ring-slate-900/10 shadow-md bg-white"
              : "border-slate-200 bg-slate-50/80"
          )}>
            <Search className="absolute left-3 w-4 h-4 text-slate-400 pointer-events-none" />
            <input
              value={q}
              onChange={e => setQ(e.target.value)}
              onFocus={() => setFocused(true)}
              onBlur={() => setFocused(false)}
              placeholder="Search tenders, portals, or notes..."
              className="w-full h-10 pl-10 pr-4 text-sm bg-transparent outline-none text-slate-800 placeholder:text-slate-400 font-medium"
            />
          </div>
        </form>
      )}

      <div className={pathname.startsWith("/search") || pathname.startsWith("/chat") ? "flex-1" : ""} />

      {/* Live status badge */}
      <div className="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-full bg-slate-900 text-white shadow-sm">
        <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
        <span className="text-xs font-semibold tracking-widest uppercase text-white/90">Live</span>
      </div>

      {/* Notifications */}
      <button className="p-2 rounded-full text-slate-500 hover:text-slate-900 hover:bg-slate-100 transition-colors relative border border-slate-200 bg-white shadow-sm">
        <Bell className="w-4 h-4" />
      </button>

      {/* Avatar */}
      <div className="w-9 h-9 rounded-[10px] bg-slate-900 flex items-center justify-center text-white text-sm font-semibold cursor-pointer shadow-md border border-slate-800">
        ID
      </div>
    </header>
  );
}
