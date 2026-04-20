"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  FileText, BarChart2, Globe, Zap,
  LayoutDashboard, Kanban, Search as SearchIcon, MessageCircle,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { NAV_ITEMS } from "@/lib/constants";

const ICON_MAP = {
  FileText, BarChart2, Globe, LayoutDashboard, Kanban, SearchIcon, MessageCircle,
} as const;
type IconName = keyof typeof ICON_MAP;

export function Sidebar({ collapsed }: { collapsed: boolean }) {
  const pathname = usePathname();

  return (
    <div className="flex flex-col h-full bg-transparent relative z-20">

      {/* Brand */}
      <div className={cn(
        "flex items-center gap-3 px-4 py-6",
        "border-b border-slate-200",
        collapsed && "justify-center px-0"
      )}>
        <div className="flex-shrink-0 w-8 h-8 rounded-xl bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center shadow-lg shadow-indigo-500/30">
          <Zap className="w-4 h-4 text-white" />
        </div>
        {!collapsed && (
          <div>
            <span className="font-black text-sm tracking-wide text-slate-800 block leading-none">
              TenderRadar
            </span>
            <span className="text-[10px] text-indigo-500 font-bold tracking-widest uppercase">
              Intelligence
            </span>
          </div>
        )}
      </div>

      {/* Nav */}
      <nav className="flex-1 px-3 py-6 space-y-1">
        {NAV_ITEMS.map(item => {
          const Icon = ICON_MAP[item.icon as IconName];
          const active = item.href === "/dashboard" ? pathname === "/dashboard" : pathname.startsWith(item.href);

          return (
            <Link
              key={item.href}
              href={item.href}
              title={collapsed ? item.label : undefined}
              className={cn(
                "flex items-center gap-3 px-3 py-3 rounded-xl text-sm font-bold",
                "transition-all duration-200",
                active
                  ? "bg-indigo-50 text-indigo-700 shadow-sm border border-indigo-100"
                  : "text-slate-500 hover:bg-slate-50 hover:text-slate-800 border border-transparent",
                collapsed && "justify-center px-0 w-11 h-11 mx-auto"
              )}
            >
              <Icon className={cn("w-5 h-5 flex-shrink-0", active ? "text-indigo-600" : "text-slate-400")} />
              {!collapsed && <span>{item.label}</span>}
            </Link>
          );
        })}
      </nav>

      {/* Global Search Hint */}
      {!collapsed && (
        <div className="px-4 mb-4">
          <button 
            onClick={() => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'k', metaKey: true }))} 
            className="w-full flex items-center justify-between px-3 py-2.5 bg-white border border-slate-200 rounded-xl hover:bg-slate-50 transition-colors shadow-sm"
          >
            <div className="flex items-center gap-2 text-slate-500 text-sm font-semibold tracking-wide">
               <SearchIcon className="w-4 h-4 text-slate-400" /> Search
            </div>
            <kbd className="text-[10px] bg-slate-50 border border-slate-200 px-1.5 py-0.5 rounded font-mono font-bold text-slate-400">⌘K</kbd>
          </button>
        </div>
      )}

      {/* Live indicator + footer */}
      <div className={cn(
        "px-4 py-6 border-t border-slate-200 space-y-3 mt-auto",
        collapsed && "px-2 flex flex-col items-center"
      )}>
        {!collapsed && (
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
            <span className="text-[11px] text-slate-500 font-bold tracking-widest uppercase">LIVE · 6h scan</span>
          </div>
        )}
        {!collapsed && (
          <p className="text-[10px] text-slate-400 font-bold uppercase tracking-widest">v2.0 · IDCG</p>
        )}
      </div>
    </div>
  );
}
