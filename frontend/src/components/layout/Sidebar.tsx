"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  FileText, BarChart2, Globe,
  LayoutDashboard, Kanban, MessageCircle,
  Building2,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { NAV_ITEMS } from "@/lib/constants";

const ICON_MAP = {
  FileText, BarChart2, Globe, LayoutDashboard, Kanban, MessageCircle,
} as const;
type IconName = keyof typeof ICON_MAP;

export function Sidebar({ collapsed }: { collapsed: boolean }) {
  const pathname = usePathname();

  return (
    <div className="flex flex-col h-full">
      {/* Brand */}
      <div className={cn(
        "flex items-center gap-2.5 h-14 px-4 border-b border-slate-200 flex-shrink-0",
        collapsed && "justify-center px-0"
      )}>
        <div className="flex-shrink-0 w-7 h-7 rounded-md bg-slate-900 flex items-center justify-center">
          <Building2 className="w-4 h-4 text-white" />
        </div>
        {!collapsed && (
          <span className="font-semibold text-sm text-slate-900 tracking-tight">
            ProcureIQ
          </span>
        )}
      </div>

      {/* Nav */}
      <nav className="flex-1 px-2 py-4 space-y-0.5">
        {NAV_ITEMS.map(item => {
          const Icon = ICON_MAP[item.icon as IconName];
          const active = item.href === "/dashboard"
            ? pathname === "/dashboard"
            : pathname.startsWith(item.href);

          return (
            <Link
              key={item.href}
              href={item.href}
              title={collapsed ? item.label : undefined}
              className={cn(
                "flex items-center gap-2.5 px-2.5 py-2 rounded-md text-sm font-medium transition-colors",
                active
                  ? "bg-slate-900 text-white"
                  : "text-slate-600 hover:bg-slate-100 hover:text-slate-900",
                collapsed && "justify-center px-0 w-10 h-10 mx-auto"
              )}
            >
              <Icon className={cn("w-4 h-4 flex-shrink-0", active ? "text-white" : "text-slate-400")} />
              {!collapsed && <span>{item.label}</span>}
            </Link>
          );
        })}
      </nav>

      {/* Footer */}
      <div className={cn(
        "px-3 py-4 border-t border-slate-200",
        collapsed && "px-0 flex flex-col items-center"
      )}>
        {!collapsed ? (
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-full bg-slate-200 flex items-center justify-center text-xs font-semibold text-slate-600">
              ID
            </div>
            <div className="min-w-0">
              <p className="text-xs font-medium text-slate-700 truncate">IDCG Analyst</p>
              <div className="flex items-center gap-1 mt-0.5">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
                <span className="text-[10px] text-slate-400">Active</span>
              </div>
            </div>
          </div>
        ) : (
          <div className="w-7 h-7 rounded-full bg-slate-200 flex items-center justify-center text-xs font-semibold text-slate-600">
            ID
          </div>
        )}
      </div>
    </div>
  );
}
