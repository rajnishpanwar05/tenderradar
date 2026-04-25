"use client";

import { usePathname } from "next/navigation";
import { Menu, PanelLeftClose, PanelLeftOpen } from "lucide-react";
import { cn } from "@/lib/utils";

const PAGE_TITLES: Record<string, string> = {
  "/dashboard": "Dashboard",
  "/tenders":   "Tenders",
  "/analytics": "Analytics",
  "/portals":   "Portals",
  "/pipeline":  "Pipeline",
  "/chat":      "AI Analyst",
};

function getPageTitle(pathname: string): string {
  for (const [key, label] of Object.entries(PAGE_TITLES)) {
    if (pathname === key || pathname.startsWith(key + "/")) return label;
  }
  return "ProcureIQ";
}

interface TopBarProps {
  sidebarCollapsed: boolean;
  onToggleSidebar:  () => void;
  onOpenMobileNav:  () => void;
}

export function TopBar({ sidebarCollapsed, onToggleSidebar, onOpenMobileNav }: TopBarProps) {
  const pathname = usePathname();
  const title = getPageTitle(pathname);

  return (
    <header className="h-14 flex-shrink-0 flex items-center gap-3 px-4 bg-white border-b border-slate-200 z-20">
      {/* Mobile hamburger */}
      <button
        onClick={onOpenMobileNav}
        className="md:hidden p-1.5 rounded-md text-slate-500 hover:text-slate-900 hover:bg-slate-100 transition-colors"
        aria-label="Open navigation"
      >
        <Menu className="w-5 h-5" />
      </button>

      {/* Sidebar toggle */}
      <button
        onClick={onToggleSidebar}
        className="hidden md:flex p-1.5 rounded-md text-slate-500 hover:text-slate-900 hover:bg-slate-100 transition-colors"
        aria-label="Toggle sidebar"
      >
        {sidebarCollapsed
          ? <PanelLeftOpen  className="w-4 h-4" />
          : <PanelLeftClose className="w-4 h-4" />}
      </button>

      {/* Page title */}
      <span className="text-sm font-semibold text-slate-900">{title}</span>

      <div className="flex-1" />

      {/* System Active badge */}
      <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-md border border-emerald-200 bg-emerald-50">
        <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
        <span className="text-xs font-medium text-emerald-700">System Active</span>
      </div>
    </header>
  );
}
