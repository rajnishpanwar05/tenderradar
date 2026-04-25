"use client";

import { useState, useEffect } from "react";
import { Sidebar } from "@/components/layout/Sidebar";
import { TopBar } from "@/components/layout/TopBar";
import { MobileNav } from "@/components/layout/MobileNav";
import { CommandPalette } from "@/components/ui/CommandPalette";

const COLLAPSED_KEY = "sidebar_collapsed";

export function AppShell({ children }: { children: React.ReactNode }) {
  const [collapsed, setCollapsed] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);

  useEffect(() => {
    const stored = localStorage.getItem(COLLAPSED_KEY);
    if (stored !== null) setCollapsed(stored === "true");
  }, []);

  const toggleSidebar = () => {
    setCollapsed(prev => {
      const next = !prev;
      localStorage.setItem(COLLAPSED_KEY, String(next));
      return next;
    });
  };

  return (
    <div className="flex h-screen overflow-hidden bg-[#f8fafc] text-slate-900">
      {/* Desktop Sidebar — fixed 240px or 64px collapsed */}
      <aside
        className={
          "hidden md:flex flex-col flex-shrink-0 bg-white border-r border-slate-200 transition-all duration-200 " +
          (collapsed ? "w-16" : "w-60")
        }
      >
        <Sidebar collapsed={collapsed} />
      </aside>

      <MobileNav open={mobileOpen} onClose={() => setMobileOpen(false)} />

      {/* Main column */}
      <div className="flex flex-col flex-1 min-w-0 overflow-hidden">
        <TopBar
          onToggleSidebar={toggleSidebar}
          onOpenMobileNav={() => setMobileOpen(true)}
          sidebarCollapsed={collapsed}
        />
        <CommandPalette />
        <main className="flex-1 overflow-y-auto scrollbar-thin bg-[#f8fafc]">
          {children}
        </main>
      </div>
    </div>
  );
}
