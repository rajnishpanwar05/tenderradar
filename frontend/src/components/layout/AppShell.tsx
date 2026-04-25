"use client";

import { useState, useEffect } from "react";
import { Sidebar } from "@/components/layout/Sidebar";
import { TopBar } from "@/components/layout/TopBar";
import { MobileNav } from "@/components/layout/MobileNav";
import { CommandPalette } from "@/components/ui/CommandPalette";
import { cn } from "@/lib/utils";

const COLLAPSED_KEY = "sidebar_collapsed";

export function AppShell({ children }: { children: React.ReactNode }) {
  const [collapsed, setCollapsed] = useState(true);
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
    <div className="flex h-screen overflow-hidden bg-transparent text-slate-900 relative selection:bg-slate-900/10 selection:text-slate-950">
      <div className="flex w-full h-full relative z-10">
        <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(to_right,rgba(148,163,184,0.08)_1px,transparent_1px),linear-gradient(to_bottom,rgba(148,163,184,0.08)_1px,transparent_1px)] bg-[size:4rem_4rem] [mask-image:linear-gradient(to_bottom,black_18%,transparent_95%)] opacity-40" />

        {/* Desktop Sidebar */}
        <aside className={cn(
          "hidden md:flex flex-col flex-shrink-0 glass-sidebar shadow-[4px_0_24px_rgba(15,23,42,0.04)]",
          "transition-all duration-300 ease-in-out",
          collapsed ? "w-16" : "w-56"
        )}>
          <Sidebar collapsed={collapsed} />
        </aside>

        <MobileNav open={mobileOpen} onClose={() => setMobileOpen(false)} />

        {/* Main Content Area */}
        <div className="flex flex-col flex-1 min-w-0 overflow-hidden relative">
          <TopBar
            onToggleSidebar={toggleSidebar}
            onOpenMobileNav={() => setMobileOpen(true)}
            sidebarCollapsed={collapsed}
          />
          <CommandPalette />
          <main className="flex-1 overflow-y-auto scrollbar-thin relative z-10 p-4 md:p-8 lg:p-10">
            {children}
          </main>
        </div>
      </div>
    </div>
  );
}
