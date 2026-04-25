export const dynamic = "force-dynamic";
import type { Metadata } from "next";
import { Suspense } from "react";
import { apiClient } from "@/lib/api";
import { PortalHealthCard } from "@/components/portals/PortalHealthCard";
import { Skeleton } from "@/components/ui/skeleton";

export const metadata: Metadata = {
  title: "Portals — ProcureIQ",
  description: "Monitor health and coverage of all integrated procurement portals.",
};

async function PortalsContent() {
  const portals = await apiClient.server.getPortals();

  if (portals.length === 0) {
    return (
      <p className="text-sm text-slate-500 py-10 text-center">
        No portal data available. Run the scraper pipeline first.
      </p>
    );
  }

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
      {portals.map(portal => (
        <PortalHealthCard key={portal.portal} portal={portal} />
      ))}
    </div>
  );
}

function PortalsSkeleton() {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
      {Array.from({ length: 8 }).map((_, i) => (
        <Skeleton key={i} className="h-48 w-full rounded-lg bg-slate-100" />
      ))}
    </div>
  );
}

export default function PortalsPage() {
  return (
    <div className="p-6 lg:p-8 max-w-7xl mx-auto space-y-4">
      <div>
        <h1 className="text-base font-semibold text-slate-900">Portals</h1>
        <p className="text-sm text-slate-500 mt-0.5">
          Health and coverage metrics for all integrated procurement portals
        </p>
      </div>
      <Suspense fallback={<PortalsSkeleton />}>
        <PortalsContent />
      </Suspense>
    </div>
  );
}
