import type { Metadata } from "next";
import { Suspense } from "react";
import { apiClient } from "@/lib/api";
import { PortalHealthCard } from "@/components/portals/PortalHealthCard";
import { Skeleton } from "@/components/ui/skeleton";

export const metadata: Metadata = {
  title: "Portals",
  description: "Monitor health and coverage of all integrated procurement portals.",
};

async function PortalsContent() {
  const portals = await apiClient.server.getPortals();

  if (portals.length === 0) {
    return (
      <p className="text-sm text-muted-foreground py-10 text-center">
        No portal data yet. Run the scraper pipeline first.
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
        <Skeleton key={i} className="h-52 w-full rounded-xl" />
      ))}
    </div>
  );
}

export default function PortalsPage() {
  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold">Portals</h1>
        <p className="text-sm text-muted-foreground">
          Health and coverage metrics for all integrated procurement portals
        </p>
      </div>
      <Suspense fallback={<PortalsSkeleton />}>
        <PortalsContent />
      </Suspense>
    </div>
  );
}
