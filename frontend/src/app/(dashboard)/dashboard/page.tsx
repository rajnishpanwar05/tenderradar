import { Suspense } from "react";
import { apiClient } from "@/lib/api";
import { DashboardPage } from "@/components/dashboard/DashboardPage";

export const metadata = { title: "Dashboard — TenderRadar" };

export default async function RootPage() {
  // Fetch summary server-side for immediate render (no flash)
  const summary = await apiClient.server.getSummary().catch(() => undefined);

  return (
    <Suspense
      fallback={
        <div className="flex flex-col gap-6 p-6">
          <div className="h-8 w-48 animate-pulse rounded-lg bg-muted" />
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            {[0, 1, 2, 3].map((i) => (
              <div key={i} className="h-28 animate-pulse rounded-xl bg-muted" />
            ))}
          </div>
        </div>
      }
    >
      <DashboardPage fallback={summary} />
    </Suspense>
  );
}
