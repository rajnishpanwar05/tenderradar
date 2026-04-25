export const dynamic = "force-dynamic";
import type { Metadata } from "next";
import { Suspense } from "react";
import { TenderListPage } from "@/components/tenders/TenderListPage";

export const metadata: Metadata = {
  title: "Tenders — ProcureIQ",
  description: "Browse procurement tenders from 25+ portals, ranked by priority score.",
};

function TenderListSkeleton() {
  return (
    <div className="flex flex-col gap-4 p-6">
      <div className="h-8 w-48 animate-pulse rounded-lg bg-muted" />
      <div className="h-14 animate-pulse rounded-xl bg-muted" />
      <div className="overflow-hidden rounded-lg border">
        {Array.from({ length: 12 }).map((_, i) => (
          <div key={i} className="h-14 animate-pulse border-b bg-muted/40 last:border-0" />
        ))}
      </div>
    </div>
  );
}

export default function TendersPage() {
  return (
    <Suspense fallback={<TenderListSkeleton />}>
      <TenderListPage />
    </Suspense>
  );
}
