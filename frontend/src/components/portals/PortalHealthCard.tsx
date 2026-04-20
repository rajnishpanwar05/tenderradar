"use client";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { PortalIcon } from "@/components/tenders/PortalIcon";
import { FitScoreBar } from "@/components/tenders/FitScoreBar";
import { PortalFreshnessIndicator } from "./PortalFreshnessIndicator";
import { portalLabel } from "@/lib/constants";
import type { PortalStats } from "@/lib/api-types";

interface PortalHealthCardProps {
  portal: PortalStats;
}

export function PortalHealthCard({ portal }: PortalHealthCardProps) {
  const displayName = portalLabel(portal.portal);

  return (
    <Card>
      <CardContent className="p-5">
        {/* Header */}
        <div className="mb-4 flex items-start justify-between gap-3">
          <div className="flex items-center gap-2.5">
            <PortalIcon portal={portal.portal} className="scale-125" />
            <div>
              <p className="text-sm font-semibold leading-tight">{displayName}</p>
              <p className="mt-0.5 text-xs text-muted-foreground">{portal.portal}</p>
            </div>
          </div>
          <PortalFreshnessIndicator
            lastScrapedAt={portal.last_scraped_at}
            showLabel
          />
        </div>

        {/* Stats grid */}
        <div className="mb-4 grid grid-cols-2 gap-x-4 gap-y-2.5 text-sm">
          <StatItem label="Total" value={portal.total_tenders.toLocaleString()} />
          <StatItem label="New (7d)" value={portal.new_last_7_days.toLocaleString()} />
          <StatItem label="Avg Fit" value={portal.avg_fit_score.toFixed(1)} />
          <StatItem label="HIGH" value={portal.high_fit_count.toLocaleString()} />
        </div>

        {/* Avg fit score bar */}
        <div className="mb-4">
          <FitScoreBar label="Average fit" score={portal.avg_fit_score} />
        </div>

        {/* CTA */}
        <Button
          variant="outline"
          size="sm"
          className="w-full"
          onClick={() =>
            (window.location.href = `/tenders?source_portals=${encodeURIComponent(portal.portal)}`)
          }
        >
          Browse tenders &rarr;
        </Button>
      </CardContent>
    </Card>
  );
}

function StatItem({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="text-sm font-semibold tabular-nums">{value}</p>
    </div>
  );
}
