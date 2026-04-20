"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  Sparkles,
  Loader2,
  AlertTriangle,
  ExternalLink,
  CheckCircle2,
  ListChecks,
} from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { apiClient } from "@/lib/api";
import type { CopilotResponse, TenderRecord } from "@/lib/api-types";
import { cn } from "@/lib/utils";
import { handleTenderClick } from "@/lib/tender-links";

type PanelState = "loading" | "done" | "error";

interface TenderBriefPanelProps {
  tender: TenderRecord;
}

function safeList(val: unknown): string[] {
  if (Array.isArray(val)) {
    return val.map((v) => String(v)).filter(Boolean);
  }
  return [];
}

export function TenderBriefPanel({ tender }: TenderBriefPanelProps) {
  const [state, setState] = useState<PanelState>("loading");
  const [result, setResult] = useState<CopilotResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pipelineMsg, setPipelineMsg] = useState<string | null>(null);

  const loadBrief = useCallback(async () => {
    setState("loading");
    setError(null);
    try {
      const data = await apiClient.client.getCopilotRecommendation(tender.tender_id, "deep");
      setResult(data);
      setState("done");
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to load AI summary.");
      setState("error");
    }
  }, [tender.tender_id]);

  useEffect(() => {
    loadBrief();
  }, [loadBrief]);

  const extraction = (result?.extraction || {}) as Record<string, unknown>;
  const summary = String(extraction.scope_summary || "").trim();
  const deliverables = safeList(extraction.key_deliverables);
  const nextSteps = result?.strategy ?? [];

  const fallbackSummary = useMemo(() => {
    if (summary) return summary;
    const desc = tender.description || tender.deep_scope || "";
    return desc ? desc.slice(0, 900) : "";
  }, [summary, tender.description, tender.deep_scope]);

  async function handleAddToPipeline() {
    setPipelineMsg(null);
    try {
      await apiClient.client.updatePipeline({
        tender_id: tender.tender_id,
        status: "discovered",
      });
      setPipelineMsg("Added to pipeline.");
    } catch (err: unknown) {
      setPipelineMsg(err instanceof Error ? err.message : "Failed to add to pipeline.");
    }
  }

  return (
    <Card>
      <CardContent className="p-5 space-y-4">
        <div className="flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-primary" />
          <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            AI Summary
          </h3>
        </div>

        {state === "loading" && (
          <div className="rounded-lg border border-primary/15 bg-primary/5 p-4 text-center">
            <Loader2 className="mx-auto h-5 w-5 animate-spin text-primary" />
            <p className="mt-2 text-xs text-muted-foreground">
              Generating a bid-ready summary…
            </p>
          </div>
        )}

        {state === "error" && (
          <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-700 dark:border-amber-800 dark:bg-amber-900/20 dark:text-amber-300">
            <div className="flex items-start gap-2">
              <AlertTriangle className="mt-0.5 h-4 w-4" />
              <div>
                <p className="font-semibold">AI brief unavailable</p>
                <p className="mt-1 text-muted-foreground">{error}</p>
                <Button variant="outline" size="sm" className="mt-2" onClick={loadBrief}>
                  Retry
                </Button>
              </div>
            </div>
          </div>
        )}

        {state === "done" && (
          <div className="space-y-3 text-sm">
            <div>
              <p className="font-semibold">Summary</p>
              <p className={cn("mt-1 text-muted-foreground", !fallbackSummary && "italic")}>
                {fallbackSummary || "No summary available yet."}
              </p>
            </div>

            {deliverables.length > 0 && (
              <div>
                <p className="font-semibold">Key Deliverables</p>
                <ul className="mt-1 space-y-1">
                  {deliverables.map((item, i) => (
                    <li key={i} className="flex items-start gap-2 text-muted-foreground">
                      <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 text-emerald-500" />
                      <span>{item}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {nextSteps.length > 0 && (
              <div>
                <p className="font-semibold">Next Steps</p>
                <ul className="mt-1 space-y-1">
                  {nextSteps.map((step, i) => (
                    <li key={i} className="flex items-start gap-2 text-muted-foreground">
                      <ListChecks className="mt-0.5 h-3.5 w-3.5 text-blue-500" />
                      <span>{step}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}

        <div className="space-y-2 pt-1">
          <Button className="w-full" onClick={(e) => handleTenderClick(e, tender)}>
            <ExternalLink className="mr-2 h-4 w-4" />
            Open Official Tender
          </Button>
          <Button variant="outline" className="w-full" onClick={handleAddToPipeline}>
            Add to Pipeline
          </Button>
          {pipelineMsg && (
            <p className="text-xs text-muted-foreground">{pipelineMsg}</p>
          )}
          <Link
            href="/pipeline"
            className="inline-flex w-full items-center justify-center rounded-md border border-border bg-background px-3 py-2 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors"
          >
            Open Project Workspace
          </Link>
        </div>
      </CardContent>
    </Card>
  );
}
