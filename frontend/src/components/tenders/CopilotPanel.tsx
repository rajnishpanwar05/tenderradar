"use client";
// =============================================================================
// components/tenders/CopilotPanel.tsx
//
// LLM copilot bid-recommendation panel for the tender detail page.
//
// Usage:
//   <CopilotPanel tenderId={tender.tender_id} />
//
// Renders:
//   [Ask AI] button → loading state → structured recommendation card
// =============================================================================

import { useState, useCallback } from "react";
import {
  Sparkles,
  Loader2,
  ThumbsUp,
  ThumbsDown,
  HelpCircle,
  CheckCircle2,
  AlertTriangle,
  Zap,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { apiClient } from "@/lib/api";
import type { CopilotResponse, CopilotVerdict } from "@/lib/api-types";

// ── Colour mapping per verdict ────────────────────────────────────────────────

const VERDICT_CONFIG: Record<
  CopilotVerdict,
  { label: string; icon: typeof ThumbsUp; className: string; barClass: string }
> = {
  BID: {
    label:     "Recommend Bid",
    icon:      ThumbsUp,
    className: "text-emerald-700 bg-emerald-50 border-emerald-200 dark:text-emerald-400 dark:bg-emerald-900/20 dark:border-emerald-700",
    barClass:  "bg-emerald-500",
  },
  CONSIDER: {
    label:     "Worth Considering",
    icon:      HelpCircle,
    className: "text-amber-700 bg-amber-50 border-amber-200 dark:text-amber-400 dark:bg-amber-900/20 dark:border-amber-700",
    barClass:  "bg-amber-500",
  },
  SKIP: {
    label:     "Skip",
    icon:      ThumbsDown,
    className: "text-red-700 bg-red-50 border-red-200 dark:text-red-400 dark:bg-red-900/20 dark:border-red-700",
    barClass:  "bg-red-500",
  },
};

// ── Confidence bar ────────────────────────────────────────────────────────────

function ConfidenceBar({ pct, barClass }: { pct: number; barClass: string }) {
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span>Confidence</span>
        <span className="font-semibold tabular-nums">{pct}%</span>
      </div>
      <div className="h-1.5 w-full overflow-hidden rounded-full bg-muted">
        <div
          className={cn("h-full rounded-full transition-all duration-700", barClass)}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

// ── Expandable list section ───────────────────────────────────────────────────

function Section({
  title,
  items,
  icon: Icon,
  iconClass,
  defaultExpanded = true,
}: {
  title: string;
  items: string[];
  icon: typeof CheckCircle2;
  iconClass: string;
  defaultExpanded?: boolean;
}) {
  const [open, setOpen] = useState(defaultExpanded);

  if (!items.length) return null;

  return (
    <div className="space-y-1.5">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className="flex w-full items-center justify-between text-xs font-semibold uppercase tracking-wide text-muted-foreground hover:text-foreground transition-colors"
      >
        {title}
        {open
          ? <ChevronUp className="h-3.5 w-3.5" />
          : <ChevronDown className="h-3.5 w-3.5" />}
      </button>

      {open && (
        <ul className="space-y-1.5">
          {items.map((item, i) => (
            <li key={i} className="flex items-start gap-2 text-sm leading-snug">
              <Icon className={cn("mt-0.5 h-3.5 w-3.5 flex-shrink-0", iconClass)} />
              <span>{item}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

interface CopilotPanelProps {
  tenderId: string;
}

type PanelState = "idle" | "loading" | "done" | "error";

export function CopilotPanel({ tenderId }: CopilotPanelProps) {
  const [state,  setState]  = useState<PanelState>("idle");
  const [result, setResult] = useState<CopilotResponse | null>(null);
  const [error,  setError]  = useState<string | null>(null);

  const handleAskAI = useCallback(async () => {
    if (state === "loading") return;
    setState("loading");
    setError(null);

    try {
      const data = await apiClient.client.getCopilotRecommendation(tenderId);
      setResult(data);
      setState("done");
    } catch (err: unknown) {
      const msg =
        err instanceof Error ? err.message : "Failed to get recommendation. Please try again.";
      setError(msg);
      setState("error");
    }
  }, [tenderId, state]);

  // ── Idle — show button only ───────────────────────────────────────────────
  if (state === "idle") {
    return (
      <div className="rounded-xl border border-dashed border-primary/30 bg-primary/3 p-4 text-center">
        <Sparkles className="mx-auto mb-2 h-6 w-6 text-primary/70" />
        <p className="mb-3 text-xs text-muted-foreground leading-relaxed">
          Get an AI-powered bid recommendation with strategic reasoning
        </p>
        <button
          type="button"
          onClick={handleAskAI}
          className={cn(
            "inline-flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-semibold",
            "bg-primary text-primary-foreground hover:bg-primary/90 transition-colors",
            "shadow-sm",
          )}
        >
          <Sparkles className="h-4 w-4" />
          Ask AI
        </button>
      </div>
    );
  }

  // ── Loading ───────────────────────────────────────────────────────────────
  if (state === "loading") {
    return (
      <div className="rounded-xl border border-primary/20 bg-primary/3 p-5 text-center space-y-3">
        <Loader2 className="mx-auto h-6 w-6 animate-spin text-primary" />
        <p className="text-xs text-muted-foreground">
          Analysing tender…
        </p>
        <div className="space-y-1.5">
          {["Reviewing fit", "Assessing risks", "Drafting strategy"].map((step, i) => (
            <div
              key={step}
              className="h-2 rounded-full bg-muted animate-pulse"
              style={{ animationDelay: `${i * 150}ms`, width: `${70 + i * 10}%`, margin: "0 auto" }}
            />
          ))}
        </div>
      </div>
    );
  }

  // ── Error ─────────────────────────────────────────────────────────────────
  if (state === "error") {
    return (
      <div className="rounded-xl border border-red-200 bg-red-50 p-4 dark:border-red-800 dark:bg-red-900/20">
        <p className="text-sm text-red-700 dark:text-red-400 mb-3">{error}</p>
        <button
          type="button"
          onClick={() => setState("idle")}
          className="text-xs font-semibold text-red-700 dark:text-red-400 hover:underline"
        >
          Try again
        </button>
      </div>
    );
  }

  // ── Result ────────────────────────────────────────────────────────────────
  if (!result) return null;

  const verdict = result.recommendation as CopilotVerdict;
  const cfg     = VERDICT_CONFIG[verdict] ?? VERDICT_CONFIG.CONSIDER;
  const Icon    = cfg.icon;

  return (
    <div className="space-y-4">
      {/* Verdict badge + confidence */}
      <div className={cn("rounded-xl border p-4 space-y-3", cfg.className)}>
        <div className="flex items-center gap-2.5">
          <Icon className="h-5 w-5 flex-shrink-0" />
          <span className="text-base font-bold">{cfg.label}</span>

          {/* Fallback / cached badges */}
          <div className="ml-auto flex items-center gap-1">
            {result.cached && (
              <span className="rounded-full border px-1.5 py-0.5 text-[10px] font-medium opacity-60">
                cached
              </span>
            )}
            {result.fallback && (
              <span className="rounded-full border px-1.5 py-0.5 text-[10px] font-medium opacity-60"
                title="OpenAI unavailable — heuristic result">
                heuristic
              </span>
            )}
          </div>
        </div>

        <ConfidenceBar pct={result.confidence} barClass={cfg.barClass} />
      </div>

      {/* Why */}
      <Section
        title="Why"
        items={result.why}
        icon={CheckCircle2}
        iconClass="text-emerald-500"
        defaultExpanded
      />

      {/* Risks */}
      <Section
        title="Risks"
        items={result.risks}
        icon={AlertTriangle}
        iconClass="text-orange-500"
        defaultExpanded
      />

      {/* Strategy */}
      <Section
        title="Strategy"
        items={result.strategy}
        icon={Zap}
        iconClass="text-primary"
        defaultExpanded={false}
      />

      {/* Re-run */}
      <button
        type="button"
        onClick={() => { setResult(null); setState("idle"); }}
        className="text-xs text-muted-foreground hover:text-foreground underline-offset-2 hover:underline transition-colors"
      >
        Re-run analysis
      </button>
    </div>
  );
}
