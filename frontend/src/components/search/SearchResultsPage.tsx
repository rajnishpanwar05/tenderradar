"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useState, useEffect, useCallback } from "react";
import {
  Search, AlertCircle, ArrowLeft,
  Globe, Layers, Clock, TrendingUp, ChevronRight, Activity, Command
} from "lucide-react";
import { cn } from "@/lib/utils";
import { sectorLabel, portalLabel } from "@/lib/constants";
import { useSemanticSearch } from "@/hooks/useSemanticSearch";
import { EmptyState } from "@/components/tenders/EmptyState";
import type { SemanticSearchResult } from "@/lib/api-types";

// ── Small helper chips ────────────────────────────────────────────────────────

function FilterChip({
  icon: Icon,
  label,
  className,
}: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  className?: string;
}) {
  return (
    <span className={cn(
      "inline-flex items-center gap-1.5 rounded-md border px-2 py-0.5 text-[11px] font-medium tracking-wide shadow-sm",
      className,
    )}>
      <Icon className="h-3 w-3" />
      {label}
    </span>
  );
}

// ── Main page component ───────────────────────────────────────────────────────

export function SearchResultsPage() {
  const searchParams = useSearchParams();
  const router       = useRouter();
  const initialQ     = searchParams.get("q") ?? "";

  const [inputValue, setInputValue] = useState(initialQ);
  const [activeQuery, setActiveQuery] = useState(initialQ);

  useEffect(() => {
    const q = searchParams.get("q") ?? "";
    setInputValue(q);
    setActiveQuery(q);
  }, [searchParams]);

  const { data, isLoading, isValidating } = useSemanticSearch(activeQuery);

  const results: SemanticSearchResult[] = data?.results ?? [];
  const sorted = [...results].sort((a, b) => b.composite_score - a.composite_score);

  // Search is now handled entirely by the global CommandPalette

  const handleTenderClick = useCallback(
    (id: string) => router.push(`/tenders/${encodeURIComponent(id)}`),
    [router],
  );

  const filters = data?.filters_extracted;
  const hasFilters = (filters?.sectors?.length ?? 0) > 0 ||
                     (filters?.regions?.length  ?? 0) > 0 ||
                     filters?.priority_hint || filters?.closing_soon;

  return (
    <div className="flex flex-col gap-8 p-6 mx-auto max-w-4xl w-full">

      {/* ── Back + Header ─────────────────────────────────────────────────── */}
      <div className="flex items-center gap-4">
        <button
          onClick={() => router.back()}
          className="flex items-center justify-center h-8 w-8 shrink-0 rounded-md border border-[#222] bg-[#0a0a0a] text-neutral-400 hover:bg-[#1a1a1a] hover:text-white transition-colors"
          title="Go back"
        >
          <ArrowLeft className="h-4 w-4" />
        </button>
        <div className="flex-1">
          <h1 className="text-xl font-medium tracking-tight text-white flex items-center gap-2">
            Opportunity Engine
          </h1>
        </div>
      </div>

      {/* ── Query shown + auto-detected filter chips ──────────────────────── */}
      {activeQuery && (
        <div className="flex flex-col gap-4 border-b border-[#222] pb-6">
          <div className="flex items-center gap-2 text-sm text-neutral-400 font-medium">
            {isLoading || isValidating ? (
              <span className="flex items-center gap-2">
                <Activity className="h-4 w-4 animate-spin text-neutral-500" /> Querying index...
              </span>
            ) : data ? (
              <span>
                {data.total} results <span className="text-neutral-600 px-1">/</span> <span className="text-white">"{data.query}"</span>
                <span className="text-neutral-600 ml-3 text-xs">{data.query_ms}ms</span>
              </span>
            ) : null}
          </div>

          {hasFilters && (
            <div className="flex flex-wrap items-center gap-2">
              {filters?.sectors?.map(s => (
                <FilterChip key={s} icon={Layers} label={sectorLabel(s)} className="bg-[#111] text-neutral-300 border-[#333]" />
              ))}
              {filters?.regions?.map(r => (
                <FilterChip key={r} icon={Globe} label={r} className="bg-[#111] text-neutral-300 border-[#333]" />
              ))}
              {filters?.priority_hint === "high" && (
                <FilterChip icon={TrendingUp} label="High Priority" className="bg-[#220] text-amber-500 border-[#440]" />
              )}
              {filters?.closing_soon && (
                <FilterChip icon={Clock} label="Closing Soon" className="bg-[#220] text-amber-500 border-[#440]" />
              )}
            </div>
          )}
        </div>
      )}

      {/* ── Fallback notice ───────────────────────────────────────────────── */}
      {data?.fallback && (
        <div className="flex items-start gap-3 rounded-md border border-[#440] bg-[#110] p-3">
          <AlertCircle className="h-4 w-4 text-amber-500 mt-0.5 flex-shrink-0" />
          <p className="text-xs text-amber-500/80 leading-relaxed">
             Keyword fallback activated. The semantic search index is being rebuilt in the background.
          </p>
        </div>
      )}

      {/* ── Results ───────────────────────────────────────────────────────── */}
      {!activeQuery ? (
        <SearchLanding />
      ) : isLoading ? (
        <SearchSkeleton />
      ) : sorted.length === 0 ? (
        <EmptyState onReset={() => { setInputValue(""); setActiveQuery(""); router.push("/search"); }} />
      ) : (
        <div className="grid gap-3 w-full">
          {sorted.map((t, idx) => {
            const matchScore = Math.round(t.composite_score * 10) / 10;
            const percentage = Math.round(t.similarity * 100);
            return (
              <button
                key={t.tender_id + idx}
                onClick={() => handleTenderClick(t.tender_id)}
                className="group flex flex-col items-start gap-2 rounded-lg border border-[#222] bg-[#050505] p-4 text-left transition-all hover:bg-[#111] hover:border-[#333]"
              >
                <div className="flex w-full items-start justify-between gap-4">
                  <div className="space-y-1.5 flex-1">
                    <h3 className="text-sm font-medium text-white leading-relaxed group-hover:text-blue-400 transition-colors">
                      {t.title}
                    </h3>
                  </div>
                  
                  {/* Score */}
                  <div className="flex items-center gap-2 rounded-md bg-[#111] border border-[#222] px-2 py-1 shrink-0">
                    <span className="text-xs font-semibold text-white">{matchScore}</span>
                    <span className="text-[10px] text-neutral-500 uppercase tracking-widest font-medium">Rank</span>
                  </div>
                </div>

                <div className="flex items-center gap-3 mt-1 w-full text-xs text-neutral-500">
                  <span className="truncate max-w-[50%]">{t.organization || "No Organization Provided"}</span>
                  <span className="w-1 h-1 rounded-full bg-[#333]"></span>
                  <span className="uppercase tracking-wider font-semibold text-neutral-400">
                    {portalLabel(t.source_site)}
                  </span>
                  <span className="w-1 h-1 rounded-full bg-[#333]"></span>
                  <span className="text-blue-400/80">{percentage}% Semantic Match</span>
                </div>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ── Landing state (no query yet) ──────────────────────────────────────────────

function SearchLanding() {
  return (
    <div className="flex flex-col items-center justify-center gap-6 py-20 mt-4 max-w-xl mx-auto text-center">
      <div className="flex h-16 w-16 items-center justify-center rounded-2xl border border-slate-200 bg-slate-50 shadow-sm">
        <Command className="h-6 w-6 text-slate-400" />
      </div>
      <div>
        <h2 className="text-xl font-bold tracking-tight text-slate-900 mb-2">Natural Language Engine</h2>
        <p className="text-sm text-slate-500 leading-relaxed max-w-md">
          Press <kbd className="px-1.5 py-0.5 rounded border border-slate-200 bg-white shadow-sm font-mono text-xs">Cmd+K</kbd> anywhere to search the semantic index. Try asking for specific constraints like "digital health in Africa closing soon".
        </p>
      </div>
      <button 
        onClick={() => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'k', metaKey: true }))} 
        className="px-6 py-2.5 bg-slate-900 text-white rounded-full font-medium text-sm hover:scale-105 transition-transform shadow-md mt-4"
      >
        Open Command Palette
      </button>
    </div>
  );
}

// ── Skeleton loader ───────────────────────────────────────────────────────────

function SearchSkeleton() {
  return (
    <div className="space-y-3 w-full">
      {[...Array(6)].map((_, i) => (
        <div
          key={i}
          className="h-24 animate-pulse rounded-lg bg-[#111] border border-[#222]"
          style={{ animationDelay: `${i * 60}ms` }}
        />
      ))}
    </div>
  );
}
