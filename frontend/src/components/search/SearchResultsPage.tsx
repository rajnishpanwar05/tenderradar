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
          className="flex items-center justify-center h-8 w-8 shrink-0 rounded-md border border-slate-200 bg-white text-slate-500 hover:bg-slate-50 hover:text-slate-900 transition-colors"
          title="Go back"
        >
          <ArrowLeft className="h-4 w-4" />
        </button>
        <div className="flex-1">
          <h1 className="text-xl font-semibold tracking-tight text-slate-950 flex items-center gap-2">
            Opportunity Engine
          </h1>
        </div>
      </div>

      {/* ── Query shown + auto-detected filter chips ──────────────────────── */}
      {activeQuery && (
        <div className="flex flex-col gap-4 border-b border-slate-200 pb-6">
          <div className="flex items-center gap-2 text-sm text-slate-500 font-medium">
            {isLoading || isValidating ? (
              <span className="flex items-center gap-2">
                <Activity className="h-4 w-4 animate-spin text-slate-500" /> Querying index...
              </span>
            ) : data ? (
              <span>
                {data.total} results <span className="text-slate-300 px-1">/</span> <span className="text-slate-900">"{data.query}"</span>
                <span className="text-slate-400 ml-3 text-xs">{data.query_ms}ms</span>
              </span>
            ) : null}
          </div>

          {hasFilters && (
            <div className="flex flex-wrap items-center gap-2">
              {filters?.sectors?.map(s => (
                <FilterChip key={s} icon={Layers} label={sectorLabel(s)} className="bg-white text-slate-700 border-slate-200" />
              ))}
              {filters?.regions?.map(r => (
                <FilterChip key={r} icon={Globe} label={r} className="bg-white text-slate-700 border-slate-200" />
              ))}
              {filters?.priority_hint === "high" && (
                <FilterChip icon={TrendingUp} label="High Priority" className="bg-white text-slate-700 border-slate-200" />
              )}
              {filters?.closing_soon && (
                <FilterChip icon={Clock} label="Closing Soon" className="bg-white text-slate-700 border-slate-200" />
              )}
            </div>
          )}
        </div>
      )}

      {/* ── Fallback notice ───────────────────────────────────────────────── */}
      {data?.fallback && (
        <div className="flex items-start gap-3 rounded-md border border-amber-200 bg-amber-50 p-3">
          <AlertCircle className="h-4 w-4 text-slate-600 mt-0.5 flex-shrink-0" />
          <p className="text-xs text-slate-600 leading-relaxed">
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
                className="group flex flex-col items-start gap-2 rounded-xl border border-slate-200 bg-white p-4 text-left transition-all hover:bg-slate-50 hover:border-slate-300 shadow-sm"
              >
                <div className="flex w-full items-start justify-between gap-4">
                  <div className="space-y-1.5 flex-1">
                    <h3 className="text-sm font-medium text-slate-900 leading-relaxed group-hover:text-slate-700 transition-colors">
                      {t.title}
                    </h3>
                  </div>
                  
                  {/* Score */}
                  <div className="flex items-center gap-2 rounded-md bg-slate-100 border border-slate-200 px-2 py-1 shrink-0">
                    <span className="text-xs font-semibold text-slate-900">{matchScore}</span>
                    <span className="text-[10px] text-slate-500 uppercase tracking-widest font-medium">Rank</span>
                  </div>
                </div>

                <div className="flex items-center gap-3 mt-1 w-full text-xs text-slate-500">
                  <span className="truncate max-w-[50%]">{t.organization || "No Organization Provided"}</span>
                  <span className="w-1 h-1 rounded-full bg-slate-300"></span>
                  <span className="uppercase tracking-wider font-semibold text-neutral-400">
                    {portalLabel(t.source_site)}
                  </span>
                  <span className="w-1 h-1 rounded-full bg-slate-300"></span>
                  <span className="text-slate-700">{percentage}% Semantic Match</span>
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
