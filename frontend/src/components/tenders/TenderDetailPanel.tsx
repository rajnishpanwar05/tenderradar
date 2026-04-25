"use client";
import { useRouter } from "next/navigation";
import {
  ArrowLeft,
  CheckCircle2,
  AlertTriangle,
  ExternalLink,
  Copy,
  AlertCircle,
  FileText,
  Scale,
  Users,
  Calendar,
  BadgeCheck,
  TrendingUp,
} from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { formatDate, formatBudget, timeAgo } from "@/lib/format";
import { serviceTypeLabel } from "@/lib/constants";
import type { TenderRecord } from "@/lib/api-types";
import { FitBucketBadge } from "./FitBucketBadge";
import { FitScoreBar } from "./FitScoreBar";
import { SectorBadge } from "./SectorBadge";
import { PortalIcon } from "./PortalIcon";
import { CopilotPanel } from "./CopilotPanel";
import { TenderBriefPanel } from "./TenderBriefPanel";
import { handleTenderClick } from "@/lib/tender-links";

interface TenderDetailPanelProps {
  tender: TenderRecord;
}

export function TenderDetailPanel({ tender }: TenderDetailPanelProps) {
  const router = useRouter();

  function handleCopyLink() {
    navigator.clipboard.writeText(window.location.href).catch(() => {});
  }

  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
      {/* ── Left: main content ── */}
      <div className="space-y-6 lg:col-span-2">
        {/* Back */}
        <button
          type="button"
          onClick={() => router.back()}
          className="inline-flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-900"
        >
          <ArrowLeft className="h-4 w-4" />
          Back
        </button>

        {/* Title + org + country */}
        <div>
          <h1 className="mb-2 text-2xl font-semibold leading-snug text-slate-950">
            {tender.title_clean || tender.title}
          </h1>
          <p className="text-sm text-slate-500">
            {[tender.organization, tender.country].filter(Boolean).join(" · ")}
          </p>
        </div>

        {/* Fit bucket — prominent */}
        <div className="flex items-center gap-3">
          <FitBucketBadge
            bucket={tender.fit_bucket}
            score={tender.fit_score}
            showScore
            className="px-4 py-1.5 text-sm"
          />
          <span className="text-sm text-slate-500">
            Overall fit score
          </span>
        </div>

        {/* Description */}
        {tender.has_description && tender.description && (
          <section>
            <h2 className="mb-2 text-base font-semibold">Description</h2>
            <div className="prose prose-sm max-w-none text-sm leading-relaxed text-slate-600">
              {tender.description}
            </div>
          </section>
        )}

        {/* Why this fits */}
        {tender.top_reasons && tender.top_reasons.length > 0 && (
          <section>
            <h2 className="mb-3 text-base font-semibold">Why this fits</h2>
            <ul className="space-y-2">
              {tender.top_reasons.map((reason, i) => (
                <li key={i} className="flex items-start gap-2.5 text-sm">
                  <CheckCircle2
                    className="mt-0.5 h-4 w-4 flex-shrink-0 text-slate-700"
                    aria-hidden="true"
                  />
                  <span>{reason}</span>
                </li>
              ))}
            </ul>
          </section>
        )}

        {/* Red flags */}
        {tender.red_flags && tender.red_flags.length > 0 && (
          <section>
            <h2 className="mb-3 text-base font-semibold text-slate-900">
              Red Flags
            </h2>
            <ul className="space-y-2">
              {tender.red_flags.map((flag, i) => (
                <li key={i} className="flex items-start gap-2.5 text-sm">
                  <AlertTriangle
                    className="mt-0.5 h-4 w-4 flex-shrink-0 text-slate-500"
                    aria-hidden="true"
                  />
                  <span>{flag}</span>
                </li>
              ))}
            </ul>
          </section>
        )}

        {/* Fit explanation */}
        {tender.fit_explanation && (
          <section>
            <h2 className="mb-2 text-base font-semibold">Fit Assessment</h2>
            <p className="italic text-sm text-slate-500">
              {tender.fit_explanation}
            </p>
          </section>
        )}

        {/* ── Deep Enrichment — Bid-Critical Fields ── */}
        {(tender.deep_scope || tender.deep_eligibility_raw || tender.deep_team_reqs ||
          tender.deep_eval_technical_weight || tender.deep_contract_duration ||
          tender.deep_min_similar_projects || tender.deep_eligibility_raw) && (
          <section>
            <div className="flex items-center gap-2 mb-4">
              <FileText className="h-4 w-4 text-slate-700" />
              <h2 className="text-base font-semibold">Bid Intelligence</h2>
              <span className="text-[10px] font-mono px-2 py-0.5 rounded-full bg-slate-100 text-slate-700 border border-slate-200">
                AI Extracted
              </span>
            </div>
            <div className="grid grid-cols-1 gap-3">

              {/* Scope */}
              {tender.deep_scope && (
                <DeepRow icon={FileText} label="Scope of Work">
                  {tender.deep_scope}
                </DeepRow>
              )}

              {/* Evaluation Weights */}
              {(tender.deep_eval_technical_weight != null || tender.deep_eval_financial_weight != null) && (
                <DeepRow icon={Scale} label="Evaluation Split">
                  <div className="flex items-center gap-3">
                    {tender.deep_eval_technical_weight != null && (
                      <div className="flex items-center gap-1.5">
                        <div className="h-2 rounded-full bg-slate-900" style={{ width: `${tender.deep_eval_technical_weight}px`, maxWidth: "80px", minWidth: "16px" }} />
                        <span className="font-semibold">{tender.deep_eval_technical_weight}%</span>
                        <span className="text-slate-500">Technical</span>
                      </div>
                    )}
                    {tender.deep_eval_financial_weight != null && (
                      <div className="flex items-center gap-1.5">
                        <div className="h-2 rounded-full bg-slate-600" style={{ width: `${tender.deep_eval_financial_weight}px`, maxWidth: "80px", minWidth: "16px" }} />
                        <span className="font-semibold">{tender.deep_eval_financial_weight}%</span>
                        <span className="text-slate-500">Financial</span>
                      </div>
                    )}
                  </div>
                </DeepRow>
              )}

              {/* Contract Duration */}
              {tender.deep_contract_duration && (
                <DeepRow icon={Calendar} label="Contract Duration">
                  {tender.deep_contract_duration}
                </DeepRow>
              )}

              {/* Eligibility */}
              {tender.deep_eligibility_raw && (
                <DeepRow icon={BadgeCheck} label="Eligibility">
                  {tender.deep_eligibility_raw}
                </DeepRow>
              )}

              {/* Team Requirements */}
              {tender.deep_team_reqs && (
                <DeepRow icon={Users} label="Team Requirements">
                  {tender.deep_team_reqs}
                </DeepRow>
              )}

              {/* Experience / Similar Projects */}
              {(tender.deep_min_years_experience != null || tender.deep_min_similar_projects != null || tender.deep_min_turnover_raw) && (
                <DeepRow icon={TrendingUp} label="Minimum Qualifications">
                  <ul className="space-y-1">
                    {tender.deep_min_years_experience != null && (
                      <li>Experience: <strong>{tender.deep_min_years_experience} years</strong></li>
                    )}
                    {tender.deep_min_similar_projects != null && (
                      <li>Similar projects: <strong>{tender.deep_min_similar_projects}</strong></li>
                    )}
                    {tender.deep_min_turnover_raw && (
                      <li>Min turnover: <strong>{tender.deep_min_turnover_raw}</strong></li>
                    )}
                  </ul>
                </DeepRow>
              )}

              {/* Key Dates */}
              {(tender.deep_date_pre_bid || tender.deep_date_qa_deadline || tender.deep_date_contract_start) && (
                <DeepRow icon={Calendar} label="Key Dates">
                  <ul className="space-y-1">
                    {tender.deep_date_pre_bid && <li>Pre-bid meeting: <strong>{tender.deep_date_pre_bid}</strong></li>}
                    {tender.deep_date_qa_deadline && <li>Q&A deadline: <strong>{tender.deep_date_qa_deadline}</strong></li>}
                    {tender.deep_date_contract_start && <li>Contract start: <strong>{tender.deep_date_contract_start}</strong></li>}
                  </ul>
                </DeepRow>
              )}

              {/* Contact */}
              {tender.deep_contact_block && (
                <DeepRow icon={Users} label="Contact">
                  <span className="whitespace-pre-line">{tender.deep_contact_block}</span>
                </DeepRow>
              )}
            </div>

            {/* Amendment alert */}
            {tender.amendment_count != null && tender.amendment_count > 0 && (
              <div className="mt-3 flex items-start gap-2 rounded-md border border-amber-200 bg-amber-50 p-3 dark:border-amber-800 dark:bg-amber-900/20">
                <AlertTriangle className="mt-0.5 h-4 w-4 flex-shrink-0 text-slate-500" />
                <div className="text-xs text-slate-600">
                  <p className="font-semibold">{tender.amendment_count} amendment{tender.amendment_count > 1 ? "s" : ""} detected</p>
                  {tender.last_amended_at && (
                    <p className="mt-0.5 text-slate-500">Last changed: {new Date(tender.last_amended_at).toLocaleDateString()}</p>
                  )}
                  <p className="mt-0.5">Verify scope, budget, and deadline before submitting.</p>
                </div>
              </div>
            )}
          </section>
        )}
      </div>

      {/* ── Right: sidebar ── */}
      <aside className="space-y-4 lg:sticky lg:top-6 lg:self-start">
        <TenderBriefPanel tender={tender} />
        <Card className="shadow-sm">
          <CardContent className="space-y-5 p-5">
            {/* Metadata */}
            <section>
              <h3 className="mb-3 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Details
              </h3>
              <dl className="space-y-2 text-sm">
                <MetaRow label="Portal">
                  <PortalIcon portal={tender.source_portal} showLabel />
                </MetaRow>
                <MetaRow label="Deadline">
                  <span className={cn(tender.is_expired && "text-red-500")}>
                    {tender.deadline ? formatDate(tender.deadline) : "—"}
                    {tender.is_expired && " (Expired)"}
                  </span>
                </MetaRow>
                <MetaRow label="Scraped">
                  {timeAgo(tender.scraped_at)}
                </MetaRow>
                {tender.word_count > 0 && (
                  <MetaRow label="Word count">
                    {tender.word_count.toLocaleString()} words
                  </MetaRow>
                )}
                {tender.estimated_budget_usd != null && (
                  <MetaRow label="Budget">
                    {formatBudget(tender.estimated_budget_usd)}
                  </MetaRow>
                )}
              </dl>
            </section>

            {/* Scores */}
            <section>
              <h3 className="mb-3 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Scores
              </h3>
              <div className="space-y-2.5">
                <FitScoreBar label="Overall fit" score={tender.fit_score} />
                <FitScoreBar label="Semantic" score={tender.semantic_score} />
                <FitScoreBar label="Keyword" score={tender.keyword_score} />
              </div>
            </section>

            {/* Sectors */}
            {tender.sectors && tender.sectors.length > 0 && (
              <section>
                <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  Sectors
                </h3>
                <div className="flex flex-wrap gap-1.5">
                  {tender.sectors.map((s) => (
                    <SectorBadge key={s} sector={s} />
                  ))}
                </div>
              </section>
            )}

            {/* Service types */}
            {tender.service_types && tender.service_types.length > 0 && (
              <section>
                <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  Service Types
                </h3>
                <div className="flex flex-wrap gap-1.5">
                  {tender.service_types.map((st) => (
                    <span
                      key={st}
                      className="inline-flex items-center rounded-full border bg-muted px-2.5 py-0.5 text-xs font-medium"
                    >
                      {serviceTypeLabel(st)}
                    </span>
                  ))}
                </div>
              </section>
            )}

            {/* Duplicate warning */}
            {tender.is_duplicate && (
              <div className="flex items-start gap-2 rounded-md border border-amber-200 bg-amber-50 p-3 dark:border-amber-800 dark:bg-amber-900/20">
                <AlertCircle className="mt-0.5 h-4 w-4 flex-shrink-0 text-amber-600 dark:text-amber-400" />
                <div className="text-xs text-amber-700 dark:text-amber-400">
                  <p className="font-semibold">Possible duplicate</p>
                  {tender.duplicate_of && (
                    <p className="mt-0.5 text-muted-foreground">
                      Similar to: {tender.duplicate_of}
                    </p>
                  )}
                </div>
              </div>
            )}

            {/* CTA buttons */}
            <div className="space-y-2 pt-1">
              <Button
                className="w-full"
                onClick={(e) => handleTenderClick(e, tender)}
              >
                <ExternalLink className="mr-2 h-4 w-4" />
                Open Tender
              </Button>
              <Button variant="outline" className="w-full" onClick={handleCopyLink}>
                <Copy className="mr-2 h-4 w-4" />
                Copy Link
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* AI Copilot recommendation */}
        <Card>
          <CardContent className="p-5 space-y-3">
            <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              AI Bid Advisor
            </h3>
            <CopilotPanel tenderId={tender.tender_id} />
          </CardContent>
        </Card>
      </aside>
    </div>
  );
}

function MetaRow({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex items-start justify-between gap-2">
      <dt className="flex-shrink-0 text-muted-foreground">{label}</dt>
      <dd className="text-right font-medium">{children}</dd>
    </div>
  );
}

function DeepRow({
  icon: Icon,
  label,
  children,
}: {
  icon: React.ElementType;
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm">
      <div className="flex items-center gap-1.5 mb-1.5">
        <Icon className="h-3.5 w-3.5 text-blue-500 flex-shrink-0" />
        <span className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
          {label}
        </span>
      </div>
      <div className="leading-relaxed text-foreground/90">{children}</div>
    </div>
  );
}
