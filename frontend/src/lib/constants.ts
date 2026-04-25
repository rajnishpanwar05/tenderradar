// =============================================================================
// lib/constants.ts — Display labels, colors, and mapping tables
// =============================================================================

import type { FitBucket } from "@/lib/api-types";

// ---------------------------------------------------------------------------
// Sector labels (slug → human-readable)
// ---------------------------------------------------------------------------

export const SECTOR_LABELS: Record<string, string> = {
  health:                "Health & Nutrition",
  education:             "Education & Skills",
  environment:           "Environment & Climate",
  agriculture:           "Agriculture & Rural",
  water_sanitation:      "Water & Sanitation",
  urban_development:     "Urban Development",
  energy:                "Energy & Power",
  governance:            "Governance & Institutional",
  gender_inclusion:      "Gender & Inclusion",
  infrastructure:        "Infrastructure",
  research:              "Research & Documentation",
  finance:               "Finance & Audit",
  communications:        "Communications & Media",
  circular_economy:      "Circular Economy",
  tourism:               "Tourism & Ecology",
  evaluation_monitoring: "M&E / Evaluation",
};

// ---------------------------------------------------------------------------
// Service type labels
// ---------------------------------------------------------------------------

export const SERVICE_TYPE_LABELS: Record<string, string> = {
  evaluation_monitoring: "Evaluation / Monitoring",
  consulting_advisory:   "Consulting / Advisory",
  research_study:        "Research / Study",
  capacity_building:     "Capacity Building",
  audit_finance:         "Audit / Finance",
  communications_media:  "Communications / Media",
  project_management:    "Project Management",
};

// ---------------------------------------------------------------------------
// Portal labels (source_portal slug → display name)
// ---------------------------------------------------------------------------

export const PORTAL_LABELS: Record<string, string> = {
  worldbank:          "World Bank",
  undp:               "UNDP",
  gem:                "GeM (India)",
  giz:                "GIZ",
  devnet:             "DevNet Jobs",
  cg_eprocure:        "CG eProcure",
  meghalaya:          "Meghalaya MBDA",
  sikkim:             "Sikkim",
  ngobox:             "NGO Box",
  afd:                "AFD (France)",
  afdb:               "AfDB",
  iucn:               "IUCN",
  sidbi:              "SIDBI",
  ungm:               "UNGM",
  usaid:              "USAID",
  sam_gov:            "SAM.gov",
  welthungerhilfe:    "Welthungerhilfe",
  dtvp:               "DTVP",
  ted:                "TED (EU)",
  taneps:             "TANEPS",
  icfre:              "ICFRE",
  jtds:               "JTDS Jharkhand",
  phfi:               "PHFI",
  karnataka:          "Karnataka eProcure",
  nic_states:         "NIC State Portals",
};

export function portalLabel(slug: string): string {
  return PORTAL_LABELS[slug] ?? slug.toUpperCase().replace(/_/g, " ");
}

export function sectorLabel(slug: string): string {
  return SECTOR_LABELS[slug] ?? slug.replace(/_/g, " ");
}

export function serviceTypeLabel(slug: string): string {
  return SERVICE_TYPE_LABELS[slug] ?? slug.replace(/_/g, " ");
}

// ---------------------------------------------------------------------------
// Fit bucket styling
// ---------------------------------------------------------------------------

export const FIT_BUCKET_CONFIG: Record<FitBucket, {
  label:     string;
  badgeCls:  string;   // Tailwind classes for the badge
  barColor:  string;   // Tailwind bg-* for progress bar
  dotColor:  string;   // Tailwind bg-* for dot indicator
}> = {
  HIGH: {
    label:    "HIGH",
    badgeCls: "bg-emerald-50 text-emerald-700 border-emerald-200 dark:bg-emerald-900/30 dark:text-emerald-400",
    barColor: "bg-emerald-500",
    dotColor: "bg-emerald-500",
  },
  GOOD: {
    label:    "GOOD",
    badgeCls: "bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-900/30 dark:text-blue-400",
    barColor: "bg-blue-500",
    dotColor: "bg-blue-500",
  },
  FAIR: {
    label:    "FAIR",
    badgeCls: "bg-amber-50 text-amber-700 border-amber-200 dark:bg-amber-900/30 dark:text-amber-400",
    barColor: "bg-amber-400",
    dotColor: "bg-amber-400",
  },
  LOW:  {
    label:    "LOW",
    badgeCls: "bg-slate-100 text-slate-500 border-slate-200 dark:bg-slate-800 dark:text-slate-400",
    barColor: "bg-slate-300",
    dotColor: "bg-slate-300",
  },
};

// ---------------------------------------------------------------------------
// Portal freshness thresholds (hours)
// ---------------------------------------------------------------------------

export const FRESHNESS = {
  FRESH:  6,   // green — scraped within 6 hours
  RECENT: 24,  // amber — scraped within 24 hours
  // > 24h → red (stale)
} as const;

// ---------------------------------------------------------------------------
// Sector color palette (for charts)
// ---------------------------------------------------------------------------

export const SECTOR_CHART_COLORS: Record<string, string> = {
  health:                "#10b981",
  education:             "#3b82f6",
  environment:           "#22c55e",
  agriculture:           "#84cc16",
  water_sanitation:      "#06b6d4",
  urban_development:     "#8b5cf6",
  energy:                "#f59e0b",
  governance:            "#6366f1",
  gender_inclusion:      "#ec4899",
  infrastructure:        "#64748b",
  research:              "#0ea5e9",
  finance:               "#f43f5e",
  communications:        "#a855f7",
  evaluation_monitoring: "#1F3864",
};

// ---------------------------------------------------------------------------
// Nav items — used by Sidebar and MobileNav
// ---------------------------------------------------------------------------

export const NAV_ITEMS = [
  { label: "Dashboard",   href: "/dashboard", icon: "LayoutDashboard" },
  { label: "Tenders",     href: "/tenders",   icon: "FileText" },
  { label: "Analytics",   href: "/analytics", icon: "BarChart2" },
  { label: "Portals",     href: "/portals",   icon: "Globe" },
  { label: "Pipeline",    href: "/pipeline",  icon: "Kanban" },
  { label: "AI Analyst",  href: "/chat",      icon: "MessageCircle" },
] as const;

// ---------------------------------------------------------------------------
// Priority score styling
// ---------------------------------------------------------------------------

export const PRIORITY_CONFIG = {
  high:   { range: [70, 100], cls: "bg-rose-50   text-rose-700   border-rose-200   dark:bg-rose-900/30   dark:text-rose-400",   bar: "bg-rose-500"   },
  medium: { range: [40, 69],  cls: "bg-amber-50  text-amber-700  border-amber-200  dark:bg-amber-900/30  dark:text-amber-400",  bar: "bg-amber-400"  },
  low:    { range: [0,  39],  cls: "bg-slate-100 text-slate-500  border-slate-200  dark:bg-slate-800     dark:text-slate-400",  bar: "bg-slate-300"  },
} as const;

export function priorityConfig(score: number) {
  if (score >= 70) return PRIORITY_CONFIG.high;
  if (score >= 40) return PRIORITY_CONFIG.medium;
  return PRIORITY_CONFIG.low;
}

// ---------------------------------------------------------------------------
// Pipeline column config
// ---------------------------------------------------------------------------

export type PipelineColId = "discovered" | "shortlisted" | "proposal_in_progress" | "submitted" | "won" | "lost";

export const PIPELINE_COLUMNS: { id: PipelineColId; label: string; color: string; accent: string }[] = [
  { id: "discovered",           label: "Discovered",         color: "bg-slate-50  dark:bg-slate-800/50", accent: "border-slate-300  dark:border-slate-600" },
  { id: "shortlisted",          label: "Shortlisted",        color: "bg-blue-50   dark:bg-blue-900/20",  accent: "border-blue-300   dark:border-blue-700" },
  { id: "proposal_in_progress", label: "Proposal in Progress",color: "bg-violet-50 dark:bg-violet-900/20",accent: "border-violet-300 dark:border-violet-700" },
  { id: "submitted",            label: "Submitted",          color: "bg-amber-50  dark:bg-amber-900/20", accent: "border-amber-300  dark:border-amber-700" },
  { id: "won",                  label: "Won",                color: "bg-emerald-50 dark:bg-emerald-900/20",accent:"border-emerald-300 dark:border-emerald-700"},
  { id: "lost",                 label: "Lost",               color: "bg-red-50    dark:bg-red-900/20",   accent: "border-red-200    dark:border-red-800" },
];
