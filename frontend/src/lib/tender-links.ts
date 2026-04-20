import { toast } from "sonner";

export interface TenderClickAction {
  action: "open" | "copy_and_open";
  url: string;
  message?: string;
}

/**
 * Map each portal's source_site to the correct live project page URL strategy.
 *
 * "open"          → open tender.url directly (scraper stores the specific project page link)
 * "copy_and_open" → copy tender_id to clipboard and open portal search/home page
 *                    (used for portals that don't have stable deep links or require login)
 */
export function getTenderClickAction(tender: {
  source_site?: string;
  source_portal?: string;
  tender_id?: string;
  url?: string;
}): TenderClickAction {
  const portal = (tender.source_site || tender.source_portal || "").trim();
  const id  = tender.tender_id || "";
  const url = (tender.url || "").trim();

  // ── Portals with STABLE deep links stored in the `url` field ──────────────
  // The scrapers already record the full project-page URL for these portals.
  // Just open it directly.
  const DIRECT_DEEP_LINK_PORTALS = new Set([
    "World Bank",       // https://projects.worldbank.org/…  or WB procurement notice
    "UNDP",            // https://procurement-notices.undp.org/view_negotiation.cfm?…
    "UNGM",            // https://www.ungm.org/Public/Notice/…
    "NGOBox",          // https://ngobox.org/rfp_eoi_detail.php?…
    "GIZ",             // https://ausschreibungen.giz.de/…
    "AfDB",            // https://www.afdb.org/…
    "IUCN",            // https://iucn.org/procurement/…
    "Welthungerhilfe", // https://www.welthungerhilfe.org/tenders/…
    "SAM.gov",         // https://sam.gov/opp/{noticeId}/view
    "USAID",           // https://www.usaid.gov/… or sam.gov link
    "TED-EU",          // https://ted.europa.eu/en/notice/-/detail/…
    "DTVP",            // https://www.dtvp.de/Center/public/project/…
    "AFD",             // https://afd.dgmarket.com/tender/…
    "SIDBI",           // https://sidbi.in/en/tenders
    "PHFI",            // https://phfi.org/tenders/…
    "ICFRE",           // https://icfre.gov.in/en/tenders
    "JTDS",            // http://jtdsjharkhand.com/tender/…
    "MBDA",            // https://meghalaya.gov.in/… (Meghalaya MBDA)
  ]);

  if (DIRECT_DEEP_LINK_PORTALS.has(portal) && url && url !== "#") {
    return { action: "open", url };
  }

  // ── GeM BidPlus ───────────────────────────────────────────────────────────
  // GeM uses dynamic JS rendering — the "all-bids" search is the public entry point.
  // Copy the Bid Number so the user can paste it into the GeM search bar.
  if (portal === "GeM" || portal === "GeM BidPlus") {
    return {
      action: "copy_and_open",
      url: "https://bidplus.gem.gov.in/all-bids",
      message: `Copied Bid ID: ${id}. Paste it into the GeM search bar to view the project.`,
    };
  }

  // ── NICGEP portals ────────────────────────────────────────────────────────
  // Session-locked URLs expire — send to the public portal search page instead.
  if (portal === "CG") {
    return {
      action: "copy_and_open",
      url: "https://eproc.cgstate.gov.in/ETENDERS/",
      message: `Copied Tender ID: ${id}. Use the CG eProcurement search to find it.`,
    };
  }
  if (portal === "UP eTender") {
    return {
      action: "copy_and_open",
      url: "https://etender.up.nic.in/nicgep/app",
      message: `Copied Tender ID: ${id}. Paste it into the UP eTender search box.`,
    };
  }
  if (portal === "Maharashtra") {
    return {
      action: "copy_and_open",
      url: "https://mahatenders.gov.in/nicgep/app",
      message: `Copied Tender ID: ${id}. Paste it into the Maharashtra Tenders search box.`,
    };
  }
  if (portal === "SIKKIM" || portal === "Sikkim") {
    return {
      action: "copy_and_open",
      url: "https://sikkimtenders.gov.in/nicgep/app",
      message: `Copied Tender ID: ${id}. Paste it into the Sikkim Tenders search box.`,
    };
  }

  // ── Karnataka ─────────────────────────────────────────────────────────────
  if (portal === "Karnataka") {
    return {
      action: "copy_and_open",
      url: "https://kppp.karnataka.gov.in/portal/searchTender/live",
      message: `Copied Tender ID: ${id}. Paste it into the Karnataka eProcure search.`,
    };
  }

  // ── TANEPS (Tanzania) ─────────────────────────────────────────────────────
  if (portal === "TANEPS") {
    return {
      action: "copy_and_open",
      url: "https://www.ppra.go.tz/index.php/procuring-entities/tenders",
      message: `Copied Tender ID: ${id}. Search for it on TANEPS.`,
    };
  }

  // ── DevNet (India) ─────────────────────────────────────────────────────────
  // DevNet URLs are not stable project links — send to home page for search
  if (portal === "DevNet") {
    return {
      action: "copy_and_open",
      url: "https://devnetjobsindia.org/",
      message: `Copied Job ID: ${id}. Search for it on DevNet.`,
    };
  }

  // ── Fallback: if the portal URL is a valid http link, open it directly ────
  if (url && url.startsWith("http")) {
    return { action: "open", url };
  }

  // ── Last resort: no URL available ─────────────────────────────────────────
  return { action: "open", url: "#" };
}

/**
 * Shared click handler for Tenders across the UI.
 * Handles auto-copying the tender ID to the clipboard for portals with complex deep-linking.
 */
export function handleTenderClick(e: React.MouseEvent | undefined, tender: any) {
  if (e) e.stopPropagation();

  const { action, url, message } = getTenderClickAction(tender);

  if (action === "copy_and_open") {
    const id = tender.tender_id || "";
    if (navigator.clipboard) {
      navigator.clipboard.writeText(id).catch(err => console.error("Clipboard failed", err));
    }
    toast.success(message || "Tender ID copied to clipboard!");
  }

  if (url && url !== "#") {
    window.open(url, "_blank", "noopener,noreferrer");
  }
}

