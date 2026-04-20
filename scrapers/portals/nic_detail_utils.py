import re
from typing import Dict, List
from urllib.parse import urljoin

from bs4 import BeautifulSoup


_STOP_LABELS = [
    "Tender Reference Number",
    "Tender ID",
    "Withdrawal Allowed",
    "Tender Type",
    "Form Of Contract",
    "Tender Category",
    "No. of Covers",
    "General Technical Evaluation Allowed",
    "ItemWise Technical Evaluation Allowed",
    "Payment Mode",
    "Allow Two Stage Bidding",
    "Payment Instruments",
    "Fee/PreQual/Technical",
    "Fee/PreQual/Technical/Finance",
    "EMD Amount in ₹",
    "EMD Exemption Allowed",
    "EMD Fee Type",
    "Tender Fee in ₹",
    "Work Item Details",
    "Title",
    "Work Description",
    "NDA/Pre Qualification",
    "Independent External Monitor/Remarks",
    "Tender Value in ₹",
    "Product Category",
    "Contract Type",
    "Bid Validity(Days)",
    "Period Of Work(Days)",
    "Pincode",
    "Pre Bid Meeting Place",
    "Pre Bid Meeting Address",
    "Pre Bid Meeting Date",
    "Bid Opening Place",
    "Should Allow NDA Tender",
    "Allow Preferential Bidder",
    "Critical Dates",
    "Tender Documents",
    "Work Item Documents",
    "Tender Inviting Authority",
    "Name",
    "Address",
]


def seed_nic_listing_metadata(row: Dict[str, str], source_portal: str) -> Dict[str, str]:
    """
    Enrich a NIC-style listing row with stable generic fields so the backend
    retains useful context even when the detail page cannot be revisited later.
    """
    org = str(row.get("Organisation") or "").strip()
    title = str(row.get("Title") or "").strip()
    published = str(row.get("Published Date") or "").strip()
    closing = str(row.get("Closing Date") or "").strip()
    opening = str(row.get("Opening Date") or "").strip()
    tender_id = str(row.get("Tender ID") or "").strip()

    summary_parts = [part for part in [
        f"Organisation: {org}" if org else "",
        f"Published: {published}" if published else "",
        f"Closing: {closing}" if closing else "",
        f"Opening: {opening}" if opening else "",
        f"Tender ID: {tender_id}" if tender_id else "",
        f"Portal: {source_portal}" if source_portal else "",
    ] if part]

    row.setdefault("Description", f"{title}. " + " | ".join(summary_parts) if summary_parts else title)
    row.setdefault("organization", org)
    row.setdefault("source_portal", source_portal)
    row.setdefault("source_site", source_portal)
    row.setdefault("tender_id", tender_id)
    return row


def enrich_nic_row_with_detail(session, row: Dict[str, str], timeout: int = 30) -> Dict[str, str]:
    """
    Fetch and parse a NIC GePNIC tender detail page while the listing session is
    still alive. This is the durable way to enrich CG/UP/Maharashtra rows,
    because their detail URLs become stale outside the scraper session.
    """
    url = str(row.get("URL") or row.get("url") or "").strip()
    if not url:
        return row

    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
    except Exception:
        return row

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    if not text:
        return row

    if "Stale Session" in text or "session has timed out" in text.lower():
        return row

    organization = _extract_field(text, ["Organisation Chain"], ["Tender Reference Number", "Tender ID", "Withdrawal Allowed"])
    reference_no = _extract_field(text, ["Tender Reference Number", "Tender Ref No"], ["Tender ID", "Withdrawal Allowed", "Tender Type"])
    category = _extract_field(text, ["Tender Category"], ["No. of Covers", "General Technical Evaluation Allowed"])
    emd_amount = _extract_field(text, ["EMD Amount in ₹", "EMD Amount"], ["EMD Exemption Allowed", "EMD Fee Type", "Tender Fee in ₹"])
    tender_value = _extract_field(text, ["Tender Value in ₹", "Estimated Value"], ["Product Category", "Contract Type", "Bid Validity(Days)", "Period Of Work(Days)"])
    work_description = _extract_field(text, ["Work Description"], ["NDA/Pre Qualification", "Independent External Monitor/Remarks", "Tender Value in ₹", "Product Category"])
    prequal = _extract_field(text, ["NDA/Pre Qualification"], ["Independent External Monitor/Remarks", "Tender Value in ₹", "Product Category", "Bid Validity(Days)"])
    authority_name = _extract_field(text, ["Tender Inviting Authority Name"], ["Address"])
    authority_address = _extract_field(text, ["Address"], ["Location", "Tender Creator Details", "Contact Details", "Pre Bid Meeting Date", "Bid Opening Place", "Critical Dates"])
    bid_opening_place = _extract_field(text, ["Bid Opening Place"], ["Should Allow NDA Tender", "Allow Preferential Bidder", "Critical Dates"])
    pre_bid_date = _extract_field(text, ["Pre Bid Meeting Date"], ["Bid Opening Place", "Should Allow NDA Tender", "Critical Dates"])
    documents = _extract_document_names(text)
    document_urls = _extract_document_urls(soup, url)

    description_parts = [part for part in [
        work_description,
        f"Category: {category}" if category else "",
        f"Estimated value: {tender_value}" if tender_value else "",
        f"EMD: {emd_amount}" if emd_amount else "",
        f"Pre-qualification: {prequal}" if prequal else "",
        f"Tender reference: {reference_no}" if reference_no else "",
        f"Inviting authority: {authority_name}" if authority_name else "",
        f"Authority address: {authority_address}" if authority_address else "",
        f"Bid opening place: {bid_opening_place}" if bid_opening_place else "",
        f"Pre-bid meeting: {pre_bid_date}" if pre_bid_date else "",
        f"Documents: {', '.join(documents[:8])}" if documents else "",
        f"Document links: {' | '.join(document_urls[:8])}" if document_urls else "",
    ] if part]

    if organization:
        row["Organisation"] = organization
        row["organization"] = organization
    if category:
        row["Category"] = category
    if tender_value:
        row["Value"] = tender_value
    if reference_no:
        row["Tender Reference Number"] = reference_no
    if emd_amount:
        row["EMD Amount"] = emd_amount
    if bid_opening_place:
        row["Bid Opening Place"] = bid_opening_place
    if documents:
        row["Document Names"] = ", ".join(documents[:10])
    if document_urls:
        row["Document URLs"] = " | ".join(document_urls[:20])
    if description_parts:
        row["Description"] = " | ".join(description_parts)[:10000]

    return row


def _extract_field(text: str, labels: List[str], stop_labels: List[str]) -> str:
    starts = "|".join(re.escape(label) for label in labels)
    stops = "|".join(re.escape(label) for label in (stop_labels or _STOP_LABELS))
    pattern = rf"(?:{starts})\s*(.+?)(?=\s+(?:{stops})\b|$)"
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    value = re.sub(r"\s+", " ", match.group(1)).strip(" :-|")
    if len(value) > 1000:
        value = value[:1000].rstrip()
    cleaned = re.sub(r"[^A-Za-z0-9]+", "", value).lower()
    if not cleaned or cleaned in {"na", "notavailable"}:
        return ""
    return value


def _extract_document_names(text: str) -> List[str]:
    names = re.findall(r"\b([A-Za-z0-9][A-Za-z0-9._ -]{1,120}\.(?:pdf|docx?|xlsx?|xlsm|zip|rar|csv))\b", text, flags=re.IGNORECASE)
    out: List[str] = []
    seen = set()
    for raw in names:
        name = re.sub(r"\s+", " ", raw).strip()
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def _extract_document_urls(soup: BeautifulSoup, base_url: str) -> List[str]:
    out: List[str] = []
    seen = set()
    keyword_markers = (
        "download",
        "tenderdoc",
        "tenderdocument",
        "documentdownload",
        "attachment",
        "bid-doc",
    )
    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "").strip()
        if not href:
            continue
        low = href.lower()
        if (
            not re.search(r"\.(pdf|docx?|xlsx?|xlsm|zip|rar|csv)(?:$|[?#])", low)
            and not any(marker in low for marker in keyword_markers)
        ):
            continue
        full = urljoin(base_url, href)
        key = full.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(full)
    return out
