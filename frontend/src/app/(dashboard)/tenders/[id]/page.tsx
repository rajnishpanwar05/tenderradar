import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { apiClient, ApiError } from "@/lib/api";
import { TenderDetailPanel } from "@/components/tenders/TenderDetailPanel";
import { TenderDetailLoader } from "@/components/tenders/TenderDetailLoader";

interface TenderDetailPageProps {
  params: { id: string };
}

export async function generateMetadata(
  { params }: TenderDetailPageProps
): Promise<Metadata> {
  try {
    const tender = await apiClient.server.getTender(params.id);
    return {
      title: tender.title_clean || tender.title,
      description: tender.description?.slice(0, 160) || undefined,
    };
  } catch {
    return { title: "Tender Details" };
  }
}

export default async function TenderDetailPage({ params }: TenderDetailPageProps) {
  let tender;
  try {
    tender = await apiClient.server.getTender(params.id);
  } catch (err) {
    if (err instanceof ApiError && err.isNotFound) {
      notFound();
    }
    return <TenderDetailLoader id={params.id} />;
  }

  return <TenderDetailPanel tender={tender} />;
}
