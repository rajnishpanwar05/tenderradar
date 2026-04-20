"use client";

import { useCallback, useEffect, useState } from "react";
import { Loader2, AlertTriangle } from "lucide-react";
import { apiClient } from "@/lib/api";
import type { TenderRecord } from "@/lib/api-types";
import { TenderDetailPanel } from "./TenderDetailPanel";
import { Button } from "@/components/ui/button";

type LoaderState = "loading" | "error" | "ready";

interface TenderDetailLoaderProps {
  id: string;
}

export function TenderDetailLoader({ id }: TenderDetailLoaderProps) {
  const [state, setState] = useState<LoaderState>("loading");
  const [tender, setTender] = useState<TenderRecord | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setState("loading");
    setError(null);
    try {
      const data = await apiClient.client.getTender(id);
      setTender(data);
      setState("ready");
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to load tender.");
      setState("error");
    }
  }, [id]);

  useEffect(() => {
    load();
  }, [load]);

  if (state === "ready" && tender) {
    return <TenderDetailPanel tender={tender} />;
  }

  if (state === "error") {
    return (
      <div className="flex flex-col items-center justify-center min-h-[50vh] gap-4 text-center">
        <AlertTriangle className="h-8 w-8 text-amber-500" />
        <p className="text-sm text-muted-foreground">
          {error || "Unable to load tender details right now."}
        </p>
        <Button variant="outline" size="sm" onClick={load}>
          Retry
        </Button>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-[50vh] gap-3 text-center">
      <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      <p className="text-sm text-muted-foreground">Loading tender details…</p>
    </div>
  );
}
