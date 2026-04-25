"use client";

import { useEffect } from "react";
import { Button } from "@/components/ui/button";
import Link from "next/link";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error(error);
  }, [error]);

  return (
    <div className="flex flex-col items-center justify-center min-h-screen gap-4 text-center px-4">
      <div className="shell-panel rounded-[2rem] px-8 py-10 max-w-md w-full">
        <div className="text-3xl font-semibold text-slate-950 mb-2">Something went wrong</div>
        <p className="text-sm text-slate-500 max-w-sm mx-auto">
        {error.message || "An unexpected error occurred."}
        </p>
        <div className="mt-6 flex items-center justify-center gap-3">
          <Button onClick={reset} variant="outline" size="sm" className="border-slate-200 bg-white text-slate-700 hover:bg-slate-50">
            Try again
          </Button>
          <Button asChild variant="outline" size="sm" className="border-slate-200 bg-white text-slate-700 hover:bg-slate-50">
            <Link href="/dashboard">Go to dashboard</Link>
          </Button>
        </div>
      </div>
    </div>
  );
}
