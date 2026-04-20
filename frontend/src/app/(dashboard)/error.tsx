"use client";

import { useEffect } from "react";

export default function DashboardError({
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
    <div className="flex flex-col items-center justify-center min-h-[60vh] gap-5 text-center px-4">
      <div className="flex h-16 w-16 items-center justify-center rounded-2xl border border-red-500/20 bg-red-500/10">
        <span className="text-3xl">⚠</span>
      </div>
      <div>
        <p className="text-xl font-semibold text-white/80">Something went wrong</p>
        <p className="mt-1 text-sm text-white/40 max-w-sm">
          {error.message || "An unexpected error occurred. Please try again."}
        </p>
      </div>
      <button
        onClick={reset}
        className="rounded-lg border border-white/10 bg-white/[0.06] px-5 py-2 text-sm font-medium text-white/70 hover:bg-white/[0.10] hover:text-white/90 transition-all"
      >
        Try again
      </button>
    </div>
  );
}
