"use client";

import { useEffect } from "react";
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
    <div className="flex flex-col items-center justify-center min-h-screen bg-[#f8fafc] px-4 text-center">
      <div className="bg-white border border-slate-200 rounded-lg shadow-sm px-8 py-10 max-w-sm w-full">
        <p className="text-sm font-semibold text-red-600 mb-2">Something went wrong</p>
        <h2 className="text-base font-semibold text-slate-900 mb-2">An error occurred</h2>
        <p className="text-sm text-slate-500 mb-6">
          {error.message || "An unexpected error occurred. Please try again."}
        </p>
        <div className="flex items-center justify-center gap-3">
          <button
            onClick={reset}
            className="inline-flex h-9 items-center justify-center rounded-md bg-slate-900 px-4 text-sm font-medium text-white hover:bg-slate-800 transition-colors"
          >
            Try again
          </button>
          <Link
            href="/dashboard"
            className="inline-flex h-9 items-center justify-center rounded-md border border-slate-200 bg-white px-4 text-sm font-medium text-slate-700 hover:bg-slate-50 transition-colors"
          >
            Dashboard
          </Link>
        </div>
      </div>
    </div>
  );
}
