import Link from "next/link";

export default function NotFound() {
  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-[#f8fafc] gap-6 text-center px-4">
      <div className="bg-white border border-slate-200 rounded-lg shadow-sm px-8 py-10 max-w-sm w-full">
        <p className="text-4xl font-semibold text-slate-900 mb-2">404</p>
        <h2 className="text-base font-semibold text-slate-900 mb-2">Page not found</h2>
        <p className="text-sm text-slate-500 mb-6">
          The page you&apos;re looking for doesn&apos;t exist or has been moved.
        </p>
        <div className="flex items-center justify-center gap-3">
          <Link
            href="/dashboard"
            className="inline-flex h-9 items-center justify-center rounded-md bg-slate-900 px-4 text-sm font-medium text-white hover:bg-slate-800 transition-colors"
          >
            Go to Dashboard
          </Link>
          <Link
            href="/tenders"
            className="inline-flex h-9 items-center justify-center rounded-md border border-slate-200 bg-white px-4 text-sm font-medium text-slate-700 hover:bg-slate-50 transition-colors"
          >
            Tenders
          </Link>
        </div>
      </div>
    </div>
  );
}
