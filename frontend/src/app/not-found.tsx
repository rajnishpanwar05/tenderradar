import Link from "next/link";

export default function NotFound() {
  return (
    <div className="flex flex-col items-center justify-center min-h-screen gap-4 text-center px-4">
      <div className="shell-panel rounded-[2rem] px-8 py-10 max-w-md w-full">
        <div className="text-5xl font-semibold text-slate-900">404</div>
        <h2 className="mt-2 text-xl font-semibold text-slate-950">Page not found</h2>
        <p className="text-sm text-slate-500 max-w-sm mx-auto mt-2">
          The page or tender you&apos;re looking for doesn&apos;t exist or has been removed.
        </p>
        <div className="mt-6 flex items-center justify-center gap-3">
          <Link
            href="/dashboard"
            className="inline-flex h-8 items-center justify-center rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 hover:bg-slate-50"
          >
            Dashboard
          </Link>
          <Link
            href="/tenders"
            className="inline-flex h-8 items-center justify-center rounded-md bg-slate-900 px-3 text-sm text-white hover:bg-slate-800"
          >
            Tenders
          </Link>
        </div>
      </div>
    </div>
  );
}
