import Link from "next/link";

export default function NotFound() {
  return (
    <div className="flex flex-col items-center justify-center min-h-screen gap-4 text-center px-4">
      <div className="text-6xl font-bold text-muted-foreground">404</div>
      <h2 className="text-xl font-semibold">Page not found</h2>
      <p className="text-sm text-muted-foreground max-w-sm">
        The page or tender you&apos;re looking for doesn&apos;t exist or has been removed.
      </p>
      <Link
        href="/tenders"
        className="text-sm text-primary hover:underline underline-offset-4"
      >
        ← Back to Tenders
      </Link>
    </div>
  );
}
