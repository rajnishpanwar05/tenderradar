import { Skeleton } from "@/components/ui/skeleton";

export default function TendersLoading() {
  return (
    <div className="flex gap-6">
      <div className="hidden md:block w-64 flex-shrink-0 space-y-3">
        <Skeleton className="h-6 w-24" />
        <Skeleton className="h-9 w-full" />
        {Array.from({ length: 8 }).map((_, i) => (
          <Skeleton key={i} className="h-5 w-full" />
        ))}
      </div>
      <div className="flex-1 space-y-3">
        <Skeleton className="h-9 w-full" />
        {Array.from({ length: 12 }).map((_, i) => (
          <Skeleton key={i} className="h-12 w-full rounded-md" />
        ))}
      </div>
    </div>
  );
}
