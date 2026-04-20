import { FileSearch } from "lucide-react";
import { Button } from "@/components/ui/button";

interface EmptyStateProps {
  message?: string;
  onReset?: () => void;
}

export function EmptyState({ message, onReset }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-20 text-center">
      <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-muted">
        <FileSearch className="h-8 w-8 text-muted-foreground" aria-hidden="true" />
      </div>
      <h3 className="mb-2 text-lg font-semibold">No tenders found</h3>
      {message && (
        <p className="mb-6 max-w-sm text-sm text-muted-foreground">{message}</p>
      )}
      {!message && (
        <p className="mb-6 max-w-sm text-sm text-muted-foreground">
          Try adjusting your filters or search query to find relevant tenders.
        </p>
      )}
      {onReset && (
        <Button variant="outline" size="sm" onClick={onReset}>
          Reset filters
        </Button>
      )}
    </div>
  );
}
