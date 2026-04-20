import { Suspense } from "react";
import { PipelinePage } from "@/components/pipeline/PipelinePage";

export const metadata = { title: "Pipeline — TenderRadar" };

export default function Pipeline() {
  return (
    <Suspense
      fallback={
        <div className="flex gap-3 overflow-x-auto p-6 pb-10">
          {[0, 1, 2, 3, 4, 5].map((i) => (
            <div
              key={i}
              className="flex w-60 shrink-0 flex-col gap-2"
            >
              <div className="h-9 animate-pulse rounded-lg bg-muted" />
              {[0, 1, 2].map((j) => (
                <div key={j} className="h-28 animate-pulse rounded-xl bg-muted" />
              ))}
            </div>
          ))}
        </div>
      }
    >
      <PipelinePage />
    </Suspense>
  );
}
