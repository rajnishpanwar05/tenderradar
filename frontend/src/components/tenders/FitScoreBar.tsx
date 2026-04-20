import { cn } from "@/lib/utils";
import { Progress } from "@/components/ui/progress";

interface FitScoreBarProps {
  label: string;
  score: number;
  className?: string;
}

function barColor(score: number): string {
  if (score >= 80) return "bg-emerald-500";
  if (score >= 65) return "bg-blue-500";
  if (score >= 50) return "bg-amber-400";
  return "bg-slate-300";
}

export function FitScoreBar({ label, score, className }: FitScoreBarProps) {
  const clamped = Math.max(0, Math.min(100, Math.round(score)));

  return (
    <div className={cn("space-y-1", className)}>
      <div className="flex items-center justify-between">
        <span className="text-xs text-muted-foreground">{label}</span>
        <span className="text-xs font-medium tabular-nums">{clamped}</span>
      </div>
      <Progress
        value={clamped}
        className="h-1.5"
        indicatorClassName={barColor(clamped)}
      />
    </div>
  );
}
