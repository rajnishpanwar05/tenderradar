"use client";
import { useCallback } from "react";
import { useRouter, usePathname, useSearchParams } from "next/navigation";
import type { TenderFilters } from "@/lib/api-types";
import { DEFAULT_FILTERS } from "@/lib/api-types";
import {
  parseFiltersFromUrl,
  filtersToSearchParams,
  toggleArrayItem,
} from "@/lib/url-state";

type ArrayFilterKey = "sectors" | "service_types" | "countries" | "source_portals";

export function useFilterState() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const filters: TenderFilters = parseFiltersFromUrl(
    new URLSearchParams(searchParams.toString())
  );

  const push = useCallback(
    (updated: TenderFilters) => {
      const qs = filtersToSearchParams(updated).toString();
      router.push(pathname + (qs ? "?" + qs : ""), { scroll: false });
    },
    [router, pathname]
  );

  const setFilter = useCallback(
    <K extends keyof TenderFilters>(key: K, value: TenderFilters[K]) => {
      const updated: TenderFilters = {
        ...filters,
        [key]: value,
        // Reset page whenever any filter other than page itself changes
        page: key === "page" ? (value as number) : 1,
      };
      push(updated);
    },
    [filters, push]
  );

  const toggleArrayFilter = useCallback(
    (key: ArrayFilterKey, value: string) => {
      const current = filters[key] as string[];
      const updated: TenderFilters = {
        ...filters,
        [key]: toggleArrayItem(current, value),
        page: 1,
      };
      push(updated);
    },
    [filters, push]
  );

  const resetFilters = useCallback(() => {
    push(DEFAULT_FILTERS);
  }, [push]);

  return { filters, setFilter, toggleArrayFilter, resetFilters };
}
