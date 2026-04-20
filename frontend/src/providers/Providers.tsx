"use client";

import { ThemeProvider } from "next-themes";
import { SWRConfig } from "swr";
import { Toaster } from "sonner";
import { ApiError } from "@/lib/api";
import { toast } from "sonner";

interface ProvidersProps {
  children: React.ReactNode;
}

export function Providers({ children }: ProvidersProps) {
  return (
    <ThemeProvider attribute="class" defaultTheme="light" enableSystem disableTransitionOnChange>
      <SWRConfig
        value={{
          // Global error handler — shows toast for 5xx errors
          onError: (error: unknown) => {
            if (error instanceof ApiError && error.isServerError) {
              toast.error("Server error", { description: error.detail });
            }
          },
          revalidateOnReconnect: true,
          revalidateOnFocus:     false,
          dedupingInterval:      5_000,
        }}
      >
        {children}
        <Toaster richColors position="bottom-right" />
      </SWRConfig>
    </ThemeProvider>
  );
}
