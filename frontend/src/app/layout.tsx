import type { Metadata } from "next";
import { GeistSans } from "geist/font/sans";
import { GeistMono } from "geist/font/mono";
import { Providers } from "@/providers/Providers";
import "@/app/globals.css";

export const metadata: Metadata = {
  title: {
    default:  "ProcureIQ — Procurement Intelligence",
    template: "%s — ProcureIQ",
  },
  description:
    "AI-powered tender monitoring and relevance scoring for development consulting firms.",
  icons: { icon: "/favicon.ico" },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html
      lang="en"
      className={`${GeistSans.variable} ${GeistMono.variable}`}
      suppressHydrationWarning
    >
      <body className="min-h-screen bg-background font-sans antialiased">
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
