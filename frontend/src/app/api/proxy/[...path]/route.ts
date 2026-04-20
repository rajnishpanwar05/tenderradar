// =============================================================================
// app/api/proxy/[...path]/route.ts
//
// Reverse proxy: client-side fetch to /api/proxy/* → FastAPI /api/v1/*
//
// Keeps the backend host (API_URL) out of the browser bundle.
// In development with CORS open, client-side SWR hits FastAPI directly.
// In production, set NEXT_PUBLIC_API_URL to the same host as this Next.js
// server and let this proxy forward to the private backend API_URL.
// =============================================================================

import { type NextRequest, NextResponse } from "next/server";

export const dynamic = "force-dynamic";
export const maxDuration = 120; // Allow up to 2 minutes for LLM processing

const BACKEND = (process.env.API_URL ?? "http://localhost:8000").replace(/\/$/, "");

async function handler(req: NextRequest, { params }: { params: { path: string[] } }) {
  const path = params.path.join("/");
  const qs   = req.nextUrl.search ?? "";
  const url  = `${BACKEND}/api/v1/${path}${qs}`;

  // Read body as text first — avoids ReadableStream forwarding issues in App Router
  const isBodyMethod = !["GET", "HEAD"].includes(req.method);
  let bodyText: string | undefined;
  if (isBodyMethod) {
    try {
      bodyText = await req.text();
    } catch {
      bodyText = undefined;
    }
  }

  // Build clean headers — strip hop-by-hop headers that break proxying
  const headers = new Headers();
  headers.set("content-type", "application/json");
  headers.set("accept", "application/json");
  if (bodyText) {
    headers.set("content-length", Buffer.byteLength(bodyText).toString());
  }
  // Attach backend API key — injected server-side so it never reaches the browser
  const apiKey = process.env.API_SECRET_KEY ?? "";
  if (apiKey) headers.set("x-api-key", apiKey);

  try {
    const upstream = await fetch(url, {
      method:  req.method,
      headers,
      body:    isBodyMethod ? bodyText : undefined,
      cache:   "no-store",
    });

    const responseBody = await upstream.arrayBuffer();

      return new NextResponse(responseBody, {
        status:  upstream.status,
        headers: {
          "content-type": upstream.headers.get("content-type") ?? "application/json",
        },
      });
    } catch (err) {
      console.error("[proxy] upstream fetch failed:", err);
      return NextResponse.json(
        { error: "Proxy error", detail: err instanceof Error ? err.message : "Unknown backend proxy error" },
        { status: 502 }
      );
    }
}

export { handler as GET, handler as POST, handler as PUT, handler as DELETE, handler as OPTIONS };
