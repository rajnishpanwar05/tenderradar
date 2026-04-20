"use client";

import React, { useEffect } from "react";
import Lenis from "@studio-freight/lenis";
import { ReactLenis, useLenis } from "@studio-freight/react-lenis";

export function SmoothScroll({ children }: { children: React.ReactNode }) {
  // Configured for optimal, non-intrusive SaaS scrolling
  return (
    <ReactLenis root options={{ lerp: 0.1, duration: 1.5, smoothWheel: true }}>
      {children}
    </ReactLenis>
  );
}
