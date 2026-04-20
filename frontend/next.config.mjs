/** @type {import('next').NextConfig} */
const config = {
  // NOTE: /api/proxy/* is handled by the Route Handler at
  // src/app/api/proxy/[...path]/route.ts which injects the API key
  // server-side. Do NOT add a rewrite here — it would bypass the Route
  // Handler and send unauthenticated requests directly to FastAPI (401).

  // Allow images from any HTTPS source (portal logos, avatars)
  images: {
    remotePatterns: [{ protocol: "https", hostname: "**" }],
  },

  // Minimal bundle output — removes console.log in production
  compiler: {
    removeConsole: process.env.NODE_ENV === "production"
      ? { exclude: ["error", "warn"] }
      : false,
  },
};

export default config;
