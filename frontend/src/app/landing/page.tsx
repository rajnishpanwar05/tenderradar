"use client";

import { useEffect, useRef, useState, Suspense } from "react";
import { Canvas, useFrame, useThree } from "@react-three/fiber";
import { Float, Stars, MeshDistortMaterial, Sphere, Text3D, Center } from "@react-three/drei";
import { motion, useScroll, useTransform, AnimatePresence, useMotionValue, useSpring } from "framer-motion";
import * as THREE from "three";
import Link from "next/link";

/* ─── Types ─── */
interface Particle {
  x: number; y: number; z: number;
  vx: number; vy: number; vz: number;
  size: number; color: string;
}

/* ─────────────────────── 3-D NEURAL ORBS ─────────────────────── */
function NeuralOrb({ position, color, speed = 1, distort = 0.4 }: {
  position: [number, number, number];
  color: string; speed?: number; distort?: number;
}) {
  const meshRef = useRef<THREE.Mesh>(null);
  useFrame((state) => {
    if (!meshRef.current) return;
    meshRef.current.rotation.x = state.clock.elapsedTime * 0.3 * speed;
    meshRef.current.rotation.y = state.clock.elapsedTime * 0.2 * speed;
  });
  return (
    <Float speed={speed * 1.5} rotationIntensity={0.5} floatIntensity={2}>
      <mesh ref={meshRef} position={position}>
        <Sphere args={[1, 64, 64]}>
          <MeshDistortMaterial
            color={color} attach="material" distort={distort}
            speed={speed * 2} roughness={0} metalness={0.8}
            transparent opacity={0.7}
          />
        </Sphere>
      </mesh>
    </Float>
  );
}

/* ─────────────────────── PARTICLE FIELD ─────────────────────── */
function ParticleField() {
  const pointsRef = useRef<THREE.Points>(null);

  const [geometry] = useState(() => {
    const geo = new THREE.BufferGeometry();
    const count = 3000;
    const positions = new Float32Array(count * 3);
    const colors = new Float32Array(count * 3);
    for (let i = 0; i < count; i++) {
      positions[i * 3]     = (Math.random() - 0.5) * 80;
      positions[i * 3 + 1] = (Math.random() - 0.5) * 80;
      positions[i * 3 + 2] = (Math.random() - 0.5) * 80;
      const colorChoice = Math.random();
      if (colorChoice < 0.33) { colors[i*3]=0; colors[i*3+1]=0.82; colors[i*3+2]=1; }
      else if (colorChoice < 0.66) { colors[i*3]=0.48; colors[i*3+1]=0.23; colors[i*3+2]=0.93; }
      else { colors[i*3]=0.06; colors[i*3+1]=0.47; colors[i*3+2]=0.57; }
    }
    geo.setAttribute("position", new THREE.BufferAttribute(positions, 3));
    geo.setAttribute("color", new THREE.BufferAttribute(colors, 3));
    return geo;
  });

  useFrame((state) => {
    if (!pointsRef.current) return;
    pointsRef.current.rotation.y = state.clock.elapsedTime * 0.03;
    pointsRef.current.rotation.x = state.clock.elapsedTime * 0.01;
  });

  return (
    <points ref={pointsRef} geometry={geometry}>
      <pointsMaterial size={0.08} vertexColors transparent opacity={0.8} sizeAttenuation />
    </points>
  );
}

/* ─────────────────────── FLOATING GRID ─────────────────────── */
function FloatingGrid() {
  const meshRef = useRef<THREE.Mesh>(null);
  useFrame((state) => {
    if (!meshRef.current) return;
    meshRef.current.position.y = Math.sin(state.clock.elapsedTime * 0.5) * 0.3 - 4;
  });
  return (
    <mesh ref={meshRef} rotation={[-Math.PI / 2, 0, 0]} position={[0, -4, 0]}>
      <planeGeometry args={[60, 60, 40, 40]} />
      <meshBasicMaterial color="#0D4F5E" wireframe transparent opacity={0.15} />
    </mesh>
  );
}

/* ─────────────────────── MOUSE-REACTIVE CAMERA ─────────────────────── */
function CameraRig({ mouseX, mouseY }: { mouseX: number; mouseY: number }) {
  const { camera } = useThree();
  useFrame(() => {
    camera.position.x += (mouseX * 3 - camera.position.x) * 0.05;
    camera.position.y += (-mouseY * 2 - camera.position.y) * 0.05;
    camera.lookAt(0, 0, 0);
  });
  return null;
}

/* ─────────────────────── 3-D SCENE ─────────────────────── */
function Scene({ mouseX, mouseY }: { mouseX: number; mouseY: number }) {
  return (
    <>
      <ambientLight intensity={0.1} />
      <pointLight position={[10, 10, 10]} intensity={2} color="#00D4FF" />
      <pointLight position={[-10, -10, -10]} intensity={1.5} color="#7C3AED" />
      <pointLight position={[0, 5, 0]} intensity={1} color="#06B6D4" />
      <Stars radius={100} depth={50} count={5000} factor={4} saturation={0} fade speed={1} />
      <ParticleField />
      <FloatingGrid />
      <NeuralOrb position={[0, 0, -5]} color="#0891B2" speed={1} distort={0.5} />
      <NeuralOrb position={[-6, 2, -8]} color="#7C3AED" speed={0.7} distort={0.3} />
      <NeuralOrb position={[6, -2, -6]} color="#00D4FF" speed={1.3} distort={0.6} />
      <NeuralOrb position={[3, 4, -10]} color="#6D28D9" speed={0.5} distort={0.35} />
      <CameraRig mouseX={mouseX} mouseY={mouseY} />
    </>
  );
}

/* ─────────────────────── ANIMATED COUNTER ─────────────────────── */
function AnimatedNumber({ value, suffix = "", prefix = "" }: { value: number; suffix?: string; prefix?: string }) {
  const [display, setDisplay] = useState(0);
  useEffect(() => {
    let start = 0;
    const step = value / 60;
    const timer = setInterval(() => {
      start += step;
      if (start >= value) { setDisplay(value); clearInterval(timer); }
      else setDisplay(Math.floor(start));
    }, 16);
    return () => clearInterval(timer);
  }, [value]);
  return <span>{prefix}{display.toLocaleString()}{suffix}</span>;
}

/* ─────────────────────── PORTAL BADGE ─────────────────────── */
const PORTALS = [
  { name: "World Bank", color: "#00D4FF" },
  { name: "UNDP", color: "#7C3AED" },
  { name: "GIZ", color: "#06B6D4" },
  { name: "GeM India", color: "#8B5CF6" },
  { name: "USAID", color: "#0891B2" },
  { name: "ADB", color: "#6D28D9" },
  { name: "AFDB", color: "#00B4D8" },
  { name: "SAM.gov", color: "#9333EA" },
  { name: "EU TED", color: "#0E7490" },
  { name: "UNICEF", color: "#5B21B6" },
  { name: "FAO", color: "#0284C7" },
  { name: "UNGM", color: "#7E22CE" },
];

/* ─────────────────────── FEATURE CARD ─────────────────────── */
function FeatureCard({ icon, title, desc, delay, gradient }: {
  icon: string; title: string; desc: string; delay: number; gradient: string;
}) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 40 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.6, delay }}
      whileHover={{ y: -8, scale: 1.02 }}
      className="relative group cursor-default"
    >
      <div className="absolute inset-0 rounded-2xl opacity-0 group-hover:opacity-100 transition-opacity duration-500"
        style={{ background: gradient, filter: "blur(20px)", transform: "translateY(10px)" }} />
      <div className="relative rounded-2xl border border-white/10 bg-black/60 backdrop-blur-xl p-8 h-full overflow-hidden">
        <div className="absolute inset-0 opacity-0 group-hover:opacity-10 transition-opacity duration-500 rounded-2xl"
          style={{ background: gradient }} />
        <div className="text-4xl mb-4">{icon}</div>
        <div className="text-sm font-mono text-cyan-400 mb-2 tracking-widest uppercase">{title}</div>
        <p className="text-white/60 text-sm leading-relaxed">{desc}</p>
        <div className="absolute bottom-0 left-0 right-0 h-px opacity-0 group-hover:opacity-100 transition-opacity duration-500"
          style={{ background: gradient }} />
      </div>
    </motion.div>
  );
}

/* ─────────────────────── TICKER TAPE ─────────────────────── */
function TickerTape() {
  const items = [
    "🔥 New: World Bank M&E tender · India · 12 days left · FIT 94%",
    "⭐ UNDP Health Systems · Nepal · 8 days left · FIT 87%",
    "🎯 GIZ Capacity Building · Bangladesh · 21 days left · FIT 91%",
    "🔥 ADB Education Reform · Sri Lanka · 5 days left · FIT 88%",
    "⭐ USAID Governance · India · 15 days left · FIT 82%",
    "🎯 GeM Evaluation · Maharashtra · 3 days left · FIT 95%",
  ];
  const doubled = [...items, ...items];
  return (
    <div className="relative overflow-hidden py-3 border-y border-white/5">
      <div className="absolute left-0 top-0 bottom-0 w-20 z-10 bg-gradient-to-r from-black to-transparent" />
      <div className="absolute right-0 top-0 bottom-0 w-20 z-10 bg-gradient-to-l from-black to-transparent" />
      <motion.div
        className="flex gap-12 whitespace-nowrap"
        animate={{ x: ["0%", "-50%"] }}
        transition={{ duration: 30, repeat: Infinity, ease: "linear" }}
      >
        {doubled.map((item, i) => (
          <span key={i} className="text-sm text-white/40 font-mono shrink-0">{item}</span>
        ))}
      </motion.div>
    </div>
  );
}

/* ─────────────────────── MAIN LANDING PAGE ─────────────────────── */
export default function LandingPage() {
  const [mouseX, setMouseX] = useState(0);
  const [mouseY, setMouseY] = useState(0);
  const [isLoaded, setIsLoaded] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const { scrollYProgress } = useScroll();

  const heroOpacity = useTransform(scrollYProgress, [0, 0.2], [1, 0]);
  const heroScale = useTransform(scrollYProgress, [0, 0.2], [1, 0.95]);
  const heroY = useTransform(scrollYProgress, [0, 0.3], [0, -80]);

  const cursorX = useMotionValue(0);
  const cursorY = useMotionValue(0);
  const springX = useSpring(cursorX, { damping: 25, stiffness: 300 });
  const springY = useSpring(cursorY, { damping: 25, stiffness: 300 });

  useEffect(() => {
    const timer = setTimeout(() => setIsLoaded(true), 100);
    return () => clearTimeout(timer);
  }, []);

  const handleMouseMove = (e: React.MouseEvent) => {
    const rect = containerRef.current?.getBoundingClientRect();
    if (!rect) return;
    const x = ((e.clientX - rect.left) / rect.width - 0.5) * 2;
    const y = ((e.clientY - rect.top) / rect.height - 0.5) * 2;
    setMouseX(x); setMouseY(y);
    cursorX.set(e.clientX - 16);
    cursorY.set(e.clientY - 16);
  };

  return (
    <div ref={containerRef} onMouseMove={handleMouseMove}
      className="bg-black text-white overflow-x-hidden cursor-none">

      {/* ── Custom cursor ── */}
      <motion.div
        className="fixed top-0 left-0 w-8 h-8 rounded-full border border-cyan-400/60 pointer-events-none z-[9999] mix-blend-screen"
        style={{ x: springX, y: springY }}
      />
      <motion.div
        className="fixed top-0 left-0 w-1.5 h-1.5 rounded-full bg-cyan-400 pointer-events-none z-[9999] -translate-x-1/2 -translate-y-1/2"
        style={{ x: cursorX, y: cursorY }}
      />

      {/* ── NAV ── */}
      <motion.nav
        initial={{ y: -80, opacity: 0 }}
        animate={{ y: 0, opacity: 1 }}
        transition={{ duration: 0.8, delay: 0.2 }}
        className="fixed top-0 left-0 right-0 z-50 flex items-center justify-between px-8 py-5"
        style={{ background: "rgba(0,0,0,0.4)", backdropFilter: "blur(20px)", borderBottom: "1px solid rgba(255,255,255,0.05)" }}
      >
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-400 to-purple-600 flex items-center justify-center font-bold text-sm">TR</div>
          <span className="font-semibold tracking-tight">TenderRadar</span>
          <span className="text-xs text-white/30 font-mono border border-white/10 px-2 py-0.5 rounded-full">v2.0</span>
        </div>

        <div className="hidden md:flex items-center gap-8 text-sm text-white/60">
          {["Product", "Features", "Portals", "Pricing"].map((item) => (
            <motion.a key={item} href={`#${item.toLowerCase()}`} whileHover={{ color: "#ffffff" }}
              className="transition-colors hover:text-white">{item}</motion.a>
          ))}
        </div>

        <div className="flex items-center gap-3">
          <Link href="/dashboard" className="text-sm text-white/60 hover:text-white transition-colors px-4 py-2">Sign in</Link>
          <Link href="/dashboard">
            <motion.button whileHover={{ scale: 1.05 }} whileTap={{ scale: 0.95 }}
              className="text-sm px-5 py-2 rounded-lg font-medium bg-gradient-to-r from-cyan-500 to-purple-600 text-white relative overflow-hidden group">
              <span className="relative z-10">Get Started →</span>
              <div className="absolute inset-0 bg-gradient-to-r from-purple-600 to-cyan-500 opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
            </motion.button>
          </Link>
        </div>
      </motion.nav>

      {/* ─────────── HERO SECTION ─────────── */}
      <motion.section style={{ opacity: heroOpacity, scale: heroScale, y: heroY }}
        className="relative h-screen flex items-center justify-center overflow-hidden">

        {/* 3D Canvas — full screen */}
        <div className="absolute inset-0">
          <Canvas camera={{ position: [0, 0, 12], fov: 75 }} dpr={[1, 2]}>
            <Suspense fallback={null}>
              <Scene mouseX={mouseX} mouseY={mouseY} />
            </Suspense>
          </Canvas>
        </div>

        {/* Radial gradient overlay */}
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,rgba(0,212,255,0.05)_0%,rgba(0,0,0,0.7)_70%)]" />

        {/* Hero content */}
        <div className="relative z-10 text-center max-w-5xl mx-auto px-6">
          <AnimatePresence>
            {isLoaded && (
              <>
                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.6, delay: 0.5 }}
                  className="inline-flex items-center gap-2 px-4 py-1.5 rounded-full border border-cyan-400/30 bg-cyan-400/5 text-cyan-400 text-sm font-mono mb-8"
                >
                  <span className="w-2 h-2 rounded-full bg-cyan-400 animate-pulse" />
                  Live · 33 portals · 24,000+ tenders indexed
                </motion.div>

                <motion.h1
                  initial={{ opacity: 0, y: 30 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.8, delay: 0.7 }}
                  className="text-7xl md:text-8xl font-bold tracking-tighter mb-6 leading-none"
                >
                  <span className="block">Find tenders.</span>
                  <span className="block bg-gradient-to-r from-cyan-400 via-blue-400 to-purple-500 bg-clip-text text-transparent">
                    Win contracts.
                  </span>
                  <span className="block text-white/40">Automatically.</span>
                </motion.h1>

                <motion.p
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.6, delay: 1.0 }}
                  className="text-xl text-white/50 max-w-2xl mx-auto mb-10 leading-relaxed"
                >
                  AI-powered procurement intelligence that scans 33 global portals, scores every tender against your firm&apos;s profile, and tells you exactly what to bid on — before your competitors even notice it.
                </motion.p>

                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.6, delay: 1.2 }}
                  className="flex flex-col sm:flex-row items-center justify-center gap-4"
                >
                  <Link href="/dashboard">
                    <motion.button
                      whileHover={{ scale: 1.05, boxShadow: "0 0 40px rgba(0, 212, 255, 0.4)" }}
                      whileTap={{ scale: 0.97 }}
                      className="group relative px-8 py-4 rounded-xl font-semibold text-base overflow-hidden bg-gradient-to-r from-cyan-500 to-purple-600"
                    >
                      <span className="relative z-10 flex items-center gap-2">
                        Enter Dashboard
                        <motion.span animate={{ x: [0, 4, 0] }} transition={{ repeat: Infinity, duration: 1.5 }}>→</motion.span>
                      </span>
                      <div className="absolute inset-0 bg-gradient-to-r from-purple-600 to-cyan-500 opacity-0 group-hover:opacity-100 transition-opacity duration-500" />
                    </motion.button>
                  </Link>

                  <motion.button
                    whileHover={{ scale: 1.03, borderColor: "rgba(255,255,255,0.3)" }}
                    className="px-8 py-4 rounded-xl font-semibold text-base border border-white/10 bg-white/5 backdrop-blur-sm hover:bg-white/10 transition-all duration-300"
                  >
                    Watch Demo ▶
                  </motion.button>
                </motion.div>
              </>
            )}
          </AnimatePresence>
        </div>

        {/* Scroll indicator */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 2 }}
          className="absolute bottom-8 left-1/2 -translate-x-1/2 flex flex-col items-center gap-2"
        >
          <span className="text-xs text-white/30 font-mono tracking-widest uppercase">Scroll</span>
          <motion.div
            animate={{ y: [0, 8, 0] }}
            transition={{ repeat: Infinity, duration: 1.5 }}
            className="w-px h-8 bg-gradient-to-b from-white/30 to-transparent"
          />
        </motion.div>
      </motion.section>

      {/* ─────────── TICKER ─────────── */}
      <div className="relative z-10 bg-black">
        <TickerTape />
      </div>

      {/* ─────────── STATS ─────────── */}
      <section className="relative z-10 bg-black py-24 px-6">
        <div className="max-w-6xl mx-auto">
          <motion.div
            initial={{ opacity: 0 }}
            whileInView={{ opacity: 1 }}
            viewport={{ once: true }}
            className="grid grid-cols-2 md:grid-cols-4 gap-px bg-white/5 rounded-2xl overflow-hidden border border-white/5"
          >
            {[
              { value: 24000, suffix: "+", label: "Tenders Indexed", color: "from-cyan-400 to-blue-500" },
              { value: 33, suffix: "", label: "Live Portals", color: "from-purple-400 to-pink-500" },
              { value: 98, suffix: "%", label: "Dedup Accuracy", color: "from-cyan-400 to-teal-500" },
              { value: 5, suffix: "s", label: "Semantic Search", color: "from-violet-400 to-purple-600" },
            ].map((stat, i) => (
              <motion.div
                key={i}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ delay: i * 0.1 }}
                className="bg-black px-8 py-10 text-center group hover:bg-white/3 transition-colors duration-300"
              >
                <div className={`text-5xl font-bold bg-gradient-to-r ${stat.color} bg-clip-text text-transparent mb-2`}>
                  <AnimatedNumber value={stat.value} suffix={stat.suffix} />
                </div>
                <div className="text-sm text-white/40 font-mono">{stat.label}</div>
              </motion.div>
            ))}
          </motion.div>
        </div>
      </section>

      {/* ─────────── PORTALS SECTION ─────────── */}
      <section id="portals" className="relative z-10 bg-black py-24 px-6 overflow-hidden">
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top,rgba(124,58,237,0.08)_0%,transparent_60%)]" />
        <div className="max-w-6xl mx-auto relative">
          <motion.div initial={{ opacity: 0, y: 20 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} className="text-center mb-16">
            <span className="text-xs font-mono text-cyan-400 tracking-widest uppercase">Coverage</span>
            <h2 className="text-4xl md:text-5xl font-bold mt-3 tracking-tight">33 Portals. One Dashboard.</h2>
            <p className="text-white/40 mt-4 max-w-lg mx-auto">Every major development funding portal, scraped and scored in real time.</p>
          </motion.div>

          <div className="flex flex-wrap justify-center gap-3">
            {PORTALS.map((portal, i) => (
              <motion.div
                key={portal.name}
                initial={{ opacity: 0, scale: 0.8 }}
                whileInView={{ opacity: 1, scale: 1 }}
                transition={{ delay: i * 0.05 }}
                whileHover={{ scale: 1.08, y: -4 }}
                viewport={{ once: true }}
                className="px-5 py-2.5 rounded-xl border text-sm font-medium relative group"
                style={{ borderColor: `${portal.color}30`, background: `${portal.color}08` }}
              >
                <span style={{ color: portal.color }}>{portal.name}</span>
                <div className="absolute inset-0 rounded-xl opacity-0 group-hover:opacity-20 transition-opacity duration-300" style={{ background: portal.color }} />
              </motion.div>
            ))}
            <motion.div
              initial={{ opacity: 0 }}
              whileInView={{ opacity: 1 }}
              viewport={{ once: true }}
              className="px-5 py-2.5 rounded-xl border border-white/10 text-sm text-white/40"
            >
              + 21 more
            </motion.div>
          </div>
        </div>
      </section>

      {/* ─────────── FEATURES ─────────── */}
      <section id="features" className="relative z-10 bg-black py-24 px-6">
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_bottom,rgba(0,212,255,0.06)_0%,transparent_60%)]" />
        <div className="max-w-6xl mx-auto relative">
          <motion.div initial={{ opacity: 0, y: 20 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} className="text-center mb-16">
            <span className="text-xs font-mono text-purple-400 tracking-widest uppercase">Intelligence Layer</span>
            <h2 className="text-4xl md:text-5xl font-bold mt-3 tracking-tight">Not just alerts. <span className="text-white/40">Decisions.</span></h2>
          </motion.div>

          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-5">
            {[
              { icon: "🧠", title: "3-Pass AI Copilot", desc: "GPT-4o analyzes every tender across 6 dimensions: technical fit, geography, timeline, budget, competition, and team capacity. Returns BID / CONSIDER / SKIP with strategic rationale.", delay: 0, gradient: "linear-gradient(135deg, #00D4FF, #0891B2)" },
              { icon: "⚡", title: "Real-Time Intelligence", desc: "33 scrapers run every 6 hours. New tenders detected within minutes of posting, filtered through your firm's custom keyword taxonomy and priority scoring.", delay: 0.1, gradient: "linear-gradient(135deg, #7C3AED, #a855f7)" },
              { icon: "🎯", title: "Semantic Fit Scoring", desc: "70% semantic similarity + 30% keyword match. Your firm profile (sectors, clients, geography, contract types) is encoded as a vector. Every tender scores against it.", delay: 0.2, gradient: "linear-gradient(135deg, #06B6D4, #0E7490)" },
              { icon: "🔴", title: "Red Flag Detection", desc: "Auto-flags: GOODS_ONLY, EXPIRED, TOO_LARGE ($5M+), INELIGIBLE (UN-only), DUPLICATE (cross-portal). Zero false positives from junk tenders.", delay: 0.3, gradient: "linear-gradient(135deg, #DC2626, #991B1B)" },
              { icon: "📈", title: "ML Feedback Loop", desc: "Every bid decision you make trains the model. After 25 decisions, LogisticRegression predicts bid probability. After 6 months, the system knows your firm better than your own team.", delay: 0.4, gradient: "linear-gradient(135deg, #059669, #047857)" },
              { icon: "🌐", title: "Private White-Label", desc: "Deploy a completely private instance. Your keywords, your branding, your data — isolated from every other firm. Enterprise-grade, on your infrastructure.", delay: 0.5, gradient: "linear-gradient(135deg, #F59E0B, #D97706)" },
            ].map((f) => <FeatureCard key={f.title} {...f} />)}
          </div>
        </div>
      </section>

      {/* ─────────── HOW IT WORKS ─────────── */}
      <section className="relative z-10 bg-black py-24 px-6">
        <div className="max-w-4xl mx-auto">
          <motion.div initial={{ opacity: 0, y: 20 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} className="text-center mb-16">
            <h2 className="text-4xl md:text-5xl font-bold tracking-tight">From scrape to decision <span className="text-white/30">in 90 seconds.</span></h2>
          </motion.div>

          <div className="space-y-px">
            {[
              { step: "01", label: "Scrape", desc: "33 portals scraped every 6 hours. Custom anti-bot logic, CSRF handling, session management.", color: "#00D4FF" },
              { step: "02", label: "Filter & Score", desc: "Hard reject filter removes 90% of noise. Semantic + keyword scoring ranks what remains.", color: "#7C3AED" },
              { step: "03", label: "Deduplicate", desc: "Same tender posted on 4 portals? Detected and merged. 98% accuracy via content hashing + vector similarity.", color: "#06B6D4" },
              { step: "04", label: "AI Enrich", desc: "GPT-4o extracts: budget, deliverables, team required, evaluation weights, eligibility. In structured JSON.", color: "#8B5CF6" },
              { step: "05", label: "Copilot Decides", desc: "3-pass reasoning: Extract → Assess → Recommend. Returns BID/CONSIDER/SKIP + specific strategy steps.", color: "#0891B2" },
              { step: "06", label: "You Win", desc: "Telegram alert fires. Dashboard updates. You act on intelligence, not noise.", color: "#6D28D9" },
            ].map((item, i) => (
              <motion.div
                key={item.step}
                initial={{ opacity: 0, x: -20 }}
                whileInView={{ opacity: 1, x: 0 }}
                viewport={{ once: true }}
                transition={{ delay: i * 0.1 }}
                className="flex items-start gap-6 p-6 rounded-xl hover:bg-white/3 transition-colors duration-300 group border border-transparent hover:border-white/5"
              >
                <div className="text-2xl font-mono font-bold shrink-0 w-10" style={{ color: item.color }}>{item.step}</div>
                <div>
                  <div className="font-semibold mb-1" style={{ color: item.color }}>{item.label}</div>
                  <p className="text-white/40 text-sm leading-relaxed">{item.desc}</p>
                </div>
              </motion.div>
            ))}
          </div>
        </div>
      </section>

      {/* ─────────── FINAL CTA ─────────── */}
      <section className="relative z-10 bg-black py-32 px-6 overflow-hidden">
        <div className="absolute inset-0 h-full">
          <Canvas camera={{ position: [0, 0, 8], fov: 75 }}>
            <ambientLight intensity={0.05} />
            <pointLight position={[0, 0, 5]} intensity={3} color="#00D4FF" />
            <NeuralOrb position={[0, 0, 0]} color="#0891B2" speed={0.5} distort={0.7} />
            <ParticleField />
          </Canvas>
        </div>
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,rgba(0,0,0,0.4)_0%,rgba(0,0,0,0.95)_70%)]" />

        <div className="relative z-10 max-w-3xl mx-auto text-center">
          <motion.div initial={{ opacity: 0, y: 30 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }}>
            <h2 className="text-5xl md:text-7xl font-bold tracking-tighter mb-6">
              Stop missing <span className="bg-gradient-to-r from-cyan-400 to-purple-500 bg-clip-text text-transparent">tenders.</span>
            </h2>
            <p className="text-xl text-white/40 mb-10">
              One World Bank contract pays for 10 years of TenderRadar. You&apos;ve been missing them for years already.
            </p>
            <Link href="/dashboard">
              <motion.button
                whileHover={{ scale: 1.06, boxShadow: "0 0 60px rgba(0,212,255,0.5)" }}
                whileTap={{ scale: 0.97 }}
                className="px-12 py-5 rounded-2xl text-lg font-bold bg-gradient-to-r from-cyan-500 via-blue-500 to-purple-600 relative overflow-hidden group"
              >
                <span className="relative z-10">Open Dashboard →</span>
                <div className="absolute inset-0 bg-gradient-to-r from-purple-600 via-blue-500 to-cyan-500 opacity-0 group-hover:opacity-100 transition-opacity duration-500" />
              </motion.button>
            </Link>
          </motion.div>
        </div>
      </section>

      {/* ─────────── FOOTER ─────────── */}
      <footer className="relative z-10 bg-black border-t border-white/5 py-8 px-6">
        <div className="max-w-6xl mx-auto flex flex-col md:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-2">
            <div className="w-6 h-6 rounded bg-gradient-to-br from-cyan-400 to-purple-600 flex items-center justify-center text-xs font-bold">TR</div>
            <span className="text-sm text-white/40">TenderRadar — Procurement Intelligence</span>
          </div>
          <span className="text-xs text-white/20 font-mono">Proprietary · IDCG · 2026</span>
        </div>
      </footer>
    </div>
  );
}
