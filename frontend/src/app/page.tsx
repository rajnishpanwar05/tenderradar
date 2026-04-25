"use client";

import React, { useRef, useEffect } from "react";
import { motion, useScroll, useTransform, useInView } from "framer-motion";
import { ReactLenis } from "@studio-freight/react-lenis";
import { Canvas } from "@react-three/fiber";
import { Stars } from "@react-three/drei";
import { ArrowRight, Radar, CheckCircle2, Workflow, Database, Cpu, Search, Activity, ChevronRight } from "lucide-react";
import Link from "next/link";
import { cn } from "@/lib/utils";

// ── UTILS ───────────────────────────────────────────────────────────────────
const ease = [0.16, 1, 0.3, 1] as const;

function FadeIn({ children, delay = 0, className = "" }: { children: React.ReactNode; delay?: number; className?: string }) {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: "-100px" });
  
  return (
    <motion.div
      ref={ref}
      initial={{ opacity: 0, y: 50, scale: 0.9, rotateX: 20 }}
      animate={isInView ? { opacity: 1, y: 0, scale: 1, rotateX: 0 } : { opacity: 0, y: 50, scale: 0.9, rotateX: 20 }}
      transition={{ duration: 1, delay, type: "spring", bounce: 0.4 }}
      style={{ transformPerspective: 1000 }}
      className={className}
    >
      {children}
    </motion.div>
  );
}

// ── FIXED STAR BACKGROUND ───────────────────────────────────────────────────

function StarBackground() {
  return (
    <div className="fixed inset-0 z-0 pointer-events-none bg-black">
      <Canvas camera={{ position: [0, 0, 1] }}>
        <Stars radius={100} depth={50} count={5000} factor={4} saturation={0} fade speed={2} />
      </Canvas>
      {/* Dark vignette to blend edges */}
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,_transparent_0%,_#000000_100%)] opacity-80" />
    </div>
  );
}

// ── PERFORMANT BACKGROUNDS ──────────────────────────────────────────────────

function GridBackground() {
  return (
    <div className="absolute inset-0 pointer-events-none overflow-hidden z-0">
      {/* Hardware-accelerated CSS grid, incredibly lightweight */}
      <div 
        className="absolute inset-0" 
        style={{
          backgroundImage: `linear-gradient(to right, rgba(255,255,255,0.05) 1px, transparent 1px), linear-gradient(to bottom, rgba(255,255,255,0.05) 1px, transparent 1px)`,
          backgroundSize: '4rem 4rem',
          maskImage: 'linear-gradient(to bottom, black 20%, transparent 100%)',
          WebkitMaskImage: 'linear-gradient(to bottom, black 20%, transparent 100%)'
        }}
      />
      {/* Subtle performant glow at the very top */}
      <div className="absolute top-[-20%] left-1/2 -translate-x-1/2 w-[600px] h-[300px] bg-indigo-500/20 blur-[100px] rounded-full mix-blend-screen" />
    </div>
  );
}

// ── SECTIONS ────────────────────────────────────────────────────────────────

function HeroSection() {
  const { scrollY } = useScroll();
  const y = useTransform(scrollY, [0, 500], [0, 200]);
  const opacity = useTransform(scrollY, [0, 300], [1, 0]);
  const scale = useTransform(scrollY, [0, 500], [1, 0.8]);

  return (
    <section className="relative min-h-screen flex flex-col items-center justify-center pt-20 px-6 overflow-hidden">
      <GridBackground />
      
      <motion.div style={{ y, opacity, scale }} className="relative z-10 w-full max-w-5xl mx-auto flex flex-col items-center text-center">
        
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.8, ease }}
          className="inline-flex items-center gap-3 px-4 py-2 rounded-full border border-indigo-500/30 bg-indigo-500/10 mb-10 shadow-[0_0_20px_rgba(99,102,241,0.2)]"
        >
          <span className="w-2 h-2 rounded-full bg-indigo-400 animate-pulse" />
          <span className="text-xs font-bold text-indigo-300 tracking-[0.2em] uppercase">Intelligence Network Online</span>
        </motion.div>

        <motion.h1 
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, delay: 0.1, ease }}
          className="text-6xl md:text-8xl font-black tracking-tighter text-white leading-[1.05] mb-8"
        >
          Discover Bids. <br />
          <span className="text-transparent bg-clip-text bg-gradient-to-r from-indigo-400 via-cyan-400 to-emerald-400 pb-2">
            Dominate Markets.
          </span>
        </motion.h1>

        <motion.p 
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, delay: 0.2, ease }}
          className="text-xl md:text-2xl text-slate-400 max-w-2xl font-light leading-relaxed mb-12"
        >
          An AI-powered procurement intelligence platform that monitors 26+ global portals and surfaces the highest-fit opportunities for your firm.
        </motion.p>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, delay: 0.3, ease }}
          className="flex flex-col sm:flex-row gap-4 w-full justify-center"
        >
          <Link href="/dashboard" className="inline-flex items-center justify-center gap-3 px-8 py-4 rounded-xl bg-white text-black font-bold text-sm hover:scale-105 transition-transform shadow-[0_0_40px_rgba(255,255,255,0.2)]">
            Open Command Center <ArrowRight className="w-4 h-4" />
          </Link>
          <a href="#how-it-works" className="inline-flex items-center justify-center gap-3 px-8 py-4 rounded-xl bg-white/5 border border-white/10 text-white font-bold text-sm hover:bg-white/10 transition-colors">
            See the Engine Works
          </a>
        </motion.div>

      </motion.div>
    </section>
  );
}

function DarkBentoFeatures() {
  return (
    <section id="how-it-works" className="relative py-32 px-6 z-10 border-t border-white/5">
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-20">
          <FadeIn>
            <h2 className="text-4xl md:text-5xl font-black tracking-tighter text-white mb-6">
              The Architecture of Insight.
            </h2>
            <p className="text-lg text-slate-400 max-w-2xl mx-auto">
              Our massive data ingestion pipeline brings order to the chaos of global portals.
            </p>
          </FadeIn>
        </div>

        {/* BENTO GRID */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <FadeIn delay={0} className="md:col-span-2 relative group rounded-[2rem] bg-[#0a0a0a] border border-white/10 p-10 overflow-hidden hover:border-indigo-500/50 transition-colors">
            <div className="absolute inset-0 bg-gradient-to-br from-indigo-500/10 to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
            <Database className="w-10 h-10 text-indigo-400 mb-6" />
            <h3 className="text-2xl font-bold text-white mb-4">Unprecedented Scale</h3>
            <p className="text-slate-400 mb-12 max-w-md">We continuously pull unstructured PDFs from 26+ obscure portals into a singular, lightning-fast semantic database.</p>
            <div className="flex gap-8 border-t border-white/10 pt-8">
              <div>
                <div className="text-4xl font-black text-white">26+</div>
                <div className="text-xs text-indigo-400 uppercase tracking-widest font-bold mt-1">Live Portals</div>
              </div>
              <div>
                <div className="text-4xl font-black text-white">2M</div>
                <div className="text-xs text-cyan-400 uppercase tracking-widest font-bold mt-1">Vectors Encoded</div>
              </div>
            </div>
          </FadeIn>

          <FadeIn delay={0.1} className="md:col-span-1 relative group rounded-[2rem] bg-[#0a0a0a] border border-white/10 p-10 overflow-hidden hover:border-emerald-500/50 transition-colors flex flex-col justify-between">
            <div className="absolute inset-0 bg-gradient-to-bl from-emerald-500/10 to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
            <div>
              <Activity className="w-10 h-10 text-emerald-400 mb-6" />
              <h3 className="text-2xl font-bold text-white mb-4">Zero Latency</h3>
              <p className="text-slate-400 mb-8">When a tender drops, we know within minutes. Not weeks.</p>
            </div>
            <div className="p-4 rounded-xl bg-white/5 border border-white/5 flex items-center justify-between">
               <span className="text-xs font-mono text-emerald-400">Ping: 14ms</span>
               <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
            </div>
          </FadeIn>
        </div>
      </div>
    </section>
  );
}

function BrightWhiteFeatures() {
  return (
    <section className="relative py-32 px-6 z-20 mt-32">
      {/* Massive floating white glass plateau */}
      <div className="absolute inset-0 bg-white/95 backdrop-blur-3xl rounded-t-[4rem] border-t border-white shadow-[0_-20px_50px_rgba(255,255,255,0.1)] overflow-hidden" />
      
      <div className="max-w-7xl mx-auto relative z-10 pt-16">
        <div className="flex flex-col md:flex-row items-center gap-16">
          
          <div className="flex-1 w-full">
            <FadeIn>
              <div className="inline-flex items-center gap-3 px-4 py-2 rounded-full border border-slate-200 bg-slate-50 mb-8">
                <Cpu className="w-4 h-4 text-rose-500" />
                <span className="text-xs font-bold text-slate-800 tracking-[0.2em] uppercase">The Neural Agent</span>
              </div>
              <h2 className="text-4xl md:text-6xl font-black tracking-tighter mb-8 text-slate-900 leading-[1.05]">
                Let the AI read <br /> the 300-page RFP.
              </h2>
              <p className="text-xl text-slate-500 font-light leading-relaxed mb-8">
                Outdated systems make you do the reading. Our LLM-powered matrix automatically compares the precise requirements of every bid against your firm's historical capabilities.
              </p>
              <ul className="space-y-4">
                {[
                  "100% data isolation for your capability profile",
                  "Automated mathematical fit-scoring",
                  "Instant summaries of critical mandates"
                ].map((item, i) => (
                  <li key={i} className="flex items-center gap-4 text-slate-700 font-medium">
                    <CheckCircle2 className="w-5 h-5 text-emerald-500" /> {item}
                  </li>
                ))}
              </ul>
            </FadeIn>
          </div>

          <div className="flex-1 w-full space-y-6">
            <FadeIn delay={0.2}>
              <div className="p-8 rounded-[2rem] bg-slate-50 border border-slate-200 shadow-xl flex items-start gap-6 hover:-translate-y-1 hover:shadow-2xl hover:shadow-indigo-500/10 transition-all">
                <div className="w-14 h-14 rounded-2xl bg-indigo-500 flex items-center justify-center shrink-0 shadow-lg shadow-indigo-500/30">
                  <Radar className="w-6 h-6 text-white" />
                </div>
                <div>
                  <h3 className="text-xl font-bold text-slate-900 mb-2">Automated Target Lock</h3>
                  <p className="text-slate-500 leading-relaxed text-sm">We find the exact tender matching your "African Infrastructure" parameters and present it with a 98% Fit Score.</p>
                </div>
              </div>
            </FadeIn>

            <FadeIn delay={0.3}>
              <div className="p-8 rounded-[2rem] bg-white border border-slate-200 shadow-xl flex items-start gap-6 hover:-translate-y-1 hover:shadow-2xl hover:shadow-rose-500/10 transition-all">
                <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-rose-500 to-orange-500 flex items-center justify-center shrink-0 shadow-lg shadow-rose-500/30">
                  <Workflow className="w-6 h-6 text-white" />
                </div>
                <div>
                  <h3 className="text-xl font-bold text-slate-900 mb-2">Beat The Competition</h3>
                  <p className="text-slate-500 leading-relaxed text-sm">You are notified 14 seconds after the procurement portal updates, granting you weeks of extra preparation time.</p>
                </div>
              </div>
            </FadeIn>
          </div>

        </div>

        {/* Call To Action in White Section */}
        <FadeIn delay={0.4} className="mt-32">
          <div className="rounded-[3rem] bg-slate-900 p-16 text-center relative overflow-hidden shadow-2xl">
            <div className="absolute inset-0 bg-gradient-to-br from-indigo-600/40 via-transparent to-emerald-600/20" />
            <h2 className="text-4xl md:text-5xl font-black text-white tracking-tighter mb-6 relative z-10">Stop searching. Start winning.</h2>
            <p className="text-slate-300 text-lg mb-10 max-w-xl mx-auto relative z-10">
              Access the Command Center today and leverage the world's most intelligent procurement capability matrix.
            </p>
            <Link href="/dashboard" className="relative z-10 inline-flex items-center gap-3 px-10 py-5 rounded-xl bg-white text-black font-bold text-sm hover:scale-105 transition-transform shadow-xl">
              Launch Platform <ArrowRight className="w-4 h-4" />
            </Link>
          </div>
        </FadeIn>

      </div>
    </section>
  );
}

function Navbar() {
  const { scrollY } = useScroll();
  const bg = useTransform(scrollY, [0, 100], ["rgba(0,0,0,0)", "rgba(0,0,0,0.85)"]);
  const border = useTransform(scrollY, [0, 100], ["rgba(255,255,255,0)", "rgba(255,255,255,0.1)"]);

  return (
    <motion.nav 
      style={{ backgroundColor: bg, borderBottomColor: border, borderBottomWidth: 1 }}
      className="fixed top-0 inset-x-0 z-50 transition-colors duration-200 backdrop-blur-xl"
    >
      <div className="max-w-7xl mx-auto px-6 h-20 flex items-center justify-between">
        <Link href="/" className="font-black text-lg tracking-tight flex items-center gap-2 text-white">
          <div className="w-3 h-3 bg-indigo-500 rounded-sm shadow-[0_0_10px_rgba(99,102,241,0.8)]" /> ProcureIQ
        </Link>
        <Link href="/dashboard" className="text-sm font-bold text-slate-300 hover:text-white transition-colors">
          Sign In
        </Link>
      </div>
    </motion.nav>
  );
}

export default function LandingPage() {
  return (
    <ReactLenis root>
      <main className="min-h-screen font-sans selection:bg-indigo-500/30 selection:text-white relative bg-transparent">
        <StarBackground />
        <Navbar />
        <HeroSection />
        <DarkBentoFeatures />
        <BrightWhiteFeatures />
      </main>
    </ReactLenis>
  );
}
