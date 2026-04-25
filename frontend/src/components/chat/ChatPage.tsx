"use client";

import { useState, useRef, useEffect } from "react";
import { Send, Bot, User, Sparkles, ExternalLink, Loader2, Eye, Search, BarChart2, TrendingUp, Target } from "lucide-react";
import { useRouter } from "next/navigation";
import { cn } from "@/lib/utils";
import { handleTenderClick } from "@/lib/tender-links";

interface ChatMsg {
  role: "user" | "assistant";
  content: string;
  sources?: TenderSource[];
}

interface TenderSource {
  tender_id?: string;
  title?: string;
  source_site?: string;
  url?: string;
  sector?: string;
  region?: string;
  composite_score?: number;
  similarity?: number;
  opportunity_insight?: string;
}

const CHAT_URL =
  typeof window !== "undefined"
    ? `${window.location.origin}/api/proxy/chat`
    : "/api/proxy/chat";

async function sendChat(
  messages: { role: string; content: string }[]
): Promise<{ reply: string; source_tenders: TenderSource[] }> {
  const res = await fetch(CHAT_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ messages }),
  });
  if (!res.ok) throw new Error(`Chat API ${res.status}`);
  return res.json();
}

const SUGGESTED_CATEGORIES = [
  {
    label: "Find Opportunities",
    icon: Search,
    color: "text-blue-600 bg-blue-50",
    queries: [
      "Find World Bank education projects in South Asia",
      "Show high-priority UNGM tenders closing this week",
      "List UNDP governance opportunities in East Africa",
    ],
  },
  {
    label: "Compare Tenders",
    icon: BarChart2,
    color: "text-violet-600 bg-violet-50",
    queries: [
      "Compare top 5 health sector tenders by score",
      "Which GEM or UNGM tender has the best fit?",
      "Compare infrastructure vs governance opportunities",
    ],
  },
  {
    label: "Sector Analysis",
    icon: TrendingUp,
    color: "text-amber-600 bg-amber-50",
    queries: [
      "What sectors have the most active tenders?",
      "Analyze climate & environment tender trends",
      "Which regions have the most WASH opportunities?",
    ],
  },
  {
    label: "Bid Strategy",
    icon: Target,
    color: "text-emerald-600 bg-emerald-50",
    queries: [
      "Should we bid on World Bank M&E tenders?",
      "What are the red flags in current urgent tenders?",
      "Recommend top 3 tenders to pursue this week",
    ],
  },
];

const STARTER_CARDS = [
  "Find World Bank education projects in South Asia",
  "Which infrastructure tenders close this week?",
  "Compare top health sector opportunities",
  "Show high-priority GEM tenders above 50 lakhs",
  "Analyze WASH sector trends across portals",
  "Recommend top 3 tenders to pursue this week",
];

export function ChatPage() {
  const router = useRouter();
  const [messages, setMessages] = useState<ChatMsg[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [docCount, setDocCount] = useState<number | null>(null);
  const [activeCategory, setActiveCategory] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: "smooth" });
  }, [messages, loading]);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  useEffect(() => {
    const load = async () => {
      try {
        const url = typeof window !== "undefined"
          ? `${window.location.origin}/api/proxy/stats`
          : "/api/proxy/stats";
        const res = await fetch(url);
        if (res.ok) {
          const d = await res.json();
          setDocCount(d.total_tenders || d.vector_store_docs || null);
        }
      } catch {/* silent */}
    };
    load();
  }, []);

  const handleSend = async (overrideInput?: string) => {
    const q = (overrideInput ?? input).trim();
    if (!q || loading) return;

    const userMsg: ChatMsg = { role: "user", content: q };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");
    setLoading(true);

    try {
      const apiMessages = [...messages, userMsg].map((m) => ({ role: m.role, content: m.content }));
      const data = await sendChat(apiMessages);
      setMessages((prev) => [...prev, {
        role: "assistant",
        content: data.reply,
        sources: data.source_tenders,
      }]);
    } catch {
      setMessages((prev) => [...prev, {
        role: "assistant",
        content: "Unable to reach the analysis engine. Please try again.",
      }]);
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  };

  const activeCat = SUGGESTED_CATEGORIES.find(c => c.label === activeCategory);

  return (
    <div className="flex h-[calc(100vh-56px)] overflow-hidden">

      {/* Left panel — suggestions */}
      <div className="w-[280px] flex-shrink-0 bg-white border-r border-slate-200 flex flex-col overflow-hidden hidden lg:flex">
        {/* Dark header */}
        <div className="bg-slate-900 px-4 py-4">
          <div className="flex items-center gap-2 mb-1">
            <Sparkles className="h-4 w-4 text-white" />
            <span className="text-sm font-semibold text-white">AI Analyst</span>
          </div>
          <p className="text-[11px] text-slate-400">Powered by GPT-4o</p>
          <p className="text-[11px] text-slate-400">
            {docCount ? `${docCount.toLocaleString()} tenders indexed` : "Connecting…"}
          </p>
        </div>

        <div className="flex-1 overflow-y-auto scrollbar-thin p-3 space-y-1">
          <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider px-2 py-1.5">Suggested Queries</p>
          {SUGGESTED_CATEGORIES.map((cat) => {
            const Icon = cat.icon;
            const isActive = activeCategory === cat.label;
            return (
              <div key={cat.label}>
                <button
                  onClick={() => setActiveCategory(isActive ? null : cat.label)}
                  className={cn(
                    "w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-colors text-left",
                    isActive ? "bg-slate-100 text-slate-900" : "text-slate-700 hover:bg-slate-50"
                  )}
                >
                  <div className={cn("w-6 h-6 rounded flex items-center justify-center flex-shrink-0", cat.color)}>
                    <Icon className="h-3.5 w-3.5" />
                  </div>
                  {cat.label}
                </button>
                {isActive && (
                  <div className="ml-3 mt-1 space-y-1 mb-1">
                    {cat.queries.map((q) => (
                      <button
                        key={q}
                        onClick={() => { setInput(q); inputRef.current?.focus(); }}
                        className="w-full text-left text-xs text-slate-600 px-3 py-2 rounded-md bg-slate-50 hover:bg-slate-100 border border-slate-200 hover:border-slate-300 transition-colors line-clamp-2"
                      >
                        {q}
                      </button>
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* Right panel — chat */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Top bar */}
        <div className="flex items-center gap-3 px-6 py-3 bg-white border-b border-slate-200 flex-shrink-0">
          <div className="h-7 w-7 rounded-md bg-slate-900 flex items-center justify-center">
            <Bot className="h-3.5 w-3.5 text-white" />
          </div>
          <div>
            <h1 className="text-sm font-semibold text-slate-900">AI Analyst</h1>
            <p className="text-[11px] text-slate-500">
              {docCount ? `${docCount.toLocaleString()} tenders indexed` : "Connecting…"}
            </p>
          </div>
          <div className="ml-auto flex items-center gap-1.5">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
            <span className="text-xs text-slate-500">Online</span>
          </div>
        </div>

        {/* Messages */}
        <div ref={scrollRef} className="flex-1 overflow-y-auto scrollbar-thin px-6 py-6 space-y-4 bg-[#f8fafc]">

          {messages.length === 0 && !loading && (
            <div className="flex flex-col items-center justify-center h-full text-center space-y-6 max-w-xl mx-auto">
              <div className="h-14 w-14 rounded-xl bg-white border border-slate-200 shadow-sm flex items-center justify-center">
                <Bot className="h-7 w-7 text-slate-700" />
              </div>
              <div>
                <h2 className="text-base font-semibold text-slate-900 mb-1">Ask about your tenders</h2>
                <p className="text-sm text-slate-500">
                  Search, compare, summarize, or analyze any of the {docCount?.toLocaleString() ?? "…"} indexed procurement opportunities.
                </p>
              </div>
              <div className="grid grid-cols-2 gap-2 w-full">
                {STARTER_CARDS.map((s) => (
                  <button
                    key={s}
                    onClick={() => handleSend(s)}
                    className="text-left text-xs text-slate-600 px-3 py-3 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 hover:border-slate-300 hover:shadow-sm transition-all leading-relaxed"
                  >
                    {s}
                  </button>
                ))}
              </div>
            </div>
          )}

          {messages.map((msg, i) => (
            <div key={i} className={cn("flex gap-2.5", msg.role === "user" ? "justify-end" : "")}>
              {msg.role === "assistant" && (
                <div className="flex-shrink-0 h-7 w-7 rounded-md bg-slate-900 flex items-center justify-center mt-0.5">
                  <span className="text-[9px] font-bold text-white tracking-tight">AI</span>
                </div>
              )}
              <div className={cn(
                "max-w-[82%] rounded-xl px-4 py-3 text-sm leading-relaxed",
                msg.role === "user"
                  ? "bg-slate-900 text-white"
                  : "bg-white border border-slate-200 text-slate-800 shadow-sm"
              )}>
                <div className="whitespace-pre-wrap">{msg.content}</div>

                {msg.sources && msg.sources.length > 0 && (
                  <div className="mt-3 pt-3 border-t border-slate-200 space-y-1.5">
                    <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider">
                      Sources ({msg.sources.length})
                    </p>
                    {msg.sources.slice(0, 8).map((s, j) => (
                      <div
                        key={j}
                        className="flex items-start gap-2 p-2 rounded-lg bg-slate-50 border border-slate-200 hover:bg-white hover:border-slate-300 transition-colors cursor-pointer group"
                        onClick={() => s.tender_id && router.push(`/tenders/${encodeURIComponent(s.tender_id)}`)}
                      >
                        <span className="text-[10px] text-slate-400 mt-0.5 flex-shrink-0 font-mono">[{j + 1}]</span>
                        <div className="flex-1 min-w-0">
                          <p className="text-xs font-medium text-slate-900 truncate">{s.title || "Untitled"}</p>
                          <div className="flex items-center gap-2 mt-0.5 flex-wrap">
                            {s.source_site && (
                              <span className="text-[10px] text-slate-500">{s.source_site}</span>
                            )}
                            {s.composite_score != null && s.composite_score > 0 && (
                              <span className="text-[10px] text-emerald-700 font-semibold bg-emerald-50 px-1 rounded">
                                {Math.round(s.composite_score * 100)}% match
                              </span>
                            )}
                          </div>
                        </div>
                        <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                          {s.tender_id && (
                            <button
                              type="button"
                              onClick={(e) => { e.stopPropagation(); router.push(`/tenders/${encodeURIComponent(s.tender_id!)}`); }}
                              className="text-slate-400 hover:text-slate-700 p-0.5"
                            >
                              <Eye className="h-3 w-3" />
                            </button>
                          )}
                          {s.url && (
                            <button
                              type="button"
                              onClick={(e) => handleTenderClick(e as any, s)}
                              className="text-slate-400 hover:text-slate-700 p-0.5"
                            >
                              <ExternalLink className="h-3 w-3" />
                            </button>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
              {msg.role === "user" && (
                <div className="flex-shrink-0 h-7 w-7 rounded-md bg-slate-100 border border-slate-200 flex items-center justify-center mt-0.5">
                  <User className="h-3.5 w-3.5 text-slate-600" />
                </div>
              )}
            </div>
          ))}

          {loading && (
            <div className="flex gap-2.5">
              <div className="flex-shrink-0 h-7 w-7 rounded-md bg-slate-900 flex items-center justify-center">
                <span className="text-[9px] font-bold text-white tracking-tight">AI</span>
              </div>
              <div className="bg-white border border-slate-200 rounded-xl px-4 py-3 flex items-center gap-2 shadow-sm">
                <Loader2 className="h-3.5 w-3.5 text-slate-400 animate-spin" />
                <span className="text-xs text-slate-500">Analyzing {docCount?.toLocaleString()} tenders…</span>
              </div>
            </div>
          )}
        </div>

        {/* Input */}
        <div className="flex-shrink-0 px-6 py-4 bg-white border-t border-slate-200">
          <div className="flex items-center gap-2 border border-slate-300 rounded-xl bg-white focus-within:border-slate-500 focus-within:shadow-sm transition-all">
            <input
              ref={inputRef}
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleSend()}
              placeholder="Ask about tenders, sectors, portals, or bid strategy…"
              disabled={loading}
              className="flex-1 px-4 py-3 text-sm bg-transparent border-none outline-none text-slate-900 placeholder:text-slate-400 disabled:opacity-50"
            />
            <button
              onClick={() => handleSend()}
              disabled={!input.trim() || loading}
              className={cn(
                "flex items-center justify-center h-9 w-9 rounded-lg mr-1.5 flex-shrink-0 transition-colors",
                input.trim() && !loading
                  ? "bg-slate-900 text-white hover:bg-slate-800"
                  : "bg-slate-100 text-slate-400 cursor-not-allowed"
              )}
            >
              <Send className="h-3.5 w-3.5" />
            </button>
          </div>
          <p className="text-[10px] text-slate-400 text-center mt-2">
            AI-generated responses. Verify critical details with primary sources.
          </p>
        </div>
      </div>
    </div>
  );
}
