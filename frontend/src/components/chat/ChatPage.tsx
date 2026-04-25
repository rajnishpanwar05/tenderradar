"use client";

import { useState, useRef, useEffect } from "react";
import { Send, Bot, User, Sparkles, ExternalLink, Loader2, ArrowLeft, Eye } from "lucide-react";
import Link from "next/link";
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

export function ChatPage() {
  const router = useRouter();
  const [messages, setMessages] = useState<ChatMsg[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [projectCount, setProjectCount] = useState<number | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: "smooth" });
  }, [messages, loading]);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  useEffect(() => {
    const loadStats = async () => {
      try {
        const statsUrl =
          typeof window !== "undefined"
            ? `${window.location.origin}/api/proxy/stats`
            : "/api/proxy/stats";
        const res = await fetch(statsUrl);
        if (res.ok) {
          const data = await res.json();
          setProjectCount(data.vector_store_docs || data.total_tenders || null);
        }
      } catch (err) {
        console.error("Failed to load stats:", err);
      }
    };
    loadStats();
  }, []);

  const handleSend = async () => {
    const q = input.trim();
    if (!q || loading) return;

    const userMsg: ChatMsg = { role: "user", content: q };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");
    setLoading(true);

    try {
      const apiMessages = [...messages, userMsg].map((m) => ({
        role: m.role,
        content: m.content,
      }));
      const data = await sendChat(apiMessages);
      const assistantMsg: ChatMsg = {
        role: "assistant",
        content: data.reply,
        sources: data.source_tenders,
      };
      setMessages((prev) => [...prev, assistantMsg]);
    } catch {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: "Sorry, I encountered an error reaching the intelligence engine. Please try again.",
        },
      ]);
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  };

  const suggestions = [
    "Find me World Bank education projects in South Asia",
    "Which infrastructure tenders are closing this week?",
    "Compare the top 5 health sector opportunities",
    "Show me high-priority GEM tenders above 50 lakhs",
  ];

  return (
    <div className="flex flex-col h-full max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
      <div className="shell-panel rounded-[1.5rem] px-5 py-4 flex items-center gap-4">
        <Link href="/dashboard" className="text-slate-500 hover:text-slate-900 transition-colors">
          <ArrowLeft className="h-4 w-4" />
        </Link>
        <div className="flex items-center gap-2">
          <div className="h-8 w-8 rounded-lg bg-slate-900 flex items-center justify-center">
            <Sparkles className="h-4 w-4 text-white" />
          </div>
          <div>
            <h1 className="text-sm font-semibold text-slate-950 leading-none">TenderRadar AI</h1>
            <p className="text-[10px] text-slate-500 font-medium uppercase tracking-widest">
              RAG intelligence · {projectCount ? `${projectCount.toLocaleString()} projects indexed` : "Loading..."}
            </p>
          </div>
        </div>
        <div className="ml-auto flex items-center gap-2">
          <span className="h-2 w-2 rounded-full bg-emerald-500 animate-pulse" />
          <span className="text-[10px] text-slate-500 font-medium">ONLINE</span>
        </div>
      </div>

      <div ref={scrollRef} className="flex-1 overflow-y-auto scrollbar-thin py-6 space-y-6">
        {messages.length === 0 && !loading && (
          <div className="shell-panel rounded-[2rem] flex flex-col items-center justify-center h-full text-center animate-soft-rise px-6 py-16">
            <div className="h-16 w-16 rounded-2xl bg-slate-900 flex items-center justify-center mb-6">
              <Bot className="h-8 w-8 text-white" />
            </div>
            <h2 className="text-xl font-semibold text-slate-950 mb-2">Ask me anything about your tenders</h2>
            <p className="text-sm text-slate-500 max-w-md mb-8">
              I have indexed {projectCount ? `all ${projectCount.toLocaleString()} projects` : "all projects"} in your database. Ask me to find, compare, summarize, or analyze any opportunity.
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 w-full max-w-lg">
              {suggestions.map((suggestion) => (
                <button
                  key={suggestion}
                  onClick={() => {
                    setInput(suggestion);
                    inputRef.current?.focus();
                  }}
                  className="text-left text-xs text-slate-600 px-3 py-2.5 rounded-lg border border-slate-200 bg-white hover:border-slate-300 hover:text-slate-900 transition-all"
                >
                  &ldquo;{suggestion}&rdquo;
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} className={cn("flex gap-3 animate-soft-rise", msg.role === "user" ? "justify-end" : "")}>
            {msg.role === "assistant" && (
              <div className="flex-shrink-0 h-7 w-7 rounded-lg bg-slate-900 flex items-center justify-center mt-0.5">
                <Bot className="h-3.5 w-3.5 text-white" />
              </div>
            )}
            <div
              className={cn(
                "max-w-[82%] rounded-2xl px-4 py-3 text-sm leading-relaxed shadow-sm",
                msg.role === "user"
                  ? "bg-slate-900 text-white rounded-br-md"
                  : "bg-white border border-slate-200 text-slate-800 rounded-bl-md"
              )}
            >
              <div className="whitespace-pre-wrap">{msg.content}</div>

              {msg.sources && msg.sources.length > 0 && (
                <div className="mt-4 pt-3 border-t border-slate-200 space-y-2">
                  <p className="text-[10px] font-medium text-slate-500 uppercase tracking-wider mb-2">
                    Sources ({msg.sources.length} projects)
                  </p>
                  {msg.sources.map((s, j) => (
                    <div
                      key={j}
                      className="flex items-start gap-2 p-2.5 rounded-xl bg-slate-50 border border-slate-200 hover:border-slate-300 transition-colors group cursor-pointer"
                      onClick={() => {
                        if (s.tender_id) {
                          router.push(`/tenders/${encodeURIComponent(s.tender_id)}`);
                        }
                      }}
                    >
                      <span className="text-[10px] font-medium text-slate-500 mt-0.5">[{j + 1}]</span>
                      <div className="flex-1 min-w-0">
                        <p className="text-xs font-medium text-slate-900 truncate leading-snug group-hover:text-slate-700 transition-colors">
                          {s.title || "Untitled"}
                        </p>
                        <div className="flex items-center gap-2 mt-1 flex-wrap">
                          {s.source_site && (
                            <span className="text-[10px] px-1.5 py-0.5 rounded bg-white text-slate-500 border border-slate-200">
                              {s.source_site}
                            </span>
                          )}
                          {s.sector && s.sector !== "unknown" && (
                            <span className="text-[10px] text-slate-500">{s.sector}</span>
                          )}
                          {s.region && s.region !== "global" && (
                            <span className="text-[10px] text-slate-500">{s.region}</span>
                          )}
                          {s.composite_score != null && s.composite_score > 0 && (
                            <span className="text-[10px] text-slate-700 font-medium">
                              {Math.round(s.composite_score * 100)}% relevance
                            </span>
                          )}
                        </div>
                        {s.opportunity_insight && (
                          <p className="text-[10px] text-slate-500 mt-1 line-clamp-1">
                            {s.opportunity_insight}
                          </p>
                        )}
                      </div>
                      <div className="flex items-center gap-1">
                        {s.tender_id && (
                          <button
                            type="button"
                            onClick={(e) => {
                              e.stopPropagation();
                              router.push(`/tenders/${encodeURIComponent(s.tender_id!)}`);
                            }}
                            className="opacity-0 group-hover:opacity-100 transition-opacity text-slate-500 hover:text-slate-900"
                            title="View Details"
                          >
                            <Eye className="h-3 w-3" />
                          </button>
                        )}
                        {s.url && (
                          <button
                            type="button"
                            onClick={(e) => handleTenderClick(e as any, s)}
                            className="opacity-0 group-hover:opacity-100 transition-opacity text-slate-500 hover:text-slate-900"
                            title="Open on Portal"
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
              <div className="flex-shrink-0 h-7 w-7 rounded-lg bg-slate-100 border border-slate-200 flex items-center justify-center mt-0.5">
                <User className="h-3.5 w-3.5 text-slate-700" />
              </div>
            )}
          </div>
        ))}

        {loading && (
          <div className="flex gap-3 animate-soft-rise">
            <div className="flex-shrink-0 h-7 w-7 rounded-lg bg-slate-900 flex items-center justify-center">
              <Bot className="h-3.5 w-3.5 text-white" />
            </div>
            <div className="bg-white border border-slate-200 rounded-2xl rounded-bl-md px-4 py-3 flex items-center gap-2 shadow-sm">
              <Loader2 className="h-3.5 w-3.5 text-slate-500 animate-spin" />
              <span className="text-xs text-slate-500">
                Analyzing your {projectCount ? `${projectCount.toLocaleString()} ` : ""}projects...
              </span>
            </div>
          </div>
        )}
      </div>

      <div className="pt-2">
        <div className="shell-panel rounded-[1.5rem] p-2 flex items-center gap-2 focus-within:border-slate-400 transition-colors">
          <input
            ref={inputRef}
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSend()}
            placeholder="Ask about your tenders..."
            disabled={loading}
            className="flex-1 bg-transparent border-none outline-none text-sm text-slate-900 placeholder:text-slate-400 px-3 py-2 disabled:opacity-50"
          />
          <button
            onClick={handleSend}
            disabled={!input.trim() || loading}
            className={cn(
              "flex items-center justify-center h-8 w-8 rounded-lg transition-all",
              input.trim() && !loading
                ? "bg-slate-900 text-white hover:bg-slate-800"
                : "bg-slate-100 text-slate-400 cursor-not-allowed"
            )}
          >
            <Send className="h-3.5 w-3.5" />
          </button>
        </div>
        <p className="text-[10px] text-slate-500 text-center mt-2">
          Ask for comparisons, summaries, ranking, or bid/no-bid recommendations.
        </p>
      </div>
    </div>
  );
}
