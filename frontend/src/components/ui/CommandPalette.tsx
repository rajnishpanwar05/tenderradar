"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { Dialog, DialogContent, DialogOverlay } from "@radix-ui/react-dialog";
import { Search, Globe, ChevronRight } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";

export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const router = useRouter();

  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      // Allow Cmd+K or Ctrl+K
      if (e.key === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setOpen((open) => !open);
      }
    };
    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, []);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;
    setOpen(false);
    router.push(`/search?q=${encodeURIComponent(query)}`);
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <AnimatePresence>
        {open && (
          <DialogOverlay forceMount asChild>
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="fixed inset-0 z-[100] bg-slate-900/40 backdrop-blur-sm"
            >
              <DialogContent className="fixed left-[50%] top-[15%] z-[100] w-full max-w-2xl translate-x-[-50%] p-4 outline-none">
                <motion.div
                  initial={{ opacity: 0, scale: 0.95, y: -20 }}
                  animate={{ opacity: 1, scale: 1, y: 0 }}
                  exit={{ opacity: 0, scale: 0.95, y: -20 }}
                  transition={{ duration: 0.2, ease: "easeOut" }}
                  className="overflow-hidden rounded-2xl bg-white shadow-2xl border border-slate-200"
                >
                  <form onSubmit={handleSearch} className="flex items-center px-4 py-4 border-b border-slate-100">
                    <Search className="w-5 h-5 text-slate-400 mr-3" />
                    <input
                      autoFocus
                      className="flex-1 bg-transparent border-none outline-none text-slate-900 placeholder:text-slate-400 text-lg font-medium"
                      placeholder="Search semantic database..."
                      value={query}
                      onChange={(e) => setQuery(e.target.value)}
                    />
                    <div className="flex gap-1 ml-2">
                       <kbd className="bg-slate-100 border border-slate-200 text-slate-500 px-2 py-1 rounded text-xs font-mono font-bold shadow-sm">Enter</kbd>
                    </div>
                  </form>
                  
                  <div className="p-3 space-y-1">
                     <div className="px-3 py-2 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Suggestions</div>
                     {["Health tenders in Africa", "Digital transformation RFPs", "Agriculture in Kenya", "High priority solar panel bids"].map((suggestion) => (
                       <button 
                         key={suggestion}
                         onClick={() => { setQuery(suggestion); setOpen(false); router.push(`/search?q=${encodeURIComponent(suggestion)}`); }} 
                         className="w-full flex items-center justify-between px-3 py-3 rounded-xl hover:bg-slate-50 text-slate-600 transition-colors"
                       >
                         <div className="flex items-center gap-3">
                           <Globe className="w-4 h-4 text-slate-400" />
                           <span className="font-medium">{suggestion}</span>
                         </div>
                         <ChevronRight className="w-4 h-4 text-slate-300" />
                       </button>
                     ))}
                  </div>
                </motion.div>
              </DialogContent>
            </motion.div>
          </DialogOverlay>
        )}
      </AnimatePresence>
    </Dialog>
  );
}
