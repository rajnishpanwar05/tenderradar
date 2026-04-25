export const dynamic = "force-dynamic";
import { ChatPage } from "@/components/chat/ChatPage";

export const metadata = {
  title: "AI Analyst — ProcureIQ",
  description: "Chat with the ProcureIQ AI Analyst to find, compare, and analyse procurement opportunities across 19 portals.",
};

export default function ChatRoute() {
  return <ChatPage />;
}
