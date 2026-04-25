export const dynamic = "force-dynamic";
import { ChatPage } from "@/components/chat/ChatPage";

export const metadata = {
  title: "AI Chat — TenderRadar",
  description: "Chat with the TenderRadar AI Analyst to find, compare, and analyse procurement opportunities across 19 portals.",
};

export default function ChatRoute() {
  return <ChatPage />;
}
