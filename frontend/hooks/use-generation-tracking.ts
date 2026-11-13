"use client";

import { useEffect } from "react";
import { useArtifactStore } from "@/lib/artifact-store";
import { useAuthStore } from "@/lib/auth-store";
import { usePaperStore } from "@/lib/paper-store";
import { getGenerationBatch, getPaperGenerations } from "@/lib/api/generation";

// Batch outputs are server-orchestrated. Poll durable status to recover from
// dropped event streams, background tabs, reloads and missed completion events.
export function useGenerationTracking() {
  const token = useAuthStore(s => s.token);
  const paperId = usePaperStore(s => s.paperId);

  useEffect(() => {
    if (!token) return;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout>;
    let recoveredPaper = false;
    const isCurrent = () => !stopped && useAuthStore.getState().token === token;

    async function poll() {
      try {
        if (paperId && !recoveredPaper) {
          const { batches } = await getPaperGenerations(paperId);
          if (!isCurrent()) return;
          for (const batch of batches) await useArtifactStore.getState().syncGenerationBatch(batch);
          recoveredPaper = true;
        }
        const pending = useArtifactStore.getState().artifacts.filter(a => a.generationBatchId && a.status !== "done" && a.status !== "error");
        const ids = [...new Set(pending.map(a => a.generationBatchId!))];
        await Promise.all(ids.map(async id => {
          const batch = await getGenerationBatch(id);
          if (isCurrent()) await useArtifactStore.getState().syncGenerationBatch(batch);
        }));
      } catch {
        // A transport failure must not mark otherwise healthy server jobs as
        // failed. Retry status reads; users can safely refresh this page.
      } finally {
        if (isCurrent()) timer = setTimeout(poll, 2500);
      }
    }
    void poll();
    return () => { stopped = true; clearTimeout(timer); };
  }, [token, paperId]);
}
