"use client";

import { useArtifactStore } from "@/lib/artifact-store";
import { usePaperStore } from "@/lib/paper-store";

export default function GenerationSummary() {
  const paperId = usePaperStore(s => s.paperId);
  const artifacts = useArtifactStore(s => s.artifacts);
  const outputs = artifacts.filter(a => a.paperId === paperId && a.generationBatchId);
  if (!outputs.length) return null;
  const completed = outputs.filter(a => a.status === "done").length;
  const failed = outputs.filter(a => a.status === "error").length;
  const processing = outputs.length - completed - failed;
  return (
    <div role="status" aria-live="polite" className="mb-5 rounded-xl border border-saral-forest/25 bg-saral-forest/5 px-4 py-3 text-sm text-ink dark:text-white">
      <p className="font-semibold">One upload, {outputs.length} outputs</p>
      <p className="mt-1 text-xs text-ink-muted dark:text-white/70">
        {completed} ready{processing > 0 ? ` · ${processing} generating` : ""}{failed > 0 ? ` · ${failed} failed — retry individual outputs below` : ""}. Your selected formats share the same extracted paper.
      </p>
    </div>
  );
}
