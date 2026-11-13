import type { StateCreator } from "zustand";
import type { ArtifactStore } from "../artifact-store-types";
import type { Artifact } from "../artifact-types";
import type { GenerationBatch, GenerationFormat, GenerationOutput } from "../../api/generation";
import {
  getAudioSlide, getScript, getVideoUrl, getPodcastScript, getPodcastAudio,
  getPodcastVideoUrl, getPaperToSlidesDeck, getPaperToSlidesScript,
  retryGenerationOutput,
  getGenerationBatch,
} from "../../api";
import { scriptSectionsFromRaw, patchArtifact } from "../artifact-helpers";

const artifactType = (format: GenerationFormat) => format === "slides" ? "presentation" : format;

export function generationProgress(output: GenerationOutput): number {
  if (output.status === "completed") return 100;
  const stages = output.format === "podcast"
    ? ["podcast_script_gen", "podcast_tts", "ffmpeg_stitch"]
    : output.format === "slides" ? ["script_gen", "beamer_compile"]
      : ["script_gen", "beamer_compile", "audio_gen", "ffmpeg_stitch"];
  const index = stages.indexOf(output.current_step);
  return index < 0 ? 5 : Math.round(10 + (index / stages.length) * 85);
}

async function loadOutput(output: GenerationOutput): Promise<Partial<Artifact>> {
  if (output.format === "video") {
    const [downloadUrl, script, audio] = await Promise.all([
      getVideoUrl(output.run_id), getScript(output.run_id), getAudioSlide(output.run_id),
    ]);
    return { downloadUrl, rawScript: script, scripts: scriptSectionsFromRaw(script.sections), audioSlides: audio.slides };
  }
  if (output.format === "podcast") {
    const [script, audio, video] = await Promise.all([
      getPodcastScript(output.run_id), getPodcastAudio(output.run_id), getPodcastVideoUrl(output.run_id),
    ]);
    return { podcastScript: script, downloadUrl: audio.url, podcastVideoUrl: video, podcastStep: "complete" };
  }
  const [deck, script] = await Promise.all([getPaperToSlidesDeck(output.run_id), getPaperToSlidesScript(output.run_id)]);
  return {
    slidesPdfUrl: deck.slides_pdf_url, slidesPptxUrl: deck.slides_pptx_url,
    downloadUrl: deck.slides_pdf_url ?? deck.slides_pptx_url,
    rawScript: script, scripts: scriptSectionsFromRaw(script.sections),
  };
}

export const createGenerationSlice: StateCreator<ArtifactStore, [], [], {
  syncGenerationBatch: ArtifactStore["syncGenerationBatch"];
  retryGenerationArtifact: ArtifactStore["retryGenerationArtifact"];
}> = (set, get) => ({
  syncGenerationBatch: async (batch: GenerationBatch) => {
    // Add every output synchronously before fetching completed media. Stable
    // server run IDs prevent duplicate cards after retries or page refreshes.
    set({ artifacts: batch.outputs.reduce<Artifact[]>((artifacts, output) => {
      const existing = artifacts.find(a => a.generationBatchId === batch.id && a.type === artifactType(output.format));
      const id = existing?.id ?? `generation-${batch.id}-${output.format}`;
      if (existing?.generationUpdatedAt && Date.parse(output.updated_at) < Date.parse(existing.generationUpdatedAt)) return artifacts;
      const newAttempt = existing?.runId !== output.run_id;
      const failed = output.status === "failed";
      const artifact: Artifact = {
        ...(existing ?? {
          id, type: artifactType(output.format), config: {
            language: "english", audioLanguage: "English", textLanguage: "English",
            voiceGender: "female", presentationOutputFormat: "beamer_pdf",
          }, scripts: [], imageAssignments: {},
        }),
        generationBatchId: batch.id, runId: output.run_id, paperId: batch.paper_id,
        generationUpdatedAt: output.updated_at,
        ...(newAttempt ? { downloadUrl: undefined, slidesPdfUrl: undefined, slidesPptxUrl: undefined, rawScript: undefined, scripts: [], audioSlides: undefined, podcastScript: undefined, podcastVideoUrl: undefined } : {}),
        videoPaperRunId: batch.source_run_id, podcastRenderVideo: true,
        status: failed ? "error" : output.status === "completed" && !newAttempt && existing?.status === "done" ? "done" : "generating",
        progress: Math.max(newAttempt ? 0 : existing?.progress ?? 0, generationProgress(output)),
        statusMessage: failed ? output.error_message ?? "Generation failed" : output.status === "completed"
          ? "Your output is ready" : `Creating ${output.format === "slides" ? "slides" : output.format}…`,
        errorMessage: failed ? output.error_message ?? "Generation failed" : undefined,
      };
      return existing ? artifacts.map(a => a.id === id ? artifact : a) : [...artifacts, artifact];
    }, get().artifacts) });

    await Promise.all(batch.outputs.map(async output => {
      const current = get().artifacts.find(a => a.generationBatchId === batch.id && a.type === artifactType(output.format));
      if (!current || current.runId !== output.run_id) return;
      const id = current.id;
      if (output.status !== "completed" || current?.status === "done" || current?.generationUpdatedAt !== output.updated_at) return;
      try {
        const media = await loadOutput(output);
        if (get().artifacts.find(a => a.id === id)?.generationUpdatedAt !== output.updated_at) return;
        // Logout/reset while a request is in flight must not restore old cards.
        set({ artifacts: patchArtifact(get().artifacts, id, { ...media, status: "done", progress: 100, errorMessage: undefined }) });
      } catch {
        // Keep a completed backend output retryable on the next status poll if
        // loading its links failed. Never restart expensive generation for it.
        if (get().artifacts.find(a => a.id === id)?.generationUpdatedAt === output.updated_at) {
          set({ artifacts: patchArtifact(get().artifacts, id, { statusMessage: "Output ready. Reconnecting to load preview…" }) });
        }
      }
    }));
  },

  retryGenerationArtifact: async (id) => {
    const artifact = get().artifacts.find(a => a.id === id);
    if (!artifact?.generationBatchId || artifact.status !== "error") return;
    const format = artifact.type === "presentation" ? "slides" : artifact.type as GenerationFormat;
    set({ artifacts: patchArtifact(get().artifacts, id, { status: "generating", progress: 5, errorMessage: undefined, statusMessage: "Retrying this output…" }) });
    try {
      await retryGenerationOutput(artifact.generationBatchId, format);
    } catch (error) {
      set({ artifacts: patchArtifact(get().artifacts, id, { status: "error", errorMessage: error instanceof Error ? error.message : "Retry failed" }) });
      return;
    }
    // If this read fails, keep polling the optimistic running state. A lost
    // status response must not turn a successfully queued retry into a failure.
    const batch = await getGenerationBatch(artifact.generationBatchId).catch(() => null);
    if (batch) await get().syncGenerationBatch(batch);
  },
});
