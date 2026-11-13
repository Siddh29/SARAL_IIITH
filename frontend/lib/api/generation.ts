import { request } from "./client";

export type GenerationFormat = "video" | "podcast" | "slides";

export interface GenerationOutput {
  run_id: string;
  format: GenerationFormat;
  status: "pending" | "processing" | "completed" | "failed";
  current_step: string;
  error_message?: string;
  updated_at: string;
}

export interface GenerationBatch {
  id: string;
  source_run_id: string;
  paper_id: string;
  formats: GenerationFormat[];
  status: "pending" | "processing" | "completed" | "failed" | "partial_failure";
  outputs: GenerationOutput[];
}

export function generateFormats(sourceRunId: string, formats: GenerationFormat[]): Promise<GenerationBatch> {
  return request(`/api/papertovideo/${sourceRunId}/generate-formats`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ formats }),
  });
}

export function getGenerationBatch(id: string): Promise<GenerationBatch> {
  return request(`/api/generations/${id}`);
}

export function getPaperGenerations(paperId: string): Promise<{ batches: GenerationBatch[] }> {
  return request(`/api/papers/${paperId}/generations`);
}

export function retryGenerationOutput(id: string, format: GenerationFormat): Promise<{ run_id: string; status: string }> {
  return request(`/api/generations/${id}/outputs/${format}/retry`, { method: "POST" });
}
