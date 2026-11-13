/* eslint-disable @typescript-eslint/no-require-imports */
const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const ts = require("typescript");

// Exercise the actual store slice without a DOM, Firebase or provider calls.
function load(relative, dependencies = {}) {
  const exports = {};
  const source = fs.readFileSync(path.join(__dirname, "..", relative), "utf8");
  const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
  vm.runInNewContext(compiled, { exports, require: name => {
    if (!(name in dependencies)) throw new Error(`Unexpected runtime dependency ${name}`);
    return dependencies[name];
  }, console, setTimeout, clearTimeout });
  return exports;
}
const helpers = load("lib/stores/artifact-helpers.ts");

function fixture(overrides = {}) {
  const calls = [];
  const script = { sections: [{ id: "intro", narration: "Research", bullets: ["Finding"] }] };
  const api = {
    getVideoUrl: async id => `http://localhost/video/${id}`,
    getScript: async () => script,
    getAudioSlide: async () => ({ slides: [] }),
    getPodcastScript: async () => ({ turns: [] }),
    getPodcastAudio: async id => ({ url: `http://localhost/audio/${id}` }),
    getPodcastVideoUrl: async id => `http://localhost/podcast/${id}`,
    getPaperToSlidesDeck: async () => ({ slides_pdf_url: "http://localhost/slides.pdf" }),
    getPaperToSlidesScript: async () => script,
    retryGenerationOutput: async (id, format) => {
      calls.push({ batch: id, format });
      const output = batch.outputs.find(output => output.format === format);
      output.status = "processing";
      output.current_step = format === "podcast" ? "podcast_script_gen" : "script_gen";
      output.updated_at = "2026-10-26T10:01:00Z";
    },
    getGenerationBatch: async () => batch,
    ...overrides,
  };
  const { createGenerationSlice } = load("lib/stores/slices/generation-slice.ts", { "../../api": api, "../artifact-helpers": helpers });
  let state = { artifacts: [] };
  const set = patch => { state = { ...state, ...patch }; };
  const get = () => state;
  state = { ...state, ...createGenerationSlice(set, get, {}) };
  const batch = {
    id: "batch-1", source_run_id: "upload-1", paper_id: "paper-1",
    formats: ["video", "podcast", "slides"], status: "processing",
    outputs: ["video", "podcast", "slides"].map(format => ({ run_id: `run-${format}`, format, status: "processing", current_step: format === "podcast" ? "podcast_script_gen" : "script_gen", updated_at: "2026-10-26T10:00:00Z" })),
  };
  return { get, set, batch, calls };
}

test("one batch creates three stable output cards and repeated sync does not duplicate them", async () => {
  const f = fixture();
  await f.get().syncGenerationBatch(f.batch);
  await f.get().syncGenerationBatch(f.batch);
  assert.equal(f.get().artifacts.length, 3);
  assert.deepEqual(Array.from(f.get().artifacts, a => a.type), ["video", "podcast", "presentation"]);
  assert.ok(f.get().artifacts.every(a => a.generationBatchId === "batch-1" && a.paperId === "paper-1"));
});

test("one failed output leaves siblings running; completed outputs populate existing previews", async () => {
  const f = fixture();
  f.batch.outputs[0].status = "failed";
  f.batch.outputs[0].error_message = "TTS unavailable";
  f.batch.outputs[2].status = "completed";
  await f.get().syncGenerationBatch(f.batch);
  assert.equal(f.get().artifacts[0].status, "error");
  assert.equal(f.get().artifacts[1].status, "generating");
  assert.equal(f.get().artifacts[2].status, "done");
  assert.equal(f.get().artifacts[2].slidesPdfUrl, "http://localhost/slides.pdf");
});

test("all three completion results hydrate the corresponding media/script fields", async () => {
  const f = fixture();
  f.batch.outputs.forEach(output => { output.status = "completed"; });
  await f.get().syncGenerationBatch(f.batch);
  const [video, podcast, slides] = f.get().artifacts;
  assert.ok(f.get().artifacts.every(a => a.status === "done" && a.progress === 100));
  assert.equal(video.downloadUrl, "http://localhost/video/run-video");
  assert.equal(video.scripts[0].voiceoverScript, "Research");
  assert.equal(podcast.downloadUrl, "http://localhost/audio/run-podcast");
  assert.equal(podcast.podcastVideoUrl, "http://localhost/podcast/run-podcast");
  assert.equal(slides.downloadUrl, "http://localhost/slides.pdf");
});

test("a temporary preview fetch failure is recovered without restarting generation", async () => {
  let fail = true;
  const f = fixture({ getPaperToSlidesDeck: async () => { if (fail) throw new Error("offline"); return { slides_pdf_url: "http://localhost/recovered.pdf" }; } });
  f.batch.outputs[2].status = "completed";
  await f.get().syncGenerationBatch(f.batch);
  assert.equal(f.get().artifacts[2].status, "generating");
  fail = false;
  await f.get().syncGenerationBatch(f.batch);
  assert.equal(f.get().artifacts[2].status, "done");
  assert.equal(f.get().artifacts[2].downloadUrl, "http://localhost/recovered.pdf");
});

test("retry targets only the failed branch, including slides-to-presentation mapping", async () => {
  const f = fixture();
  f.batch.outputs[2].status = "failed";
  await f.get().syncGenerationBatch(f.batch);
  await f.get().retryGenerationArtifact(f.get().artifacts[2].id);
  assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].batch, "batch-1");
  assert.equal(f.calls[0].format, "slides");
  assert.equal(f.get().artifacts[2].status, "generating");
  assert.ok(f.get().artifacts[2].progress < 100);
  await f.get().retryGenerationArtifact(f.get().artifacts[2].id);
  assert.equal(f.calls.length, 1);
});

test("pending preview requests cannot restore cards after logout/reset", async () => {
  let resolve;
  const f = fixture({ getPaperToSlidesDeck: () => new Promise(r => { resolve = r; }) });
  f.batch.outputs[2].status = "completed";
  const sync = f.get().syncGenerationBatch(f.batch);
  f.set({ artifacts: [] });
  resolve({ slides_pdf_url: "http://localhost/old-user.pdf" });
  await sync;
  assert.equal(f.get().artifacts.length, 0);
});

test("a stale status response cannot overwrite a newer retry", async () => {
  const f = fixture();
  f.batch.outputs[0].status = "failed";
  const old = JSON.parse(JSON.stringify(f.batch));
  await f.get().syncGenerationBatch(f.batch);
  await f.get().retryGenerationArtifact(f.get().artifacts[0].id);
  await f.get().syncGenerationBatch(old);
  assert.equal(f.get().artifacts[0].status, "generating");
});

test("a status read failure after retry does not mark the queued output failed", async () => {
  const f = fixture({ getGenerationBatch: async () => { throw new Error("network"); } });
  f.batch.outputs[0].status = "failed";
  await f.get().syncGenerationBatch(f.batch);
  await f.get().retryGenerationArtifact(f.get().artifacts[0].id);
  assert.equal(f.get().artifacts[0].status, "generating");
});

test("a new retry run replaces the failed attempt without duplicate cards or stale media", async () => {
  const f = fixture();
  f.batch.outputs[0].status = "failed";
  await f.get().syncGenerationBatch(f.batch);
  const id = f.get().artifacts[0].id;
  f.batch.outputs[0] = { ...f.batch.outputs[0], run_id: "retry-video", status: "processing", updated_at: "2026-10-26T10:01:00Z" };
  await f.get().syncGenerationBatch(f.batch);
  assert.equal(f.get().artifacts.length, 3);
  assert.equal(f.get().artifacts[0].id, id);
  assert.equal(f.get().artifacts[0].runId, "retry-video");
  assert.equal(f.get().artifacts[0].status, "generating");
  assert.equal(f.get().artifacts[0].downloadUrl, undefined);
});
