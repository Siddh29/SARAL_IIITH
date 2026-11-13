"use client";

import { FileSliders, Mic, Video } from "lucide-react";
import type { GenerationFormat } from "@/lib/api/generation";

const formats = [
  { id: "video", label: "Video", description: "Narrated presentation", Icon: Video },
  { id: "podcast", label: "Podcast", description: "Two-host conversation", Icon: Mic },
  { id: "slides", label: "Slides", description: "Presentation PDF", Icon: FileSliders },
] as const;

export default function GenerationFormatPicker({ selected, onChange, disabled = false }: {
  selected: GenerationFormat[];
  onChange: (formats: GenerationFormat[]) => void;
  disabled?: boolean;
}) {
  return (
    <fieldset disabled={disabled} className="my-6 space-y-3">
      <legend className="font-sans text-sm font-semibold text-ink dark:text-white">Create outputs together</legend>
      <p className="text-xs text-ink-muted dark:text-white/70">Choose any combination. Upload once and we’ll create your selected outputs in English.</p>
      <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
        {formats.map(({ id, label, description, Icon }) => (
          <label key={id} className={`flex cursor-pointer items-start gap-3 rounded-xl border p-3 ${selected.includes(id) ? "border-saral-forest bg-saral-forest/10" : "border-pill-border dark:border-darkcardborder"}`}>
            <input type="checkbox" checked={selected.includes(id)} onChange={event => onChange(event.target.checked ? [...selected, id] : selected.filter(format => format !== id))} className="mt-1 accent-saral-forest" />
            <span><span className="flex items-center gap-1.5 text-sm font-semibold text-ink dark:text-white"><Icon size={15} />{label}</span><span className="mt-1 block text-xs text-ink-muted dark:text-white/70">{description}</span></span>
          </label>
        ))}
      </div>
      <p className="text-xs text-ink-muted dark:text-white/60">Leave all unchecked to save the paper and choose outputs later.</p>
    </fieldset>
  );
}
