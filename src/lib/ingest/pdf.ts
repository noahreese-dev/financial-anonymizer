import { getDocument, GlobalWorkerOptions, type TextItem } from 'pdfjs-dist';
// Vite-friendly PDF worker URL (only loaded when this module is imported)
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import pdfWorkerUrl from 'pdfjs-dist/build/pdf.worker?url';

import type { PdfTextExtractOptions, PdfExtractMeta } from '../ingest';

GlobalWorkerOptions.workerSrc = pdfWorkerUrl as string;

export async function rowsFromPdfText(
  buf: ArrayBuffer,
  opts?: PdfTextExtractOptions
): Promise<{ rows: string[][]; meta: PdfExtractMeta }> {
  const pdf = await getDocument({ data: new Uint8Array(buf) }).promise;
  const pageCount = pdf.numPages;
  const start = Math.max(1, Math.min(opts?.pageStart ?? 1, pageCount));
  const end = Math.max(start, Math.min(opts?.pageEnd ?? pageCount, pageCount));

  const linesAll: string[] = [];
  const colCounts: number[] = [];

  // Extract per page
  for (let p = start; p <= end; p++) {
    const page = await pdf.getPage(p);
    const content = await page.getTextContent();
    const items = (content.items ?? []) as TextItem[];

    // Group items into lines by y coordinate (tolerance buckets)
    const byY = new Map<number, Array<{ x: number; s: string }>>();
    for (const it of items) {
      const s = (it.str ?? '').trim();
      if (!s) continue;
      // transform[4]=x, transform[5]=y (PDF units)
      const t = (it as any).transform as number[] | undefined;
      const x = t?.[4] ?? 0;
      const y = t?.[5] ?? 0;
      const yKey = Math.round(y * 2) / 2; // 0.5-unit buckets
      const arr = byY.get(yKey) ?? [];
      arr.push({ x, s });
      byY.set(yKey, arr);
    }

    const ys = Array.from(byY.keys()).sort((a, b) => b - a); // top-down
    for (const y of ys) {
      const parts = byY.get(y)!;
      parts.sort((a, b) => a.x - b.x);
      const line = parts.map((p2) => p2.s).join(' ').replace(/\s+/g, ' ').trim();
      if (!line) continue;
      // Skip obvious header/footer noise
      if (/page\s+\d+\s+of\s+\d+/i.test(line)) continue;
      linesAll.push(line);
    }
  }

  // Reconstruct rows by splitting on 2+ spaces OR pipe-ish separators
  const splitLine = (line: string) => {
    const cols = line
      .split(/\s{2,}|\s\|\s|\t+/g)
      .map((c) => c.trim())
      .filter((c) => c.length > 0);
    return cols;
  };

  const reconstructed: string[][] = [];
  for (const line of linesAll) {
    const cols = splitLine(line);
    if (cols.length < 2) continue;
    reconstructed.push(cols);
    colCounts.push(cols.length);
  }

  // Determine modal column count and compute a simple confidence
  const freq: Record<number, number> = {};
  for (const n of colCounts) freq[n] = (freq[n] || 0) + 1;
  const modeColumnCount = Object.entries(freq).sort(([, a], [, b]) => b - a)[0]?.[0];
  const mode = modeColumnCount ? parseInt(modeColumnCount, 10) : 0;
  const consistent = mode ? colCounts.filter((n) => n === mode).length : 0;
  const confidence = colCounts.length ? consistent / colCounts.length : 0;

  // Ensure a synthetic header so downstream detection doesn’t treat first data row as header
  const maxCols = Math.max(0, ...reconstructed.map((r) => r.length));
  const header = Array.from({ length: maxCols }, (_, i) => `Column_${i + 1}`);
  const rows: string[][] = [header, ...reconstructed.map((r) => [...r, ...Array(Math.max(0, maxCols - r.length)).fill('')])];

  return {
    rows,
    meta: {
      pageCount,
      pagesProcessed: end - start + 1,
      lineCount: reconstructed.length,
      modeColumnCount: mode,
      confidence
    }
  };
}


