import { getDocument, GlobalWorkerOptions, type TextItem } from 'pdfjs-dist';
// Vite-friendly PDF worker URL (only loaded when this module is imported)
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import pdfWorkerUrl from 'pdfjs-dist/build/pdf.worker?url';

import type { PdfTextExtractOptions, PdfExtractMeta } from '../ingest';

GlobalWorkerOptions.workerSrc = pdfWorkerUrl as string;

// Shared helper: Reconstruct rows from lines
function reconstructRowsFromLines(linesAll: string[]): { rows: string[][]; colCounts: number[] } {
  const splitLine = (line: string) => {
    const cols = line
      .split(/\s{2,}|\s\|\s|\t+/g)
      .map((c) => c.trim())
      .filter((c) => c.length > 0);
    return cols;
  };

  const reconstructed: string[][] = [];
  const colCounts: number[] = [];
  for (const line of linesAll) {
    const cols = splitLine(line);
    if (cols.length < 2) continue;
    reconstructed.push(cols);
    colCounts.push(cols.length);
  }

  return { rows: reconstructed, colCounts };
}

// Shared helper: Create final rows with header
function createFinalRows(reconstructed: string[][]): string[][] {
  const maxCols = Math.max(0, ...reconstructed.map((r) => r.length));
  const header = Array.from({ length: maxCols }, (_, i) => `Column_${i + 1}`);
  return [header, ...reconstructed.map((r) => [...r, ...Array(Math.max(0, maxCols - r.length)).fill('')])];
}

// Shared helper: Calculate confidence
function calculateConfidence(colCounts: number[]): { modeColumnCount: number; confidence: number } {
  const freq: Record<number, number> = {};
  for (const n of colCounts) freq[n] = (freq[n] || 0) + 1;
  const modeColumnCount = Object.entries(freq).sort(([, a], [, b]) => b - a)[0]?.[0];
  const mode = modeColumnCount ? parseInt(modeColumnCount, 10) : 0;
  const consistent = mode ? colCounts.filter((n) => n === mode).length : 0;
  const confidence = colCounts.length ? consistent / colCounts.length : 0;
  return { modeColumnCount: mode, confidence };
}

// Extract text from PDF using text layers (fast, for digital PDFs)
async function extractTextFromPdf(
  pdf: Awaited<ReturnType<typeof getDocument>>['promise'],
  start: number,
  end: number
): Promise<{ linesAll: string[] }> {
  const linesAll: string[] = [];

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

  return { linesAll };
}

// OCR-based extraction for scanned PDFs
async function rowsFromPdfOcr(
  pdf: Awaited<ReturnType<typeof getDocument>>['promise'],
  start: number,
  end: number
): Promise<{ linesAll: string[] }> {
  // Lazy-load Tesseract.js only when OCR is needed
  const { createWorker } = await import('tesseract.js');
  
  // Initialize worker with English language
  // Note: OCR processing is slower than text extraction (may take several seconds per page)
  const worker = await createWorker('eng', 1, {
    logger: () => {} // Suppress console logs for cleaner output
  });
  
  // Configure for financial documents (tables, numbers, dates)
  // PSM 11: Sparse text (good for tables with columns)
  await worker.setParameters({
    tessedit_pageseg_mode: '11', // Sparse text / table mode
  });

  const linesAll: string[] = [];
  const scale = 2; // Scale factor for better OCR quality (higher = better quality but slower)

  try {
    for (let p = start; p <= end; p++) {
      const page = await pdf.getPage(p);
      const viewport = page.getViewport({ scale });
      
      // Create canvas to render PDF page
      const canvas = document.createElement('canvas');
      canvas.width = viewport.width;
      canvas.height = viewport.height;
      const context = canvas.getContext('2d');
      
      if (!context) {
        throw new Error('Failed to get canvas context for OCR');
      }

      // Render PDF page to canvas
      await page.render({
        canvasContext: context,
        viewport: viewport
      }).promise;

      // Perform OCR on the canvas (Tesseract.js accepts canvas elements directly)
      const { data: { text } } = await worker.recognize(canvas);
      
      // Split OCR text into lines and filter empty lines
      const pageLines = text
        .split(/\r?\n/)
        .map(line => line.trim())
        .filter(line => {
          // Filter out empty lines and page number footers
          return line.length > 0 && !/page\s+\d+\s+of\s+\d+/i.test(line);
        });
      
      linesAll.push(...pageLines);
    }
  } catch (error) {
    // If OCR fails, throw error to allow fallback to text extraction results
    throw new Error(`OCR processing failed: ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    // Always clean up worker to free resources
    try {
      await worker.terminate();
    } catch (e) {
      // Ignore termination errors
      console.warn('Error terminating Tesseract worker:', e);
    }
  }

  return { linesAll };
}

export async function rowsFromPdfText(
  buf: ArrayBuffer,
  opts?: PdfTextExtractOptions
): Promise<{ rows: string[][]; meta: PdfExtractMeta }> {
  const pdf = await getDocument({ data: new Uint8Array(buf) }).promise;
  const pageCount = pdf.numPages;
  const start = Math.max(1, Math.min(opts?.pageStart ?? 1, pageCount));
  const end = Math.max(start, Math.min(opts?.pageEnd ?? pageCount, pageCount));

  let linesAll: string[] = [];
  let usedOcr = false;

  // First, try text extraction (fast, works for digital PDFs)
  try {
    const textResult = await extractTextFromPdf(pdf, start, end);
    linesAll = textResult.linesAll;
  } catch (e) {
    // Text extraction failed, will try OCR
    console.warn('Text extraction failed, will try OCR:', e);
    linesAll = [];
  }

  // Check if text extraction yielded poor results
  // Threshold: < 10 lines or empty = likely scanned PDF
  const shouldUseOcr = linesAll.length < 10;

  if (shouldUseOcr) {
    try {
      // Fall back to OCR for scanned PDFs
      const ocrResult = await rowsFromPdfOcr(pdf, start, end);
      linesAll = ocrResult.linesAll;
      usedOcr = true;
    } catch (e) {
      // If OCR also fails, use whatever text we got (might be empty)
      console.warn('OCR failed, using text extraction results:', e);
    }
  }

  // Reconstruct rows from extracted lines
  const { rows: reconstructed, colCounts } = reconstructRowsFromLines(linesAll);
  const { modeColumnCount, confidence } = calculateConfidence(colCounts);
  const rows = createFinalRows(reconstructed);

  return {
    rows,
      meta: {
        pageCount,
        pagesProcessed: end - start + 1,
        lineCount: reconstructed.length,
        modeColumnCount,
        confidence,
        usedOcr: usedOcr || undefined
      }
  };
}
