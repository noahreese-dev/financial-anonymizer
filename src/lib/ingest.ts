export type FileKind = 'csv' | 'xlsx' | 'pdf' | 'unknown';

export interface PdfTextExtractOptions {
  pageStart?: number; // 1-based inclusive
  pageEnd?: number; // 1-based inclusive
}

export interface PdfExtractMeta {
  pageCount: number;
  pagesProcessed: number;
  lineCount: number;
  modeColumnCount: number;
  confidence: number; // 0..1
  usedOcr?: boolean; // true if OCR was used instead of text extraction
}

export function detectFileKind(file: File): FileKind {
  const name = (file.name ?? '').toLowerCase();
  if (name.endsWith('.csv')) return 'csv';
  if (name.endsWith('.xlsx') || name.endsWith('.xls')) return 'xlsx';
  if (name.endsWith('.pdf')) return 'pdf';
  // fallback to mime hints
  if (file.type === 'text/csv') return 'csv';
  if (file.type === 'application/pdf') return 'pdf';
  return 'unknown';
}

export function readFileText(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error('Failed to read file.'));
    reader.onload = () => resolve(typeof reader.result === 'string' ? reader.result : '');
    reader.readAsText(file);
  });
}

export function readFileArrayBuffer(file: File): Promise<ArrayBuffer> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error('Failed to read file.'));
    reader.onload = () => {
      const res = reader.result;
      if (res instanceof ArrayBuffer) resolve(res);
      else reject(new Error('Failed to read file as ArrayBuffer.'));
    };
    reader.readAsArrayBuffer(file);
  });
}

// Small, deterministic CSV parser (supports quotes and commas)
export function rowsFromCsvText(text: string): string[][] {
  const rows: string[][] = [];
  let currentRow: string[] = [];
  let currentVal = '';
  let insideQuote = false;

  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    const nextChar = text[i + 1];

    if (char === '"') {
      if (insideQuote && nextChar === '"') {
        currentVal += '"';
        i++;
      } else {
        insideQuote = !insideQuote;
      }
      continue;
    }

    if (char === ',' && !insideQuote) {
      currentRow.push(currentVal.trim());
      currentVal = '';
      continue;
    }

    if ((char === '\n' || char === '\r') && !insideQuote) {
      if (currentVal || currentRow.length > 0) {
        currentRow.push(currentVal.trim());
        rows.push(currentRow);
        currentRow = [];
        currentVal = '';
      }
      if (char === '\r' && nextChar === '\n') i++;
      continue;
    }

    currentVal += char;
  }

  if (currentVal || currentRow.length > 0) {
    currentRow.push(currentVal.trim());
    rows.push(currentRow);
  }

  return rows;
}

export async function rowsFromXlsx(buf: ArrayBuffer, opts?: { sheetName?: string }): Promise<string[][]> {
  const mod = await import('./ingest/xlsx');
  return mod.rowsFromXlsx(buf, opts);
}

export async function rowsFromPdfText(
  buf: ArrayBuffer,
  opts?: PdfTextExtractOptions
): Promise<{ rows: string[][]; meta: PdfExtractMeta }> {
  const mod = await import('./ingest/pdf');
  return mod.rowsFromPdfText(buf, opts);
}
