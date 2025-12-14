import * as XLSX from 'xlsx';

export function rowsFromXlsx(buf: ArrayBuffer, opts?: { sheetName?: string }): string[][] {
  const wb = XLSX.read(buf, { type: 'array' });
  const sheetName = opts?.sheetName ?? wb.SheetNames?.[0];
  if (!sheetName) return [];
  const ws = wb.Sheets[sheetName];
  const raw = XLSX.utils.sheet_to_json(ws, { header: 1, raw: false }) as any[];
  const rows = raw.map((r) => (Array.isArray(r) ? r : [r]).map((c) => (c ?? '').toString().trim()));
  return rows as string[][];
}


