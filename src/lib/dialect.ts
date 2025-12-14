export type RowOrder = 'asc' | 'desc' | 'unknown';

export type DirectionStrategy = 'split' | 'type' | 'signed' | 'balance' | 'fallback';

export interface DirectionDecision {
  strategy: DirectionStrategy;
  order: RowOrder;
  confidence: number; // 0..1
}

const TYPE_TOKENS = [
  { token: /^(debit|db|dr|withdrawal|withdraw|payment|purchase|pos|fee)$/i, kind: 'expense' as const },
  { token: /^(credit|cr|deposit|refund|reversal|reimb(ursement)?)$/i, kind: 'income' as const }
];

export function normalizeHeaders(headers: string[]): string[] {
  return headers.map((h) =>
    (h ?? '')
      .toString()
      .trim()
      .toLowerCase()
      .replace(/\s+/g, ' ')
  );
}

export function signatureFromHeaders(headers: string[]): string {
  // Stable, privacy-friendly signature: normalized headers joined (no data content)
  // e.g. "date|description|withdrawals|deposits|balance"
  return normalizeHeaders(headers)
    .map((h) => h.replace(/[^a-z0-9 ]/g, ''))
    .join('|');
}

export function inferRowOrderFromDates(dateValues: Array<string | null>): RowOrder {
  const isoDates = dateValues.filter(Boolean) as string[];
  if (isoDates.length < 3) return 'unknown';

  let asc = 0;
  let desc = 0;
  for (let i = 1; i < isoDates.length; i++) {
    const prev = isoDates[i - 1];
    const curr = isoDates[i];
    if (curr > prev) asc++;
    else if (curr < prev) desc++;
  }
  if (asc === 0 && desc === 0) return 'unknown';
  return asc >= desc ? 'asc' : 'desc';
}

export function detectTypeColumn(rows: string[][]): { typeIdx: number; mapping: Record<string, 'income' | 'expense'> } | null {
  if (rows.length < 2) return null;

  const sampleRows = rows.slice(1, Math.min(rows.length, 25));
  const colCount = rows[0].length;

  let bestIdx = -1;
  let bestScore = 0;
  let bestMapping: Record<string, 'income' | 'expense'> = {};

  for (let c = 0; c < colCount; c++) {
    let score = 0;
    const mapping: Record<string, 'income' | 'expense'> = {};

    for (const row of sampleRows) {
      const raw = (row[c] ?? '').trim();
      if (!raw) continue;
      const val = raw.toLowerCase();

      for (const t of TYPE_TOKENS) {
        if (t.token.test(val)) {
          score += 3;
          mapping[val] = t.kind;
          break;
        }
      }

      // soft matches (e.g. "Debit Card", "Credit Interest")
      if (/debit/i.test(raw)) score += 1;
      if (/credit/i.test(raw)) score += 1;
    }

    if (score > bestScore) {
      bestScore = score;
      bestIdx = c;
      bestMapping = mapping;
    }
  }

  if (bestIdx === -1 || bestScore < 6) return null;
  return { typeIdx: bestIdx, mapping: bestMapping };
}

export function mapTypeToDirection(rawType: string | undefined | null): 'income' | 'expense' | null {
  const val = (rawType ?? '').trim();
  if (!val) return null;

  for (const t of TYPE_TOKENS) {
    if (t.token.test(val)) return t.kind;
  }
  if (/debit|withdraw|payment|purchase|fee/i.test(val)) return 'expense';
  if (/credit|deposit|refund|reversal|reimb/i.test(val)) return 'income';
  return null;
}

export function chooseBalanceOrientation(
  balances: number[],
  amounts: number[],
  tolerance = 0.05
): { orientation: 'forward' | 'reverse'; matchRate: number } {
  // orientation=forward => delta = bal[i] - bal[i-1]
  // orientation=reverse => delta = bal[i-1] - bal[i]
  let forwardMatches = 0;
  let reverseMatches = 0;
  let comparisons = 0;

  for (let i = 1; i < balances.length; i++) {
    const amt = Math.abs(amounts[i] ?? 0);
    if (!amt) continue;
    const f = balances[i] - balances[i - 1];
    const r = balances[i - 1] - balances[i];
    comparisons++;
    if (Math.abs(Math.abs(f) - amt) <= tolerance) forwardMatches++;
    if (Math.abs(Math.abs(r) - amt) <= tolerance) reverseMatches++;
  }

  if (comparisons === 0) return { orientation: 'forward', matchRate: 0 };
  const forwardRate = forwardMatches / comparisons;
  const reverseRate = reverseMatches / comparisons;
  return forwardRate >= reverseRate
    ? { orientation: 'forward', matchRate: forwardRate }
    : { orientation: 'reverse', matchRate: reverseRate };
}

export function inferDirectionDecision(args: {
  hasSplit: boolean;
  hasType: boolean;
  hasSignedAmount: boolean;
  hasBalance: boolean;
  rowOrder: RowOrder;
  balanceMatchRate?: number;
}): DirectionDecision {
  const { hasSplit, hasType, hasSignedAmount, hasBalance, rowOrder, balanceMatchRate } = args;

  if (hasSplit) return { strategy: 'split', order: rowOrder, confidence: 1.0 };
  if (hasType) return { strategy: 'type', order: rowOrder, confidence: 0.95 };
  if (hasSignedAmount) return { strategy: 'signed', order: rowOrder, confidence: 0.9 };
  if (hasBalance) return { strategy: 'balance', order: rowOrder, confidence: Math.min(0.6 + (balanceMatchRate ?? 0) * 0.4, 0.95) };
  return { strategy: 'fallback', order: rowOrder, confidence: 0.3 };
}


