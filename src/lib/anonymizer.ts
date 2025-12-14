/**
 * ------------------------------------------------------------------
 * TYPE DEFINITIONS
 * ------------------------------------------------------------------
 */

export interface SanitizedData {
  metadata: {
    sanitized: boolean;
    originalRowCount: number;
    sanitizedRowCount: number;
    detectedColumns: Record<string, ColumnAnalysis>;
    detectedColumnsList?: ColumnAnalysis[];
    rowOrder?: 'asc' | 'desc' | 'unknown';
    directionStrategy?: 'split' | 'type' | 'signed' | 'balance' | 'fallback';
    directionConfidence?: number; // 0..1
    inferenceSummary?: Record<string, number>;
    skipSummary?: Record<string, number>;
    rowsWithBalance?: number;
    rowsWithAmount?: number;
    rowsWithDebitCredit?: number;
    removalReport?: RemovalReport;
  };
  transactions: SanitizedTransaction[];
}

export interface RemovalReport {
  redactions: Record<string, number>;
  idTokensRemoved: number;
  merchantNormalized: number;
  customRemoved: number;
  customTermsCount: number;
  ultraCleanApplied: boolean;
}

export interface PreflightReport {
  headerColumns: number;
  sampleRows: number;
  detectedColumnsList: ColumnAnalysis[];
  rowOrder: 'asc' | 'desc' | 'unknown';
  likelyDirectionStrategy: 'split' | 'type' | 'signed' | 'balance' | 'fallback';
  plannedRedactions: Record<string, number>;
  plannedIdRemovals: number;
  plannedMerchantNormalizations: number;
  plannedCustomRemovals: Record<string, number>;
  examples: Array<{ raw: string; sanitized: string; merchant: string }>;
}

export interface SanitizedTransaction {
  date: string; // ISO 8601 YYYY-MM-DD
  merchant: string; // Stable normalized merchant label (analysis-ready)
  description: string; // Cleaned & Anonymized
  category: string; // Analysis-ready category label
  categoryConfidence: number; // 0..1
  amount: number; // Signed (Negative = Expense, Positive = Income)
  type: 'income' | 'expense';
  inferenceSource: 'explicit' | 'derived_from_balance' | 'derived_from_desc' | 'fallback';
}

export interface ColumnAnalysis {
  columnIndex: number;
  columnType: 'date' | 'amount' | 'debit' | 'credit' | 'balance' | 'description' | 'type' | 'unknown' | 'metadata';
  confidence: number; // 0.0 to 1.0
  sampleValue?: string;
}

export interface AnonymizerOptions {
  maskPii: boolean;
  scrubContacts: boolean;
  fuzzLocation: boolean;
  cleanMetadata: boolean;
  ultraClean: boolean;
  customRemoveTerms: string[];
}

export type OutputFormat = 'json' | 'markdown' | 'csv' | 'text' | 'storyline';
export type DetailLevel = 'minimal' | 'standard' | 'detailed' | 'debug';
export type ExportProfile = 'ai_safe' | 'audit' | 'debug';

/**
 * ------------------------------------------------------------------
 * UTILITIES & HEURISTICS
 * ------------------------------------------------------------------
 */

const PATTERNS = {
  DATE_ISO: /^\d{4}-\d{2}-\d{2}$/,
  DATE_US: /^\d{1,2}\/\d{1,2}\/\d{2,4}$/,
  DATE_EU: /^\d{1,2}[\.-]\d{1,2}[\.-]\d{2,4}$/,
  MONEY: /^-?[$£€¥]?\s?[\d,]+(\.\d+)?$/,
  PII_CREDIT_CARD: /\b(?:\d[ -]*?){13,16}\b/g,
  PII_SSN: /\b\d{3}-\d{2}-\d{4}\b/g,
  PII_EMAIL: /[\w\.-]+@[\w\.-]+\.\w+/g,
  ID_PATTERNS: [
    /(REF|TXN|ID|TRX)[:#]?\s*[A-Z0-9-]{6,}/gi, // IDs labeled
    /\b[A-F0-9]{10,}\b/g, // Long hex strings
    /\b(Store|Shop|#)\s?\#?\d{3,}\b/gi // Store numbers
  ],
  MERCHANT_PREFIXES: /^(SQ \*|PAYPAL \*|UBER \*|TST \*)/i,
  PII_PHONE: /\b(?:\+?\d{1,2}[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}\b/g,
  PII_URL: /\bhttps?:\/\/\S+|\bwww\.\S+/gi,
  PII_ZIP: /\b\d{5}(?:-\d{4})?\b/g,
  PII_ADDRESS: /\b\d{1,6}\s+[A-Za-z0-9.'-]+(?:\s+[A-Za-z0-9.'-]+){0,4}\s+(st|street|ave|avenue|rd|road|blvd|boulevard|ln|lane|dr|drive|ct|court|pl|place|way)\b/gi,
  NOISE_TOKENS: /\b(_[A-Z])\b/g,
  // New Patterns for features
  LOCATIONS: /\b(NY|CA|TX|FL|IL|PA|OH|GA|NC|MI|NJ|VA|WA|AZ|MA|TN|IN|MO|MD|WI|CO|MN|SC|AL|LA|KY|OR|OK|CT|UT|IA|NV|AR|MS|KS|NM|NE|WV|ID|HI|NH|ME|RI|MT|DE|SD|ND|AK|DC|VT|WY)\b|(\s(USA|US|CAN|UK)\s?$)/g,
  NAMES: /\b([A-Z][a-z]+ [A-Z][a-z]+)\b/g, // Very basic name detection (Capitalized Word + Capitalized Word)
  TYPE_VALUES: /\b(debit|credit|dr|cr|withdrawal|deposit|purchase|payment|fee|refund|reversal)\b/i
};

import {
  chooseBalanceOrientation,
  detectTypeColumn,
  inferDirectionDecision,
  inferRowOrderFromDates,
  mapTypeToDirection
} from './dialect';

export class FinancialAnonymizer {
  private options: AnonymizerOptions;

  constructor(options?: Partial<AnonymizerOptions>) {
    this.options = {
      maskPii: true,
      scrubContacts: false,
      fuzzLocation: false,
      cleanMetadata: false,
      ultraClean: false,
      customRemoveTerms: [],
      ...options
    };
  }

  public process(csvText: string): SanitizedData {
    const rows = this.shapeRows(this.parseCSV(csvText));
    return this.processRows(rows);
  }

  public processRows(inputRows: string[][]): SanitizedData {
    const rows = this.shapeRows(inputRows);
    if (rows.length < 2) {
      throw new Error(
        "Not enough data to process. " +
        "Please upload a file with at least a header row and one transaction row. " +
        "This tool is designed for financial statements like bank exports, credit card statements, or transaction histories."
      );
    }

    const columnAnalysis = this.detectColumnsWithContext(rows);
    
    const dateIdx = columnAnalysis.findIndex(c => c.columnType === 'date');
    const descIdx = columnAnalysis.findIndex(c => c.columnType === 'description');
    const amountIdx = columnAnalysis.findIndex(c => c.columnType === 'amount');
    const debitIdx = columnAnalysis.findIndex(c => c.columnType === 'debit');
    const creditIdx = columnAnalysis.findIndex(c => c.columnType === 'credit');
    const balanceIdx = columnAnalysis.findIndex(c => c.columnType === 'balance');
    const typeIdx = columnAnalysis.findIndex(c => c.columnType === 'type');

    if (dateIdx === -1) {
      // Generate a helpful message showing what we DID find
      const detectedTypes = columnAnalysis
        .filter(c => c.columnType !== 'unknown')
        .map(c => `${c.columnType} (column ${c.columnIndex + 1})`)
        .join(', ');
      
      const helpText = detectedTypes 
        ? `We detected: ${detectedTypes}. However, financial data must include dates for each transaction.`
        : "We couldn't identify any standard financial data columns in your file.";

      throw new Error(
        "This doesn't appear to be financial data. " +
        "We couldn't find a Date column. " +
        helpText + " " +
        "Financial data should have columns like: Date, Description, Amount (or Debit/Credit), and optionally Balance. " +
        "Please upload a bank statement, credit card statement, or transaction history (CSV, XLSX, or PDF format)."
      );
    }
    if (amountIdx === -1 && debitIdx === -1 && creditIdx === -1 && balanceIdx === -1) {
      // Show what columns we DID detect
      const dateCol = columnAnalysis.find(c => c.columnType === 'date');
      const descCol = columnAnalysis.find(c => c.columnType === 'description');
      
      let helpText = "Financial statements need monetary values (amounts, debits/credits, or balances).";
      if (dateCol && descCol) {
        helpText = `We found dates and descriptions, but couldn't find any monetary value columns. ${helpText}`;
      } else if (dateCol) {
        helpText = `We found dates, but couldn't find descriptions or monetary values. ${helpText}`;
      }

      throw new Error(
        "This doesn't appear to be financial data. " +
        "We couldn't find any monetary value columns (Amount, Debit/Credit, or Balance). " +
        helpText + " " +
        "Financial data should have columns like: Date, Description, Amount (or Debit/Credit), and optionally Balance. " +
        "Please ensure your file includes transaction amounts in a clear column format."
      );
    }

    // Infer row order from parsed ISO dates
    const isoDates = rows.slice(1).map(r => this.parseDateString(r[dateIdx]));
    const rowOrder = inferRowOrderFromDates(isoDates);

    const transactions: SanitizedTransaction[] = [];
    const balancesForCorrelation: number[] = [];
    const amountsForCorrelation: number[] = [];
    const skipSummary: Record<string, number> = {};
    const incSkip = (key: string) => { skipSummary[key] = (skipSummary[key] || 0) + 1; };

    const removalReport: RemovalReport = {
      redactions: {},
      idTokensRemoved: 0,
      merchantNormalized: 0,
      customRemoved: 0,
      customTermsCount: (this.options.customRemoveTerms ?? []).filter(Boolean).length,
      ultraCleanApplied: !!this.options.ultraClean
    };

    let lastValidBalance: number | null = null;
    const recentAbsDeltas: number[] = [];
    const median = (xs: number[]) => {
      if (xs.length === 0) return 0;
      const s = [...xs].sort((a, b) => a - b);
      const mid = Math.floor(s.length / 2);
      return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
    };

    // Skip header
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      if (row.length < 2) continue; 

      const dateStr = this.parseDateString(row[dateIdx]);
      if (!dateStr) { incSkip('bad_date'); continue; } 

      const rawDesc = descIdx !== -1 ? row[descIdx] : 'Unknown';
      const cleanDesc = this.sanitizeDescription(rawDesc, removalReport);
      const merchant = this.normalizeMerchant(cleanDesc, removalReport);
      const cat = this.categorizeTransaction(rawDesc, cleanDesc, merchant);

      let finalAmount = 0;
      let type: 'income' | 'expense' = 'expense';
      let inference: SanitizedTransaction['inferenceSource'] = 'fallback';

      // --- PRIMARY LOGIC: BALANCE DELTA (Truth source when Balance exists) ---
      // If balances are present, compute delta as the transaction amount.
      // Balance going UP = income, Balance going DOWN = expense
      if (balanceIdx !== -1) {
        const currentBal = this.cleanCurrency(row[balanceIdx]);
        const hasValidCurrent = !isNaN(currentBal) && isFinite(currentBal);

        // Skip obvious statement/summary rows (common source of giant balance jumps)
        const looksLikeBalanceSummary = /(beginning balance|ending balance|starting balance|balance forward|opening balance|closing balance)/i.test(rawDesc);

        if (hasValidCurrent) {
          if (lastValidBalance !== null) {
            // Calculate raw difference between this row and the previous valid balance in file order
            let delta = currentBal - lastValidBalance;

            // If rows are Newest-First (descending), then previous in file is the FUTURE state.
            // Money flow = FutureBalance - PastBalance = -delta
            if (rowOrder === 'desc') delta = -delta;

            const absDelta = Math.abs(delta);
            const recentMed = median(recentAbsDeltas);
            const isImplausible =
              looksLikeBalanceSummary ||
              (recentMed > 0 && absDelta > recentMed * 50 && absDelta > 2500);

            if (isImplausible) {
              incSkip('suspect_balance_row');
            } else if (absDelta > 0.01) {
              finalAmount = delta;
              type = delta > 0 ? 'income' : 'expense';
              inference = 'derived_from_balance';
              recentAbsDeltas.push(absDelta);
              if (recentAbsDeltas.length > 15) recentAbsDeltas.shift();
            }
          }

          // Update last valid balance after processing delta
          lastValidBalance = currentBal;
        }
      }

      // If no balance-derived amount, fall back to explicit signals.
      // --- LOGIC 1: SPLIT COLUMNS ---
      if (finalAmount === 0 && debitIdx !== -1 && creditIdx !== -1) {
         const debitVal = Math.abs(this.cleanCurrency(row[debitIdx]));
         const creditVal = Math.abs(this.cleanCurrency(row[creditIdx]));
         
         if (creditVal > 0) {
           finalAmount = creditVal;
           type = 'income';
           inference = 'explicit';
         } else if (debitVal > 0) {
           finalAmount = -debitVal;
           type = 'expense';
           inference = 'explicit';
         }
      } 
      // --- LOGIC 2: SINGLE AMOUNT COLUMN ---
      else if (finalAmount === 0 && amountIdx !== -1) {
         finalAmount = this.cleanCurrency(row[amountIdx]);
         // If a type column exists, prefer it to set direction even if amount is unsigned
         const typeDir = typeIdx !== -1 ? mapTypeToDirection(row[typeIdx]) : null;
         if (typeDir && finalAmount !== 0) {
           type = typeDir;
           finalAmount = typeDir === 'expense' ? -Math.abs(finalAmount) : Math.abs(finalAmount);
           inference = 'explicit';
         } else {
           if (finalAmount < 0) { type = 'expense'; inference = 'explicit'; }
           else if (finalAmount > 0) { type = 'income'; inference = 'explicit'; }
         }
      }

      if (finalAmount !== 0) {
         transactions.push({
            date: dateStr,
            merchant,
            description: this.options.ultraClean ? merchant : cleanDesc,
            category: cat.category,
            categoryConfidence: cat.confidence,
            amount: finalAmount,
            type: finalAmount > 0 ? 'income' : 'expense',
            inferenceSource: inference
         });

         // Track for balance correlation after initial pass
         if (balanceIdx !== -1) {
           balancesForCorrelation.push(this.cleanCurrency(row[balanceIdx]));
           amountsForCorrelation.push(finalAmount);
         } else {
           balancesForCorrelation.push(0);
           amountsForCorrelation.push(finalAmount);
         }
      }
    }

    // Balance is now the primary signal when present; we keep correlation for diagnostics only.

    // Direction decision for diagnostics
    const hasSplit = debitIdx !== -1 && creditIdx !== -1;
    const hasType = typeIdx !== -1 && amountIdx !== -1;
    // signed amount detection (sample)
    const hasSignedAmount =
      amountIdx !== -1 &&
      rows.slice(1, Math.min(rows.length, 25)).some(r => (r[amountIdx] ?? '').includes('-') || (r[amountIdx] ?? '').includes('('));
    const hasBalance = balanceIdx !== -1;

    const balanceMatchRate = hasBalance && transactions.length >= 2
      ? chooseBalanceOrientation(balancesForCorrelation, amountsForCorrelation, 0.05).matchRate
      : undefined;

    const decision = inferDirectionDecision({ hasSplit, hasType, hasSignedAmount, hasBalance, rowOrder, balanceMatchRate });

    const inferenceSummary: Record<string, number> = {};
    for (const t of transactions) {
      inferenceSummary[t.inferenceSource] = (inferenceSummary[t.inferenceSource] || 0) + 1;
    }

    // Simple presence stats for diagnostics (sampled over processed rows)
    const dataRows = rows.length - 1;
    let rowsWithBalance = 0;
    let rowsWithAmount = 0;
    let rowsWithDebitCredit = 0;
    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      if (balanceIdx !== -1) {
        const b = this.cleanCurrency(r[balanceIdx]);
        if (!isNaN(b) && isFinite(b)) rowsWithBalance++;
      }
      if (amountIdx !== -1) {
        const a = this.cleanCurrency(r[amountIdx]);
        if (!isNaN(a) && isFinite(a) && (r[amountIdx] ?? '') !== '') rowsWithAmount++;
      }
      if (debitIdx !== -1 && creditIdx !== -1) {
        const d = (r[debitIdx] ?? '').trim();
        const c = (r[creditIdx] ?? '').trim();
        if (d !== '' || c !== '') rowsWithDebitCredit++;
      }
    }

    return {
      metadata: {
        sanitized: true,
        originalRowCount: rows.length,
        sanitizedRowCount: transactions.length,
        detectedColumns: Object.fromEntries(columnAnalysis.map(c => [c.columnType, c])),
        detectedColumnsList: columnAnalysis,
        rowOrder,
        directionStrategy: hasBalance ? 'balance' : decision.strategy,
        directionConfidence: decision.confidence,
        inferenceSummary,
        skipSummary,
        rowsWithBalance: dataRows > 0 ? rowsWithBalance : 0,
        rowsWithAmount: dataRows > 0 ? rowsWithAmount : 0,
        rowsWithDebitCredit: dataRows > 0 ? rowsWithDebitCredit : 0,
        removalReport
      },
      transactions
    };
  }

  public preflightRows(inputRows: string[][], opts?: { sampleSize?: number }): PreflightReport {
    const rows = this.shapeRows(inputRows);
    const sampleSize = Math.max(10, Math.min(opts?.sampleSize ?? 100, rows.length - 1));
    const sample = rows.slice(0, 1 + sampleSize);

    const detectedColumnsList = this.detectColumnsWithContext(sample);
    const dateIdx = detectedColumnsList.findIndex(c => c.columnType === 'date');
    const amountIdx = detectedColumnsList.findIndex(c => c.columnType === 'amount');
    const debitIdx = detectedColumnsList.findIndex(c => c.columnType === 'debit');
    const creditIdx = detectedColumnsList.findIndex(c => c.columnType === 'credit');
    const balanceIdx = detectedColumnsList.findIndex(c => c.columnType === 'balance');
    const typeIdx = detectedColumnsList.findIndex(c => c.columnType === 'type');

    const isoDates = dateIdx !== -1 ? sample.slice(1).map(r => this.parseDateString(r[dateIdx])) : [];
    const rowOrder = inferRowOrderFromDates(isoDates);

    const hasSplit = debitIdx !== -1 && creditIdx !== -1;
    const hasType = typeIdx !== -1 && amountIdx !== -1;
    const hasSignedAmount =
      amountIdx !== -1 &&
      sample.slice(1).some(r => (r[amountIdx] ?? '').includes('-') || (r[amountIdx] ?? '').includes('('));
    const hasBalance = balanceIdx !== -1;
    const decision = inferDirectionDecision({ hasSplit, hasType, hasSignedAmount, hasBalance, rowOrder, balanceMatchRate: undefined });

    // Planned removals: run sanitization in dry mode on the description column if present,
    // otherwise scan all cells for PII-like tokens.
    const plannedRedactions: Record<string, number> = {};
    let plannedIdRemovals = 0;
    let plannedMerchantNormalizations = 0;
    const plannedCustomRemovals: Record<string, number> = {};
    const examples: Array<{ raw: string; sanitized: string; merchant: string }> = [];

    const count = (k: string, n = 1) => { plannedRedactions[k] = (plannedRedactions[k] || 0) + n; };
    const sampleRows = sample.slice(1);
    for (const r of sampleRows) {
      const raw = (dateIdx !== -1 && r[dateIdx]) ? (r.join(' ') ?? '') : (r.join(' ') ?? '');
      const descRaw = detectedColumnsList.some(c => c.columnType === 'description') ? (r[detectedColumnsList.findIndex(c => c.columnType === 'description')] ?? '') : raw;

      // Count planned pattern removals in the raw text (before actual replace)
      const s = descRaw ?? '';
      if (PATTERNS.PII_EMAIL.test(s)) count('email');
      if (PATTERNS.PII_PHONE.test(s)) count('phone');
      if (PATTERNS.PII_URL.test(s)) count('url');
      if (PATTERNS.PII_CREDIT_CARD.test(s)) count('card');
      if (PATTERNS.PII_SSN.test(s)) count('ssn');
      if (PATTERNS.PII_ADDRESS.test(s)) count('address');
      if (PATTERNS.PII_ZIP.test(s)) count('zip');
      if (PATTERNS.LOCATIONS.test(s)) count('location');
      if (PATTERNS.ID_PATTERNS.some(p => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (p as any).lastIndex = 0;
        return p.test(s);
      })) plannedIdRemovals++;

      const sanitized = this.sanitizeDescription(descRaw);
      const merchant = this.normalizeMerchant(sanitized);
      if (merchant && merchant !== (sanitized || '').trim()) plannedMerchantNormalizations++;

      // Custom planned removals (per term)
      for (const term of (this.options.customRemoveTerms ?? []).map(t => (t ?? '').trim()).filter(Boolean)) {
        const re = new RegExp(this.escapeRegExp(term), 'gi');
        const matches = (descRaw ?? '').match(re);
        if (matches && matches.length) plannedCustomRemovals[term] = (plannedCustomRemovals[term] || 0) + matches.length;
      }

      if (examples.length < 8 && (sanitized !== (descRaw ?? '').trim() || merchant !== sanitized)) {
        examples.push({ raw: (descRaw ?? '').trim().slice(0, 160), sanitized: sanitized.slice(0, 160), merchant });
      }
    }

    return {
      headerColumns: rows[0]?.length ?? 0,
      sampleRows: sampleRows.length,
      detectedColumnsList,
      rowOrder,
      likelyDirectionStrategy: hasBalance ? 'balance' : decision.strategy,
      plannedRedactions,
      plannedIdRemovals,
      plannedMerchantNormalizations,
      plannedCustomRemovals,
      examples
    };
  }

  public formatData(
    data: SanitizedData,
    format: OutputFormat,
    opts?: { maxRows?: number; highlightTerm?: string; detailLevel?: DetailLevel; profile?: ExportProfile }
  ): string {
    if (!data || !data.transactions.length) return '';
    const maxRows = opts?.maxRows;
    const highlightTerm = opts?.highlightTerm;
    const profile: ExportProfile = opts?.profile ?? 'ai_safe';
    const detailLevel = opts?.detailLevel ?? (profile === 'audit' ? 'standard' : profile === 'debug' ? 'debug' : 'minimal');
    const rows = typeof maxRows === 'number' ? data.transactions.slice(0, Math.max(0, maxRows)) : data.transactions;
    const truncated = typeof maxRows === 'number' && data.transactions.length > maxRows;

    // Helper to highlight text if term is present (case-insensitive)
    const hl = (text: string) => {
      if (!highlightTerm || !text) return text;
      // We need to return a string with <mark> tags.
      // Since this is for display in a <pre> via innerHTML, we must escape existing HTML entities first,
      // BUT formatData is usually creating raw text. 
      // The app.ts sets innerHTML. So we should escape the base text, then wrap matches.
      
      // 1. Escape HTML specials to prevent injection
      const safe = text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
      
      // 2. Highlight
      const escapedTerm = highlightTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const re = new RegExp(`(${escapedTerm})`, 'gi');
      return safe.replace(re, '<mark class="bg-brand-500/30 text-white rounded-sm px-0.5">$1</mark>');
    };

    // Helper: When NOT highlighting, we still need to sanitize for CSV/JSON if we were doing strict escaping,
    // but typically formatData returns raw strings. 
    // HOWEVER, if highlightTerm is ON, we are returning HTML-ready strings.
    // This creates a divergence. 
    // Ideally, app.ts handles HTML escaping. But since we inject <mark> here, we must handle it here.
    
    // Strategy: If highlightTerm is provided, we assume the output is for innerHTML display.
    // We escape ALL values.
    // If highlightTerm is NOT provided, we return raw text (standard behavior).
    
    const fmtVal = (val: string | number | undefined, applyHl = false) => {
      const s = String(val ?? '');
      if (highlightTerm) {
        return applyHl ? hl(s) : s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
      }
      return s;
    };

    switch (format) {
      case 'json':
        let jsonObject: any;
        
        switch (detailLevel) {
          case 'minimal':
            jsonObject = rows.map(t => ({
              date: t.date,
              description: (t.merchant && t.merchant !== 'Unknown') ? t.merchant : t.description,
              amount: t.amount
            }));
            break;
          case 'standard':
            jsonObject = rows.map(t => ({
              date: t.date,
              merchant: t.merchant,
              description: t.description,
              amount: t.amount
            }));
            break;
          case 'detailed':
            jsonObject = rows.map(t => ({
              date: t.date,
              merchant: t.merchant,
              category: t.category,
              description: t.description,
              amount: t.amount
            }));
            break;
          case 'debug':
            jsonObject = rows.map(t => ({
              date: t.date,
              merchant: t.merchant,
              category: t.category,
              description: t.description,
              amount: t.amount,
              type: t.type,
              inferenceSource: t.inferenceSource,
              categoryConfidence: t.categoryConfidence
            }));
            break;
        }
        
        return JSON.stringify(jsonObject, null, 2);
      
      case 'csv':
        let csvHeader = '';
        let csvRows: string[] = [];
        
        switch (detailLevel) {
          case 'minimal':
            csvHeader = 'Date,Description,Amount\n';
            csvRows = rows.map(t => {
              const cleanDesc = (t.merchant && t.merchant !== 'Unknown') ? t.merchant : t.description;
              return `"${fmtVal(t.date)}","${fmtVal(cleanDesc, true).replace(/"/g, '""')}",${t.amount.toFixed(2)}`;
            });
            break;
          case 'standard':
            csvHeader = 'Date,Merchant,Description,Amount\n';
            csvRows = rows.map(t => 
              `"${fmtVal(t.date)}","${fmtVal(t.merchant, true).replace(/"/g, '""')}","${fmtVal(t.description, true).replace(/"/g, '""')}",${t.amount.toFixed(2)}`
            );
            break;
          case 'detailed':
            csvHeader = 'Date,Merchant,Category,Description,Amount\n';
            csvRows = rows.map(t => 
              `"${fmtVal(t.date)}","${fmtVal(t.merchant, true).replace(/"/g, '""')}","${fmtVal(t.category, true).replace(/"/g, '""')}","${fmtVal(t.description, true).replace(/"/g, '""')}",${t.amount.toFixed(2)}`
            );
            break;
          case 'debug':
            csvHeader = 'Date,Merchant,Category,Description,Amount,Type,Source,CategoryConfidence\n';
            csvRows = rows.map(t => 
              `"${fmtVal(t.date)}","${fmtVal(t.merchant, true).replace(/"/g, '""')}","${fmtVal(t.category, true).replace(/"/g, '""')}","${fmtVal(t.description, true).replace(/"/g, '""')}",${t.amount.toFixed(2)},${t.type},${t.inferenceSource},${(t.categoryConfidence ?? 0).toFixed(2)}`
            );
            break;
        }
        // AI Safe profile removes preview comments (cleaner CSV for models)
        return ((profile === 'ai_safe' || !truncated) ? '' : `# PREVIEW: first ${maxRows} rows\n`) + csvHeader + csvRows.join('\n');

      case 'markdown':
        let mdHeader = '';
        let mdRows: string[] = [];
        
        switch (detailLevel) {
          case 'minimal':
            mdHeader = '| Date | Description | Amount |\n|---|---|---:|\n';
            mdRows = rows.map(t => {
              const cleanDesc = (t.merchant && t.merchant !== 'Unknown') ? t.merchant : t.description;
              return `| ${fmtVal(t.date)} | ${fmtVal(cleanDesc, true)} | ${t.amount.toFixed(2)} |`;
            });
            break;
          case 'standard':
            mdHeader = '| Date | Merchant | Description | Amount |\n|---|---|---|---:|\n';
            mdRows = rows.map(t => 
              `| ${fmtVal(t.date)} | ${fmtVal(t.merchant, true)} | ${fmtVal(t.description, true)} | ${t.amount.toFixed(2)} |`
            );
            break;
          case 'detailed':
            mdHeader = '| Date | Merchant | Category | Description | Amount |\n|---|---|---|---|---:|\n';
            mdRows = rows.map(t => 
              `| ${fmtVal(t.date)} | ${fmtVal(t.merchant, true)} | ${fmtVal(t.category, true)} | ${fmtVal(t.description, true)} | ${t.amount.toFixed(2)} |`
            );
            break;
          case 'debug':
            mdHeader = '| Date | Merchant | Category | Description | Amount | Type | Source | Conf |\n|---|---|---|---|---:|---|---|---:|\n';
            mdRows = rows.map(t => 
              `| ${fmtVal(t.date)} | ${fmtVal(t.merchant, true)} | ${fmtVal(t.category, true)} | ${fmtVal(t.description, true)} | ${t.amount.toFixed(2)} | **${t.type.toUpperCase()}** | ${t.inferenceSource} | ${(t.categoryConfidence ?? 0).toFixed(2)} |`
            );
            break;
        }
        // AI Safe profile: omit headers/preview notes for a cleaner AI-ready paste
        if (profile === 'ai_safe') {
          return `${mdHeader}${mdRows.join('\n')}`;
        }
        return `### Sanitized Financial Data\n${truncated ? `\n> PREVIEW: showing first ${maxRows} rows of ${data.transactions.length}\n` : '\n'}\n${mdHeader}${mdRows.join('\n')}`;

      case 'storyline': {
        // Conversational, personalized monthly financial narrative
        type MonthAgg = {
          month: string;
          income: number;
          expense: number;
          merchants: Record<string, { total: number; count: number }>;
          largestInflow?: SanitizedTransaction;
          largestOutflow?: SanitizedTransaction;
          dailySpend: Record<string, number>;
        };

        const byMonth: Record<string, MonthAgg> = {};
        for (const t of rows) {
          const month = (t.date || '').slice(0, 7) || 'unknown';
          const bucket = (byMonth[month] ??= {
            month,
            income: 0,
            expense: 0,
            merchants: {},
            dailySpend: {}
          });

          const merchant = (t.merchant && t.merchant !== 'Unknown') ? t.merchant : t.description;

          if (t.amount > 0) {
            bucket.income += t.amount;
            if (!bucket.largestInflow || t.amount > bucket.largestInflow.amount) {
              bucket.largestInflow = t;
            }
          } else {
            const abs = Math.abs(t.amount);
            bucket.expense += abs;
            if (!bucket.largestOutflow || abs > Math.abs(bucket.largestOutflow.amount)) {
              bucket.largestOutflow = t;
            }

            // Track merchant frequency
            if (!bucket.merchants[merchant]) {
              bucket.merchants[merchant] = { total: 0, count: 0 };
            }
            bucket.merchants[merchant].total += abs;
            bucket.merchants[merchant].count += 1;

            // Track daily spending
            const day = t.date || 'unknown';
            bucket.dailySpend[day] = (bucket.dailySpend[day] || 0) + abs;
          }
        }

        const months = Object.keys(byMonth).sort();
        const lines: string[] = [];

        const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 
                           'July', 'August', 'September', 'October', 'November', 'December'];

        for (const monthKey of months) {
          const b = byMonth[monthKey];
          const [year, monthNum] = monthKey.split('-');
          const monthName = monthNames[parseInt(monthNum) - 1] || monthNum;
          const net = b.income - b.expense;

          lines.push(`## ${monthName} ${year}\n`);

          // Opening summary
          if (b.income > 0 && b.expense > 0) {
            const netVerb = net >= 0 ? 'leaving you with a positive balance of' : 'putting you in the red by';
            lines.push(`You earned $${b.income.toFixed(2)} this month and spent $${b.expense.toFixed(2)}, ${netVerb} $${Math.abs(net).toFixed(2)}.\n`);
          } else if (b.expense > 0) {
            lines.push(`This month you spent $${b.expense.toFixed(2)}.\n`);
          } else if (b.income > 0) {
            lines.push(`You received $${b.income.toFixed(2)} in income this month.\n`);
          }

          // Top spending insights
          const topMerchants = Object.entries(b.merchants)
            .sort(([, a], [, c]) => c.total - a.total)
            .slice(0, 3);

          if (topMerchants.length > 0) {
            const top1 = topMerchants[0];
            lines.push(`Most of your spending went to ${top1[0]} ($${top1[1].total.toFixed(2)})`);
            
            if (topMerchants.length > 1) {
              const others = topMerchants.slice(1).map(([name, data]) => `${name} ($${data.total.toFixed(2)})`).join(' and ');
              lines.push(`, followed by ${others}`);
            }
            lines.push('.\n');
          }

          // Largest transactions
          if (b.largestOutflow) {
            const merchant = (b.largestOutflow.merchant && b.largestOutflow.merchant !== 'Unknown') 
              ? b.largestOutflow.merchant 
              : b.largestOutflow.description;
            const dateStr = new Date(b.largestOutflow.date + 'T00:00:00').toLocaleDateString('en-US', { month: 'long', day: 'numeric' });
            lines.push(`Your biggest single expense was ${merchant} for $${Math.abs(b.largestOutflow.amount).toFixed(2)} on ${dateStr}.`);
          }

          if (b.largestInflow) {
            const merchant = (b.largestInflow.merchant && b.largestInflow.merchant !== 'Unknown') 
              ? b.largestInflow.merchant 
              : b.largestInflow.description;
            const dateStr = new Date(b.largestInflow.date + 'T00:00:00').toLocaleDateString('en-US', { month: 'long', day: 'numeric' });
            lines.push(` On the income side, ${merchant} for $${b.largestInflow.amount.toFixed(2)} came through on ${dateStr}.`);
          }
          lines.push('\n');

          // Daily spending patterns
          const dailyEntries = Object.entries(b.dailySpend);
          if (dailyEntries.length > 0) {
            const spendDays = dailyEntries.length;
            const avgDaily = b.expense / Math.max(1, spendDays);
            const busiest = dailyEntries.sort(([, a], [, c]) => c - a)[0];
            const busiestDate = new Date(busiest[0] + 'T00:00:00').toLocaleDateString('en-US', { month: 'long', day: 'numeric' });
            
            lines.push(`You had ${spendDays} active spending days this month, averaging about $${avgDaily.toFixed(2)} per day. The busiest day was ${busiestDate} when you spent $${busiest[1].toFixed(2)}.\n`);
          }

          // Repeating patterns (potential subscriptions or habits)
          const repeating = Object.entries(b.merchants)
            .filter(([, data]) => data.count >= 3)
            .sort(([, a], [, c]) => c.count - a.count)
            .slice(0, 3);

          if (repeating.length > 0) {
            lines.push(`A few patterns caught my eye: `);
            const patterns = repeating.map(([name, data]) => 
              `${name} appeared ${data.count} times for a total of $${data.total.toFixed(2)}`
            ).join(', ');
            lines.push(`${patterns}. These might be worth reviewing if they're unexpected.\n`);
          }

          lines.push('\n---\n');
        }

        return lines.join('');
      }

      case 'text':
        let textLines: string[] = [];
        
        switch (detailLevel) {
          case 'minimal':
            textLines = rows.map(t => {
              const cleanDesc = (t.merchant && t.merchant !== 'Unknown') ? t.merchant : t.description;
              return `${fmtVal(t.date)} | ${fmtVal(cleanDesc, true)} | ${t.amount.toFixed(2)}`;
            });
            break;
          case 'standard':
            textLines = rows.map(t => 
              `${fmtVal(t.date)} | ${fmtVal(t.merchant, true)} | ${fmtVal(t.description, true)} | ${t.amount.toFixed(2)}`
            );
            break;
          case 'detailed':
            textLines = rows.map(t => 
              `${fmtVal(t.date)} | ${fmtVal(t.merchant, true)} | ${fmtVal(t.category, true)} | ${fmtVal(t.description, true)} | ${t.amount.toFixed(2)}`
            );
            break;
          case 'debug':
            textLines = rows.map(t => 
              `${fmtVal(t.date)} | ${fmtVal(t.merchant, true)} | ${fmtVal(t.category, true)} | ${fmtVal(t.description, true)} | ${t.amount.toFixed(2)} | ${t.type} | ${t.inferenceSource} | conf:${(t.categoryConfidence ?? 0).toFixed(2)}`
            );
            break;
        }
        
        // AI Safe profile removes preview headers (cleaner for prompts)
        return ((profile === 'ai_safe' || !truncated) ? '' : `PREVIEW: first ${maxRows} rows\n\n`) + textLines.join('\n');
        
      default:
        return '';
    }
  }

  private parseCSV(text: string): string[][] {
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
      } else if (char === ',' && !insideQuote) {
        currentRow.push(currentVal.trim());
        currentVal = '';
      } else if ((char === '\n' || char === '\r') && !insideQuote) {
        if (currentVal || currentRow.length > 0) {
          currentRow.push(currentVal.trim());
          rows.push(currentRow);
          currentRow = [];
          currentVal = '';
        }
        if (char === '\r' && nextChar === '\n') i++;
      } else {
        currentVal += char;
      }
    }
    if (currentVal || currentRow.length > 0) {
      currentRow.push(currentVal.trim());
      rows.push(currentRow);
    }
    return rows;
  }

  /**
   * Ensure consistent row width across the CSV.
   * - Pads short rows to header width
   * - Extends header when rows have more columns than the header
   * - Strips BOM from first header cell
   */
  private shapeRows(rows: string[][]): string[][] {
    if (!rows || rows.length === 0) return [];

    // Remove completely empty trailing rows
    const cleaned = rows.filter(r => r && r.some(cell => (cell ?? '').trim() !== ''));
    if (cleaned.length === 0) return [];

    // Ensure there is a header row
    const header = [...(cleaned[0] ?? [])];
    if (header.length > 0) header[0] = header[0].replace(/^\uFEFF/, '');

    let maxCols = header.length;
    for (let i = 1; i < cleaned.length; i++) {
      maxCols = Math.max(maxCols, cleaned[i]?.length ?? 0);
    }

    // Extend header if any row exceeds header width
    while (header.length < maxCols) {
      header.push(`Column_${header.length + 1}`);
    }

    const out: string[][] = [];
    out.push(header);

    for (let i = 1; i < cleaned.length; i++) {
      const row = [...(cleaned[i] ?? [])];
      // Pad missing cells
      while (row.length < maxCols) row.push('');
      // Keep extra cells (already accounted for in maxCols/header)
      out.push(row);
    }
    return out;
  }

  private cleanCurrency(val: string): number {
    if (!val) return 0;
    // Handle ($100) accounting format for negatives
    let clean = val.replace(/[^\d\.\-,()]/g, '');
    if (clean.startsWith('(') && clean.endsWith(')')) {
      clean = '-' + clean.replace(/[\(\)]/g, '');
    }
    return parseFloat(clean.replace(/,/g, ''));
  }

  private parseDateString(val: string): string | null {
    if (!val) return null;
    const d = new Date(val);
    if (!isNaN(d.getTime())) {
      return d.toISOString().split('T')[0];
    }
    return null;
  }

  private detectColumnsWithContext(rows: string[][]): ColumnAnalysis[] {
    if (rows.length < 2) return [];
    
    const headers = rows[0].map(h => h.toLowerCase());
    const sampleRows = rows.slice(1, Math.min(rows.length, 25)); // Look at up to 25 rows
    const colCount = rows[0].length;
    const analysis: ColumnAnalysis[] = [];

    // Track potential split columns
    const numericCols: number[] = [];

    const typeColHint = detectTypeColumn(rows);

    for (let c = 0; c < colCount; c++) {
      let typeScores = { date: 0, amount: 0, balance: 0, description: 0, debit: 0, credit: 0, metadata: 0, type: 0 };
      let validSamples = 0;
      let emptyCount = 0;
      let numericCount = 0;
      let dateLikeCount = 0;
      let textLikeCount = 0;
      let idLikeCount = 0;
      const numericVals: number[] = [];

      // Header Heuristics
      const header = headers[c] || '';
      if (header.includes('date') || header.includes('time')) typeScores.date += 5;
      if (header.includes('desc') || header.includes('memo') || header.includes('particular')) typeScores.description += 5;
      if (header.includes('bal')) typeScores.balance += 5;
      if (header.includes('debit') || header.includes('withdr') || header.includes('out')) typeScores.debit += 5;
      if (header.includes('credit') || header.includes('deposit') || header.includes('in')) typeScores.credit += 5;
      if (header.includes('id') || header.includes('ref') || header.includes('code')) typeScores.metadata += 2;
      if (header.includes('type') || header.includes('transaction type') || header.includes('tran type')) typeScores.type += 5;
      if (typeColHint && typeColHint.typeIdx === c) typeScores.type += 6;
      
      // Content Heuristics
      sampleRows.forEach(row => {
        const val = row[c];
        if (!val) {
          emptyCount++;
          return;
        }
        validSamples++;

        // Date check
        if (PATTERNS.DATE_ISO.test(val) || PATTERNS.DATE_US.test(val) || PATTERNS.DATE_EU.test(val) || !isNaN(Date.parse(val))) {
          typeScores.date += 3;
          dateLikeCount++;
        }

        // Numeric check
        if (PATTERNS.MONEY.test(val)) {
          const num = this.cleanCurrency(val);
          if (!isNaN(num)) {
               numericCount++;
               numericVals.push(num);
               typeScores.amount += 1;
               if (header.includes('debit') && num > 0) typeScores.debit += 2; 
          }
        }

        // Description check
        if (val.length > 5 && isNaN(parseFloat(val)) && !PATTERNS.DATE_ISO.test(val)) {
          typeScores.description += 2;
          textLikeCount++;
        }

        // Metadata check (IDs)
        const looksLikeId = PATTERNS.ID_PATTERNS.some(p => {
          // Some patterns include /g, which makes .test() stateful; reset to be safe.
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (p as any).lastIndex = 0;
          return p.test(val);
        });
        if (looksLikeId) {
          idLikeCount++;
          typeScores.metadata += 2;
        }

        // Type value check
        if (PATTERNS.TYPE_VALUES.test(val)) {
          typeScores.type += 2;
        }
      });

      // Density-based refinements (more robust than strict "all rows present")
      const sampleN = sampleRows.length || 1;
      const nonEmptyDensity = validSamples / sampleN;
      const numericDensity = numericCount / sampleN;

      // Strongly prefer columns that are mostly numeric for amount/balance
      if (numericDensity >= 0.7) typeScores.amount += 2;
      if (numericDensity >= 0.85) typeScores.amount += 2;

      // Balance-like continuity heuristic:
      // Running balance tends to have large magnitude values with relatively small step changes.
      if (numericVals.length >= 3) {
        const absVals = numericVals.map(v => Math.abs(v));
        const meanAbs = absVals.reduce((a, b) => a + b, 0) / absVals.length;
        let deltaCount = 0;
        let meanAbsDelta = 0;
        for (let i = 1; i < numericVals.length; i++) {
          const d = Math.abs(numericVals[i] - numericVals[i - 1]);
          if (isNaN(d)) continue;
          meanAbsDelta += d;
          deltaCount++;
        }
        meanAbsDelta = deltaCount ? meanAbsDelta / deltaCount : 0;
        const ratio = meanAbs > 0 ? meanAbsDelta / meanAbs : 1;
        if (ratio < 0.5 && nonEmptyDensity >= 0.6) typeScores.balance += 2;
        if (ratio < 0.25 && nonEmptyDensity >= 0.7) typeScores.balance += 3;
      }

      // Description: prefer text-rich columns that are not mostly IDs
      const textDensity = textLikeCount / sampleN;
      const idDensity = idLikeCount / sampleN;
      if (textDensity >= 0.5 && idDensity < 0.2) typeScores.description += 2;
      if (idDensity >= 0.4) typeScores.description = Math.max(0, typeScores.description - 2);

      // Normalize Scores
      const maxScore = Math.max(...Object.values(typeScores));
      let bestType: ColumnAnalysis['columnType'] = 'unknown';
      
      // If column is completely empty, mark as unknown
      if (validSamples === 0) {
        bestType = 'unknown';
      } else if (maxScore > 0) {
        bestType = (Object.keys(typeScores) as Array<keyof typeof typeScores>)
          .reduce((a, b) => typeScores[a] > typeScores[b] ? a : b);
      }

      // Refinement: Balance usually has fewer empty cells than debit/credit split columns
      if (bestType === 'amount' && validSamples === sampleRows.length && header.includes('bal')) {
        bestType = 'balance';
      }

      // Force-detect: If this is the LAST column and it's mostly numeric, strongly favor Balance.
      // This handles common bank exports where balance is the rightmost numeric field (even if some rows are blank).
      if (c === colCount - 1 && numericDensity >= 0.5 && nonEmptyDensity >= 0.5) {
        typeScores.balance += 25; // Very strong boost
        bestType = 'balance';
      }

      // Refinement: If it's "amount" but has many empty cells, it might be part of a split pair
      if (bestType === 'amount' && emptyCount > 0) {
         numericCols.push(c);
      }
      
      // Fallback: If heuristic detected 'amount' but header strongly says 'debit'
      if (bestType === 'amount' && typeScores.debit > 2) bestType = 'debit';
      if (bestType === 'amount' && typeScores.credit > 2) bestType = 'credit';

      // Feature: Clean Metadata
      if (this.options.cleanMetadata && bestType === 'metadata') {
        // We just mark it as metadata, so it's ignored in processing loop
      }

      analysis.push({
        columnIndex: c,
        columnType: bestType,
        confidence: Math.min(maxScore / (validSamples * 2 + 5), 1.0),
        sampleValue: sampleRows[0]?.[c]
      });
    }

    // POST-ANALYSIS: Check for "Split" Columns
    if (numericCols.length >= 2) {
        const c1 = numericCols[0];
        const c2 = numericCols[1];
        
        let collision = false;
        let c1Count = 0;
        let c2Count = 0;
        
        sampleRows.forEach(row => {
            const v1 = row[c1];
            const v2 = row[c2];
            if (v1 && v2) collision = true;
            if (v1) c1Count++;
            if (v2) c2Count++;
        });

        if (!collision) {
            const h1 = headers[c1] || '';
            let c1Type: 'debit' | 'credit' = 'debit'; 
            
            if (h1.includes('dep') || h1.includes('cr')) c1Type = 'credit';
            else if (h1.includes('with') || h1.includes('dr')) c1Type = 'debit';
            else if (c1Count > c2Count) c1Type = 'debit'; 
            else c1Type = 'credit';

            analysis[c1].columnType = c1Type;
            analysis[c2].columnType = c1Type === 'debit' ? 'credit' : 'debit';
        }
    }

    return analysis;
  }

  private sanitizeDescription(raw: string, report?: RemovalReport): string {
    if (!raw) return '';
    let clean = raw.trim();

    // 0. Custom removals (user-provided terms)
    clean = this.applyCustomRemovals(clean, report);

    // 1. Mask Account Numbers / Cards (Always on usually, or via maskPii)
    if (this.options.maskPii) {
      const before = clean;
      clean = clean.replace(PATTERNS.PII_CREDIT_CARD, '****');
      if (report && clean !== before) report.redactions.card = (report.redactions.card || 0) + 1;
      const b2 = clean;
      clean = clean.replace(PATTERNS.PII_SSN, '[SSN]');
      if (report && clean !== b2) report.redactions.ssn = (report.redactions.ssn || 0) + 1;
      const b3 = clean;
      clean = clean.replace(PATTERNS.PII_EMAIL, '[EMAIL]');
      if (report && clean !== b3) report.redactions.email = (report.redactions.email || 0) + 1;
      const b4 = clean;
      clean = clean.replace(PATTERNS.PII_PHONE, '[PHONE]');
      if (report && clean !== b4) report.redactions.phone = (report.redactions.phone || 0) + 1;
      const b5 = clean;
      clean = clean.replace(PATTERNS.PII_URL, '[URL]');
      if (report && clean !== b5) report.redactions.url = (report.redactions.url || 0) + 1;
      const b6 = clean;
      clean = clean.replace(PATTERNS.PII_ADDRESS, '[ADDRESS]');
      if (report && clean !== b6) report.redactions.address = (report.redactions.address || 0) + 1;
      const b7 = clean;
      clean = clean.replace(PATTERNS.PII_ZIP, '[ZIP]');
      if (report && clean !== b7) report.redactions.zip = (report.redactions.zip || 0) + 1;
    }

    // 2. Remove Transaction IDs / Refs
    for (const pat of PATTERNS.ID_PATTERNS) {
      const before = clean;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (pat as any).lastIndex = 0;
      clean = clean.replace(pat, '');
      if (report && clean !== before) report.idTokensRemoved += 1;
    }

    // 3. Scrub Contacts / Names
    if (this.options.scrubContacts) {
       // Look for "Payment to John Doe" or similar patterns if needed, 
       // or just apply general name heuristic
       // This is a naive implementation; in prod would use NLP
       clean = clean.replace(PATTERNS.NAMES, 'Individual');
    }

    // 4. Fuzz Location
    if (this.options.fuzzLocation) {
      const before = clean;
      clean = clean.replace(PATTERNS.LOCATIONS, ' [LOC]');
      if (report && clean !== before) report.redactions.location = (report.redactions.location || 0) + 1;
    }

    // 5. Clean up generic noise
    clean = clean.replace(PATTERNS.NOISE_TOKENS, '');
    clean = clean.replace(/[_/\\]+/g, ' ');
    clean = clean.replace(/[|]+/g, ' ');
    clean = clean.replace(/\s+/g, ' ').trim();
    
    // 6. Handle Merchant Prefixes
    clean = clean.replace(PATTERNS.MERCHANT_PREFIXES, (match) => {
       return ''; 
    });

    // 7. Force Title Case
    return clean.replace(/\w\S*/g, (txt) => txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()).trim();
  }

  private normalizeMerchant(cleanedDesc: string, report?: RemovalReport): string {
    if (!cleanedDesc) return 'Unknown';

    const original = cleanedDesc;
    let s = cleanedDesc;

    // Apply custom removals again at merchant-level (after sanitization)
    s = this.applyCustomRemovals(s, report);

    // Remove common payment rails / prefixes / modes
    s = s.replace(/^(pos|purchase|debit|credit|card|visa|mastercard|mc|amex)\b[:\-\s]*/i, '');
    s = s.replace(/^(ach|wire|zelle|venmo|cash app|paypal)\b[:\-\s]*/i, '');
    s = s.replace(/^(online transfer|mobile deposit|direct deposit)\b[:\-\s]*/i, '');

    // Remove bank/ACH junk codes
    s = s.replace(/\b(DES:|ID:|INDN:|CO:|PPD|WEB|CCD|ARC|EFT|CTX|POP|RCK|TEL|TRX)\b/gi, '');
    
    // Remove dates in description (mm/dd or mm/dd/yy)
    s = s.replace(/\b\d{1,2}[\/-]\d{1,2}([\/-]\d{2,4})?\b/g, '');

    // Remove decimal numbers (like "14.68 V" in "Adobe Inc 14.68 V")
    s = s.replace(/\b\d+\.\d+\b/g, ''); 

    // Remove store/branch numbers and identifiers (e.g. "Tim Hortons #15" -> "Tim Hortons")
    s = s.replace(/\b(#|store|stn|branch|location|loc)\s*[-#:]?\s*\d+\b/gi, '');
    
    // Remove ALL standalone numbers of 2+ digits (e.g. "Pioneer Stn 24" -> "Pioneer Stn", "Uh521" -> "Uh")
    // But preserve single digits for brands like "3M" or "7-Eleven"
    s = s.replace(/\b\d{2,}\b/g, '');

    // Cleanup bank transfer abbreviations - make them clearer
    s = s.replace(/\b(ssv to|tfr-to|tfr to|transfer to)\s*\[?phone\]?/gi, 'Mobile Payment');
    s = s.replace(/\b(ssv from|tfr-fr|tfr from|transfer from)\s*\[?phone\]?/gi, 'Mobile Transfer');
    s = s.replace(/\b(ssv to|tfr-to|tfr to|transfer to)\b/gi, 'Payment');
    s = s.replace(/\b(ssv from|tfr-fr|tfr from|transfer from)\b/gi, 'Incoming Transfer');
    s = s.replace(/\b(ssv|tfr)[\s-]*(to|from)?\b/gi, 'Transfer');

    // Remove single-letter suffixes that are noise (e.g. "V" in "Adobe Inc V")
    s = s.replace(/\b[A-Z]\b/g, '');

    // Remove location state codes at the end
    s = s.replace(/\b(NY|CA|TX|FL|IL|PA|OH|GA|NC|MI|NJ|VA|WA|AZ|MA|TN|IN|MO|MD|WI|CO|MN|SC|AL|LA|KY|OR|OK|CT|UT|IA|NV|AR|MS|KS|NM|NE|WV|ID|HI|NH|ME|RI|MT|DE|SD|ND|AK|DC|VT|WY)\b\s*$/gi, '');

    // Remove generic words that add no value
    s = s.replace(/\b(inc|llc|ltd|corp|company|co\.?)\b/gi, '');

    // Collapse separators/punctuation
    s = s.replace(/[^\w\s&'.-]+/g, ' ');
    s = s.replace(/[-_]+/g, ' ');
    s = s.replace(/\s+/g, ' ').trim();

    if (!s) return 'Unknown';

    // Title Case for readability
    const out = s.replace(/\w\S*/g, (txt) => txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()).trim();
    if (report && out && out !== original) report.merchantNormalized += 1;
    return out;
  }

  private escapeRegExp(input: string): string {
    return input.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  private applyCustomRemovals(input: string, report?: RemovalReport): string {
    const terms = (this.options.customRemoveTerms ?? []).map(t => (t ?? '').trim()).filter(Boolean);
    if (!terms.length || !input) return input;

    let out = input;
    for (const term of terms) {
      const re = new RegExp(this.escapeRegExp(term), 'gi');
      const matches = out.match(re);
      if (matches && matches.length) {
        if (report) report.customRemoved += matches.length;
        out = out.replace(re, '');
      }
    }
    // Clean up extra whitespace after removals
    out = out.replace(/\s+/g, ' ').trim();
    return out;
  }

  private categorizeTransaction(
    rawDesc: string,
    cleanedDesc: string,
    merchant: string
  ): { category: string; confidence: number } {
    const text = `${rawDesc ?? ''} ${cleanedDesc ?? ''} ${merchant ?? ''}`.toLowerCase();

    // High-confidence exact-ish merchant matches
    const subscriptionMerchants: Array<[RegExp, string]> = [
      [/netflix/i, 'Subscriptions: Streaming'],
      [/spotify/i, 'Subscriptions: Streaming'],
      [/amazon\s+prime|prime\s+video/i, 'Subscriptions: Membership'],
      [/apple\.com\/bill|apple\s+services|itunes/i, 'Subscriptions: Digital'],
      [/google\s+\*?storage|google\s+one|google\s+services/i, 'Subscriptions: Digital'],
      [/microsoft\s+365|office\s+365/i, 'Subscriptions: SaaS'],
      [/adobe/i, 'Subscriptions: SaaS']
    ];
    for (const [re, cat] of subscriptionMerchants) {
      if (re.test(text)) return { category: cat, confidence: 0.9 };
    }

    // Income
    if (/(payroll|salary|direct deposit|paychex|adp|payroll dep)/i.test(text)) return { category: 'Income: Salary', confidence: 0.85 };
    if (/(interest|dividend|distribution|capital gain)/i.test(text)) return { category: 'Income: Investment', confidence: 0.8 };
    if (/(refund|reversal|chargeback|reimb)/i.test(text)) return { category: 'Income: Refund', confidence: 0.75 };

    // Fees / finance charges
    if (/(monthly fee|service fee|maintenance fee|overdraft|nsf|finance charge|late fee)/i.test(text)) {
      return { category: 'Fees', confidence: 0.8 };
    }

    // Transfers / banking
    if (/(credit card payment|cc payment|loan payment|mortgage payment)/i.test(text)) return { category: 'Payments', confidence: 0.7 };
    if (/(transfer|ach|wire|zelle|venmo|cash app|paypal)/i.test(text)) return { category: 'Transfer', confidence: 0.65 };
    if (/(atm|cash withdrawal)/i.test(text)) return { category: 'Cash/ATM', confidence: 0.7 };

    // Spending
    if (/(grocery|supermarket|market|whole foods|trader joe|costco)/i.test(text)) return { category: 'Groceries', confidence: 0.75 };
    if (/(walmart|target|best buy|home depot|lowes)/i.test(text)) return { category: 'Shopping: Retail', confidence: 0.7 };
    if (/(restaurant|cafe|coffee|starbucks|dunkin|pizza|doordash|ubereats|grubhub)/i.test(text)) return { category: 'Dining', confidence: 0.75 };
    if (/(uber|lyft|taxi|transit|metro|train|bus)/i.test(text)) return { category: 'Transport', confidence: 0.7 };
    if (/(gas|fuel|shell|chevron|exxon|bp)\b/i.test(text)) return { category: 'Fuel', confidence: 0.75 };
    if (/(rent|mortgage|landlord|property mgmt)/i.test(text)) return { category: 'Housing', confidence: 0.7 };
    if (/(electric|water|utility|internet|comcast|verizon|att|t-mobile|phone bill)/i.test(text)) return { category: 'Bills: Utilities', confidence: 0.7 };
    if (/(insurance|geico|progressive|state farm|premium)/i.test(text)) return { category: 'Bills: Insurance', confidence: 0.7 };
    if (/(medical|pharmacy|clinic|hospital|dental|vision)/i.test(text)) return { category: 'Healthcare', confidence: 0.75 };
    if (/(subscription|membership|recurring)/i.test(text)) return { category: 'Subscriptions', confidence: 0.6 };
    if (/(education|tuition|school|university|course)/i.test(text)) return { category: 'Education', confidence: 0.7 };

    return { category: 'Other', confidence: 0.4 };
  }
}

