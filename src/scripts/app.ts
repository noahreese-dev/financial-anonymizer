import { FinancialAnonymizer, type OutputFormat, type DetailLevel, type ExportProfile, type PreflightReport, type SanitizedData } from '../lib/anonymizer';
import { signatureFromHeaders } from '../lib/dialect';
import { detectFileKind, readFileArrayBuffer, readFileText, rowsFromCsvText, rowsFromXlsx, rowsFromPdfText, type PdfExtractMeta } from '../lib/ingest';

// Theme management
const THEME_KEY = 'financial-anonymizer-theme';

function getTheme(): 'light' | 'dark' {
  const stored = localStorage.getItem(THEME_KEY);
  return (stored === 'light' || stored === 'dark') ? stored : 'dark';
}

function setTheme(theme: 'light' | 'dark') {
  localStorage.setItem(THEME_KEY, theme);
  applyTheme(theme);
}

function applyTheme(theme: 'light' | 'dark') {
  const body = document.body;
  const iconSun = document.getElementById('icon-sun');
  const iconMoon = document.getElementById('icon-moon');
  
  if (theme === 'light') {
    body.classList.add('light-mode');
    // Light (sun) mode: show sun icon
    iconSun?.classList.remove('hidden');
    iconMoon?.classList.add('hidden');
  } else {
    body.classList.remove('light-mode');
    // Dark (moon) mode: show moon icon
    iconSun?.classList.add('hidden');
    iconMoon?.classList.remove('hidden');
  }
}

function toggleTheme() {
  const current = getTheme();
  const next = current === 'dark' ? 'light' : 'dark';
  setTheme(next);
}

let activeFormat: OutputFormat = 'markdown';
let activeDetailLevel: DetailLevel = 'minimal'; // driven by Profile now (no separate UI)
let activeProfile: ExportProfile = 'ai_safe';
let outputJSON: SanitizedData | null = null;
let isProcessing = false;
let stagedRows: string[][] | null = null;
let stagedKind: 'csv' | 'xlsx' | 'pdf' | 'unknown' = 'unknown';
let stagedPdfMeta: PdfExtractMeta | null = null;
let preflight: PreflightReport | null = null;
let customRemoveTerms: string[] = [];
let latestSourceText = '';
let compareMode = false;
let highlightChanges = false;

const $ = (id: string) => document.getElementById(id);

function escapeHtml(s: string): string {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function updatePlanInfoStats() {
  const statsEl = $('plan-info-stats');
  if (!statsEl) return;

  const rep = outputJSON?.metadata?.removalReport;
  if (!outputJSON || !rep) {
    statsEl.classList.add('hidden');
    statsEl.innerHTML = '';
    return;
  }

  const red = rep.redactions ?? {};
  const items: Array<{ label: string; count: number }> = [];

  if ((red as any)?.phone > 0) items.push({ label: 'phone', count: (red as any).phone });
  if ((red as any)?.email > 0) items.push({ label: 'email', count: (red as any).email });
  if ((red as any)?.ssn > 0) items.push({ label: 'ssn', count: (red as any).ssn });
  if ((red as any)?.url > 0) items.push({ label: 'url', count: (red as any).url });
  if ((red as any)?.zip > 0) items.push({ label: 'zip', count: (red as any).zip });
  if ((red as any)?.card > 0) items.push({ label: 'card', count: (red as any).card });

  const totalPII = items.reduce((s, i) => s + i.count, 0);
  const idsRemoved = rep.idTokensRemoved ?? 0;

  if (totalPII === 0 && idsRemoved === 0) {
    statsEl.classList.add('hidden');
    return;
  }

  statsEl.classList.remove('hidden');
  statsEl.innerHTML = `
    <div class="font-semibold text-slate-400 mb-1.5">Current Scan Results</div>
    ${totalPII > 0 ? `<div class="flex items-center gap-2"><span class="text-red-400">${totalPII}</span> PII items found</div>` : ''}
    ${items.length > 0 ? `<div class="text-slate-600 ml-2">${items.map(i => `${i.label}: ${i.count}`).join(' • ')}</div>` : ''}
    ${idsRemoved > 0 ? `<div class="flex items-center gap-2"><span class="text-slate-400">${idsRemoved}</span> Transaction IDs/tokens</div>` : ''}
  `;
}

function renderFindingsBar() {
  const bar = $('findings-bar');
  const chips = $('findings-chips');
  if (!bar || !chips) return;

  const rep = outputJSON?.metadata?.removalReport;
  if (!outputJSON || !rep) {
    bar.classList.add('hidden');
    chips.innerHTML = '';
    updatePlanInfoStats();
    return;
  }

  const red = rep.redactions ?? {};
  const items: Array<{ label: string; count: number; icon?: string }> = [];

  // Add PII items with descriptive labels
  const addPII = (label: string, key: string, icon: string) => {
    const count = Number((red as any)?.[key] ?? 0);
    if (count > 0) items.push({ label, count, icon });
  };

  // Account numbers (phone pattern catches bank account numbers too)
  addPII('Account/Phone #s', 'phone', '🔢');
  addPII('Email Addresses', 'email', '✉️');
  addPII('SSN/SIN', 'ssn', '🔐');
  addPII('URLs/Links', 'url', '🔗');
  addPII('Street Addresses', 'address', '📍');
  addPII('ZIP/Postal Codes', 'zip', '📮');
  addPII('Card Numbers', 'card', '💳');
  addPII('Location Names', 'location', '🌍');

  // Transaction IDs & Reference Numbers
  if ((rep.idTokensRemoved ?? 0) > 0) {
    items.push({ label: 'Transaction IDs/Refs', count: rep.idTokensRemoved, icon: '🏷️' });
  }

  // Merchant normalization count
  if ((rep.merchantNormalized ?? 0) > 0) {
    items.push({ label: 'Merchants Cleaned', count: rep.merchantNormalized, icon: '🏪' });
  }

  // Custom terms removed - show count per term if available
  const customTermsRemoved = rep.customTermsRemoved ?? {};
  const customTotal = Object.values(customTermsRemoved).reduce((a: number, b: any) => a + (Number(b) || 0), 0);
  if (customTotal > 0) {
    items.push({ label: 'Custom Terms', count: customTotal, icon: '✂️' });
  }

  // Calculate total
  const totalRemoved = items.reduce((sum, it) => sum + it.count, 0);

  // Render as non-clickable chips with more detail
  const html = items.map((it) => {
    return `<span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full border border-red-500/20 bg-red-500/10 text-red-300 text-[11px] font-mono whitespace-nowrap">
      <span class="opacity-70">${it.icon || '•'}</span>
      ${escapeHtml(it.label)}
      <span class="opacity-70 font-semibold">${it.count}</span>
    </span>`;
  }).join('');

  // Add total count summary
  const summaryHtml = totalRemoved > 0 
    ? `<span class="text-[11px] text-slate-400 font-mono ml-auto whitespace-nowrap">${totalRemoved} total</span>`
    : '';

  chips.innerHTML = html 
    ? `${html}${summaryHtml}`
    : '<span class="text-[11px] text-slate-500 font-mono">No sensitive data found</span>';
  bar.classList.remove('hidden');
  updatePlanInfoStats();
}

function renderShareSafety() {
  const badge = $('share-safety-badge');
  const itemsEl = $('share-safety-items');
  if (!badge || !itemsEl) return;

  if (!outputJSON) {
    badge.textContent = 'Safety: —';
    badge.className = 'px-2.5 py-1 rounded-full border border-navy-800/60 bg-navy-950/30 text-[11px] font-mono text-slate-400';
    itemsEl.innerHTML = '';
    return;
  }

  const text = outputJSON.transactions.map(t => `${t.merchant}\n${t.description}`).join('\n');
  const counts = {
    email: (text.match(/[\w\.-]+@[\w\.-]+\.\w+/g) || []).length,
    phone: (text.match(/\b(?:\+?\d{1,2}[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}\b/g) || []).length,
    url: (text.match(/\bhttps?:\/\/\S+|\bwww\.\S+/gi) || []).length,
    ssn: (text.match(/\b\d{3}-\d{2}-\d{4}\b/g) || []).length,
    card: (text.match(/\b(?:\d[ -]*?){13,16}\b/g) || []).length
  };

  const total = counts.email + counts.phone + counts.url + counts.ssn + counts.card;
  const risk = (counts.ssn > 0 || counts.card > 0) ? 'High' : total > 0 ? 'Medium' : 'Low';

  const badgeBase = 'px-2.5 py-1 rounded-full border text-[11px] font-mono';
  const badgeTone =
    risk === 'Low'
      ? 'border-green-500/25 bg-green-500/10 text-green-300'
      : risk === 'Medium'
        ? 'border-yellow-500/25 bg-yellow-500/10 text-yellow-300'
        : 'border-red-500/25 bg-red-500/10 text-red-300';

  badge.className = `${badgeBase} ${badgeTone}`;
  badge.textContent = `Safety: ${risk}`;

  const mk = (label: string, ok: boolean, find?: string) => {
    const base = 'px-2.5 py-1 rounded-full border text-[11px] font-mono transition-colors whitespace-nowrap';
    const tone = ok
      ? 'border-green-500/20 bg-green-500/10 text-green-300'
      : 'border-red-500/20 bg-red-500/10 text-red-300 hover:bg-red-500/15';
    const tag = (!ok && find) ? 'button' : 'span';
    const attrs = (!ok && find) ? `data-find="${escapeHtml(find)}" title="Find remaining: ${escapeHtml(label)}"` : '';
    return `<${tag} ${attrs} class="${base} ${tone}">${ok ? '✓' : '!' } ${escapeHtml(label)}</${tag}>`;
  };

  itemsEl.innerHTML =
    mk('Email', counts.email === 0, '@') +
    mk('Phone', counts.phone === 0, '(') +
    mk('URL', counts.url === 0, 'http') +
    mk('SSN', counts.ssn === 0, '-') +
    mk('Card', counts.card === 0, '****');
}

function formatCurrency(val: number): string {
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(val);
}

function computeStats(data: SanitizedData) {
  let income = 0;
  let expense = 0;
  const merchants: Record<string, number> = {};

  for (const t of data.transactions) {
    if (t.amount > 0) income += t.amount;
    else expense += Math.abs(t.amount);

    const key = t.merchant || t.description;
    merchants[key] = (merchants[key] || 0) + 1;
  }

  const topMerchants = Object.entries(merchants)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 4);

  return { income, expense, net: income - expense, topMerchants };
}

function renderOutput(highlightTerm?: string) {
  const outputEl = $('output');
  const outputContainer = $('output-container');
  const emptyEl = $('output-empty');
  const copyBtn = $('btn-copy') as HTMLButtonElement | null;
  const downloadBtn = $('btn-download') as HTMLButtonElement | null;
  const aiBtn = $('btn-ai-analysis') as HTMLButtonElement | null;
  const filenameEl = $('output-filename');
  const outputNote = $('output-note');
  const btnSearchOpen = $('btn-search-open') as HTMLButtonElement | null;
  const highlightBtn = $('btn-highlight-changes') as HTMLButtonElement | null;

  if (!outputEl || !outputContainer || !emptyEl || !filenameEl) return;

  if (!outputJSON) {
    outputEl.textContent = '';
    outputContainer.classList.add('hidden');
    emptyEl.classList.remove('hidden');
    copyBtn && (copyBtn.disabled = true);
    downloadBtn && (downloadBtn.disabled = true);
    aiBtn && (aiBtn.disabled = true);
    btnSearchOpen && (btnSearchOpen.disabled = true);
    highlightBtn && (highlightBtn.disabled = true);
    outputNote?.classList.add('hidden');
    
    // Deep Clean button - disabled when no output
    const deepCleanBtn = $('btn-deep-clean') as HTMLButtonElement | null;
    if (deepCleanBtn) deepCleanBtn.disabled = true;
    
    // Stats reset
    const statIncome = $('stat-income');
    const statExpense = $('stat-expense');
    const statNet = $('stat-net');
    const topList = $('top-merchants');

    if (statIncome) statIncome.textContent = '$0.00';
    if (statExpense) statExpense.textContent = '$0.00';
    if (statNet) statNet.textContent = '$0.00';
    if (topList) topList.innerHTML = '<li class="text-xs text-slate-600">Analysis will appear here</li>';
    renderFindingsBar();
    renderShareSafety();
    return;
  }

  const anonymizer = new FinancialAnonymizer(); // defaults match base behavior
  // Show all rows (no preview limit - user explicitly requested full display)
  const maxRows = Infinity;

  // Get clean formatted output (no highlighting in the data itself)
  const out = anonymizer.formatData(outputJSON, activeFormat, {
    maxRows,
    detailLevel: activeDetailLevel,
    profile: activeProfile
  });

  emptyEl.classList.add('hidden');
  outputContainer.classList.remove('hidden');
  
  // Check if PII highlighting is enabled (global flag)
  const shouldHighlightPII = (window as any).__highlightChanges === true;
  
  // Apply highlighting for UI display only (never in the actual data)
  let finalOutput = out;
  
  if (highlightTerm || shouldHighlightPII) {
    // Escape HTML first for safety (only once)
    finalOutput = finalOutput
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
    
    // Apply search term highlighting if provided
    if (highlightTerm) {
      const escapedTerm = highlightTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const re = new RegExp(`(${escapedTerm})`, 'gi');
      finalOutput = finalOutput.replace(re, '<mark class="bg-brand-500/30 text-white rounded-sm px-0.5">$1</mark>');
    }
    
    // Apply PII highlighting if enabled
    if (shouldHighlightPII) {
      // Highlight both bracketed [PHONE] and unbracketed Phone/Email/etc. PII placeholders
      finalOutput = finalOutput
        .replace(/\[(PHONE|EMAIL|SSN|NAME|ADDRESS|CARD|ZIP|URL|ID|REF|MEMO)\]/gi, 
          '<mark class="bg-yellow-500/40 text-yellow-200 px-0.5 rounded">[$1]</mark>')
        .replace(/\b(Phone|Email|SSN|Card|Address|Zip|URL)\b/g, 
          '<mark class="bg-yellow-500/40 text-yellow-200 px-0.5 rounded">$1</mark>');
    }
  }
  
  // If highlighted (search or PII), use innerHTML. If not, textContent is safer.
  // Note: 'out' is always clean (no mark tags), 'finalOutput' has highlighting for display only
  if (highlightTerm || shouldHighlightPII) outputEl.innerHTML = finalOutput;
  else outputEl.textContent = out;
  
  copyBtn && (copyBtn.disabled = !out);
  downloadBtn && (downloadBtn.disabled = !out);
  aiBtn && (aiBtn.disabled = !out);
  btnSearchOpen && (btnSearchOpen.disabled = !out);
  highlightBtn && (highlightBtn.disabled = !out);
  
  // Deep Clean button (Presidio) - enable when we have output
  const deepCleanBtn = $('btn-deep-clean') as HTMLButtonElement | null;
  if (deepCleanBtn) deepCleanBtn.disabled = !out;

  const ext =
    activeFormat === 'markdown' || activeFormat === 'storyline'
      ? 'md'
      : activeFormat === 'text'
        ? 'txt'
        : activeFormat;
  filenameEl.textContent = `output.${ext}`;

  // Hide output note - we now show all rows
  if (outputNote) {
    outputNote.classList.add('hidden');
  }

  renderFindingsBar();
  renderShareSafety();
}

function renderAnalysis() {
  if (!outputJSON) return;

  const { income, expense, net, topMerchants } = computeStats(outputJSON);
  const statIncome = $('stat-income');
  const statExpense = $('stat-expense');
  const statNet = $('stat-net');
  const topList = $('top-merchants');

  if (statIncome) statIncome.textContent = formatCurrency(income);
  if (statExpense) statExpense.textContent = formatCurrency(expense);
  if (statNet) {
    statNet.textContent = formatCurrency(net);
    statNet.classList.toggle('text-red-300', net < 0);
    statNet.classList.toggle('text-slate-100', net >= 0);
  }

  if (topList) {
    topList.innerHTML = '';
    for (const [name, count] of topMerchants) {
      const li = document.createElement('li');
      li.className = 'flex items-center justify-between text-xs';
      li.innerHTML = `
        <span class="text-slate-300 truncate max-w-[220px]" title="${name.replaceAll('"', '&quot;')}">${name}</span>
        <span class="px-2 py-0.5 bg-navy-950/40 border border-navy-800 text-slate-300 rounded-lg text-[10px] font-mono">${count}x</span>
      `;
      topList.appendChild(li);
    }
    if (topMerchants.length === 0) {
      const li = document.createElement('li');
      li.className = 'text-xs text-slate-600';
      li.textContent = 'No merchants detected';
      topList.appendChild(li);
    }
  }
}

function setActiveFormat(fmt: OutputFormat) {
  activeFormat = fmt;
  document.querySelectorAll('.format-tab').forEach(b => {
    const isFmt = b.getAttribute('data-format') === fmt;
    if (isFmt) {
      b.classList.remove('text-slate-400', 'hover:text-white', 'hover:bg-white/5');
      b.classList.add('bg-white/10', 'text-white', 'shadow-sm');
    } else {
      b.classList.add('text-slate-400', 'hover:text-white', 'hover:bg-white/5');
      b.classList.remove('bg-white/10', 'text-white', 'shadow-sm');
    }
  });
  renderOutput();
}

function parseCSVHeaderRow(text: string): string[] {
  const headers: string[] = [];
  let currentVal = '';
  let insideQuote = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (ch === '"') {
      if (insideQuote && text[i + 1] === '"') {
        currentVal += '"';
        i++;
      } else {
        insideQuote = !insideQuote;
      }
      continue;
    }
    if (!insideQuote && ch === ',') {
      headers.push(currentVal.trim());
      currentVal = '';
      continue;
    }
    if (!insideQuote && (ch === '\n' || ch === '\r')) {
      break;
    }
    currentVal += ch;
  }
  if (currentVal.length || headers.length) headers.push(currentVal.trim());
  return headers;
}

function setDiagnosticsFromOutput(data: SanitizedData | null) {
  const rowOrderEl = $('diag-row-order');
  const strategyEl = $('diag-strategy');
  const confEl = $('diag-confidence');
  const infEl = $('diag-inference-counts');
  const coverageEl = $('diag-coverage');
  const skipsEl = $('diag-skips');
  const colsEl = $('diag-columns');

  if (!data) {
    rowOrderEl && (rowOrderEl.textContent = 'unknown');
    strategyEl && (strategyEl.textContent = 'fallback');
    confEl && (confEl.textContent = '0.00');
    infEl && (infEl.textContent = '—');
    coverageEl && (coverageEl.textContent = '—');
    skipsEl && (skipsEl.textContent = '—');
    colsEl && (colsEl.textContent = '—');
    return;
  }

  rowOrderEl && (rowOrderEl.textContent = data.metadata.rowOrder ?? 'unknown');
  strategyEl && (strategyEl.textContent = data.metadata.directionStrategy ?? 'fallback');
  confEl && (confEl.textContent = (data.metadata.directionConfidence ?? 0).toFixed(2));

  if (infEl) {
    const summary = data.metadata.inferenceSummary ?? {};
    const lines = Object.entries(summary)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}: ${v}`)
      .join('\n');
    infEl.textContent = lines || '—';
  }

  if (coverageEl) {
    const total = Math.max(0, (data.metadata.originalRowCount ?? 0) - 1);
    const bal = data.metadata.rowsWithBalance ?? 0;
    const pct = total > 0 ? ((bal / total) * 100).toFixed(1) : '0';
    coverageEl.textContent = `rows_with_balance: ${bal}/${total} (${pct}%)`;
  }

  if (skipsEl) {
    const skips = data.metadata.skipSummary ?? {};
    const lines = Object.entries(skips)
      .map(([k, v]) => `${k}: ${v}`)
      .join('\n');
    skipsEl.textContent = lines || 'None';
  }

  if (colsEl) {
    const cols = data.metadata.detectedColumnsList ?? [];
    colsEl.textContent = cols
      .map(c => `[${c.columnIndex}] ${c.columnType} (${(c.confidence * 100).toFixed(0)}%)`)
      .join('\n');
  }
}

function setProcessing(next: boolean) {
  isProcessing = next;
  const btn = $('btn-process') as HTMLButtonElement | null;
  const btnText = $('btn-process-text');
  const input = $('input-csv') as HTMLTextAreaElement | null;
  // Gate on staged rows (file flow) OR textarea content (paste flow)
  const canRun = !!stagedRows || !!(input?.value?.trim());
  if (btn) btn.disabled = next || !canRun;
  if (btnText) btnText.textContent = next ? 'Processing...' : 'Parse & Clean';
}

document.addEventListener('DOMContentLoaded', () => {
  // Apply saved theme on load
  applyTheme(getTheme());

  // Theme toggle button
  const themeToggle = document.getElementById('theme-toggle');
  if (themeToggle) {
    themeToggle.addEventListener('click', toggleTheme);
  }

  const fileInput = $('file-input') as HTMLInputElement | null;
  const btnUpload = $('btn-upload') as HTMLButtonElement | null;
  const btnClear = $('btn-clear') as HTMLButtonElement | null;
  const btnProcess = $('btn-process') as HTMLButtonElement | null;
  const btnCopy = $('btn-copy') as HTMLButtonElement | null;
  const btnDownload = $('btn-download') as HTMLButtonElement | null;
  const dropzone = $('dropzone');
  const dropOverlay = $('drop-overlay');
  const textarea = $('input-csv') as HTMLTextAreaElement | null;
  const customTermsWrap = $('custom-terms-chips');
  const customTermInput = $('custom-term-input') as HTMLInputElement | null;
  const btnAddTerm = $('btn-add-term') as HTMLButtonElement | null;

  const pdfControls = $('pdf-controls');
  const pdfPageStart = $('pdf-page-start') as HTMLInputElement | null;
  const pdfPageEnd = $('pdf-page-end') as HTMLInputElement | null;
  const pdfMeta = $('pdf-meta');
  const pdfOcrBadge = $('pdf-ocr-badge');
  const btnPreflightRerun = $('btn-preflight-rerun') as HTMLButtonElement | null;

  if (!textarea || !btnProcess) return;
  const textareaEl: HTMLTextAreaElement = textarea;
  latestSourceText = textareaEl.value || '';

  // Workflow stepper (Upload → Review → Export/AI)
  const wfStep1 = $('wf-step-1');
  const wfStep2 = $('wf-step-2');
  const wfStep3 = $('wf-step-3');
  const wfConn1 = $('wf-conn-1');
  const wfConn2 = $('wf-conn-2');

  function setWorkflowStep(step: 1 | 2 | 3) {
    const steps = [
      { el: wfStep1, idx: 1, connAfter: wfConn1 },
      { el: wfStep2, idx: 2, connAfter: wfConn2 },
      { el: wfStep3, idx: 3, connAfter: null },
    ] as const;

    for (const s of steps) {
      if (!s.el) continue;
      const isActive = s.idx === step;
      const isDone = s.idx < step;

      const dot = s.el.querySelector('.wf-dot') as HTMLElement | null;
      const numEl = s.el.querySelector('.wf-num') as HTMLElement | null;
      const checkEl = s.el.querySelector('.wf-check') as HTMLElement | null;
      const labelEl = s.el.querySelector('.wf-label') as HTMLElement | null;

      // Opacity: done/active = full, upcoming = dim
      s.el.classList.remove('opacity-60', 'opacity-70', 'opacity-100');
      s.el.classList.add(isActive || isDone ? 'opacity-100' : 'opacity-50');

      // Background and border
      s.el.classList.remove('bg-navy-950/40', 'bg-navy-950/20', 'border-brand-500/40', 'border-green-500/40', 'border-navy-800/60');
      if (isActive) {
        s.el.classList.add('bg-navy-950/40', 'border-brand-500/40');
      } else if (isDone) {
        s.el.classList.add('bg-green-500/10', 'border-green-500/30');
      } else {
        s.el.classList.add('bg-navy-950/20', 'border-navy-800/60');
      }

      // Dot styling: green for done, brand for active, gray for upcoming
      if (dot) {
        dot.classList.remove('bg-green-500/30', 'border-green-500/50', 'bg-brand-500/30', 'border-brand-500/50', 'bg-navy-800/50', 'bg-navy-800/70', 'border-navy-700/50', 'border-navy-700/60');
        if (isDone) {
          dot.classList.add('bg-green-500/30', 'border-green-500/50');
        } else if (isActive) {
          dot.classList.add('bg-brand-500/30', 'border-brand-500/50');
        } else {
          dot.classList.add('bg-navy-800/50', 'border-navy-700/50');
        }
      }

      // Show checkmark for done steps, number otherwise
      if (isDone) {
        numEl?.classList.add('hidden');
        checkEl?.classList.remove('hidden');
      } else {
        numEl?.classList.remove('hidden');
        checkEl?.classList.add('hidden');
        // Color the number based on state
        if (numEl) {
          numEl.classList.remove('text-slate-300', 'text-slate-400', 'text-slate-500', 'text-brand-300');
          numEl.classList.add(isActive ? 'text-brand-300' : 'text-slate-500');
        }
      }

      // Label color
      if (labelEl) {
        labelEl.classList.remove('text-slate-200', 'text-slate-300', 'text-slate-400', 'text-green-300', 'text-brand-300');
        if (isDone) {
          labelEl.classList.add('text-green-300');
        } else if (isActive) {
          labelEl.classList.add('text-brand-300');
        } else {
          labelEl.classList.add('text-slate-400');
        }
      }

      // Connector line color (green if step before is done)
      if (s.connAfter) {
        s.connAfter.classList.remove('bg-navy-800/70', 'bg-navy-800/60', 'bg-green-500/50');
        s.connAfter.classList.add(isDone ? 'bg-green-500/50' : 'bg-navy-800/60');
      }
    }
  }

  let step3Completed = false;

  function updateWorkflowFromState() {
    const hasInput = !!stagedRows || !!textareaEl.value.trim();
    if (!hasInput) {
      step3Completed = false;
      return setWorkflowStep(1);
    }
    if (!outputJSON) {
      step3Completed = false;
      return setWorkflowStep(2);
    }
    // If step 3 was marked complete, show all steps as done
    if (step3Completed) {
      setWorkflowStep(3);
      markStep3Done();
    } else {
      setWorkflowStep(3);
    }
  }

  // Mark step 3 as completed (green checkmark)
  function markStep3Done() {
    step3Completed = true;
    if (!wfStep3) return;
    
    const dot = wfStep3.querySelector('.wf-dot') as HTMLElement | null;
    const numEl = wfStep3.querySelector('.wf-num') as HTMLElement | null;
    const checkEl = wfStep3.querySelector('.wf-check') as HTMLElement | null;
    const labelEl = wfStep3.querySelector('.wf-label') as HTMLElement | null;

    // Apply "done" styling
    wfStep3.classList.remove('opacity-50', 'bg-navy-950/40', 'border-brand-500/40');
    wfStep3.classList.add('opacity-100', 'bg-green-500/10', 'border-green-500/30');

    if (dot) {
      dot.classList.remove('bg-brand-500/30', 'border-brand-500/50');
      dot.classList.add('bg-green-500/30', 'border-green-500/50');
    }

    // Show checkmark, hide number
    numEl?.classList.add('hidden');
    checkEl?.classList.remove('hidden');

    // Green label
    if (labelEl) {
      labelEl.classList.remove('text-brand-300', 'text-slate-400');
      labelEl.classList.add('text-green-300');
    }

    // Green connector before step 3
    wfConn2?.classList.remove('bg-navy-800/60');
    wfConn2?.classList.add('bg-green-500/50');
  }

  // Custom remove terms (session only, per user request)
  customRemoveTerms = [];

  function saveCustomTerms() {
     // No-op for persistence to avoid auto-adding old terms
  }

  function setError(msg: string | null) {
    const el = $('error');
    const txt = $('error-text');
    if (el && txt) {
      if (msg) {
        el.classList.remove('hidden');
        txt.textContent = msg;
      } else {
        el.classList.add('hidden');
      }
    }
  }

  function renderCustomTerms() {
    if (!customTermsWrap) return;
    customTermsWrap.innerHTML = '';
    for (const term of customRemoveTerms) {
      const wrap = document.createElement('div');
      wrap.className =
        'inline-flex items-center gap-2 px-2.5 py-1 rounded-full border border-navy-800 bg-navy-950/40 text-slate-200 text-[11px] font-mono';

      const label = document.createElement('span');
      label.textContent = term;

      const remove = document.createElement('button');
      remove.type = 'button';
      remove.className = 'text-slate-500 hover:text-red-300 transition-colors';
      remove.setAttribute('aria-label', `Remove "${term}"`);
      remove.textContent = '×';
      remove.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        customRemoveTerms = customRemoveTerms.filter((t) => t !== term);
        saveCustomTerms();
        renderCustomTerms();
        rerunPreflightFromStaged();
      });

      wrap.appendChild(label);
      wrap.appendChild(remove);
      customTermsWrap.appendChild(wrap);
    }
    if (customRemoveTerms.length === 0) {
      const hint = document.createElement('div');
      hint.className = 'text-[11px] text-slate-600 font-mono';
      hint.textContent = 'No custom terms yet.';
      customTermsWrap.appendChild(hint);
    }
    
    // Update plan tooltip with custom terms
    updatePlanCustomTerms();
  }
  
  function updatePlanCustomTerms() {
    const container = $('plan-custom-terms');
    const list = $('plan-custom-terms-list');
    if (!container || !list) return;
    
    if (customRemoveTerms.length === 0) {
      container.classList.add('hidden');
      list.innerHTML = '';
      return;
    }
    
    container.classList.remove('hidden');
    list.innerHTML = customRemoveTerms.map(term => `
      <div class="flex items-center gap-2">
        <span class="w-1.5 h-1.5 rounded-full bg-orange-400/70"></span>
        <span class="truncate" title="${escapeHtml(term)}">${escapeHtml(term)}</span>
      </div>
    `).join('');
  }

  renderCustomTerms();

  // Collapsible fine-tune section with localStorage persistence
  const fineTuneSection = $('configuration-section') as HTMLDetailsElement | null;
  if (fineTuneSection) {
    // Restore saved state
    try {
      const savedOpen = localStorage.getItem('fa:fineTuneOpen');
      if (savedOpen === 'true') fineTuneSection.open = true;
    } catch {
      // ignore
    }

    // Save state on toggle
    fineTuneSection.addEventListener('toggle', () => {
      try {
        localStorage.setItem('fa:fineTuneOpen', fineTuneSection.open ? 'true' : 'false');
      } catch {
        // ignore
      }
    });
  }

  // Custom card click -> open fine-tune section
  const statCardCustom = $('stat-card-custom');
  if (statCardCustom && fineTuneSection) {
    statCardCustom.addEventListener('click', () => {
      fineTuneSection.open = true;
      // Scroll to fine-tune section
      fineTuneSection.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      // Focus custom term input after a short delay
      setTimeout(() => {
        const input = $('custom-term-input') as HTMLInputElement | null;
        input?.focus();
      }, 300);
    });
  }

  customTermInput?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      const val = (customTermInput.value ?? '').trim();
      if (!val) return;
      addCustomTerm(val);
    } else if (e.key === 'Backspace' && (customTermInput.value ?? '') === '' && customRemoveTerms.length) {
      customRemoveTerms.shift();
      saveCustomTerms();
      renderCustomTerms();
      rerunPreflightFromStaged();
    }
  });

  function addCustomTerm(val: string) {
    const term = (val ?? '').trim();
    if (!term) return;

    // Case-insensitive de-dupe (capitals don't matter)
    const lower = term.toLowerCase();
    const existingLower = new Set(customRemoveTerms.map((t) => (t ?? '').toLowerCase()));
    if (!existingLower.has(lower)) customRemoveTerms.unshift(term);
    if (customRemoveTerms.length > 50) customRemoveTerms = customRemoveTerms.slice(0, 50);

    if (customTermInput) customTermInput.value = '';
    saveCustomTerms();
    renderCustomTerms();
    rerunPreflightFromStaged();
  }

  btnAddTerm?.addEventListener('click', () => {
    const val = (customTermInput?.value ?? '').trim();
    if (!val) return;
    addCustomTerm(val);
    customTermInput?.focus();
  });

  function renderPlannedRemovals(rep: PreflightReport | null) {
    const plannedRemovalsEl = $('planned-removals');
    
    if (!plannedRemovalsEl) return;

    if (!rep) {
      // Reset to default text
      plannedRemovalsEl.innerHTML = `• Credit/Debit card numbers<br>• Email addresses & phone numbers<br>• SSN patterns<br>• Transaction IDs & reference codes<br>• URLs & physical addresses`;
      return;
    }

    // Calculate totals
    const piiTotal = Object.values(rep.plannedRedactions ?? {}).reduce((a, b) => a + b, 0);
    const idsTotal = rep.plannedIdRemovals ?? 0;
    const customTotal = Object.values(rep.plannedCustomRemovals ?? {}).reduce((a, b) => a + b, 0);

    // Build list of planned removals
    const lines: string[] = [];
    
    if (piiTotal > 0) {
      const piiBreakdown = Object.entries(rep.plannedRedactions ?? {})
        .filter(([, v]) => v > 0)
        .sort(([, a], [, b]) => b - a)
        .map(([k, v]) => `  - ${k}: ${v}`)
        .join('<br>');
      lines.push(`• <strong>${piiTotal} PII items</strong><br>${piiBreakdown}`);
    }
    
    if (idsTotal > 0) {
      lines.push(`• <strong>${idsTotal} Transaction IDs/tokens</strong>`);
    }
    
    if (customTotal > 0 && customRemoveTerms.length > 0) {
      const customBreakdown = Object.entries(rep.plannedCustomRemovals ?? {})
        .filter(([, v]) => v > 0)
        .sort(([, a], [, b]) => b - a)
        .slice(0, 5)
        .map(([k, v]) => `  - "${k}": ${v}`)
        .join('<br>');
      lines.push(`• <strong>${customTotal} Custom term matches</strong><br>${customBreakdown}`);
    }

    plannedRemovalsEl.innerHTML = lines.length > 0 
      ? lines.join('<br>') 
      : '• No sensitive data detected';
  }

  function rerunPreflightFromStaged() {
    if (!stagedRows) return;
    try {
      const a = new FinancialAnonymizer({ customRemoveTerms });
      preflight = a.preflightRows(stagedRows);
      renderPlannedRemovals(preflight);
    } catch {
      // ignore
    }
  }

  function renderPdfMeta(meta: PdfExtractMeta | null) {
    if (!pdfMeta) return;
    if (!meta) { 
      pdfMeta.textContent = '—';
      pdfOcrBadge?.classList.add('hidden');
      return; 
    }
    pdfMeta.textContent =
      `pdf_pages: ${meta.pageCount}\n` +
      `pages_processed: ${meta.pagesProcessed}\n` +
      `mode: ${meta.usedOcr ? 'OCR (scanned)' : 'Text Extraction (digital)'}\n` +
      `lines: ${meta.lineCount}\n` +
      `mode_columns: ${meta.modeColumnCount}\n` +
      `confidence: ${meta.confidence.toFixed(2)}`;
    
    // Show/hide OCR badge
    if (meta.usedOcr) {
      pdfOcrBadge?.classList.remove('hidden');
    } else {
      pdfOcrBadge?.classList.add('hidden');
    }
  }

  async function runPreflightForFile(file: File, opts?: { pageStart?: number; pageEnd?: number }) {
    preflight = null;
    stagedRows = null;
    stagedPdfMeta = null;
    stagedKind = detectFileKind(file);
    renderPlannedRemovals(null);
    renderPdfMeta(null);
    pdfControls?.classList.add('hidden');

    try {
      const anonymizer = new FinancialAnonymizer({ customRemoveTerms });

      if (stagedKind === 'csv') {
        const text = await readFileText(file);
        // Keep textarea populated for transparency/debugging
        textareaEl.value = text;
        latestSourceText = text;
        const rows = rowsFromCsvText(text);
        stagedRows = rows;
        preflight = anonymizer.preflightRows(rows);
      } else if (stagedKind === 'xlsx') {
        const buf = await readFileArrayBuffer(file);
        const rows = await rowsFromXlsx(buf);
        stagedRows = rows;
        // Don’t dump binary into textarea; show a hint
        textareaEl.value = `# Loaded ${file.name}\n# Parsed as XLSX into ${rows.length} rows\n`;
        latestSourceText = textareaEl.value;
        preflight = anonymizer.preflightRows(rows);
      } else if (stagedKind === 'pdf') {
        const buf = await readFileArrayBuffer(file);
        const { rows, meta } = await rowsFromPdfText(buf, { pageStart: opts?.pageStart, pageEnd: opts?.pageEnd });
        stagedRows = rows;
        stagedPdfMeta = meta;
        textareaEl.value = `# Loaded ${file.name}\n# Extracted PDF text into ${rows.length} rows\n`;
        latestSourceText = textareaEl.value;
        preflight = anonymizer.preflightRows(rows);

        // Show page range controls if confidence is low or if user wants to fine-tune
        if (pdfControls) {
          pdfControls.classList.remove('hidden');
          if (pdfPageStart && !pdfPageStart.value) pdfPageStart.value = (opts?.pageStart ?? 1).toString();
          if (pdfPageEnd && !pdfPageEnd.value) pdfPageEnd.value = '';
        }
        renderPdfMeta(meta);
      } else {
        throw new Error('Unsupported file type. Please upload CSV, XLSX, or PDF.');
      }

      renderPlannedRemovals(preflight);
      setError(null);
      updateWorkflowFromState();
    } catch (e: any) {
      setError(e?.message ?? 'Preflight failed.');
      stagedRows = null;
      stagedPdfMeta = null;
      preflight = null;
      updateWorkflowFromState();
    } finally {
      if (btnProcess) btnProcess.disabled = isProcessing || !(!!stagedRows || !!textareaEl.value.trim());
    }
  }

  // Attempt to load saved mapping (best-effort) when CSV changes (header signature match)
  function tryLoadSavedMapping() {
    try {
      const headers = parseCSVHeaderRow(textareaEl.value);
      if (!headers.length) return;
      const sig = signatureFromHeaders(headers);
      const key = `fa:dialect:${sig}`;
      const raw = localStorage.getItem(key);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      // For now we only surface this through diagnostics after processing,
      // but keeping the hook here enables future UI to show "recognized format" immediately.
      void parsed;
    } catch {
      // ignore
    }
  }

  // Tabs
  document.querySelectorAll<HTMLButtonElement>('.format-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      const fmt = btn.dataset.format as OutputFormat | undefined;
      if (fmt) setActiveFormat(fmt);
    });
  });

  // Upload
  btnUpload?.addEventListener('click', () => fileInput?.click());
  fileInput?.addEventListener('change', async () => {
    if (!fileInput.files?.[0]) return;
    setError(null);
    try {
      await runPreflightForFile(fileInput.files[0]);
    } catch (e: any) {
      setError(e?.message ?? 'Failed to read file.');
    }
  });

  // Drag/drop
  dropzone?.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropOverlay?.classList.remove('hidden');
  });
  dropzone?.addEventListener('dragleave', (e) => {
    e.preventDefault();
    dropOverlay?.classList.add('hidden');
  });
  dropzone?.addEventListener('drop', async (e) => {
    e.preventDefault();
    dropOverlay?.classList.add('hidden');
    const file = e.dataTransfer?.files?.[0];
    if (!file) return;
    setError(null);
    try {
      await runPreflightForFile(file);
    } catch (err: any) {
      setError(err?.message ?? 'Failed to read file.');
    }
  });

  // Text changes
  textarea.addEventListener('input', () => {
    btnProcess.disabled = isProcessing || !textarea.value.trim();
    // best effort; only meaningful once header exists
    if (textarea.value.length < 2000) tryLoadSavedMapping();
    latestSourceText = textarea.value;
    updateWorkflowFromState();
  });

  // Clear
  btnClear?.addEventListener('click', () => {
    textarea.value = '';
    latestSourceText = '';
    outputJSON = null;
    stagedRows = null;
    stagedPdfMeta = null;
    stagedKind = 'unknown';
    preflight = null;
    setError(null);
    btnProcess.disabled = true;
    setDiagnosticsFromOutput(null);
    renderOutput();
    renderPlannedRemovals(null);
    
    // Hide analysis panels
    $('analysis')?.classList.add('hidden');
    $('diagnostics')?.classList.add('hidden');
    updateWorkflowFromState();
  });

  // Process
  function runProcessing() {
    if (!textarea || (!stagedRows && !textarea.value.trim())) return;
    setError(null);
    outputJSON = null;
    renderOutput();
    
    // Hide analysis & diagnostics initially
    $('analysis')?.classList.add('hidden');
    $('diagnostics')?.classList.add('hidden');
    setDiagnosticsFromOutput(null);

    setProcessing(true);
    updateWorkflowFromState();

    setTimeout(() => {
      try {
        // Defaults replicate the original React logic (mask PII, remove IDs/prefixes, title-case)
        const anonymizer = new FinancialAnonymizer({
          customRemoveTerms
        });
        outputJSON = stagedRows ? anonymizer.processRows(stagedRows) : anonymizer.process(textarea.value);
        // setDiagnosticsFromOutput(outputJSON); // Delayed until AI button click

        // Remember mapping per header signature (privacy-first local storage)
        try {
          const headers = parseCSVHeaderRow(textarea.value);
          const sig = signatureFromHeaders(headers);
          const key = `fa:dialect:${sig}`;
          const payload = {
            savedAt: new Date().toISOString(),
            signature: sig,
            detectedColumnsList: outputJSON.metadata.detectedColumnsList ?? [],
            rowOrder: outputJSON.metadata.rowOrder ?? 'unknown',
            directionStrategy: outputJSON.metadata.directionStrategy ?? 'fallback',
            directionConfidence: outputJSON.metadata.directionConfidence ?? 0
          };
          localStorage.setItem(key, JSON.stringify(payload));
        } catch {
          // ignore localStorage issues
        }
      } catch (err: any) {
        setError(err?.message ?? 'Failed to process CSV.');
      } finally {
        setProcessing(false);
        renderOutput();
        updateWorkflowFromState();
      }
    }, 350); // UI breathing room
  }

  btnProcess.addEventListener('click', runProcessing);

  // Initial workflow state on load
  updateWorkflowFromState();

  // Search & Destroy Logic
  const btnSearchOpen = $('btn-search-open') as HTMLButtonElement | null;
  const outputCompareWrap = $('output-compare-wrap');
  const outputOriginalPane = $('output-original-pane');
  const outputOriginalEl = $('output-original');
  const outputSanitizedLabel = $('output-sanitized-label');
  const defaultHeader = $('default-header');
  const searchBar = $('search-bar');
  const searchInput = $('search-input') as HTMLInputElement | null;
  const searchCount = $('search-count');
  const btnSearchClose = $('btn-search-close');
  const searchActions = $('search-actions');
  const btnRedactAll = $('btn-redact-all');
  const btnRedactOne = $('btn-redact-one');
  const btnSearchPrev = $('btn-search-prev') as HTMLButtonElement | null;
  const btnSearchNext = $('btn-search-next') as HTMLButtonElement | null;
  const searchNav = $('search-nav');
  const findingsChips = $('findings-chips');

  let currentMatchIndex = -1;
  let matchElements: HTMLElement[] = [];
  let searchMatches: Array<{ tIdx: number, field: 'description' | 'merchant', start?: number, length?: number }> = [];

  const applyCompareLayout = () => {
    if (!outputCompareWrap || !outputOriginalPane || !outputSanitizedLabel) return;
    if (compareMode) {
      outputCompareWrap.classList.add('md:grid-cols-2');
      outputOriginalPane.classList.remove('hidden');
      outputSanitizedLabel.classList.remove('hidden');
    } else {
      outputCompareWrap.classList.remove('md:grid-cols-2');
      outputOriginalPane.classList.add('hidden');
      outputSanitizedLabel.classList.add('hidden');
    }
  };

  const highlightSource = (raw: string) => {
    const safe = escapeHtml(raw || '');
    // Basic redaction highlights (mirrors anonymizer patterns without importing them)
    const patterns: Array<[RegExp, string]> = [
      [/\b(?:\d[ -]*?){13,16}\b/g, 'card'],
      [/\b\d{3}-\d{2}-\d{4}\b/g, 'ssn'],
      [/[\w\.-]+@[\w\.-]+\.\w+/g, 'email'],
      [/\b(?:\+?\d{1,2}[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}\b/g, 'phone'],
      [/\bhttps?:\/\/\S+|\bwww\.\S+/gi, 'url'],
      [/\b\d{5}(?:-\d{4})?\b/g, 'zip'],
    ];
    let out = safe;
    for (const [re] of patterns) {
      out = out.replace(re, (m) => `<mark>${m}</mark>`);
    }
    return out;
  };

  const renderOriginalPane = () => {
    if (!outputOriginalEl) return;
    if (!compareMode) {
      outputOriginalEl.textContent = '';
      return;
    }
    if (highlightChanges) outputOriginalEl.innerHTML = highlightSource(latestSourceText);
    else outputOriginalEl.textContent = latestSourceText || '';
  };

  // Compare mode is disabled (feature removed) - always force false
  compareMode = false;
  try { localStorage.removeItem('fa:compare'); } catch { /* ignore */ }
  applyCompareLayout();

  // Highlight button - toggles PII highlighting in the OUTPUT
  const btnHighlightChanges = $('btn-highlight-changes') as HTMLButtonElement | null;
  
  // Always start with highlight OFF (don't persist - user preference per session)
  highlightChanges = false;
  (window as any).__highlightChanges = false;
  try { localStorage.removeItem('fa:highlightChanges'); } catch { /* ignore */ }
  
  btnHighlightChanges?.addEventListener('click', () => {
    highlightChanges = !highlightChanges;
    (window as any).__highlightChanges = highlightChanges;
    try { localStorage.setItem('fa:highlightChanges', highlightChanges ? 'true' : 'false'); } catch { /* ignore */ }
    
    // Re-render output with/without highlights
    renderOutput();
    
    // Visual feedback - toggle active state
    if (highlightChanges) {
      btnHighlightChanges.classList.add('bg-white/10', 'text-white');
      btnHighlightChanges.classList.remove('text-slate-500');
    } else {
      btnHighlightChanges.classList.remove('bg-white/10', 'text-white');
      btnHighlightChanges.classList.add('text-slate-500');
    }
  });

  function openSearch() {
    console.log('openSearch called', { outputJSON: !!outputJSON, defaultHeader: !!defaultHeader, searchBar: !!searchBar });
    if (!outputJSON) {
      console.log('No output JSON, aborting');
      return;
    }
    defaultHeader?.classList.add('hidden');
    searchBar?.classList.remove('hidden');
    searchBar?.classList.add('flex');
    searchInput?.focus();
    if (searchInput?.value) searchInput.dispatchEvent(new Event('input'));
  }

  function closeSearch() {
    searchBar?.classList.add('hidden');
    searchBar?.classList.remove('flex');
    defaultHeader?.classList.remove('hidden');
    renderOutput(); 
    if (searchInput) searchInput.value = '';
    if (searchCount) searchCount.textContent = '0';
    searchActions?.classList.add('hidden');
    searchActions?.classList.remove('flex');
    searchNav?.classList.add('hidden');
    matchElements = [];
    currentMatchIndex = -1;
  }

  btnSearchOpen?.addEventListener('click', () => {
    console.log('Search button clicked!', { btnSearchOpen, outputJSON: !!outputJSON });
    openSearch();
  });
  btnSearchClose?.addEventListener('click', closeSearch);

  // Findings chips → jump to matches using existing search tooling
  findingsChips?.addEventListener('click', (e) => {
    const target = (e.target as HTMLElement | null)?.closest?.('button[data-find]') as HTMLButtonElement | null;
    const term = target?.dataset?.find;
    if (!term) return;
    openSearch();
    if (searchInput) {
      searchInput.value = term;
      searchInput.dispatchEvent(new Event('input'));
    }
  });

  // Findings info tooltip (show/hide on click)
  const findingsInfoBtn = $('findings-info-btn');
  const findingsTooltip = $('findings-tooltip');
  
  findingsInfoBtn?.addEventListener('click', (e) => {
    e.stopPropagation();
    findingsTooltip?.classList.toggle('hidden');
  });
  
  // Close tooltip when clicking elsewhere
  document.addEventListener('click', (e) => {
    if (!findingsInfoBtn?.contains(e.target as Node)) {
      findingsTooltip?.classList.add('hidden');
    }
  });

  // Share-safety items (failed checks) → open search with a helpful seed term
  const shareSafetyItems = $('share-safety-items');
  shareSafetyItems?.addEventListener('click', (e) => {
    const target = (e.target as HTMLElement | null)?.closest?.('button[data-find]') as HTMLButtonElement | null;
    const term = target?.dataset?.find;
    if (!term) return;
    openSearch();
    if (searchInput) {
      searchInput.value = term;
      searchInput.dispatchEvent(new Event('input'));
    }
  });
  
  console.log('Search event listeners attached', { 
    btnSearchOpen: !!btnSearchOpen, 
    btnSearchClose: !!btnSearchClose,
    defaultHeader: !!defaultHeader,
    searchBar: !!searchBar 
  });

  function scrollToMatch(index: number) {
    if (!matchElements.length) return;
    if (index < 0) index = matchElements.length - 1;
    if (index >= matchElements.length) index = 0;
    
    currentMatchIndex = index;
    
    if (searchCount) searchCount.textContent = `${currentMatchIndex + 1}/${matchElements.length}`;
    
    matchElements.forEach((el, i) => {
      if (i === index) {
        el.classList.add('bg-brand-500', 'text-white');
        el.classList.remove('bg-brand-500/30');
        
        // Get the scroll container
        const scrollContainer = $('output-scroll-container');
        
        if (scrollContainer) {
          // Calculate position within the scroll container
          const containerRect = scrollContainer.getBoundingClientRect();
          const elementRect = el.getBoundingClientRect();
          
          // Calculate scroll offset - element position relative to container
          const offsetTop = scrollContainer.scrollTop + elementRect.top - containerRect.top;
          const centerOffset = offsetTop - (containerRect.height / 2) + (elementRect.height / 2);
          
          // Scroll the container (NOT the page)
          scrollContainer.scrollTop = Math.max(0, centerOffset);
        }
      } else {
        el.classList.remove('bg-brand-500', 'text-white');
        el.classList.add('bg-brand-500/30');
      }
    });
  }

  btnSearchPrev?.addEventListener('click', () => scrollToMatch(currentMatchIndex - 1));
  btnSearchNext?.addEventListener('click', () => scrollToMatch(currentMatchIndex + 1));

  // Global Cmd+F / Ctrl+F
  document.addEventListener('keydown', (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'f') {
      if (outputJSON) {
        e.preventDefault();
        openSearch();
      }
    }
    // Close on Escape
    if (e.key === 'Escape' && searchBar && !searchBar.classList.contains('hidden')) {
      e.preventDefault();
      closeSearch();
    }
    // Nav with Enter / Shift+Enter
    if (e.key === 'Enter' && searchBar && !searchBar.classList.contains('hidden') && document.activeElement === searchInput) {
      e.preventDefault();
      if (e.shiftKey) scrollToMatch(currentMatchIndex - 1);
      else scrollToMatch(currentMatchIndex + 1);
    }
  });

  searchInput?.addEventListener('input', () => {
    const term = (searchInput.value ?? '').trim();
    const outputEl = $('output');
    
    if (!term || !outputJSON || !outputEl) {
      if (searchCount) searchCount.textContent = '0';
      searchActions?.classList.add('hidden');
      searchActions?.classList.remove('flex');
      searchNav?.classList.add('hidden');
      searchNav?.classList.remove('flex');
      
      // Restore clean text
      renderOutput();
      
      matchElements = [];
      searchMatches = [];
      currentMatchIndex = -1;
      return;
    }

    // Render with Highlighting
    renderOutput(term);
    
    // Find highlighted elements in DOM
    matchElements = Array.from(outputEl.querySelectorAll('mark'));
    const count = matchElements.length;

    // Populate searchMatches (Data Model) - strictly for Delete operations
    // We need to search both merchant AND description since output uses merchant when available
    searchMatches = [];
    const termLower = term.toLowerCase();
    // Search all rows (no limit)
    const rowsToScan = outputJSON.transactions;
    
    for (let i = 0; i < rowsToScan.length; i++) {
      const t = rowsToScan[i];
      
      // Search in merchant field (primary display field)
      const merchant = t.merchant || '';
      let pos = 0;
      while (true) {
        const idx = merchant.toLowerCase().indexOf(termLower, pos);
        if (idx === -1) break;
        searchMatches.push({ tIdx: i, field: 'merchant', start: idx, length: term.length });
        pos = idx + 1;
      }
      
      // Also search in description field (fallback display)
      const desc = t.description || '';
      pos = 0;
      while (true) {
        const idx = desc.toLowerCase().indexOf(termLower, pos);
        if (idx === -1) break;
        searchMatches.push({ tIdx: i, field: 'description', start: idx, length: term.length });
        pos = idx + 1;
      }
    }

    if (count > 0) {
      if (searchCount) searchCount.textContent = `${count}`;
      searchActions?.classList.remove('hidden');
      searchActions?.classList.add('flex');
      
      // Show nav
      searchNav?.classList.remove('hidden');
      searchNav?.classList.add('flex');
      if (btnSearchPrev) btnSearchPrev.disabled = false;
      if (btnSearchNext) btnSearchNext.disabled = false;
      if (btnRedactOne) (btnRedactOne as HTMLButtonElement).disabled = false;

      // Jump to first match
      scrollToMatch(0);
    } else {
      if (searchCount) searchCount.textContent = '0';
      searchActions?.classList.add('hidden');
      searchActions?.classList.remove('flex');
      searchNav?.classList.add('hidden');
      matchElements = [];
      searchMatches = [];
    }
  });

  btnRedactOne?.addEventListener('click', () => {
    // We can only delete if we can map the current visual match to a data match.
    // Simple heuristic: If visual count == data count, we assume 1:1 mapping.
    // If not, we disable "Delete This" to avoid deleting the wrong thing (safety first).
    // Or we just try to find the "closest" data match? 
    // Let's rely on the index for now but be safe.
    
    if (currentMatchIndex < 0 || !outputJSON) return;
    
    // Safety check: Does this match index exist in our data matches?
    // If highlight found 10 items (some in dates), but data has 5 (descriptions), 
    // index 8 is invalid.
    const match = searchMatches[currentMatchIndex];
    
    // If no exact match at this index (e.g. user selected a Date match which isn't in searchMatches),
    // we can't delete it (it's immutable metadata).
    if (!match) {
       // Optional: flash error or just ignore
       return;
    }
    
    const t = outputJSON.transactions[match.tIdx];
    const term = (searchInput?.value ?? '').trim();
    
    if (t && term && typeof match.start === 'number') {
      const val = t[match.field] || '';
      const len = match.length ?? term.length;
      const target = val.slice(match.start, match.start + len);
      
      // Verify match before delete
      if (target.toLowerCase() === term.toLowerCase()) {
         const before = val.slice(0, match.start);
         const after = val.slice(match.start + len);
         // Remove term and collapse spaces
         t[match.field] = (before + after).replace(/\s+/g, ' ').trim();
         
         // Refresh view
         searchInput?.dispatchEvent(new Event('input'));
      }
    }
  });

  btnRedactAll?.addEventListener('click', () => {
    const term = (searchInput?.value ?? '').trim();
    console.log('Delete All clicked for term:', term);
    if (!term) {
      console.log('No term to delete');
      return;
    }

    // Known PII placeholder terms (without brackets)
    const piiTerms = ['PHONE', 'EMAIL', 'SSN', 'URL', 'ADDRESS', 'ZIP', 'CARD', 'LOC', 'NAME'];
    const upperTerm = term.toUpperCase();
    const isPiiTerm = piiTerms.includes(upperTerm);

    // Add to custom terms (case-insensitive check)
    const existing = new Set(customRemoveTerms.map(t => t.toLowerCase()));
    
    console.log('Current custom terms:', customRemoveTerms);
    console.log('Is PII term?', isPiiTerm);

    // For PII terms, add the BRACKETED version (e.g., [URL]) so it matches the placeholder
    const termsToAdd: string[] = [];
    if (isPiiTerm) {
      const bracketed = `[${upperTerm}]`;
      if (!existing.has(bracketed.toLowerCase())) {
        termsToAdd.push(bracketed);
      }
    }
    // Also add the plain term for non-PII or edge cases
    if (!existing.has(term.toLowerCase()) && !isPiiTerm) {
      termsToAdd.push(term);
    }

    for (const t of termsToAdd) {
      customRemoveTerms.unshift(t);
    }
    if (customRemoveTerms.length > 50) customRemoveTerms = customRemoveTerms.slice(0, 50);
    saveCustomTerms();
    renderCustomTerms();
    console.log('Terms added to custom terms:', termsToAdd, 'Full list:', customRemoveTerms);

    // Always Clear search & Reprocess
    console.log('Closing search and reprocessing...');
    closeSearch();
    runProcessing();
  });

  // AI Analysis Button & Modal Logic
  const btnAiAnalysis = $('btn-ai-analysis');
  const modal = $('ai-confirm-modal');
  const btnModalCancel = $('btn-modal-cancel');
  const btnModalConfirm = $('btn-modal-confirm');

  function closeModal() {
    modal?.classList.add('hidden');
  }

  function openModal() {
    modal?.classList.remove('hidden');
  }

  btnAiAnalysis?.addEventListener('click', () => {
    if (!outputJSON) return;
    openModal();
  });

  btnModalCancel?.addEventListener('click', closeModal);

  // Close on backdrop click
  modal?.addEventListener('click', (e) => {
    if (e.target === modal) closeModal();
  });

  // Close on Escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !modal?.classList.contains('hidden')) {
      closeModal();
    }
  });

  btnModalConfirm?.addEventListener('click', () => {
    closeModal();
    
    $('analysis')?.classList.remove('hidden');
    $('diagnostics')?.classList.remove('hidden');
    
    // Run analysis
    renderAnalysis();
    if (outputJSON) setDiagnosticsFromOutput(outputJSON);
    
    // Mark workflow step 3 as complete
    markStep3Done();
    
    // Scroll to analysis
    $('analysis')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });

  btnPreflightRerun?.addEventListener('click', async () => {
    if (!fileInput?.files?.[0]) return;
    if (stagedKind !== 'pdf') return;
    const ps = parseInt(pdfPageStart?.value || '1', 10);
    const pe = parseInt(pdfPageEnd?.value || '', 10);
    await runPreflightForFile(fileInput.files[0], { pageStart: isFinite(ps) ? ps : 1, pageEnd: isFinite(pe) ? pe : undefined });
  });

  // Copy (copies all rows)
  btnCopy?.addEventListener('click', async () => {
    if (!outputJSON) return;
    const formatted = new FinancialAnonymizer().formatData(outputJSON, activeFormat, { maxRows: Infinity, detailLevel: activeDetailLevel, profile: activeProfile });
    try {
      await navigator.clipboard.writeText(formatted);
      
      // Visual feedback
      const iconCopy = btnCopy.querySelector('.copy-icon');
      const iconCheck = btnCopy.querySelector('.check-icon');
      const textSpan = btnCopy.querySelector('.btn-text');
      
      // Hide copy icon, show check icon
      iconCopy?.classList.add('hidden');
      iconCheck?.classList.remove('hidden');
      if (textSpan) textSpan.textContent = 'Copied!';
      
      // Flash the button green
      btnCopy.classList.add('text-green-400');
      
      // Mark workflow step 3 as complete
      markStep3Done();
      
      setTimeout(() => {
        iconCopy?.classList.remove('hidden');
        iconCheck?.classList.add('hidden');
        if (textSpan) textSpan.textContent = 'Copy';
        btnCopy.classList.remove('text-green-400');
      }, 2000);
    } catch {
      // fall back silently
    }
  });

  // Download (full export for current format)
  btnDownload?.addEventListener('click', () => {
    if (!outputJSON) return;
    const ext =
      activeFormat === 'markdown' || activeFormat === 'storyline'
        ? 'md'
        : activeFormat === 'text'
          ? 'txt'
          : activeFormat;
    const mime =
      activeFormat === 'json'
        ? 'application/json'
        : activeFormat === 'csv'
          ? 'text/csv'
          : 'text/plain';

    // Full export (no limit)
    const formatted = new FinancialAnonymizer().formatData(outputJSON, activeFormat, { detailLevel: activeDetailLevel, profile: activeProfile });
    const blob = new Blob([formatted], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `sanitized_data.${ext}`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    // Mark workflow step 3 as complete
    markStep3Done();
  });

  // Detail style selector (Minimal / Standard / Debug) — stored internally as ExportProfile
  const profileBtn = $('profile-btn');
  const profileMenu = $('profile-menu');
  const profileLabel = $('profile-label');
  const profileOptions = document.querySelectorAll('.profile-option');

  // Restore saved profile preference (best-effort)
  try {
    const saved = localStorage.getItem('fa:profile');
    if (saved === 'ai_safe' || saved === 'audit' || saved === 'debug') {
      activeProfile = saved;
    }
  } catch {
    // ignore
  }
  // Ensure detail level tracks the selected profile
  activeDetailLevel = activeProfile === 'audit' ? 'standard' : activeProfile === 'debug' ? 'debug' : 'minimal';

  const setProfileLabel = () => {
    if (!profileLabel) return;
    profileLabel.textContent =
      activeProfile === 'audit' ? 'Standard' : activeProfile === 'debug' ? 'Debug' : 'Minimal';
  };
  setProfileLabel();

  profileBtn?.addEventListener('click', (e) => {
    e.stopPropagation();
    profileMenu?.classList.toggle('hidden');
  });

  // Close menus when clicking outside
  document.addEventListener('click', (e) => {
    if (!profileBtn?.contains(e.target as Node) && !profileMenu?.contains(e.target as Node)) {
      profileMenu?.classList.add('hidden');
    }
  });

  profileOptions.forEach(opt => {
    opt.addEventListener('click', () => {
      const next = opt.getAttribute('data-profile') as ExportProfile | null;
      if (!next) return;

      activeProfile = next;
      try { localStorage.setItem('fa:profile', activeProfile); } catch { /* ignore */ }

      // Set a sensible default detail level for the chosen profile (user can still override)
      if (activeProfile === 'ai_safe') activeDetailLevel = 'minimal';
      if (activeProfile === 'audit') activeDetailLevel = 'standard';
      if (activeProfile === 'debug') activeDetailLevel = 'debug';
      setProfileLabel();
      renderOutput();
      profileMenu?.classList.add('hidden');
    });
  });

  // File Type Selector
  const btnFileType = $('btn-file-type');
  const fileTypeMenu = $('file-type-menu');
  const fileTypeLabel = $('file-type-label');
  const fileTypeOptions = document.querySelectorAll('.file-type-option');
  
  // File type state (for future optimization)
  let currentFileType: 'bank_statement' | 'credit_card' | 'expense_report' | 'transaction_csv' = 'bank_statement';
  
  // Restore saved file type preference
  try {
    const saved = localStorage.getItem('fa:file-type');
    if (saved === 'bank_statement' || saved === 'credit_card' || saved === 'expense_report' || saved === 'transaction_csv') {
      currentFileType = saved;
    }
  } catch {
    // ignore
  }
  
  const setFileTypeLabel = () => {
    if (!fileTypeLabel) return;
    const labels: Record<typeof currentFileType, string> = {
      'bank_statement': 'Bank Statement',
      'credit_card': 'Credit Card',
      'expense_report': 'Expense Report',
      'transaction_csv': 'Transaction CSV'
    };
    fileTypeLabel.textContent = labels[currentFileType];
    
    // Update active state in dropdown
    fileTypeOptions.forEach(opt => {
      const optionType = opt.getAttribute('data-file-type');
      if (optionType === currentFileType) {
        opt.classList.add('bg-white/5');
        opt.querySelector('.text-slate-200')?.classList.replace('text-slate-200', 'text-white');
      } else {
        opt.classList.remove('bg-white/5');
        opt.querySelector('.text-white')?.classList.replace('text-white', 'text-slate-200');
      }
    });
  };
  setFileTypeLabel();

  btnFileType?.addEventListener('click', (e) => {
    e.stopPropagation();
    fileTypeMenu?.classList.toggle('hidden');
  });

  // Close file type menu when clicking outside
  document.addEventListener('click', (e) => {
    if (!btnFileType?.contains(e.target as Node) && !fileTypeMenu?.contains(e.target as Node)) {
      fileTypeMenu?.classList.add('hidden');
    }
  });

  fileTypeOptions.forEach(opt => {
    opt.addEventListener('click', () => {
      const next = opt.getAttribute('data-file-type') as typeof currentFileType | null;
      if (!next) return;

      currentFileType = next;
      try { localStorage.setItem('fa:file-type', currentFileType); } catch { /* ignore */ }
      
      setFileTypeLabel();
      fileTypeMenu?.classList.add('hidden');
      
      // Future: Apply optimizations based on file type
      console.log(`File type selected: ${currentFileType}. Optimizations will be applied accordingly.`);
    });
  });

  // Initial state
  setActiveFormat('markdown');
  renderOutput();
  renderPlannedRemovals(null);

  // ============================================================================
  // DEEP CLEAN WITH PRESIDIO (Interactive Wizard)
  // ============================================================================
  
  const PRESIDIO_API = 'http://127.0.0.1:8000';
  
  interface Candidate {
    text: string;
    type: string;
    confidence: number;
    count: number;
    locations: Array<{ row: number; field: string; start: number; end: number }>;
  }
  
  type CandidateAction = 'undecided' | 'keep' | 'redact' | 'context';
  interface CandidateDecision {
    action: CandidateAction;
    contextLabel?: string;
    explicitlyDecided?: boolean; // True when user clicked a button (vs default state)
  }

  // Context label options for the tag picker
  // Kept intentionally user-facing + finance oriented.
  const CONTEXT_LABELS = [
    'MERCHANT',
    'SOFTWARE',
    'SUBSCRIPTION',
    'SERVICE',
    'BANK',
    'UTILITY',
    'FOOD',
    'TRANSPORT',
    'GAS',
    'GROCERIES',
    'RENT',
    'INSURANCE',
    'HEALTH',
    'EDUCATION',
    'ENTERTAINMENT',
    'TRAVEL',
    'SHOPPING',
    'TAX',
    'INCOME',
    'TRANSFER'
  ];

  // Known brands that Presidio often misdetects as PERSON
  // These are business names, not actual people - with accurate category recommendations
  const KNOWN_BRANDS: Record<string, string[]> = {
    // === PAYMENT PROCESSORS & MONEY TRANSFER ===
    'paypal': ['PAYMENT', 'TRANSFER', 'MERCHANT'],
    'stripe': ['PAYMENT', 'SOFTWARE', 'SERVICE'],
    'square': ['PAYMENT', 'MERCHANT', 'SERVICE'],
    'venmo': ['PAYMENT', 'TRANSFER', 'SERVICE'],
    'zelle': ['PAYMENT', 'TRANSFER', 'BANK'],
    'wise': ['PAYMENT', 'TRANSFER', 'BANK'],
    'transferwise': ['PAYMENT', 'TRANSFER', 'BANK'],
    'interac': ['PAYMENT', 'TRANSFER', 'BANK'],
    'e-transfer': ['PAYMENT', 'TRANSFER', 'BANK'],
    'etransfer': ['PAYMENT', 'TRANSFER', 'BANK'],
    'cash app': ['PAYMENT', 'TRANSFER', 'SERVICE'],
    'apple pay': ['PAYMENT', 'MERCHANT', 'SERVICE'],
    'google pay': ['PAYMENT', 'MERCHANT', 'SERVICE'],
    
    // === AI & SOFTWARE SUBSCRIPTIONS ===
    'suno': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'suno.ai': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'midjourney': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'claude': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'claude.ai': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'anthropic': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'openai': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'chatgpt': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'perplexity': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'cursor': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'copilot': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'github copilot': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'replit': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'runway': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'elevenlabs': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'pika': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'gamma': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'gamma.app': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'notion': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'figma': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'canva': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'adobe': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'microsoft': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'office 365': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'slack': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'zoom': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'dropbox': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'github': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'vercel': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'netlify': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'heroku': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'linear': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'asana': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'trello': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'monday.com': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'airtable': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'grammarly': ['SUBSCRIPTION', 'SOFTWARE', 'SERVICE'],
    'jasper': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    'copy.ai': ['SUBSCRIPTION', 'AI', 'SOFTWARE'],
    
    // === STREAMING & ENTERTAINMENT ===
    'netflix': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'spotify': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'apple music': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'disney': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'disney+': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'hulu': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'amazon prime': ['SUBSCRIPTION', 'ENTERTAINMENT', 'SHOPPING'],
    'prime video': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'crave': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'paramount': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'hbo': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'max': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'youtube': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'youtube premium': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'twitch': ['SUBSCRIPTION', 'ENTERTAINMENT', 'STREAMING'],
    'patreon': ['SUBSCRIPTION', 'ENTERTAINMENT', 'SERVICE'],
    'substack': ['SUBSCRIPTION', 'NEWS', 'SERVICE'],
    'audible': ['SUBSCRIPTION', 'ENTERTAINMENT', 'SERVICE'],
    'kindle': ['SUBSCRIPTION', 'ENTERTAINMENT', 'SERVICE'],
    'playstation': ['SUBSCRIPTION', 'ENTERTAINMENT', 'GAMING'],
    'xbox': ['SUBSCRIPTION', 'ENTERTAINMENT', 'GAMING'],
    'steam': ['ENTERTAINMENT', 'GAMING', 'SOFTWARE'],
    'epic games': ['ENTERTAINMENT', 'GAMING', 'SOFTWARE'],
    
    // === GAS STATIONS ===
    'pioneer': ['GAS', 'TRANSPORT', 'MERCHANT'],
    'shell': ['GAS', 'TRANSPORT', 'MERCHANT'],
    'esso': ['GAS', 'TRANSPORT', 'MERCHANT'],
    'petro': ['GAS', 'TRANSPORT', 'MERCHANT'],
    'petro canada': ['GAS', 'TRANSPORT', 'MERCHANT'],
    'ultramar': ['GAS', 'TRANSPORT', 'MERCHANT'],
    'mobil': ['GAS', 'TRANSPORT', 'MERCHANT'],
    'exxon': ['GAS', 'TRANSPORT', 'MERCHANT'],
    'chevron': ['GAS', 'TRANSPORT', 'MERCHANT'],
    'husky': ['GAS', 'TRANSPORT', 'MERCHANT'],
    'sunoco': ['GAS', 'TRANSPORT', 'MERCHANT'],
    'circle k': ['GAS', 'CONVENIENCE', 'MERCHANT'],
    '7-eleven': ['GAS', 'CONVENIENCE', 'MERCHANT'],
    '7 eleven': ['GAS', 'CONVENIENCE', 'MERCHANT'],
    
    // === FOOD & RESTAURANTS ===
    'tim hortons': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'tim horton': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'starbucks': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'dunkin': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'mcdonalds': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    "mcdonald's": ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'wendys': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    "wendy's": ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'burger king': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'subway': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'popeyes': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'chipotle': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'taco bell': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'kfc': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'pizza hut': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'dominos': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    "domino's": ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'papa johns': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'five guys': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'chick-fil-a': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'panera': ['FOOD', 'RESTAURANT', 'MERCHANT'],
    'doordash': ['FOOD', 'DELIVERY', 'SERVICE'],
    'ubereats': ['FOOD', 'DELIVERY', 'SERVICE'],
    'uber eats': ['FOOD', 'DELIVERY', 'SERVICE'],
    'skip the dishes': ['FOOD', 'DELIVERY', 'SERVICE'],
    'grubhub': ['FOOD', 'DELIVERY', 'SERVICE'],
    'instacart': ['GROCERIES', 'DELIVERY', 'SERVICE'],
    
    // === GROCERIES & RETAIL ===
    'walmart': ['GROCERIES', 'SHOPPING', 'MERCHANT'],
    'costco': ['GROCERIES', 'SHOPPING', 'MERCHANT'],
    'amazon': ['SHOPPING', 'SUBSCRIPTION', 'MERCHANT'],
    'loblaws': ['GROCERIES', 'SHOPPING', 'MERCHANT'],
    'sobeys': ['GROCERIES', 'SHOPPING', 'MERCHANT'],
    'safeway': ['GROCERIES', 'SHOPPING', 'MERCHANT'],
    'whole foods': ['GROCERIES', 'SHOPPING', 'MERCHANT'],
    'trader joes': ['GROCERIES', 'SHOPPING', 'MERCHANT'],
    "trader joe's": ['GROCERIES', 'SHOPPING', 'MERCHANT'],
    'target': ['SHOPPING', 'GROCERIES', 'MERCHANT'],
    'ikea': ['SHOPPING', 'FURNITURE', 'MERCHANT'],
    'home depot': ['SHOPPING', 'HARDWARE', 'MERCHANT'],
    'lowes': ['SHOPPING', 'HARDWARE', 'MERCHANT'],
    "lowe's": ['SHOPPING', 'HARDWARE', 'MERCHANT'],
    'best buy': ['SHOPPING', 'ELECTRONICS', 'MERCHANT'],
    'apple store': ['SHOPPING', 'ELECTRONICS', 'MERCHANT'],
    'dollarama': ['SHOPPING', 'MERCHANT', 'GROCERIES'],
    'dollar tree': ['SHOPPING', 'MERCHANT', 'GROCERIES'],
    'winners': ['SHOPPING', 'CLOTHING', 'MERCHANT'],
    'marshalls': ['SHOPPING', 'CLOTHING', 'MERCHANT'],
    'tj maxx': ['SHOPPING', 'CLOTHING', 'MERCHANT'],
    'sephora': ['SHOPPING', 'BEAUTY', 'MERCHANT'],
    'ulta': ['SHOPPING', 'BEAUTY', 'MERCHANT'],
    'lululemon': ['SHOPPING', 'CLOTHING', 'FITNESS'],
    'nike': ['SHOPPING', 'CLOTHING', 'FITNESS'],
    'adidas': ['SHOPPING', 'CLOTHING', 'FITNESS'],
    
    // === TRANSPORT & RIDESHARE ===
    'uber': ['TRANSPORT', 'RIDESHARE', 'SERVICE'],
    'lyft': ['TRANSPORT', 'RIDESHARE', 'SERVICE'],
    'taxi': ['TRANSPORT', 'RIDESHARE', 'SERVICE'],
    'presto': ['TRANSPORT', 'TRANSIT', 'SERVICE'],
    'ttc': ['TRANSPORT', 'TRANSIT', 'SERVICE'],
    'go transit': ['TRANSPORT', 'TRANSIT', 'SERVICE'],
    'via rail': ['TRANSPORT', 'TRAVEL', 'SERVICE'],
    'air canada': ['TRANSPORT', 'TRAVEL', 'AIRLINE'],
    'westjet': ['TRANSPORT', 'TRAVEL', 'AIRLINE'],
    'porter': ['TRANSPORT', 'TRAVEL', 'AIRLINE'],
    'united': ['TRANSPORT', 'TRAVEL', 'AIRLINE'],
    'delta': ['TRANSPORT', 'TRAVEL', 'AIRLINE'],
    'american airlines': ['TRANSPORT', 'TRAVEL', 'AIRLINE'],
    'southwest': ['TRANSPORT', 'TRAVEL', 'AIRLINE'],
    'airbnb': ['TRAVEL', 'ACCOMMODATION', 'SERVICE'],
    'vrbo': ['TRAVEL', 'ACCOMMODATION', 'SERVICE'],
    'booking.com': ['TRAVEL', 'ACCOMMODATION', 'SERVICE'],
    'expedia': ['TRAVEL', 'SERVICE', 'BOOKING'],
    'marriott': ['TRAVEL', 'ACCOMMODATION', 'HOTEL'],
    'hilton': ['TRAVEL', 'ACCOMMODATION', 'HOTEL'],
    'hyatt': ['TRAVEL', 'ACCOMMODATION', 'HOTEL'],
    
    // === UTILITIES & TELECOM ===
    'rogers': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'bell': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'telus': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'fido': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'koodo': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'virgin mobile': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'freedom mobile': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'shaw': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'videotron': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'at&t': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    't-mobile': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'verizon': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'comcast': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'xfinity': ['UTILITY', 'TELECOM', 'SUBSCRIPTION'],
    'hydro': ['UTILITY', 'ENERGY', 'BILL'],
    'enbridge': ['UTILITY', 'ENERGY', 'BILL'],
    'fortis': ['UTILITY', 'ENERGY', 'BILL'],
    
    // === HEALTH & FITNESS ===
    'goodlife': ['FITNESS', 'HEALTH', 'SUBSCRIPTION'],
    'planet fitness': ['FITNESS', 'HEALTH', 'SUBSCRIPTION'],
    'la fitness': ['FITNESS', 'HEALTH', 'SUBSCRIPTION'],
    'equinox': ['FITNESS', 'HEALTH', 'SUBSCRIPTION'],
    'peloton': ['FITNESS', 'SUBSCRIPTION', 'SERVICE'],
    'walgreens': ['HEALTH', 'PHARMACY', 'MERCHANT'],
    'cvs': ['HEALTH', 'PHARMACY', 'MERCHANT'],
    'shoppers drug mart': ['HEALTH', 'PHARMACY', 'MERCHANT'],
    'rexall': ['HEALTH', 'PHARMACY', 'MERCHANT'],
    
    // === INSURANCE & FINANCIAL SERVICES ===
    'state farm': ['INSURANCE', 'FINANCIAL', 'SERVICE'],
    'geico': ['INSURANCE', 'FINANCIAL', 'SERVICE'],
    'progressive': ['INSURANCE', 'FINANCIAL', 'SERVICE'],
    'allstate': ['INSURANCE', 'FINANCIAL', 'SERVICE'],
    'wealthsimple': ['INVESTMENT', 'FINANCIAL', 'SERVICE'],
    'questrade': ['INVESTMENT', 'FINANCIAL', 'SERVICE'],
    'robinhood': ['INVESTMENT', 'FINANCIAL', 'SERVICE'],
    'coinbase': ['INVESTMENT', 'CRYPTO', 'SERVICE'],
    'binance': ['INVESTMENT', 'CRYPTO', 'SERVICE'],
    'kraken': ['INVESTMENT', 'CRYPTO', 'SERVICE'],
  };

  // ===== CANDIDATE DEDUPLICATION & ROI CALCULATION =====
  
  /**
   * Check if entity type is sensitive PII that should never be auto-categorized.
   */
  function isSensitivePII(type: string): boolean {
    return ['PHONE_NUMBER', 'EMAIL_ADDRESS', 'US_SSN', 'CREDIT_CARD', 'IBAN_CODE', 'US_BANK_NUMBER', 'US_PASSPORT', 'IP_ADDRESS'].includes(type);
  }

  /**
   * Calculate Levenshtein distance between two strings.
   * Returns the minimum number of single-character edits needed to transform one string into another.
   */
  function levenshteinDistance(str1: string, str2: string): number {
    const len1 = str1.length;
    const len2 = str2.length;
    
    // Create a matrix
    const matrix: number[][] = [];
    for (let i = 0; i <= len1; i++) {
      matrix[i] = [i];
    }
    for (let j = 0; j <= len2; j++) {
      matrix[0][j] = j;
    }
    
    // Fill the matrix
    for (let i = 1; i <= len1; i++) {
      for (let j = 1; j <= len2; j++) {
        if (str1[i - 1] === str2[j - 1]) {
          matrix[i][j] = matrix[i - 1][j - 1];
        } else {
          matrix[i][j] = Math.min(
            matrix[i - 1][j] + 1,     // deletion
            matrix[i][j - 1] + 1,     // insertion
            matrix[i - 1][j - 1] + 1  // substitution
          );
        }
      }
    }
    
    return matrix[len1][len2];
  }

  /**
   * Calculate similarity between two strings (0 to 1, where 1 is identical).
   * Uses normalized versions and Levenshtein distance.
   */
  function calculateSimilarity(str1: string, str2: string): number {
    // Normalize both strings
    const n1 = normalizeForGrouping(str1);
    const n2 = normalizeForGrouping(str2);
    
    // If normalized versions are identical, similarity is 1.0
    if (n1 === n2) return 1.0;
    
    // If either is empty after normalization, return 0
    if (n1.length === 0 || n2.length === 0) return 0.0;
    
    // Calculate Levenshtein distance
    const distance = levenshteinDistance(n1, n2);
    const maxLen = Math.max(n1.length, n2.length);
    
    // Similarity = 1 - (normalized distance)
    return 1 - (distance / maxLen);
  }

  /**
   * Extract pattern from text for grouping similar transaction types.
   * Returns a pattern string if detected, null otherwise.
   */
  function extractPattern(text: string, type: string): string | null {
    const normalized = text.toLowerCase().trim();
    
    // For PERSON entities in transfer contexts
    // "Tfr To Xyt", "Tfr To Nac", "Tfr To Quj" → "Tfr To [PERSON]"
    if (type === 'PERSON' && /^tfr\s+to\s+/i.test(normalized)) {
      return 'Tfr To [PERSON]';
    }
    
    // For bank account patterns
    // "SSV TO 12345", "SSV TO 67890" → "SSV To [BANK]"
    if (/^ssv\s+to\s+/i.test(normalized)) {
      return 'SSV To [BANK]';
    }
    
    // ACH transfers
    if (/^ach\s+(to|from)\s+/i.test(normalized)) {
      return 'ACH Transfer';
    }
    
    // Wire transfers
    if (/^wire\s+(to|from)\s+/i.test(normalized)) {
      return 'Wire Transfer';
    }
    
    // Venmo, Zelle, Cash App patterns
    if (/^(venmo|zelle|cash app|paypal|interac|wise|remitly)\s+/i.test(normalized)) {
      return 'Payment Transfer';
    }
    
    // Generic transfer pattern
    if (/^(transfer|tfr|trf|send|sent)\s+(to|from)\s+/i.test(normalized)) {
      return 'Transfer [ACCOUNT]';
    }
    
    // Merchant with store/location numbers
    // "Tim Hortons Store 123", "Walmart #456" → "Tim Hortons [Store]", "Walmart [Store]"
    if (type === 'ORGANIZATION' || type === 'ORG') {
      const storeMatch = normalized.match(/^(.+?)\s+(store|#|location|stn|shop)\s*[#\d]+/i);
      if (storeMatch) {
        return `${storeMatch[1].trim()} [Store]`;
      }
    }
    
    // Recurring payment patterns (subscriptions, utilities)
    if (/^(monthly|weekly|annual|recurring|subscription|auto-pay|autopay)\s+/i.test(normalized)) {
      return 'Recurring Payment';
    }
    
    return null;
  }

  /**
   * Normalize text for grouping similar items together.
   * Enhanced to better handle transaction patterns and variable parts.
   * "PayPal *ABC123", "PAYPAL", "Paypal Inc" → all become "paypal"
   * "Tfr To Xyt", "Tfr To Nac" → both become "tfr to [person]" pattern
   */
  function normalizeForGrouping(text: string): string {
    let s = text.toLowerCase().trim();
    
    // Recognize and normalize transaction patterns first
    // "Tfr To [NAME]" → "tfr to [person]"
    s = s.replace(/^tfr\s+to\s+\w+/i, 'tfr to [person]');
    s = s.replace(/^ssv\s+to\s+\w+/i, 'ssv to [bank]');
    s = s.replace(/^ach\s+(to|from)\s+\w+/i, 'ach transfer');
    s = s.replace(/^wire\s+(to|from)\s+\w+/i, 'wire transfer');
    s = s.replace(/^(venmo|zelle|cash app|paypal|interac|wise|remitly)\s+\w+/i, 'payment transfer');
    s = s.replace(/^(transfer|tfr|trf|send|sent)\s+(to|from)\s+\w+/i, 'transfer [account]');
    // Merchant store patterns: "Tim Hortons Store 123" → "tim hortons [store]"
    s = s.replace(/\s+(store|#|location|stn|shop)\s*[#\d]+/i, ' [store]');
    // Recurring payment patterns
    s = s.replace(/^(monthly|weekly|annual|recurring|subscription|auto-pay|autopay)\s+/i, 'recurring payment ');
    
    // Remove common suffixes and prefixes
    s = s.replace(/^(sq \*|paypal \*|pp \*|tst \*|uber \*|amzn |amazon |amz )/i, '');
    // Remove trailing IDs, numbers, asterisks
    s = s.replace(/[\s*#]+[\w\d]+$/, '');
    s = s.replace(/\s+(inc|llc|ltd|corp|co|plc)\.?$/i, '');
    // Remove punctuation
    s = s.replace(/[^\w\s\[\]]/g, ''); // Keep brackets for patterns
    // Collapse whitespace
    s = s.replace(/\s+/g, ' ').trim();
    return s;
  }
  
  /**
   * Calculate financial impact for a candidate:
   * Total absolute amount across all occurrences
   */
  function calculateFinancialImpact(c: Candidate): number {
    if (!outputJSON || !c.locations) return 0;
    
    let totalAmount = 0;
    for (const loc of c.locations) {
      const row = outputJSON.transactions[loc.row];
      if (row?.amount) {
        totalAmount += Math.abs(row.amount);
      }
    }
    return totalAmount;
  }
  
  /**
   * Group similar candidates together and merge their locations.
   * Uses a two-pass approach:
   * 1. Exact match grouping (by normalized text)
   * 2. Similarity-based grouping (fuzzy matching for similar entities)
   * Returns deduplicated candidates with combined counts and locations.
   */
  function deduplicateCandidates(candidates: Candidate[]): Candidate[] {
    const SIMILARITY_THRESHOLD = 0.70; // 70% similarity required to group (lowered for better grouping)
    const groups = new Map<string, Candidate[]>();
    const ungrouped: Candidate[] = [];
    
    // PASS 1: Group by exact normalized match and pattern recognition
    for (const c of candidates) {
      // Try pattern recognition first
      const pattern = extractPattern(c.text, c.type);
      const key = pattern || normalizeForGrouping(c.text);
      
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key)!.push(c);
    }
    
    // PASS 2: For groups with single items, try similarity-based grouping
    // First, merge exact matches
    const merged: Candidate[] = [];
    const singleItemGroups: Array<{ key: string; candidate: Candidate }> = [];
    
    for (const [key, groupCandidates] of groups) {
      if (groupCandidates.length === 1) {
        // Single item - try to match with existing merged groups
        singleItemGroups.push({ key, candidate: groupCandidates[0] });
      } else {
        // Multiple items - merge them
        const mergedCandidate = mergeGroup(groupCandidates);
        merged.push(mergedCandidate);
      }
    }
    
    // PASS 3: Try to match single items with merged groups using similarity
    for (const { candidate } of singleItemGroups) {
      let matched = false;
      
      // Check pattern first
      const pattern = extractPattern(candidate.text, candidate.type);
      if (pattern) {
        // Find existing group with same pattern
        const existingGroup = merged.find(m => {
          const mPattern = extractPattern(m.text, m.type);
          return mPattern === pattern && m.type === candidate.type;
        });
        
        if (existingGroup) {
          // Merge into existing group
          existingGroup.locations.push(...(candidate.locations || []));
          existingGroup.count = (existingGroup.count || 0) + (candidate.count || 1);
          existingGroup.confidence = Math.max(existingGroup.confidence, candidate.confidence);
          matched = true;
          console.log(`Pattern-matched "${candidate.text}" with "${existingGroup.text}" (pattern: ${pattern})`);
        }
      }
      
      // If not matched by pattern, try similarity matching
      if (!matched) {
        let bestMatch: Candidate | null = null;
        let bestSimilarity = 0;
        
        for (const mergedCandidate of merged) {
          // Only match same entity type
          if (mergedCandidate.type !== candidate.type) continue;
          
          const similarity = calculateSimilarity(candidate.text, mergedCandidate.text);
          
          if (similarity >= SIMILARITY_THRESHOLD && similarity > bestSimilarity) {
            bestMatch = mergedCandidate;
            bestSimilarity = similarity;
          }
        }
        
        if (bestMatch) {
          // Merge into best matching group
          bestMatch.locations.push(...(candidate.locations || []));
          bestMatch.count = (bestMatch.count || 0) + (candidate.count || 1);
          bestMatch.confidence = Math.max(bestMatch.confidence, candidate.confidence);
          matched = true;
          console.log(`Similarity-matched "${candidate.text}" with "${bestMatch.text}" (similarity: ${(bestSimilarity * 100).toFixed(1)}%)`);
        }
      }
      
      // If still not matched, add as standalone
      if (!matched) {
        merged.push(candidate);
      }
    }
    
    return merged;
  }
  
  /**
   * Merge a group of candidates into a single candidate.
   * Finds the best representative and combines all locations/counts.
   */
  function mergeGroup(groupCandidates: Candidate[]): Candidate {
    // Find the best representative (shortest clean name, highest confidence)
    const sorted = [...groupCandidates].sort((a, b) => {
      // Prefer shorter, cleaner names
      if (a.text.length !== b.text.length) return a.text.length - b.text.length;
      return b.confidence - a.confidence;
    });
    
    const primary = sorted[0];
    
    // Merge all locations and sum counts
    const allLocations: typeof primary.locations = [];
    let totalCount = 0;
    let maxConfidence = 0;
    
    for (const c of groupCandidates) {
      allLocations.push(...(c.locations || []));
      totalCount += c.count || 1;
      maxConfidence = Math.max(maxConfidence, c.confidence);
    }
    
    // Create merged candidate using the best representative text
    const merged = {
      text: primary.text,
      type: primary.type,
      confidence: maxConfidence,
      count: totalCount,
      locations: allLocations
    };
    
    console.log(`Grouped ${groupCandidates.length} variants of "${primary.text}" (${groupCandidates.map(c => c.text).join(', ')})`);
    
    return merged;
  }
  
  /**
   * Sort candidates by ROI (Return on Investment):
   * ROI = occurrences × total_amount
   * Most financially impactful items come first.
   */
  function sortByROI(candidates: Candidate[]): Candidate[] {
    return [...candidates].sort((a, b) => {
      // Strong PII always comes first (security priority)
      const isStrongPII = (t: string) =>
        ['PHONE_NUMBER', 'EMAIL_ADDRESS', 'US_SSN', 'CREDIT_CARD', 'IBAN_CODE', 'US_BANK_NUMBER', 'US_PASSPORT', 'IP_ADDRESS'].includes(t);
      
      const sA = isStrongPII(a.type) ? 1 : 0;
      const sB = isStrongPII(b.type) ? 1 : 0;
      if (sA !== sB) return sB - sA;
      
      // Then sort by ROI (financial impact)
      const impactA = calculateFinancialImpact(a);
      const impactB = calculateFinancialImpact(b);
      if (impactA !== impactB) return impactB - impactA;
      
      // Tie-breaker: count (more occurrences = more important)
      return (b.count || 0) - (a.count || 0);
    });
  }

  /**
   * Calculate confidence score for a recommendation (0 to 1).
   * Higher confidence = more certain the label is correct.
   */
  function calculateRecommendationConfidence(text: string, entityType: string, label: string): number {
    const t = normalizeForGrouping(text);
    
    // Known brand exact match: 95%
    for (const [brand, labels] of Object.entries(KNOWN_BRANDS)) {
      if ((t === brand || t.includes(brand) || brand.includes(t)) && labels.includes(label)) {
        return 0.95;
      }
    }
    
    // Strong pattern matches: 85%
    if (entityType === 'ORGANIZATION' || entityType === 'ORG') {
      // Software/Tech patterns
      if (label === 'SOFTWARE' && /gamma|notion|figma|canva|adobe|microsoft|google|slack|zoom|dropbox|github|atlassian|jira|openai|anthropic|claude|chatgpt|ai\b|software|app\b|saas/i.test(text)) {
        return 0.85;
      }
      // Gas station patterns
      if (label === 'GAS' && /shell|esso|petro|ultramar|mobil|chevron|husky|sunoco|gas|fuel|petroleum|stn\b|station/i.test(text)) {
        return 0.85;
      }
      // Food/restaurant patterns
      if (label === 'FOOD' && /tim horton|starbucks|mcdonald|wendy|burger|subway|pizza|coffee|restaurant|cafe|bakery|donut|bagel|sushi|thai|chinese|indian|deli|bistro|grill|pub|bar\b|popeye|chipotle|taco|kfc|a&w|dairy queen|dq\b|five guys|harvey/i.test(text)) {
        return 0.85;
      }
      // Subscription patterns
      if (label === 'SUBSCRIPTION' && /netflix|spotify|disney|hulu|amazon prime|apple|youtube|hbo|crave|paramount|twitch|patreon|substack/i.test(text)) {
        return 0.85;
      }
    }
    
    // Domain patterns: 80%
    if (label === 'SOFTWARE' && /\.ai\b|\.app\b|\.io\b/i.test(text)) {
      return 0.80;
    }
    
    // Keyword-based matches: 70-75%
    if (entityType === 'ORGANIZATION' || entityType === 'ORG') {
      // Check if label matches keyword patterns
      const keywordPatterns: Record<string, RegExp> = {
        'GROCERIES': /walmart|costco|amazon|loblaws|sobeys|metro|safeway|whole foods|trader|grocery|supermarket|market\b|freshco|food basics|no frills/i,
        'SHOPPING': /canadian tire|home depot|ikea|best buy|staples|dollarama|winners|shoppers|target|marshalls|sephora|lululemon/i,
        'TRANSPORT': /uber|lyft|transit|via rail|air canada|westjet|porter|airline|taxi|cab\b|bus\b|train\b|presto|ttc|stm|oc transpo/i,
        'UTILITY': /hydro|electric|enbridge|fortis|water\b|internet/i,
        'BANK': /bank|td\b|rbc|scotia|cibc|bmo|national bank|desjardins|credit union|paypal|stripe|interac|wise\b|wealthsimple/i,
        'SERVICE': /wash|clean|repair|maintenance|salon|spa|gym|fitness|dental|clinic|medical|physio|massage|goodlife|la fitness|planet fitness/i,
        'INSURANCE': /insurance|intact|sunlife|manulife|great-west|desjardins|allstate|state farm/i,
        'HEALTH': /pharmacy|shoppers drug|rexall|medical|clinic|hospital|dental|vision|health|cvs|walgreens/i
      };
      
      if (keywordPatterns[label] && keywordPatterns[label].test(text)) {
        return 0.75;
      }
    }
    
    // Default confidence for any suggestion: 50%
    return 0.50;
  }

  /**
   * Get smart label suggestions with confidence scores.
   * Returns array of {label, confidence} objects, sorted by confidence (highest first).
   */
  function getSmartLabelSuggestionsWithConfidence(text: string, entityType: string): Array<{ label: string; confidence: number }> {
    const suggestions = getSmartLabelSuggestions(text, entityType);
    
    // Calculate confidence for each suggestion
    const withConfidence = suggestions.map(label => ({
      label,
      confidence: calculateRecommendationConfidence(text, entityType, label)
    }));
    
    // Sort by confidence (highest first)
    withConfidence.sort((a, b) => b.confidence - a.confidence);
    
    return withConfidence;
  }

  // Smart label suggestions based on Presidio entity type + keyword patterns
  function getSmartLabelSuggestions(text: string, entityType: string): string[] {
    const t = text.toLowerCase().trim();
    const suggestions: string[] = [];

    // First check: Is this a known brand that's being misdetected?
    // Check exact match first, then partial match
    for (const [brand, labels] of Object.entries(KNOWN_BRANDS)) {
      if (t === brand || t.includes(brand) || brand.includes(t)) {
        return labels.slice(0, 3); // Return top 3 for this brand
      }
    }

    // Check for domain patterns (.ai, .app, .io, .com)
    if (/\.ai\b|\.app\b|\.io\b/i.test(t)) {
      suggestions.push('SOFTWARE', 'SUBSCRIPTION', 'SERVICE');
    }

    // ORGANIZATION → classify by industry using keyword patterns
    if (entityType === 'ORGANIZATION' || entityType === 'ORG') {
      // Software & Tech (check first, many modern payments are software)
      if (/gamma|notion|figma|canva|adobe|microsoft|google|slack|zoom|dropbox|github|atlassian|jira|openai|anthropic|claude|chatgpt|ai\b|software|app\b|saas/i.test(t)) {
        suggestions.push('SOFTWARE', 'SUBSCRIPTION');
      }
      // Gas Stations
      if (/shell|esso|petro|ultramar|pioneer|mobil|chevron|husky|sunoco|gas|fuel|petroleum|stn\b|station/i.test(t)) {
        suggestions.push('GAS', 'TRANSPORT');
      }
      // Food & Restaurants
      if (/tim horton|starbucks|mcdonald|wendy|burger|subway|pizza|coffee|restaurant|cafe|bakery|donut|bagel|sushi|thai|chinese|indian|deli|bistro|grill|pub|bar\b|popeye|chipotle|taco|kfc|a&w|dairy queen|dq\b|five guys|harvey/i.test(t)) {
        suggestions.push('FOOD', 'RESTAURANT');
      }
      // Groceries & Shopping
      if (/walmart|costco|amazon|loblaws|sobeys|metro|safeway|whole foods|trader|grocery|supermarket|market\b|freshco|food basics|no frills/i.test(t)) {
        suggestions.push('GROCERIES', 'SHOPPING');
      }
      if (/canadian tire|home depot|ikea|best buy|staples|dollarama|winners|shoppers|target|marshalls|sephora|lululemon/i.test(t)) {
        suggestions.push('SHOPPING', 'MERCHANT');
      }
      // Transport & Travel
      if (/uber|lyft|transit|via rail|air canada|westjet|porter|airline|taxi|cab\b|bus\b|train\b|presto|ttc|stm|oc transpo/i.test(t)) {
        suggestions.push('TRANSPORT', 'TRAVEL');
      }
      // Subscriptions & Entertainment
      if (/netflix|spotify|disney|hulu|amazon prime|apple|youtube|hbo|crave|paramount|twitch|patreon|substack/i.test(t)) {
        suggestions.push('SUBSCRIPTION', 'ENTERTAINMENT');
      }
      // Utilities & Telecom
      if (/hydro|electric|enbridge|fortis|water\b|internet|bell\b|rogers|telus|fido|koodo|virgin|shaw|freedom|videotron/i.test(t)) {
        suggestions.push('UTILITY', 'SUBSCRIPTION');
      }
      // Banks & Financial
      if (/bank|td\b|rbc|scotia|cibc|bmo|national bank|desjardins|credit union|paypal|stripe|interac|wise\b|wealthsimple/i.test(t)) {
        suggestions.push('BANK', 'TRANSFER');
      }
      // Services
      if (/wash|clean|repair|maintenance|salon|spa|gym|fitness|dental|clinic|medical|physio|massage|goodlife|la fitness|planet fitness/i.test(t)) {
        suggestions.push('SERVICE', 'HEALTH');
      }
      // Insurance
      if (/insurance|intact|sunlife|manulife|great-west|desjardins|allstate|state farm/i.test(t)) {
        suggestions.push('INSURANCE', 'SERVICE');
      }
      // Health & Pharmacy
      if (/pharmacy|shoppers drug|rexall|medical|clinic|hospital|dental|vision|health|cvs|walgreens/i.test(t)) {
        suggestions.push('HEALTH', 'SERVICE');
      }
    }

    // PERSON → Check if it's actually a known business before assuming PII
    if (entityType === 'PERSON') {
      // Check common patterns that indicate it's NOT a person
      if (/stn\b|station|#\d+|store|shop|mart|inc\b|ltd\b|corp\b|llc\b/i.test(t)) {
        suggestions.push('MERCHANT', 'SERVICE', 'SHOPPING');
      }
      // If no business indicators found, return empty to suggest removal
      if (suggestions.length === 0) {
        return [];
      }
    }

    // URL → likely software or service
    if (entityType === 'URL') {
      if (/gamma|notion|figma|canva|adobe|microsoft|google|slack|github|claude|openai|vercel|netlify/i.test(t)) {
        suggestions.push('SOFTWARE', 'SUBSCRIPTION', 'SERVICE');
      } else {
        suggestions.push('SOFTWARE', 'SERVICE', 'SUBSCRIPTION');
      }
    }

    // LOCATION → could be travel, address, or store location
    if (entityType === 'LOCATION') {
      suggestions.push('TRAVEL', 'MERCHANT', 'SERVICE');
    }

    // Ensure we always have at least 3 suggestions
    const defaults = ['MERCHANT', 'SERVICE', 'SHOPPING'];
    while (suggestions.length < 3) {
      const next = defaults.find(d => !suggestions.includes(d));
      if (next) suggestions.push(next);
      else break;
    }

    // Return top 3 unique suggestions
    return [...new Set(suggestions)].slice(0, 3);
  }

  // Check if entity type suggests removal (actual PII)
  // Now checks against known brands to avoid false positives
  function isLikelyPII(entityType: string, text: string): boolean {
    const piiTypes = ['PHONE_NUMBER', 'EMAIL_ADDRESS', 'CREDIT_CARD', 'US_SSN', 'US_BANK_NUMBER', 'IBAN_CODE', 'US_PASSPORT', 'US_DRIVER_LICENSE', 'IP_ADDRESS'];
    
    // These are always PII
    if (piiTypes.includes(entityType)) return true;
    
    // For PERSON, check if it's a known brand first
    if (entityType === 'PERSON') {
      const t = text.toLowerCase().trim();
      
      // Check against known brands
      for (const brand of Object.keys(KNOWN_BRANDS)) {
        if (t === brand || t.includes(brand) || brand.includes(t)) {
          return false; // It's a brand, not a person
        }
      }
      
      // Check for business indicators
      if (/stn\b|station|#\d+|store|shop|mart|inc\b|ltd\b|corp\b|llc\b|\.ai\b|\.app\b|\.io\b|\.com\b/i.test(t)) {
        return false; // Has business indicators
      }
      
      return true; // Likely actual person name
    }
    
    return false;
  }
  
  let scanCandidates: Candidate[] = [];
  let candidateDecisions: Map<string, CandidateDecision> = new Map();
  let reviewOrder: Candidate[] = [];
  let reviewIndex = 0;
  let reviewHistory: string[] = [];
  
  const btnDeepClean = $('btn-deep-clean') as HTMLButtonElement | null;
  const deepCleanText = $('deep-clean-text');
  
  // Wizard Elements
  const deepCleanModal = $('deep-clean-modal');
  const stepScan = $('step-scan');
  const stepReview = $('step-review');
  const candidateStage = $('candidate-card-stage');
  const candidateList = $('candidate-list');
  const reviewEmpty = $('review-empty');
  const wizardFooter = $('wizard-footer');
  const btnWizardCancel = $('btn-wizard-cancel');
  const btnWizardApply = $('btn-wizard-apply');
  const btnDeepCleanClose = $('btn-deep-clean-close');
  const btnDeepCleanMinimize = $('btn-deep-clean-minimize');
  const btnDeepCleanRestore = $('btn-deep-clean-restore');
  const deepCleanMinimized = $('deep-clean-minimized');
  const minimizedStatus = $('minimized-status');
  const reviewStats = $('review-stats');
  const wizardStepTitle = $('wizard-step-title');
  const wizardStepSubtitle = $('wizard-step-subtitle');
  const deepcleanProgress = $('deepclean-progress');
  const deepcleanProgressBar = $('deepclean-progress-bar') as HTMLDivElement | null;
  const btnDeepcleanUndo = $('btn-deepclean-undo') as HTMLButtonElement | null;
  const btnDeepcleanPrev = $('btn-deepclean-prev') as HTMLButtonElement | null;
  const btnDeepcleanNext = $('btn-deepclean-next') as HTMLButtonElement | null;
  const btnDeepcleanModeFocus = $('btn-deepclean-mode-focus') as HTMLButtonElement | null;
  const btnDeepcleanModeList = $('btn-deepclean-mode-list') as HTMLButtonElement | null;
  const deepcleanScanBar = $('deepclean-scan-bar') as HTMLDivElement | null;
  const deepcleanScanPercent = $('deepclean-scan-percent');

  type DeepCleanMode = 'focus' | 'list';
  let deepCleanMode: DeepCleanMode = 'focus';

  // Helper: Get unique key for candidate
  const getCandKey = (c: Candidate) => `${c.text}|${c.type}`;

  // Helper: Get context lines with highlighted word for candidate (multiple occurrences)
  function getCandidateContext(c: Candidate): string {
    if (!outputJSON || !c.locations || c.locations.length === 0) return '';
    
    // Get unique examples (up to 3) with their amounts
    const examples: string[] = [];
    const seenDescriptions = new Set<string>();
    
    for (const loc of c.locations) {
      if (examples.length >= 3) break; // Show max 3 examples
      
      const row = outputJSON.transactions[loc.row];
      if (!row) continue;
      
      // Get the field value where this was found
      const fieldValue = (row as any)[loc.field] || row.description || row.merchant || '';
      if (!fieldValue || seenDescriptions.has(fieldValue)) continue;
      seenDescriptions.add(fieldValue);
      
      // Highlight the detected text in the context
      const escaped = c.text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const regex = new RegExp(`(${escaped})`, 'gi');
      const highlighted = escapeHtml(fieldValue).replace(
        regex, 
        '<span class="font-bold text-purple-300 bg-purple-500/20 px-1 rounded">$1</span>'
      );
      
      // Add amount if available for more context
      const amount = row.amount;
      const amountStr = amount ? `<span class="text-slate-500 ml-2">$${Math.abs(amount).toFixed(2)}</span>` : '';
      
      examples.push(`<div class="flex items-center justify-between gap-2"><span class="truncate">${highlighted}</span>${amountStr}</div>`);
    }
    
    if (examples.length === 0) return '';
    
    // Show how many more if there are more than 3
    const remaining = c.locations.length - examples.length;
    const moreText = remaining > 0 ? `<div class="text-[10px] text-slate-600 mt-1">+${remaining} more occurrence${remaining > 1 ? 's' : ''}</div>` : '';
    
    return examples.join('') + moreText;
  }

  // UX: default is KEEP unless the user explicitly chooses otherwise.
  const displayAction = (a: CandidateAction) => (a === 'redact' ? 'remove' : a === 'undecided' ? 'keep' : a);

  let scanProgressTimer: number | null = null;
  function startFakeScanProgress() {
    // Simulated progress so the UI never feels frozen.
    // Ramps quickly to ~70%, then slowly toward 90% until the scan completes.
    if (scanProgressTimer) window.clearInterval(scanProgressTimer);
    let pct = 0;

    const setUI = (v: number) => {
      const clamped = Math.max(0, Math.min(100, Math.round(v)));
      if (deepcleanScanBar) deepcleanScanBar.style.width = `${clamped}%`;
      if (deepcleanScanPercent) deepcleanScanPercent.textContent = `${clamped}%`;
    };

    setUI(0);
    scanProgressTimer = window.setInterval(() => {
      if (pct < 70) pct += 4;
      else if (pct < 90) pct += 1;
      setUI(pct);
    }, 220);
  }

  function finishFakeScanProgress() {
    if (scanProgressTimer) window.clearInterval(scanProgressTimer);
    scanProgressTimer = null;
    if (deepcleanScanBar) deepcleanScanBar.style.width = '100%';
    if (deepcleanScanPercent) deepcleanScanPercent.textContent = '100%';
  }

  async function runDeepClean() {
    if (!outputJSON || !outputJSON.transactions.length) return;
    
    // Reset State
    scanCandidates = [];
    candidateDecisions.clear();
    
    // UI: Show Modal, Start Scanning
    deepCleanModal?.classList.remove('hidden');
    stepScan?.classList.remove('hidden');
    stepReview?.classList.add('hidden');
    wizardFooter?.classList.add('hidden');
    if (wizardStepTitle) wizardStepTitle.textContent = 'Scanning...';
    if (wizardStepSubtitle) wizardStepSubtitle.textContent = 'Scanning locally for sensitive info…';
    startFakeScanProgress();
    
    try {
      console.log('Sending data to Presidio for scanning...');
      
      const response = await fetch(`${PRESIDIO_API}/scan`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          transactions: outputJSON.transactions.map(t => ({
            date: t.date,
            merchant: t.merchant,
            description: t.description,
            category: t.category,
            amount: t.amount,
            type: t.type
          }))
        })
      });
      
      if (!response.ok) throw new Error(`Presidio returned ${response.status}`);
      
      const result = await response.json();
      finishFakeScanProgress();
      const rawCandidates: Candidate[] = result.candidates || [];
      console.log(`Scan complete. Found ${rawCandidates.length} raw candidates.`);
      
      // FILTER OUT already-categorized items
      // Static processing already handles obvious cases (Tim Hortons → [FOOD], etc.)
      // Deep Clean should focus on nuanced edge cases, not duplicate work
      scanCandidates = rawCandidates.filter(c => {
        // Get the description line(s) where this candidate appears
        if (!outputJSON || !c.locations?.length) return true;
        
        // Check if ANY location already has a category tag in its context
        const hasExistingCategory = c.locations.some(loc => {
          const row = outputJSON!.transactions[loc.row];
          if (!row) return false;
          const fieldValue = (row as any)[loc.field] || row.description || row.merchant || '';
          // If the field already ends with [CATEGORY], skip this candidate
          return /\[[A-Z_]+\]\s*$/.test(fieldValue);
        });
        
        if (hasExistingCategory) {
          console.log(`Skipping "${c.text}" - already categorized by static processing`);
          return false;
        }
        return true;
      });
      
      console.log(`After filtering: ${scanCandidates.length} candidates for review.`);
      
      // STEP 1: Deduplicate similar items (PayPal, PAYPAL *, Paypal Inc → one item)
      const beforeDedup = scanCandidates.length;
      scanCandidates = deduplicateCandidates(scanCandidates);
      console.log(`Deduplicated: ${beforeDedup} → ${scanCandidates.length} unique items`);
      
      // IMPORTANT: Start neutral (no pre-selected decisions). This reduces confusion and
      // makes the UX feel like "Tinder" where the user explicitly chooses per card.
      scanCandidates.forEach(c => candidateDecisions.set(getCandKey(c), { action: 'undecided' }));

      // STEP 2: Sort by ROI (most financially impactful first)
      // ROI = occurrences × total_amount
      reviewOrder = sortByROI(scanCandidates);
      
      // STEP 3: Auto-categorize high-confidence items (80%+ threshold)
      const AUTO_CATEGORIZE_THRESHOLD = 0.80;
      
      let autoCategorizedCount = 0;
      reviewOrder.forEach(c => {
        const key = getCandKey(c);
        const decision = candidateDecisions.get(key);
        
        // Only auto-categorize if undecided and not sensitive PII
        if (decision?.action === 'undecided' && !isSensitivePII(c.type)) {
          const suggestionsWithConfidence = getSmartLabelSuggestionsWithConfidence(c.text, c.type);
          if (suggestionsWithConfidence.length > 0 && suggestionsWithConfidence[0].confidence >= AUTO_CATEGORIZE_THRESHOLD) {
            const autoLabel = suggestionsWithConfidence[0].label;
            candidateDecisions.set(key, {
              action: 'context',
              contextLabel: autoLabel,
              explicitlyDecided: false // Auto-applied
            });
            autoCategorizedCount++;
            console.log(`Auto-categorized "${c.text}" as [${autoLabel}] (${Math.round(suggestionsWithConfidence[0].confidence * 100)}% confidence)`);
          }
        }
      });
      
      if (autoCategorizedCount > 0) {
        console.log(`Auto-categorized ${autoCategorizedCount} items with high confidence (≥${(AUTO_CATEGORIZE_THRESHOLD * 100).toFixed(0)}%)`);
      }
      
      // Log the top items for debugging
      console.log('Review order (by ROI):');
      reviewOrder.slice(0, 5).forEach((c, i) => {
        const impact = calculateFinancialImpact(c);
        console.log(`  ${i + 1}. "${c.text}" - ${c.count}× occurrences, $${impact.toFixed(2)} total impact`);
      });
      
      reviewIndex = 0;
      reviewHistory = [];

      // Transition to Review
      showReviewStep();
      
    } catch (err: any) {
      console.error('Scan error:', err);
      finishFakeScanProgress();
      alert('Scan Failed: Is Docker running? (localhost:8000)');
      closeDeepCleanModal();
    }
  }

  function showReviewStep() {
    console.log('Transitioning to review step...');
    
    // Hide scan, show review
    if (stepScan) stepScan.classList.add('hidden');
    if (stepReview) stepReview.classList.remove('hidden');
    if (wizardFooter) wizardFooter.classList.remove('hidden');
    
    // Update title
    if (wizardStepTitle) wizardStepTitle.textContent = 'Review Findings';
    if (wizardStepSubtitle) wizardStepSubtitle.textContent = `${scanCandidates.length} items detected`;
    
    // Empty state
    if (scanCandidates.length === 0) {
      if (candidateStage) candidateStage.classList.add('hidden');
      if (reviewEmpty) {
        reviewEmpty.classList.remove('hidden');
        reviewEmpty.classList.add('flex');
      }
      updateReviewStats();
      return;
    }

    // Show stage
    if (candidateStage) candidateStage.classList.remove('hidden');
    if (reviewEmpty) {
      reviewEmpty.classList.add('hidden');
      reviewEmpty.classList.remove('flex');
    }
    
    updateReviewStats();
    setDeepCleanMode('focus');
    
    // Mode toggle
    btnDeepcleanModeFocus?.addEventListener('click', () => setDeepCleanMode('focus'));
    btnDeepcleanModeList?.addEventListener('click', () => setDeepCleanMode('list'));

    // Navigation: Undo, Prev, Next
    btnDeepcleanUndo?.addEventListener('click', () => undoLastDecision());
    btnDeepcleanPrev?.addEventListener('click', () => navigateToPrev());
    btnDeepcleanNext?.addEventListener('click', () => navigateToNext());

    // Keyboard shortcuts (fun + fast) - only in focus mode
    document.addEventListener('keydown', deepCleanKeyHandler);
  }

  function setDeepCleanMode(next: DeepCleanMode) {
    deepCleanMode = next;

    // Button styling
    if (btnDeepcleanModeFocus && btnDeepcleanModeList) {
      if (next === 'focus') {
        btnDeepcleanModeFocus.classList.add('bg-purple-500/15', 'text-slate-200');
        btnDeepcleanModeList.classList.remove('bg-purple-500/15', 'text-slate-200');
        btnDeepcleanModeList.classList.add('text-slate-400');
      } else {
        btnDeepcleanModeList.classList.add('bg-purple-500/15', 'text-slate-200');
        btnDeepcleanModeFocus.classList.remove('bg-purple-500/15', 'text-slate-200');
        btnDeepcleanModeFocus.classList.add('text-slate-400');
      }
    }

    // Toggle containers
    if (candidateStage) candidateStage.classList.toggle('hidden', next !== 'focus');
    if (candidateList) candidateList.classList.toggle('hidden', next !== 'list');

    if (next === 'focus') {
      renderCurrentCandidate();
    } else {
      renderCandidateList();
    }
  }

  function renderCandidateList() {
    if (!candidateList) return;
    candidateList.innerHTML = '';

    // Count decided items
    const decidedCount = reviewOrder.filter(c => {
      const d = candidateDecisions.get(getCandKey(c));
      return d?.explicitlyDecided;
    }).length;

    // Show ALL items with their decision state
    reviewOrder.forEach((c, idx) => {
      const key = getCandKey(c);
      const decision = candidateDecisions.get(key) || { action: 'undecided' as CandidateAction };
      const isDecided = decision.explicitlyDecided;
      const isAutoCategorized = decision.action === 'context' && !decision.explicitlyDecided;
      const typeLabel = c.type.replace(/_/g, ' ');
      const confPct = Math.round(c.confidence * 100);
      const contextHtml = getCandidateContext(c);

      // Visual styling based on decision
      let borderColor = 'border-navy-800/60';
      let bgColor = 'bg-navy-950/20';
      let statusBadge = '';
      
      if (isAutoCategorized) {
        // Auto-categorized items
        borderColor = 'border-blue-500/20';
        bgColor = 'bg-blue-500/5';
        statusBadge = `<span class="px-2 py-0.5 rounded-lg bg-blue-500/15 border border-blue-500/30 text-blue-300 text-[10px] font-bold uppercase flex items-center gap-1">
          <span>[${decision.contextLabel}]</span>
          <span class="text-[8px] opacity-70">auto</span>
        </span>`;
      } else if (isDecided) {
        if (decision.action === 'keep') {
          borderColor = 'border-green-500/30';
          bgColor = 'bg-green-500/5';
          statusBadge = '<span class="px-2 py-0.5 rounded-lg bg-green-500/20 text-green-300 text-[10px] font-bold uppercase">Keep</span>';
        } else if (decision.action === 'redact') {
          borderColor = 'border-red-500/30';
          bgColor = 'bg-red-500/5';
          statusBadge = '<span class="px-2 py-0.5 rounded-lg bg-red-500/20 text-red-300 text-[10px] font-bold uppercase">Remove</span>';
        } else if (decision.action === 'context') {
          borderColor = 'border-blue-500/30';
          bgColor = 'bg-blue-500/5';
          statusBadge = `<span class="px-2 py-0.5 rounded-lg bg-blue-500/20 text-blue-300 text-[10px] font-bold uppercase">[${decision.contextLabel}]</span>`;
        }
      }

      // Calculate financial impact for this item
      const itemImpact = calculateFinancialImpact(c);
      const impactBadge = itemImpact > 0 ? `<span class="px-2 py-0.5 rounded-lg bg-amber-500/15 text-amber-300 text-[10px] font-bold">$${itemImpact.toFixed(2)}</span>` : '';
      
      // Get top recommendation for this item
      const suggestionsWithConfidence = getSmartLabelSuggestionsWithConfidence(c.text, c.type);
      const topRecommendation = suggestionsWithConfidence.length > 0 ? suggestionsWithConfidence[0] : null;
      const recommendationBadge = topRecommendation && !isDecided ? `
        <button class="dc-list-recommendation px-2.5 py-1 rounded-lg text-[10px] font-mono font-semibold bg-green-500/15 border border-green-500/30 text-green-300 hover:bg-green-500/25 hover:border-green-500/40 transition-all flex items-center gap-1.5" data-label="${topRecommendation.label}" title="Click to apply [${topRecommendation.label}] (${Math.round(topRecommendation.confidence * 100)}% confidence)">
          <span class="text-[9px]">[${topRecommendation.label}]</span>
          <span class="text-[8px] opacity-70">${Math.round(topRecommendation.confidence * 100)}%</span>
        </button>
      ` : '';
      
      const row = document.createElement('div');
      row.className = `p-3 rounded-xl border ${borderColor} ${bgColor} cursor-pointer hover:bg-navy-900/30 transition-colors`;
      row.innerHTML = `
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0 flex-1">
            <div class="flex items-center gap-2 flex-wrap">
              <div class="font-mono text-sm font-semibold text-slate-100 truncate" title="${escapeHtml(c.text)}">${escapeHtml(c.text)}</div>
              ${statusBadge}
              ${recommendationBadge}
              ${impactBadge}
            </div>
            ${contextHtml ? `<div class="mt-1.5 text-xs text-slate-400 leading-relaxed break-words line-clamp-2">${contextHtml}</div>` : ''}
            <div class="mt-1.5 flex items-center gap-2 flex-wrap text-[11px]">
              <span class="px-2 py-0.5 rounded-lg bg-slate-800/70 text-slate-300 text-[10px] font-bold uppercase tracking-wider">${typeLabel}</span>
              <span class="text-slate-500 font-mono">${confPct}%</span>
              <span class="text-slate-600 font-mono">${c.count}× occurrences</span>
            </div>
          </div>
          <div class="flex items-center gap-2 shrink-0">
            <button class="dc-list-keep px-3 py-1.5 rounded-lg text-[11px] font-semibold border ${decision.action === 'keep' && isDecided ? 'border-green-500/50 bg-green-500/20 text-green-200 ring-1 ring-green-500/30' : 'border-green-500/25 bg-green-500/10 text-green-300 hover:bg-green-500/15'}">Keep</button>
            <button class="dc-list-redact px-3 py-1.5 rounded-lg text-[11px] font-semibold border ${decision.action === 'redact' && isDecided ? 'border-red-500/50 bg-red-500/20 text-red-200 ring-1 ring-red-500/30' : 'border-red-500/25 bg-red-500/10 text-red-300 hover:bg-red-500/15'}">Remove</button>
            <button class="dc-list-context px-3 py-1.5 rounded-lg text-[11px] font-semibold border ${decision.action === 'context' && isDecided ? 'border-blue-500/50 bg-blue-500/20 text-blue-200 ring-1 ring-blue-500/30' : 'border-blue-500/25 bg-blue-500/10 text-blue-300 hover:bg-blue-500/15'}">Context</button>
          </div>
        </div>
        <div class="dc-list-labels hidden mt-3 p-2 rounded-lg border border-navy-800/70 bg-navy-900/30">
          <div class="text-[10px] uppercase tracking-wider text-slate-500 font-bold mb-2">Choose a label</div>
          <div class="flex flex-wrap gap-2">
            ${CONTEXT_LABELS.map(lbl => `<button class="dc-list-label px-2.5 py-1.5 rounded-lg text-[11px] font-mono bg-blue-500/10 border border-blue-500/20 text-blue-300 hover:bg-blue-500/20" data-label="${lbl}">[${lbl}]</button>`).join('')}
          </div>
        </div>
      `;

      // Click on row to jump to Focus mode for that item
      row.addEventListener('click', (e) => {
        // Don't navigate if clicking on buttons
        if ((e.target as HTMLElement).closest('button')) return;
        navigateToIndex(idx);
      });

      row.querySelector('.dc-list-keep')?.addEventListener('click', (e) => {
        e.stopPropagation();
        candidateDecisions.set(key, { action: 'keep', explicitlyDecided: true });
        if (!reviewHistory.includes(key)) reviewHistory.push(key);
        updateReviewStats();
        renderCandidateList();
      });

      row.querySelector('.dc-list-redact')?.addEventListener('click', (e) => {
        e.stopPropagation();
        candidateDecisions.set(key, { action: 'redact', explicitlyDecided: true });
        if (!reviewHistory.includes(key)) reviewHistory.push(key);
        updateReviewStats();
        renderCandidateList();
      });

      row.querySelector('.dc-list-context')?.addEventListener('click', (e) => {
        e.stopPropagation();
        row.querySelector('.dc-list-labels')?.classList.toggle('hidden');
      });

      // Recommendation badge click handler
      row.querySelector('.dc-list-recommendation')?.addEventListener('click', (e) => {
        e.stopPropagation();
        const label = (e.target as HTMLElement).closest('.dc-list-recommendation')?.getAttribute('data-label') || topRecommendation?.label || 'LABEL';
        candidateDecisions.set(key, { action: 'context', contextLabel: label, explicitlyDecided: true });
        if (!reviewHistory.includes(key)) reviewHistory.push(key);
        updateReviewStats();
        renderCandidateList();
      });

      row.querySelectorAll('.dc-list-label').forEach(btn => {
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          const label = (btn as HTMLElement).dataset.label || 'LABEL';
          candidateDecisions.set(key, { action: 'context', contextLabel: label, explicitlyDecided: true });
          if (!reviewHistory.includes(key)) reviewHistory.push(key);
          updateReviewStats();
          renderCandidateList();
        });
      });

      candidateList.appendChild(row);
    });

    // Show summary at bottom
    if (decidedCount > 0) {
      const summaryRow = document.createElement('div');
      summaryRow.className = 'mt-3 px-3 py-2 rounded-lg bg-purple-500/10 border border-purple-500/20 text-[11px] text-purple-300 font-medium text-center';
      summaryRow.innerHTML = `${decidedCount} of ${reviewOrder.length} items reviewed • Click any row to edit in Focus mode`;
      candidateList.appendChild(summaryRow);
    }
  }

  function renderCurrentCandidate() {
    if (!candidateStage) return;

    // Show the item at current index (allow revisiting any item)
    if (reviewIndex < 0 || reviewIndex >= reviewOrder.length) {
      // No items or out of bounds
      candidateStage.innerHTML = `
        <div class="p-6 rounded-2xl border border-green-500/25 bg-green-500/5 text-center animate-fadeIn">
          <div class="text-sm font-semibold text-slate-200">All done!</div>
          <div class="text-xs text-slate-500 mt-1">Hit <span class="font-semibold text-slate-300">Apply</span> to update the output, or use ← → to revisit items.</div>
        </div>
      `;
      updateReviewStats();
      return;
    }

    const c = reviewOrder[reviewIndex];
    const key = getCandKey(c);
    const decision = candidateDecisions.get(key) || { action: 'undecided' as CandidateAction };

    const typeLabel = c.type.replace(/_/g, ' ');
    const confPct = Math.round(c.confidence * 100);

    // Neutral by default (no pre-selected colors)
    const badgeClass =
      decision.action === 'redact'
        ? 'bg-red-500/10 border-red-500/30 text-red-300'
        : decision.action === 'context'
          ? 'bg-blue-500/10 border-blue-500/30 text-blue-300'
          : decision.action === 'keep'
            ? 'bg-green-500/10 border-green-500/30 text-green-300'
            : 'bg-navy-900/40 border-navy-800/60 text-slate-400';

    // Get context with highlighted word
    const contextHtml = getCandidateContext(c);
    
    // Get smart label suggestions with confidence scores
    const suggestionsWithConfidence = getSmartLabelSuggestionsWithConfidence(c.text, c.type);
    const top3Suggestions = suggestionsWithConfidence.slice(0, 3);
    const smartSuggestions = suggestionsWithConfidence.map(s => s.label);
    const otherLabels = CONTEXT_LABELS.filter(lbl => !smartSuggestions.includes(lbl));
    
    // Check if we should auto-categorize (80%+ confidence threshold)
    const AUTO_CATEGORIZE_THRESHOLD = 0.80;
    const shouldAutoCategorize = top3Suggestions.length > 0 && 
                                  top3Suggestions[0].confidence >= AUTO_CATEGORIZE_THRESHOLD &&
                                  !isSensitivePII(c.type) &&
                                  decision.action === 'undecided';
    
    // Auto-categorize if threshold met
    if (shouldAutoCategorize) {
      const autoLabel = top3Suggestions[0].label;
      candidateDecisions.set(key, { 
        action: 'context', 
        contextLabel: autoLabel, 
        explicitlyDecided: false // Auto-applied, not user decision
      });
      // Update decision reference
      const updatedDecision = candidateDecisions.get(key)!;
      decision.action = updatedDecision.action;
      decision.contextLabel = updatedDecision.contextLabel;
    }
    
    // Calculate financial impact for display
    const totalImpact = calculateFinancialImpact(c);
    const impactDisplay = totalImpact > 0 ? `$${totalImpact.toFixed(2)}` : '';
    
    candidateStage.innerHTML = `
      <div class="rounded-2xl border border-navy-800/60 bg-navy-950/20 overflow-hidden animate-scaleIn">
        <div class="p-5 border-b border-navy-800/50">
          <div class="flex items-start justify-between gap-3">
            <div class="min-w-0 flex-1">
              <div class="font-mono text-lg font-bold text-slate-100 truncate" title="${escapeHtml(c.text)}">${escapeHtml(c.text)}</div>
              <div class="mt-2 flex items-center gap-2 flex-wrap">
                <span class="px-2 py-1 rounded-lg border ${badgeClass} text-[11px] font-bold uppercase tracking-wider">${displayAction(decision.action)}</span>
                <span class="px-2 py-1 rounded-lg bg-slate-800/70 text-slate-300 text-[10px] font-bold uppercase tracking-wider">${typeLabel}</span>
                <span class="text-[11px] text-slate-500 font-mono">${confPct}%</span>
                ${decision.action === 'context' ? `<span class="text-[11px] font-mono text-blue-300 bg-blue-500/15 border border-blue-500/25 px-2 py-1 rounded-lg">+ [${escapeHtml(decision.contextLabel || 'LABEL')}]</span>` : ''}
              </div>
            </div>
            <div class="text-right shrink-0">
              ${impactDisplay ? `<div class="text-sm font-bold text-amber-300">${impactDisplay}</div>` : ''}
              <div class="text-[11px] text-slate-500 font-mono">${c.count}× occurrences</div>
            </div>
          </div>
          
          ${contextHtml ? `
          <div class="mt-4 p-3 rounded-lg bg-navy-900/50 border border-navy-800/40">
            <div class="text-[10px] uppercase tracking-wider text-slate-600 font-bold mb-2">Found in:</div>
            <div class="text-sm text-slate-300 leading-relaxed space-y-1.5">${contextHtml}</div>
          </div>
          ` : ''}
        </div>

        <div class="p-5 pt-0">
          <div class="text-xs text-slate-400 leading-relaxed mb-4">
            Choose what to do with this term everywhere it appears:
          </div>
          <div class="mt-4 grid grid-cols-3 gap-3">
            <button id="btn-dc-keep" class="px-3 py-3 rounded-xl border border-navy-800/70 bg-navy-900/30 hover:bg-green-500/10 hover:border-green-500/30 transition-all text-slate-200">
              <div class="w-10 h-10 mx-auto rounded-full bg-green-500/10 border border-green-500/20 flex items-center justify-center text-green-400 font-bold mb-2">✓</div>
              <div class="text-xs font-semibold">Keep</div>
              <div class="text-[10px] text-slate-500 mt-1">Leave as-is</div>
            </button>
            <button id="btn-dc-redact" class="px-3 py-3 rounded-xl border border-navy-800/70 bg-navy-900/30 hover:bg-red-500/15 hover:border-red-500/40 transition-all text-slate-200">
              <div class="w-10 h-10 mx-auto rounded-full bg-red-500/10 border border-red-500/20 flex items-center justify-center text-red-400 font-bold mb-2">×</div>
              <div class="text-xs font-semibold">Remove</div>
              <div class="text-[10px] text-slate-500 mt-1">Delete this text</div>
            </button>
            <button id="btn-dc-label" class="px-3 py-3 rounded-xl border border-navy-800/70 bg-navy-900/30 hover:bg-blue-500/10 hover:border-blue-500/30 transition-all text-slate-200">
              <div class="w-10 h-10 mx-auto rounded-full bg-blue-500/10 border border-blue-500/20 flex items-center justify-center text-blue-400 font-bold mb-2">🏷</div>
              <div class="text-xs font-semibold">Context</div>
              <div class="text-[10px] text-slate-500 mt-1">Keep + add label</div>
            </button>
          </div>

          <div id="dc-label-panel" class="mt-4 p-4 rounded-xl border border-navy-800/70 bg-navy-900/30">
            ${top3Suggestions.length > 0 ? `
            <div class="mb-4">
              <div class="text-[9px] uppercase tracking-wider text-green-400/80 font-bold mb-3 flex items-center gap-1.5">
                <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" class="text-green-400"><path d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z"></path></svg>
                Top Recommendations
                ${shouldAutoCategorize ? `<span class="ml-2 px-1.5 py-0.5 rounded text-[8px] bg-green-500/20 border border-green-500/40 text-green-300">Auto-applied</span>` : ''}
              </div>
              <div class="flex flex-col gap-2">
                ${top3Suggestions.map((suggestion, idx) => {
                  const rank = idx + 1;
                  const confPct = Math.round(suggestion.confidence * 100);
                  const isTop = rank === 1;
                  const isAutoApplied = shouldAutoCategorize && rank === 1;
                  
                  return `
                    <button class="btn-dc-label-option btn-dc-recommendation-${rank} flex items-center justify-between px-4 py-3 rounded-xl border transition-all ${
                      isTop 
                        ? 'bg-green-500/20 border-green-500/40 hover:bg-green-500/30 hover:border-green-500/50 shadow-lg shadow-green-500/10' 
                        : rank === 2
                          ? 'bg-blue-500/15 border-blue-500/30 hover:bg-blue-500/25 hover:border-blue-500/40'
                          : 'bg-purple-500/10 border-purple-500/25 hover:bg-purple-500/20 hover:border-purple-500/35'
                    }" data-label="${suggestion.label}" data-confidence="${suggestion.confidence}">
                      <div class="flex items-center gap-3">
                        <div class="flex items-center justify-center w-6 h-6 rounded-full ${
                          isTop ? 'bg-green-500/30 text-green-300 font-bold' : rank === 2 ? 'bg-blue-500/20 text-blue-300 font-semibold' : 'bg-purple-500/15 text-purple-300 font-semibold'
                        } text-[11px]">${rank}</div>
                        <span class="text-[13px] font-mono font-semibold ${
                          isTop ? 'text-green-200' : rank === 2 ? 'text-blue-200' : 'text-purple-200'
                        }">[${suggestion.label}]</span>
                        ${isAutoApplied ? `<span class="px-1.5 py-0.5 rounded text-[9px] bg-green-500/30 border border-green-500/50 text-green-200 font-semibold">Applied</span>` : ''}
                      </div>
                      <div class="flex items-center gap-2">
                        <div class="w-16 h-1.5 rounded-full bg-navy-900/50 overflow-hidden">
                          <div class="h-full ${
                            suggestion.confidence >= 0.80 ? 'bg-green-500' : suggestion.confidence >= 0.70 ? 'bg-blue-500' : 'bg-purple-500'
                          }" style="width: ${confPct}%"></div>
                        </div>
                        <span class="text-[10px] font-mono text-slate-400 min-w-[35px] text-right">${confPct}%</span>
                      </div>
                    </button>
                  `;
                }).join('')}
              </div>
            </div>
            ` : ''}
            
            <div>
              <button id="btn-toggle-other-labels" class="text-[9px] uppercase tracking-wider text-slate-500 font-bold mb-2 flex items-center gap-1 hover:text-slate-300 transition-colors">
                <svg id="other-labels-chevron" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" class="transition-transform ${otherLabels.length > 0 ? '' : 'rotate-90'}"><polyline points="9 18 15 12 9 6"></polyline></svg>
                Other Labels ${otherLabels.length > 0 ? `(${otherLabels.length})` : ''}
              </button>
              <div id="other-labels-container" class="${otherLabels.length > 0 ? 'hidden' : ''} flex flex-wrap gap-2">
                ${otherLabels.map(lbl => `<button class="btn-dc-label-option px-2.5 py-1.5 rounded-lg text-[11px] font-mono bg-blue-500/10 border border-blue-500/20 text-blue-300 hover:bg-blue-500/20 hover:border-blue-500/30 transition-all" data-label="${lbl}">[${lbl}]</button>`).join('')}
              </div>
            </div>
          </div>
        </div>
      </div>
    `;

    const keepBtn = $('btn-dc-keep');
    const redactBtn = $('btn-dc-redact');
    const labelBtn = $('btn-dc-label');

    keepBtn?.addEventListener('click', () => decideAndAdvance(key, { action: 'keep', explicitlyDecided: true }));
    redactBtn?.addEventListener('click', () => decideAndAdvance(key, { action: 'redact', explicitlyDecided: true }));
    // Label button is now just for visual consistency - recommendations are always visible
    labelBtn?.addEventListener('click', () => {
      // Scroll to recommendations panel if needed
      const labelPanel = $('dc-label-panel');
      labelPanel?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    });

    // Toggle "Other Labels" visibility
    const toggleOtherLabels = $('btn-toggle-other-labels');
    const otherLabelsContainer = $('other-labels-container');
    const otherLabelsChevron = $('other-labels-chevron');
    toggleOtherLabels?.addEventListener('click', () => {
      otherLabelsContainer?.classList.toggle('hidden');
      otherLabelsChevron?.classList.toggle('rotate-90');
    });

    candidateStage.querySelectorAll('.btn-dc-label-option').forEach(btn => {
      btn.addEventListener('click', () => {
        const label = (btn as HTMLElement).dataset.label || 'LABEL';
        decideAndAdvance(key, { action: 'context', contextLabel: label, explicitlyDecided: true });
      });
    });
  }

  function decideAndAdvance(key: string, decision: CandidateDecision) {
    candidateDecisions.set(key, decision);
    reviewHistory.push(key);
    
    // Flash animation for visual feedback
    const cardStage = candidateStage?.querySelector('.rounded-2xl');
    if (cardStage) {
      cardStage.classList.add('animate-decisionFlash');
      setTimeout(() => cardStage.classList.remove('animate-decisionFlash'), 150);
    }
    
    // Advance to next item after deciding (if not at the end)
    if (reviewIndex < reviewOrder.length - 1) {
      reviewIndex++;
    }
    updateReviewStats();
    renderCurrentCandidate();
  }

  function undoLastDecision() {
    const lastKey = reviewHistory.pop();
    if (!lastKey) {
      updateReviewStats();
      renderCurrentCandidate();
      return;
    }
    candidateDecisions.set(lastKey, { action: 'undecided' });
    // Rewind index to the candidate's position
    const idx = reviewOrder.findIndex(c => getCandKey(c) === lastKey);
    if (idx >= 0) reviewIndex = idx;
    updateReviewStats();
    renderCurrentCandidate();
  }

  function navigateToPrev() {
    if (reviewIndex > 0) {
      reviewIndex--;
      updateReviewStats();
      renderCurrentCandidate();
    }
  }

  function navigateToNext() {
    if (reviewIndex < reviewOrder.length - 1) {
      reviewIndex++;
      updateReviewStats();
      renderCurrentCandidate();
    }
  }

  function navigateToIndex(idx: number) {
    if (idx >= 0 && idx < reviewOrder.length) {
      reviewIndex = idx;
      // Switch to Focus mode to show the item
      reviewMode = 'focus';
      btnDeepcleanModeFocus?.classList.add('bg-purple-500/20', 'border-purple-500/40', 'text-purple-200');
      btnDeepcleanModeFocus?.classList.remove('bg-navy-900/40', 'border-navy-700/60', 'text-slate-400');
      btnDeepcleanModeList?.classList.remove('bg-purple-500/20', 'border-purple-500/40', 'text-purple-200');
      btnDeepcleanModeList?.classList.add('bg-navy-900/40', 'border-navy-700/60', 'text-slate-400');
      updateReviewStats();
      renderCurrentCandidate();
    }
  }

  function findNextUndecidedIndex(fromIdx: number) {
    for (let i = fromIdx; i < reviewOrder.length; i++) {
      const k = getCandKey(reviewOrder[i]);
      const d = candidateDecisions.get(k);
      if (!d || d.action === 'undecided') return i;
    }
    for (let i = 0; i < fromIdx; i++) {
      const k = getCandKey(reviewOrder[i]);
      const d = candidateDecisions.get(k);
      if (!d || d.action === 'undecided') return i;
    }
    return -1;
  }

  function deepCleanKeyHandler(e: KeyboardEvent) {
    if (deepCleanModal?.classList.contains('hidden')) return;
    if (!reviewOrder.length) return;

    // Navigation: Left/Right arrows to move between items
    if (e.key === 'ArrowLeft') {
      e.preventDefault();
      navigateToPrev();
      return;
    }
    if (e.key === 'ArrowRight') {
      e.preventDefault();
      navigateToNext();
      return;
    }
    
    // Escape to close
    if (e.key === 'Escape') {
      e.preventDefault();
      closeDeepCleanModal();
      return;
    }

    // Decision shortcuts only work in focus mode
    if (deepCleanMode !== 'focus') return;
    
    const c = reviewOrder[reviewIndex];
    if (!c) return;
    const key = getCandKey(c);

    // 1, 2, 3 = Apply top 3 recommendations
    if (e.key === '1' || e.key === '2' || e.key === '3') {
      e.preventDefault();
      const suggestionsWithConfidence = getSmartLabelSuggestionsWithConfidence(c.text, c.type);
      const top3Suggestions = suggestionsWithConfidence.slice(0, 3);
      const rank = parseInt(e.key) - 1;
      if (top3Suggestions[rank]) {
        const label = top3Suggestions[rank].label;
        decideAndAdvance(key, { action: 'context', contextLabel: label, explicitlyDecided: true });
      }
      return;
    }

    // Space = Apply top recommendation and advance
    if (e.key === ' ') {
      e.preventDefault();
      const suggestionsWithConfidence = getSmartLabelSuggestionsWithConfidence(c.text, c.type);
      if (suggestionsWithConfidence.length > 0) {
        const label = suggestionsWithConfidence[0].label;
        decideAndAdvance(key, { action: 'context', contextLabel: label, explicitlyDecided: true });
      }
      return;
    }

    // K = Keep, R = Remove, C = Context
    if (e.key === 'k' || e.key === 'K') {
      e.preventDefault();
      decideAndAdvance(key, { action: 'keep', explicitlyDecided: true });
    }
    if (e.key === 'r' || e.key === 'R') {
      e.preventDefault();
      decideAndAdvance(key, { action: 'redact', explicitlyDecided: true });
    }
    // C = Context (apply top recommendation)
    if (e.key === 'c' || e.key === 'C' || e.key === 'ArrowUp') {
      e.preventDefault();
      const suggestionsWithConfidence = getSmartLabelSuggestionsWithConfidence(c.text, c.type);
      if (suggestionsWithConfidence.length > 0) {
        const label = suggestionsWithConfidence[0].label;
        decideAndAdvance(key, { action: 'context', contextLabel: label, explicitlyDecided: true });
      }
    }
    // Z = Undo
    if ((e.key === 'z' || e.key === 'Z') && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      undoLastDecision();
    }
  }

  function updateReviewStats() {
    const total = reviewOrder.length || scanCandidates.length;
    let redacted = 0;
    let labeled = 0;
    let kept = 0;
    let decided = 0;
    let autoCategorized = 0;
    
    candidateDecisions.forEach(d => {
      if (d.explicitlyDecided) {
        decided++;
        if (d.action === 'redact') redacted++;
        else if (d.action === 'context') labeled++;
        else if (d.action === 'keep') kept++;
      } else if (d.action === 'context') {
        // Auto-categorized (has context label but not explicitly decided)
        autoCategorized++;
        labeled++;
      }
    });
    
    if (reviewStats) {
      // Show breakdown: kept, removed, labeled (with auto-categorized count)
      const parts = [];
      if (kept > 0) parts.push(`${kept} kept`);
      if (redacted > 0) parts.push(`${redacted} removed`);
      if (labeled > 0) {
        const manualLabeled = labeled - autoCategorized;
        if (autoCategorized > 0 && manualLabeled > 0) {
          parts.push(`${labeled} labeled (${autoCategorized} auto)`);
        } else if (autoCategorized > 0) {
          parts.push(`${autoCategorized} auto-labeled`);
        } else {
          parts.push(`${labeled} labeled`);
        }
      }
      reviewStats.textContent = parts.length > 0 ? parts.join(' • ') : `${total} items`;
    }

    if (deepcleanProgress) {
      // Show actual progress: decided + auto-categorized / total
      const totalDecided = decided + autoCategorized;
      deepcleanProgress.textContent = `${totalDecided} / ${total}`;
    }

    // Update visual progress bar
    if (deepcleanProgressBar) {
      const totalDecided = decided + autoCategorized;
      const pct = total > 0 ? Math.round((totalDecided / total) * 100) : 0;
      deepcleanProgressBar.style.width = `${pct}%`;
    }

    if (btnDeepcleanUndo) {
      btnDeepcleanUndo.disabled = reviewHistory.length === 0;
    }

    // Navigation buttons
    if (btnDeepcleanPrev) {
      btnDeepcleanPrev.disabled = reviewIndex <= 0;
    }
    if (btnDeepcleanNext) {
      btnDeepcleanNext.disabled = reviewIndex >= (reviewOrder.length - 1);
    }

    // Apply should always be available; default action is Keep.
    if (btnWizardApply) btnWizardApply.disabled = false;
  }

  function applyDeepCleanResults() {
    if (!outputJSON) return;
    
    let appliedCount = 0;
    
    // Apply decisions to the dataset
    scanCandidates.forEach(c => {
       const key = getCandKey(c);
       const decision = candidateDecisions.get(key);
       // Skip if not explicitly decided to remove or add context
       if (!decision || decision.action === 'keep' || decision.action === 'undecided') return;
       
       const targetText = c.text;
       // Regex escape for safe replacement - match whole word to avoid partial replacements
       const esc = targetText.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
       const re = new RegExp(esc, 'g');
       
       // Determine replacement
       let replacement = targetText;
       if (decision.action === 'redact') {
          // Delete entirely - empty string
          replacement = '';
       } else if (decision.action === 'context') {
          const label = decision.contextLabel || c.type;
          replacement = `${targetText} [${label}]`;
       }
       
       // Apply to all transactions
       outputJSON!.transactions.forEach(t => {
          if (t.merchant.includes(targetText)) {
             let newMerchant = t.merchant.replace(re, replacement);
             // Clean up double/triple spaces and trim
             newMerchant = newMerchant.replace(/\s{2,}/g, ' ').trim();
             if (newMerchant !== t.merchant) {
               t.merchant = newMerchant;
               appliedCount++;
             }
          }
          if (t.description.includes(targetText)) {
             let newDesc = t.description.replace(re, replacement);
             // Clean up double/triple spaces and trim
             newDesc = newDesc.replace(/\s{2,}/g, ' ').trim();
             if (newDesc !== t.description) {
               t.description = newDesc;
               appliedCount++;
             }
          }
       });
    });
    
    console.log(`Deep Clean applied: ${appliedCount} changes made`);
    
    // Refresh UI
    renderOutput();
    renderFindingsBar();
    renderShareSafety();
    markStep3Done();
    
    closeDeepCleanModal();
    console.log(`Deep clean applied. ${appliedCount} instances processed.`);
  }

  function closeDeepCleanModal() {
    deepCleanModal?.classList.add('hidden');
    deepCleanMinimized?.classList.add('hidden');
    scanCandidates = [];
    candidateDecisions.clear();
    reviewOrder = [];
    reviewIndex = 0;
    reviewHistory = [];
    document.removeEventListener('keydown', deepCleanKeyHandler);
  }

  // Event Listeners
  btnDeepClean?.addEventListener('click', runDeepClean);
  btnWizardCancel?.addEventListener('click', closeDeepCleanModal);
  btnDeepCleanClose?.addEventListener('click', closeDeepCleanModal);
  btnWizardApply?.addEventListener('click', applyDeepCleanResults);
  
  // Minimize/Restore handlers
  btnDeepCleanMinimize?.addEventListener('click', () => {
    deepCleanModal?.classList.add('hidden');
    deepCleanMinimized?.classList.remove('hidden');
    // Update minimized status
    const decidedCount = Array.from(candidateDecisions.values()).filter(d => d.explicitlyDecided).length;
    const total = scanCandidates.length;
    if (minimizedStatus) {
      minimizedStatus.textContent = `${decidedCount}/${total} reviewed`;
    }
  });
  
  btnDeepCleanRestore?.addEventListener('click', () => {
    deepCleanMinimized?.classList.add('hidden');
    deepCleanModal?.classList.remove('hidden');
  });

  // Deep Clean info tooltip
  const btnDeepCleanInfo = $('btn-deep-clean-info');
  const deepCleanInfoTooltip = $('deep-clean-info-tooltip') as HTMLElement | null;

  // Move tooltip to body so fixed positioning works correctly
  // (parent containers with transforms can break fixed positioning)
  if (deepCleanInfoTooltip && deepCleanInfoTooltip.parentElement !== document.body) {
    document.body.appendChild(deepCleanInfoTooltip);
  }

  function positionInfoTooltip() {
    if (!btnDeepCleanInfo || !deepCleanInfoTooltip) return;
    
    const btnRect = btnDeepCleanInfo.getBoundingClientRect();
    const tooltipWidth = 320; // w-80 = 20rem = 320px
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    
    // Position below the button
    let top = btnRect.bottom + 8; // 8px gap
    
    // Try to align right edge of tooltip with right edge of button
    let left = btnRect.right - tooltipWidth;
    
    // Clamp to viewport bounds with padding
    const padding = 16;
    left = Math.max(padding, Math.min(left, viewportWidth - tooltipWidth - padding));
    
    // Get actual height
    deepCleanInfoTooltip.classList.remove('hidden');
    const tooltipHeight = deepCleanInfoTooltip.offsetHeight;
    
    // If it would go below the viewport, position it above the button
    if (top + tooltipHeight > viewportHeight - padding) {
      top = btnRect.top - tooltipHeight - 8;
      // If that's also off screen, pin to top with some padding
      if (top < padding) {
        top = padding;
      }
    }
    
    deepCleanInfoTooltip.style.top = `${top}px`;
    deepCleanInfoTooltip.style.left = `${left}px`;
  }

  btnDeepCleanInfo?.addEventListener('click', (e) => {
    e.stopPropagation();
    const isHidden = deepCleanInfoTooltip?.classList.contains('hidden');
    if (isHidden) {
      positionInfoTooltip();
    } else {
      deepCleanInfoTooltip?.classList.add('hidden');
    }
  });

  // Reposition on window resize
  window.addEventListener('resize', () => {
    if (deepCleanInfoTooltip && !deepCleanInfoTooltip.classList.contains('hidden')) {
      positionInfoTooltip();
    }
  });

  // Reposition on scroll
  window.addEventListener('scroll', () => {
    if (deepCleanInfoTooltip && !deepCleanInfoTooltip.classList.contains('hidden')) {
      positionInfoTooltip();
    }
  }, true);

  // Close tooltip when clicking elsewhere
  document.addEventListener('click', (e) => {
    if (!btnDeepCleanInfo?.contains(e.target as Node) && !deepCleanInfoTooltip?.contains(e.target as Node)) {
      deepCleanInfoTooltip?.classList.add('hidden');
    }
  });

  // Plan Info tooltip (P button)
  const btnPlanInfo = $('btn-plan-info');
  const planInfoTooltip = $('plan-info-tooltip') as HTMLElement | null;

  // Move tooltip to body for proper fixed positioning
  if (planInfoTooltip && planInfoTooltip.parentElement !== document.body) {
    document.body.appendChild(planInfoTooltip);
  }

  function positionPlanTooltip() {
    if (!btnPlanInfo || !planInfoTooltip) return;
    
    const btnRect = btnPlanInfo.getBoundingClientRect();
    const tooltipWidth = 288; // w-72 = 18rem = 288px
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    const padding = 16;
    
    // Position below the button
    let top = btnRect.bottom + 8;
    
    // Align left edge with button's left edge
    let left = btnRect.left;
    
    // Clamp to viewport bounds
    left = Math.max(padding, Math.min(left, viewportWidth - tooltipWidth - padding));
    
    // Get actual height
    planInfoTooltip.classList.remove('hidden');
    const tooltipHeight = planInfoTooltip.offsetHeight;
    
    // If it would go below the viewport, position it above
    if (top + tooltipHeight > viewportHeight - padding) {
      top = btnRect.top - tooltipHeight - 8;
      if (top < padding) top = padding;
    }

    planInfoTooltip.style.top = `${top}px`;
    planInfoTooltip.style.left = `${left}px`;
  }

  btnPlanInfo?.addEventListener('click', (e) => {
    e.stopPropagation();
    if (planInfoTooltip?.classList.contains('hidden')) {
      positionPlanTooltip();
    } else {
      planInfoTooltip?.classList.add('hidden');
    }
  });

  // Reposition on scroll/resize
  window.addEventListener('scroll', () => {
    if (planInfoTooltip && !planInfoTooltip.classList.contains('hidden')) {
      positionPlanTooltip();
    }
  }, true);

  window.addEventListener('resize', () => {
    if (planInfoTooltip && !planInfoTooltip.classList.contains('hidden')) {
      positionPlanTooltip();
    }
  });

  // Close plan tooltip when clicking elsewhere
  document.addEventListener('click', (e) => {
    if (!btnPlanInfo?.contains(e.target as Node) && !planInfoTooltip?.contains(e.target as Node)) {
      planInfoTooltip?.classList.add('hidden');
    }
  });
  
  // Close modal on backdrop click
  deepCleanModal?.addEventListener('click', (e) => {
    if (e.target === deepCleanModal) closeDeepCleanModal();
  });
  
  // Close on Escape
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !deepCleanModal?.classList.contains('hidden')) {
      closeDeepCleanModal();
    }
  });

  // Enable Deep Clean button when we have output
  function updateDeepCleanButtonState() {
    if (btnDeepClean) {
      btnDeepClean.disabled = !outputJSON || isProcessing;
    }
  }
  
  // Call on initial load and after any processing
  updateDeepCleanButtonState();
});

