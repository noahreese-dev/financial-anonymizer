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

function renderFindingsBar() {
  const bar = $('findings-bar');
  const chips = $('findings-chips');
  if (!bar || !chips) return;

  const rep = outputJSON?.metadata?.removalReport;
  if (!outputJSON || !rep) {
    bar.classList.add('hidden');
    chips.innerHTML = '';
    return;
  }

  const red = rep.redactions ?? {};
  const items: Array<{ label: string; count: number; term?: string; tone?: 'neutral' | 'good' | 'warn' }> = [];

  const addPII = (label: string, key: keyof typeof red, term: string) => {
    const count = Number((red as any)?.[key] ?? 0);
    if (count > 0) items.push({ label, count, term, tone: 'warn' });
  };

  addPII('Email', 'email' as any, '[EMAIL]');
  addPII('Phone', 'phone' as any, '[PHONE]');
  addPII('SSN', 'ssn' as any, '[SSN]');
  addPII('URL', 'url' as any, '[URL]');
  addPII('Address', 'address' as any, '[ADDRESS]');
  addPII('ZIP', 'zip' as any, '[ZIP]');
  // Cards are replaced with ****
  addPII('Card', 'card' as any, '****');
  // Locations are replaced with [LOC] when enabled
  addPII('Loc', 'location' as any, '[LOC]');

  if ((rep.idTokensRemoved ?? 0) > 0) items.push({ label: 'IDs', count: rep.idTokensRemoved, tone: 'neutral' });
  if ((rep.merchantNormalized ?? 0) > 0) items.push({ label: 'Normalized', count: rep.merchantNormalized, tone: 'good' });
  if ((rep.customRemoved ?? 0) > 0) items.push({ label: 'Custom', count: rep.customRemoved, tone: 'good' });

  // Render as compact chips. PII chips are clickable (data-find) to jump via search.
  const html = items.map((it) => {
    const base =
      'px-2.5 py-1 rounded-full border text-[11px] font-mono transition-colors whitespace-nowrap';
    const tone =
      it.tone === 'warn'
        ? 'border-red-500/20 bg-red-500/10 text-red-300 hover:bg-red-500/15'
        : it.tone === 'good'
          ? 'border-green-500/20 bg-green-500/10 text-green-300 hover:bg-green-500/15'
          : 'border-navy-800/60 bg-navy-950/30 text-slate-300 hover:bg-navy-950/45';

    const attrs = it.term ? `data-find="${escapeHtml(it.term)}" title="Find ${escapeHtml(it.term)}"` : '';
    const tag = it.term ? 'button' : 'span';
    return `<${tag} ${attrs} class="${base} ${tone}">${escapeHtml(it.label)} <span class="opacity-80">${it.count}</span></${tag}>`;
  }).join('');

  chips.innerHTML = html || '<span class="text-[11px] text-slate-500 font-mono">No findings</span>';
  bar.classList.remove('hidden');
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
  const compareBtn = $('btn-compare') as HTMLButtonElement | null;
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
    compareBtn && (compareBtn.disabled = true);
    highlightBtn && (highlightBtn.disabled = true);
    outputNote?.classList.add('hidden');
    
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
  // Large CSV safety: render a preview only
  const previewRows = activeFormat === 'json' ? 300 : activeFormat === 'storyline' ? 2000 : 500;

  // Profile shapes headers/preview notes (AI Safe is cleaner for prompts).
    const out = anonymizer.formatData(outputJSON, activeFormat, {
    maxRows: previewRows,
    highlightTerm,
    detailLevel: activeDetailLevel,
    profile: activeProfile
  });

  emptyEl.classList.add('hidden');
  outputContainer.classList.remove('hidden');
  
  // If highlighted, use innerHTML. If not, textContent is safer (but formatData is safe now).
  // formatData escapes HTML entities if highlightTerm is present.
  if (highlightTerm) outputEl.innerHTML = out;
  else outputEl.textContent = out;
  
  copyBtn && (copyBtn.disabled = !out);
  downloadBtn && (downloadBtn.disabled = !out);
  aiBtn && (aiBtn.disabled = !out);
  btnSearchOpen && (btnSearchOpen.disabled = !out);
  compareBtn && (compareBtn.disabled = !out);
  highlightBtn && (highlightBtn.disabled = !out);

  const ext =
    activeFormat === 'markdown' || activeFormat === 'storyline'
      ? 'md'
      : activeFormat === 'text'
        ? 'txt'
        : activeFormat;
  filenameEl.textContent = `output.${ext}`;

  if (outputNote) {
    const isTruncated = outputJSON.transactions.length > previewRows;
    if (isTruncated) {
      outputNote.textContent = `Previewing first ${previewRows} rows. Use Download for full export.`;
      outputNote.classList.remove('hidden');
    } else {
      outputNote.classList.add('hidden');
    }
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
      b.classList.remove('text-slate-400', 'text-slate-500', 'hover:text-slate-300', 'hover:bg-navy-900/50');
      b.classList.add('bg-brand-600/20', 'text-brand-300', 'border', 'border-brand-500/30');
    } else {
      b.classList.add('text-slate-500', 'hover:text-slate-300', 'hover:bg-navy-900/50');
      b.classList.remove('bg-brand-600/20', 'text-brand-300', 'border', 'border-brand-500/30');
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
  const optUltraClean = $('ultra-clean-toggle') as HTMLInputElement | null;
  const customTermsWrap = $('custom-terms-chips');
  const customTermInput = $('custom-term-input') as HTMLInputElement | null;
  const btnAddTerm = $('btn-add-term') as HTMLButtonElement | null;

  const pdfControls = $('pdf-controls');
  const pdfPageStart = $('pdf-page-start') as HTMLInputElement | null;
  const pdfPageEnd = $('pdf-page-end') as HTMLInputElement | null;
  const pdfMeta = $('pdf-meta');
  const btnPreflightRerun = $('btn-preflight-rerun') as HTMLButtonElement | null;

  if (!textarea || !btnProcess) return;
  const textareaEl: HTMLTextAreaElement = textarea;
  latestSourceText = textareaEl.value || '';

  // Workflow stepper (Upload → Review → Export/AI)
  const wfStep1 = $('wf-step-1');
  const wfStep2 = $('wf-step-2');
  const wfStep3 = $('wf-step-3');
  function setWorkflowStep(step: 1 | 2 | 3) {
    const steps = [
      { el: wfStep1, idx: 1 },
      { el: wfStep2, idx: 2 },
      { el: wfStep3, idx: 3 },
    ] as const;

    for (const s of steps) {
      if (!s.el) continue;
      const isActive = s.idx === step;
      const isDone = s.idx < step;

      s.el.classList.toggle('opacity-60', !isActive && !isDone);
      s.el.classList.toggle('opacity-70', !isActive && isDone === false);
      s.el.classList.toggle('opacity-100', isActive || isDone);

      // Active emphasis
      s.el.classList.toggle('bg-navy-950/40', isActive);
      s.el.classList.toggle('bg-navy-950/20', !isActive);

      // Border emphasis
      s.el.classList.toggle('border-brand-500/30', isActive);
      s.el.classList.toggle('border-navy-800/60', !isActive);
    }
  }

  function updateWorkflowFromState() {
    const hasInput = !!stagedRows || !!textareaEl.value.trim();
    if (!hasInput) return setWorkflowStep(1);
    if (!outputJSON) return setWorkflowStep(2);
    return setWorkflowStep(3);
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
      const a = new FinancialAnonymizer({ ultraClean: !!optUltraClean?.checked, customRemoveTerms });
      preflight = a.preflightRows(stagedRows);
      renderPlannedRemovals(preflight);
    } catch {
      // ignore
    }
  }

  function renderPdfMeta(meta: PdfExtractMeta | null) {
    if (!pdfMeta) return;
    if (!meta) { pdfMeta.textContent = '—'; return; }
    pdfMeta.textContent =
      `pdf_pages: ${meta.pageCount}\n` +
      `pages_processed: ${meta.pagesProcessed}\n` +
      `lines: ${meta.lineCount}\n` +
      `mode_columns: ${meta.modeColumnCount}\n` +
      `confidence: ${meta.confidence.toFixed(2)}`;
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
      const anonymizer = new FinancialAnonymizer({ ultraClean: !!optUltraClean?.checked, customRemoveTerms });

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
          ultraClean: !!optUltraClean?.checked,
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
  const btnCompare = $('btn-compare') as HTMLButtonElement | null;
  const btnHighlightChanges = $('btn-highlight-changes') as HTMLButtonElement | null;
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

  btnCompare?.addEventListener('click', () => {
    compareMode = !compareMode;
    try { localStorage.setItem('fa:compare', compareMode ? 'true' : 'false'); } catch { /* ignore */ }
    applyCompareLayout();
    renderOriginalPane();
  });

  btnHighlightChanges?.addEventListener('click', () => {
    highlightChanges = !highlightChanges;
    try { localStorage.setItem('fa:highlightChanges', highlightChanges ? 'true' : 'false'); } catch { /* ignore */ }
    renderOriginalPane();
  });

  // Restore compare/highlight toggles
  try {
    compareMode = localStorage.getItem('fa:compare') === 'true';
    highlightChanges = localStorage.getItem('fa:highlightChanges') === 'true';
  } catch {
    // ignore
  }
  applyCompareLayout();
  renderOriginalPane();

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
    const previewRows = activeFormat === 'json' ? 300 : activeFormat === 'storyline' ? 2000 : 500;
    const rowsToScan = outputJSON.transactions.slice(0, Math.max(0, previewRows));
    
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

    // Add to custom terms (case-insensitive check)
    const lower = term.toLowerCase();
    const existing = new Set(customRemoveTerms.map(t => t.toLowerCase()));
    
    console.log('Current custom terms:', customRemoveTerms);
    console.log('Term already exists?', existing.has(lower));
    
    if (!existing.has(lower)) {
      customRemoveTerms.unshift(term);
      if (customRemoveTerms.length > 50) customRemoveTerms = customRemoveTerms.slice(0, 50);
      saveCustomTerms();
      renderCustomTerms();
      console.log('Term added to custom terms:', customRemoveTerms);
    }

    // Always Clear search & Reprocess
    console.log('Closing search and reprocessing...');
    closeSearch();
    runProcessing();
    
    // Open fine-tune to show it was added
    if (fineTuneSection) fineTuneSection.open = true;
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

  // Copy
  btnCopy?.addEventListener('click', async () => {
    if (!outputJSON) return;
    const previewRows = activeFormat === 'json' ? 300 : activeFormat === 'storyline' ? 2000 : 500;
    const formatted = new FinancialAnonymizer().formatData(outputJSON, activeFormat, { maxRows: previewRows, detailLevel: activeDetailLevel, profile: activeProfile });
    try {
      await navigator.clipboard.writeText(formatted);
      
      // Visual feedback
      const iconCopy = btnCopy.querySelector('.copy-icon');
      const iconCheck = btnCopy.querySelector('.check-icon');
      const textSpan = btnCopy.querySelector('.btn-text');
      
      if (iconCopy && iconCheck && textSpan) {
         iconCopy.classList.add('hidden');
         iconCheck.classList.remove('hidden');
         textSpan.textContent = 'Copied!';
         
         setTimeout(() => {
           iconCopy.classList.remove('hidden');
           iconCheck.classList.add('hidden');
           textSpan.textContent = 'Copy';
         }, 2000);
      }
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

      // Update labels
      setProfileLabel();

      // Active state styling
      profileOptions.forEach(o => o.classList.remove('bg-navy-800/30'));
      opt.classList.add('bg-navy-800/30');

      // Close menu
      profileMenu?.classList.add('hidden');

      renderOutput();
    });
  });

  // Initial state
  setActiveFormat('markdown');
  renderOutput();
  renderPlannedRemovals(null);
});

