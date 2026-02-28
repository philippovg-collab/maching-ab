const state = {
  view: "dashboard",
  selectedCaseId: null,
  users: [],
  readyForMatching: true,
  selectedFiles: [],
  progressMap: {},
  quick: {
    way4: null,
    visa: [],
    runId: null,
    businessDate: null,
    resultsPage: 1,
    resultsPageSize: 50,
  },
};

const el = {
  user: document.getElementById("userSelect"),
  date: document.getElementById("businessDate"),
  refresh: document.getElementById("refreshBtn"),
  run: document.getElementById("runBtn"),
  runBtnHint: document.getElementById("runBtnHint"),
  exportUnmatchedBtn: document.getElementById("exportUnmatchedBtn"),
  goLiteBtn: document.getElementById("goLiteBtn"),
  startOnboardingBtn: document.getElementById("startOnboardingBtn"),
  openLiteGuideBtn: document.getElementById("openLiteGuideBtn"),
  openLastResultsBtn: document.getElementById("openLastResultsBtn"),
  downloadLastReportBtn: document.getElementById("downloadLastReportBtn"),
  lastRunBadge: document.getElementById("lastRunBadge"),
  lastRunTitle: document.getElementById("lastRunTitle"),
  lastRunId: document.getElementById("lastRunId"),
  lastRunBusinessDate: document.getElementById("lastRunBusinessDate"),
  lastRunMatched: document.getElementById("lastRunMatched"),
  lastRunUnmatched: document.getElementById("lastRunUnmatched"),
  lastRunPartial: document.getElementById("lastRunPartial"),

  tabDashboard: document.getElementById("tabDashboard"),
  tabIngestion: document.getElementById("tabIngestion"),
  tabLite: document.getElementById("tabLite"),
  tabResults: document.getElementById("tabResults"),
  viewDashboard: document.getElementById("viewDashboard"),
  viewIngestion: document.getElementById("viewIngestion"),
  viewLite: document.getElementById("viewLite"),
  viewResults: document.getElementById("viewResults"),

  statusFilter: document.getElementById("statusFilter"),
  runsBody: document.getElementById("runsBody"),
  exceptionsBody: document.getElementById("exceptionsBody"),
  kpiMatchRate: document.getElementById("kpiMatchRate"),
  kpiMatched: document.getElementById("kpiMatched"),
  kpiUnmatched: document.getElementById("kpiUnmatched"),
  kpiPartial: document.getElementById("kpiPartial"),
  kpiVariance: document.getElementById("kpiVariance"),

  srcWay4Records: document.getElementById("srcWay4Records"),
  srcVisaRecords: document.getElementById("srcVisaRecords"),
  srcWay4Files: document.getElementById("srcWay4Files"),
  srcVisaFiles: document.getElementById("srcVisaFiles"),
  srcRatio: document.getElementById("srcRatio"),
  srcReadyLabel: document.getElementById("srcReadyLabel"),
  srcWarnings: document.getElementById("srcWarnings"),

  matchStatusBadge: document.getElementById("matchStatusBadge"),
  matchStatusText: document.getElementById("matchStatusText"),
  matchStatusRunId: document.getElementById("matchStatusRunId"),
  matchStatusStarted: document.getElementById("matchStatusStarted"),
  matchStatusFinished: document.getElementById("matchStatusFinished"),
  matchStatusMatches: document.getElementById("matchStatusMatches"),
  matchStatusExceptions: document.getElementById("matchStatusExceptions"),
  matchProgress: document.getElementById("matchProgress"),

  emptyDetail: document.getElementById("emptyDetail"),
  detailContent: document.getElementById("detailContent"),
  caseData: document.getElementById("caseData"),
  txnData: document.getElementById("txnData"),
  actionsData: document.getElementById("actionsData"),
  diagnosticsData: document.getElementById("diagnosticsData"),
  assigneeSelect: document.getElementById("assigneeSelect"),
  assignBtn: document.getElementById("assignBtn"),
  commentInput: document.getElementById("commentInput"),
  commentBtn: document.getElementById("commentBtn"),
  resolutionInput: document.getElementById("resolutionInput"),
  closeBtn: document.getElementById("closeBtn"),
  statusBtns: Array.from(document.querySelectorAll(".statusBtn")),

  auditBody: document.getElementById("auditBody"),
  auditEmpty: document.getElementById("auditEmpty"),
  auditDetailData: document.getElementById("auditDetailData"),

  xlsxProfile: document.getElementById("xlsxProfile"),
  xlsxFiles: document.getElementById("xlsxFiles"),
  xlsxUploadBtn: document.getElementById("xlsxUploadBtn"),
  xlsxUploadRunBtn: document.getElementById("xlsxUploadRunBtn"),
  xlsxResult: document.getElementById("xlsxResult"),
  dropZone: document.getElementById("dropZone"),
  uploadProgress: document.getElementById("uploadProgress"),

  liteWay4File: document.getElementById("liteWay4File"),
  liteVisaFiles: document.getElementById("liteVisaFiles"),
  dropWay4: document.getElementById("dropWay4"),
  dropVisa: document.getElementById("dropVisa"),
  liteWay4Name: document.getElementById("liteWay4Name"),
  liteVisaNames: document.getElementById("liteVisaNames"),
  quickCompareBtn: document.getElementById("quickCompareBtn"),
  quickProgress: document.getElementById("quickProgress"),
  quickErrors: document.getElementById("quickErrors"),

  resRunId: document.getElementById("resRunId"),
  resDate: document.getElementById("resDate"),
  resMatched: document.getElementById("resMatched"),
  resUnmatchedWay4: document.getElementById("resUnmatchedWay4"),
  resUnmatchedVisa: document.getElementById("resUnmatchedVisa"),
  resPartial: document.getElementById("resPartial"),
  resDuplicates: document.getElementById("resDuplicates"),
  resAmountDelta: document.getElementById("resAmountDelta"),
  resExportReportBtn: document.getElementById("resExportReportBtn"),
  resExportWay4Btn: document.getElementById("resExportWay4Btn"),
  resExportVisaBtn: document.getElementById("resExportVisaBtn"),
  resExportMismatchBtn: document.getElementById("resExportMismatchBtn"),
  resStatusFilter: document.getElementById("resStatusFilter"),
  resSearch: document.getElementById("resSearch"),
  resAmountMin: document.getElementById("resAmountMin"),
  resAmountMax: document.getElementById("resAmountMax"),
  resCurrency: document.getElementById("resCurrency"),
  resSortBy: document.getElementById("resSortBy"),
  resSortDir: document.getElementById("resSortDir"),
  resApplyFilters: document.getElementById("resApplyFilters"),
  resRowsBody: document.getElementById("resRowsBody"),
  resPageInfo: document.getElementById("resPageInfo"),
  resPrevPage: document.getElementById("resPrevPage"),
  resNextPage: document.getElementById("resNextPage"),
  resExceptionsBody: document.getElementById("resExceptionsBody"),
  resDetailEmpty: document.getElementById("resDetailEmpty"),
  resDetailWrap: document.getElementById("resDetailWrap"),
  resLeftRecord: document.getElementById("resLeftRecord"),
  resRightRecord: document.getElementById("resRightRecord"),
  resExplain: document.getElementById("resExplain"),
  resDiffBody: document.getElementById("resDiffBody"),

  toast: document.getElementById("toast"),
  firstRunCta: document.getElementById("firstRunCta"),
  secondaryPanels: Array.from(document.querySelectorAll(".secondary-panel")),
};

const statusRu = {
  NEW: "Новый",
  TRIAGED: "Разобран",
  IN_PROGRESS: "В работе",
  CLOSED: "Закрыт",
  RUNNING: "Выполняется",
  FINISHED: "Завершен",
  FAILED: "Ошибка",
};
const categoryRu = {
  MISSING_IN_VISA: "Нет в VISA",
  MISSING_IN_WAY4: "Нет в Way4",
  AMOUNT_MISMATCH: "Расхождение суммы",
  DATE_MISMATCH: "Расхождение даты",
  OPTYPE_MISMATCH: "Расхождение типа операции",
  DUPLICATE: "Дубликат",
  STATUS_MISMATCH: "Расхождение статуса",
};
const reasonRu = {
  EXACT_RRN_AMOUNT_CURR_DATE: "Точное совпадение RRN/сумма/валюта/дата",
  ARN_MATCH_WITH_TOLERANCE: "Совпадение по ARN в рамках допуска",
  FUZZY_SCORE: "Нечеткое сопоставление по скору",
  ONE_TO_MANY_SUM_MATCH: "Частичное сопоставление one-to-many по сумме",
  MISSING_IN_WAY4: "Нет записи в Way4",
  MISSING_IN_VISA: "Нет записи в VISA",
  DUPLICATE: "Дубликат",
  AMOUNT_MISMATCH: "Расхождение суммы",
  DATE_MISMATCH: "Расхождение даты",
  OPTYPE_MISMATCH: "Расхождение типа операции",
  STATUS_MISMATCH: "Расхождение статуса",
};
const unifiedStatusRu = {
  MATCHED: "Сопоставлено",
  MISSING_IN_WAY4: "Нет в Way4",
  MISSING_IN_VISA: "Нет в VISA",
  PARTIAL: "Частично",
  DUPLICATE: "Дубликат",
  MISMATCH: "Расхождение",
};
const severityRu = { LOW: "Низкая", MEDIUM: "Средняя", HIGH: "Высокая" };
const auditActionRu = {
  MATCH_RUN_EXECUTE: "Запуск матчинга",
  INGEST_REGISTER: "Регистрация загрузки",
  EXCEPTION_ASSIGN: "Назначение кейса",
  EXCEPTION_COMMENT: "Комментарий к кейсу",
  EXCEPTION_STATUS_CHANGE: "Смена статуса кейса",
  EXCEPTION_CLOSE: "Закрытие кейса",
  RULESET_UPDATE: "Обновление правил",
};

const ruStatus = (v) => statusRu[v] || v || "-";
const ruCategory = (v) => categoryRu[v] || v || "-";
const ruSeverity = (v) => severityRu[v] || v || "-";
const ruUnifiedStatus = (v) => unifiedStatusRu[v] || v || "-";
const ruAuditAction = (v) => auditActionRu[v] || v || "-";
const ruReason = (v) => reasonRu[v] || v || "-";
const ruResult = (v) => (v === "SUCCESS" ? "Успешно" : v === "DUPLICATE" ? "Дубликат" : v || "-");

function showToast(text, isError = false) {
  el.toast.textContent = text;
  el.toast.classList.remove("hidden");
  el.toast.style.background = isError ? "#982321" : "#1c2220";
  setTimeout(() => el.toast.classList.add("hidden"), 2600);
}
const pretty = (v) => JSON.stringify(v, null, 2);
const escapeHtml = (s) =>
  String(s).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
const fmtIso = (v) => {
  if (!v) return "-";
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? v : d.toLocaleString("ru-RU");
};

function setBadge(type, text) {
  el.matchStatusBadge.className = `badge ${type}`;
  el.matchStatusBadge.textContent = text;
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      "X-User": el.user.value,
      ...(options.headers || {}),
    },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(body.error || `HTTP ${res.status}`);
    err.payload = body;
    throw err;
  }
  return body;
}

async function safeApi(path, fallbackValue, options = {}) {
  try {
    return await api(path, options);
  } catch (e) {
    if (e && String(e.message || "").toLowerCase() === "not_found") {
      return fallbackValue;
    }
    throw e;
  }
}

async function downloadFile(path, filenameFallback) {
  const res = await fetch(path, { method: "GET", headers: { "X-User": el.user.value } });
  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      msg = body.error || msg;
    } catch (_) {}
    throw new Error(msg);
  }
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  const cd = res.headers.get("Content-Disposition") || "";
  const m = cd.match(/filename=\"?([^\";]+)\"?/i);
  a.href = url;
  a.download = (m && m[1]) || filenameFallback;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function setView(view) {
  state.view = view;
  const map = {
    dashboard: el.viewDashboard,
    ingestion: el.viewIngestion,
    lite: el.viewLite,
    results: el.viewResults,
  };
  Object.entries(map).forEach(([k, node]) => node.classList.toggle("hidden", k !== view));
  el.tabDashboard.classList.toggle("active", view === "dashboard");
  el.tabIngestion.classList.toggle("active", view === "ingestion");
  el.tabLite.classList.toggle("active", view === "lite");
  el.tabResults.classList.toggle("active", view === "results");
}

function renderUploadProgress(target, progressMap, emptyMsg = "Нет активных загрузок") {
  const keys = Object.keys(progressMap);
  if (!keys.length) {
    target.innerHTML = `<div class=\"muted\">${escapeHtml(emptyMsg)}</div>`;
    return;
  }
  target.innerHTML = keys
    .map((k) => {
      const p = progressMap[k];
      const percent = Math.max(0, Math.min(100, p.percent || 0));
      return `<div class="upload-item"><div class="upload-item-head"><span>${escapeHtml(
        p.name
      )}</span><span class="upload-item-state">${escapeHtml(
        p.state
      )}</span></div><div class="upload-bar"><span style="width:${percent}%"></span></div></div>`;
    })
    .join("");
}

function setProgress(map, name, stateText, percent, target) {
  map[name] = { name, state: stateText, percent };
  renderUploadProgress(target, map);
}

function fillKPIs(a) {
  el.kpiMatchRate.textContent = `${a.match_rate_pct ?? 0}%`;
  el.kpiMatched.textContent = `${a.matched_count ?? 0}`;
  el.kpiUnmatched.textContent = `${a.unmatched_count ?? 0}`;
  el.kpiPartial.textContent = `${a.partial_count ?? 0}`;
  el.kpiVariance.textContent = `${a.variance_amount ?? 0}`;
}

function fillSourceBalance(s) {
  el.srcWay4Records.textContent = `${s.way4_records ?? 0}`;
  el.srcVisaRecords.textContent = `${s.visa_records ?? 0}`;
  el.srcWay4Files.textContent = `${s.way4_files ?? 0}`;
  el.srcVisaFiles.textContent = `${s.visa_files ?? 0}`;
  el.srcRatio.textContent = s.ratio_way4_to_visa == null ? "-" : `${s.ratio_way4_to_visa}`;
  state.readyForMatching = !!s.ready_for_matching;
  el.run.disabled = !state.readyForMatching;
  el.run.title = state.readyForMatching
    ? "Запустить матчинг за выбранную бизнес-дату"
    : "Недоступно: требуется минимум один Way4 и один VISA файл за выбранную дату";
  el.runBtnHint.textContent = state.readyForMatching
    ? "Можно запускать матчинг"
    : "Матчинг недоступен: загрузите Way4 и VISA за выбранную дату";
  el.srcReadyLabel.textContent = state.readyForMatching ? "Готово к матчингу" : "Не готово к матчингу";
  if (Array.isArray(s.warnings) && s.warnings.length) {
    el.srcWarnings.classList.remove("hidden");
    el.srcWarnings.innerHTML = s.warnings.map((w) => `<div>• ${escapeHtml(w)}</div>`).join("");
  } else {
    el.srcWarnings.classList.add("hidden");
    el.srcWarnings.innerHTML = "";
  }
}

function renderRuns(items) {
  el.runsBody.innerHTML = "";
  for (const r of items) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${r.run_id.slice(0, 8)}...</td><td>${r.business_date}</td><td>${ruStatus(
      r.status
    )}</td><td>${r.ruleset_version}</td><td>${r.created_by}</td>`;
    el.runsBody.appendChild(tr);
  }
}

function renderExceptions(items) {
  el.exceptionsBody.innerHTML = "";
  for (const c of items) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${c.case_id.slice(0, 8)}...</td><td>${ruCategory(c.category)}</td><td>${ruStatus(
      c.status
    )}</td><td>${c.owner_user_id || "-"}</td><td>${ruSeverity(c.severity)}</td>`;
    tr.addEventListener("click", () => selectCase(c.case_id));
    el.exceptionsBody.appendChild(tr);
  }
}

function renderAudit(items) {
  el.auditBody.innerHTML = "";
  if (!items.length) {
    el.auditEmpty.classList.remove("hidden");
    return;
  }
  el.auditEmpty.classList.add("hidden");
  for (const a of items) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${fmtIso(a.event_at)}</td><td>${a.actor_login || "-"}</td><td>${ruAuditAction(
      a.action
    )}</td><td>${ruResult(a.result)}</td><td>${a.details || "-"}</td><td><button class="btn link">Подробнее</button></td>`;
    tr.querySelector("button").addEventListener("click", () => (el.auditDetailData.textContent = pretty(a)));
    el.auditBody.appendChild(tr);
  }
}

function renderMatchStatus(s) {
  if (!s || !s.has_run) {
    setBadge("idle", "Нет запуска");
    el.matchStatusText.textContent = "Запусков за выбранную дату пока нет";
    [el.matchStatusRunId, el.matchStatusStarted, el.matchStatusFinished, el.matchStatusMatches, el.matchStatusExceptions].forEach(
      (n) => (n.textContent = "-")
    );
    el.matchProgress.classList.add("hidden");
    return;
  }
  el.matchStatusRunId.textContent = `${s.run_id.slice(0, 8)}...`;
  el.matchStatusStarted.textContent = fmtIso(s.started_at);
  el.matchStatusFinished.textContent = fmtIso(s.finished_at);
  el.matchStatusMatches.textContent = s.matches_created == null ? "..." : String(s.matches_created);
  el.matchStatusExceptions.textContent = s.exceptions_created == null ? "..." : String(s.exceptions_created);
  if (s.status === "RUNNING") {
    setBadge("running", "Идет матчинг");
    el.matchStatusText.textContent = "Выполняется сопоставление транзакций";
    el.matchProgress.classList.remove("hidden");
  } else {
    setBadge(s.status === "FINISHED" ? "finished" : "error", s.status === "FINISHED" ? "Завершен" : "Ошибка");
    el.matchStatusText.textContent = s.status === "FINISHED" ? "Матчинг завершен" : "Ошибка запуска";
    el.matchProgress.classList.add("hidden");
  }
}

function renderLastRun(statusData, summary) {
  if (!statusData || !statusData.has_run) {
    el.lastRunBadge.className = "badge idle";
    el.lastRunBadge.textContent = "Нет запуска";
    el.lastRunTitle.textContent = "Сверка не запускалась для выбранной даты";
    el.lastRunId.textContent = "-";
    el.lastRunBusinessDate.textContent = el.date.value || "-";
    el.lastRunMatched.textContent = "-";
    el.lastRunUnmatched.textContent = "-";
    el.lastRunPartial.textContent = "-";
    el.openLastResultsBtn.disabled = true;
    el.downloadLastReportBtn.disabled = true;
    return;
  }

  const runId = statusData.run_id;
  const unmatchedTotal = Number(summary?.unmatched_way4 || 0) + Number(summary?.unmatched_visa || 0);
  const unmatchedValue = summary ? unmatchedTotal : statusData.exceptions_created || 0;
  el.lastRunBadge.className = `badge ${statusData.status === "FINISHED" ? "finished" : statusData.status === "RUNNING" ? "running" : "error"}`;
  el.lastRunBadge.textContent = ruStatus(statusData.status);
  el.lastRunTitle.textContent = statusData.status === "FINISHED"
    ? "Последняя сверка успешно завершена"
    : statusData.status === "RUNNING"
      ? "Последняя сверка в процессе"
      : "Последняя сверка завершилась с ошибкой";
  el.lastRunId.textContent = `${runId.slice(0, 8)}...`;
  el.lastRunBusinessDate.textContent = statusData.business_date || el.date.value || "-";
  el.lastRunMatched.textContent = `${summary?.matched ?? statusData.matches_created ?? 0}`;
  el.lastRunUnmatched.textContent = `${unmatchedValue}`;
  el.lastRunPartial.textContent = `${summary?.partial ?? 0}`;
  el.openLastResultsBtn.disabled = false;
  el.downloadLastReportBtn.disabled = false;
}

function updateDashboardOnboarding(balance, status) {
  const way4Files = Number(balance?.way4_files || 0);
  const visaFiles = Number(balance?.visa_files || 0);
  const hasRun = !!status?.has_run;
  const hasData = way4Files > 0 || visaFiles > 0 || hasRun;
  el.firstRunCta.classList.toggle("hidden", hasData);
  for (const panel of el.secondaryPanels) {
    panel.classList.toggle("hidden", !hasData);
  }
}

function renderAssignees(items) {
  el.assigneeSelect.innerHTML = "";
  for (const u of items) {
    const o = document.createElement("option");
    o.value = u.login;
    o.textContent = `${u.login} (${u.roles.join(",")})`;
    el.assigneeSelect.appendChild(o);
  }
}

async function selectCase(caseId) {
  const data = await api(`/api/v1/exceptions/${caseId}`);
  state.selectedCaseId = caseId;
  el.emptyDetail.classList.add("hidden");
  el.detailContent.classList.remove("hidden");
  el.caseData.textContent = pretty(data.case);
  el.txnData.textContent = pretty(data.transaction);
  el.actionsData.textContent = pretty(data.actions);
  el.diagnosticsData.textContent = pretty(data.diagnostics || {});
}

async function loadDashboard() {
  const d = el.date.value;
  const [balance, kpi, runs, ex, audit, status] = await Promise.all([
    safeApi(`/api/v1/monitor/source-balance?business_date=${d}`, {
      business_date: d,
      way4_records: 0,
      visa_records: 0,
      way4_files: 0,
      visa_files: 0,
      ratio_way4_to_visa: null,
      ready_for_matching: false,
      warnings: [],
    }),
    safeApi(`/api/v1/analytics/hardcoded?business_date=${d}`, {
      business_date: d,
      match_rate_pct: 0,
      matched_count: 0,
      unmatched_count: 0,
      partial_count: 0,
      variance_amount: 0,
    }),
    safeApi(`/api/v1/match/runs?limit=30&business_date=${encodeURIComponent(d)}`, { items: [] }),
    safeApi(`/api/v1/exceptions?business_date=${d}`, { items: [] }),
    safeApi(`/api/v1/audit/events`, { items: [] }),
    safeApi(`/api/v1/match/status?business_date=${d}`, { business_date: d, has_run: false }),
  ]);
  let users = { items: [] };
  try {
    users = await api(`/api/v1/meta/users`);
  } catch (_) {
    users = { items: [{ login: el.user.value, roles: [] }] };
  }
  fillSourceBalance(balance);
  fillKPIs(kpi);
  updateDashboardOnboarding(balance, status);
  renderRuns(runs.items || []);
  renderExceptions(ex.items || []);
  renderAssignees(users.items || []);
  renderAudit((audit.items || []).slice(0, 20));
  renderMatchStatus(status);
  let lastSummary = null;
  if (status && status.has_run && status.run_id) {
    state.quick.runId = status.run_id;
    state.quick.businessDate = status.business_date || d;
    try {
      const r = await api(`/api/v1/results/run/${status.run_id}?page=1&page_size=1`);
      lastSummary = r.summary || null;
    } catch (_) {}
  }
  renderLastRun(status, lastSummary);
}

async function runMatching() {
  if (!state.readyForMatching) {
    showToast("Сначала загрузите Way4 и VISA файлы. Перенаправляю в Загрузка Lite", true);
    setView("lite");
    return;
  }
  try {
    setBadge("running", "Идет матчинг");
    el.matchProgress.classList.remove("hidden");
    const res = await api(`/api/v1/match/runs`, {
      method: "POST",
      body: JSON.stringify({ business_date: el.date.value, scope_filter: "ALL" }),
    });
    showToast(`Запуск создан: ${res.run_id.slice(0, 8)}...`);
    await loadDashboard();
  } catch (e) {
    showToast(e.message, true);
  }
}

async function exportUnmatched() {
  try {
    const q = new URLSearchParams({ business_date: el.date.value });
    await downloadFile(`/api/v1/export/unmatched.csv?${q.toString()}`, `unmatched_${el.date.value}.csv`);
    showToast("CSV выгружен");
  } catch (e) {
    showToast(e.message, true);
  }
}

async function exportRunReport(kind) {
  const runId = await ensureResultsRun();
  if (!runId) {
    showToast("Нет запуска для экспорта", true);
    return;
  }
  try {
    const day = state.quick.businessDate || el.date.value || "unknown";
    if (kind === "report") {
      await downloadFile(
        `/api/v1/runs/${encodeURIComponent(runId)}/export.xlsx`,
        `reconciliation_report_${day}_${runId.slice(0, 8)}.xlsx`
      );
    } else if (kind === "way4") {
      await downloadFile(
        `/api/v1/runs/${encodeURIComponent(runId)}/unmatched_way4.csv`,
        `unmatched_way4_${day}_${runId.slice(0, 8)}.csv`
      );
    } else if (kind === "visa") {
      await downloadFile(
        `/api/v1/runs/${encodeURIComponent(runId)}/unmatched_visa.csv`,
        `unmatched_visa_${day}_${runId.slice(0, 8)}.csv`
      );
    } else if (kind === "mismatch") {
      await downloadFile(
        `/api/v1/runs/${encodeURIComponent(runId)}/mismatches_partial.xlsx`,
        `mismatches_partial_${day}_${runId.slice(0, 8)}.xlsx`
      );
    } else {
      throw new Error("Неизвестный тип экспорта");
    }
    showToast("Экспорт сформирован");
  } catch (e) {
    showToast(e.message, true);
  }
}

async function act(payload) {
  if (!state.selectedCaseId) {
    showToast("Сначала выберите кейс исключения", true);
    return;
  }
  await api(`/api/v1/exceptions/${state.selectedCaseId}/actions`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
  await selectCase(state.selectedCaseId);
  await loadDashboard();
  showToast("Действие выполнено");
}

function syncProfileByFileName() {
  const files = state.selectedFiles.length ? state.selectedFiles : Array.from(el.xlsxFiles.files || []);
  if (!files.length || el.xlsxProfile.value) return;
  if (files.length === 1) {
    const name = files[0].name.toLowerCase();
    if (name.includes("1552")) el.xlsxProfile.value = "WAY4_1552_V1";
    else if (name.includes("виза") || name.includes("visa")) el.xlsxProfile.value = "VISA_MSPK_V1";
  }
}

async function toBase64(file) {
  return new Promise((resolve, reject) => {
    const fr = new FileReader();
    fr.onload = () => {
      const content = String(fr.result || "");
      resolve(content.includes(",") ? content.split(",", 2)[1] : content);
    };
    fr.onerror = () => reject(fr.error || new Error("Не удалось прочитать файл"));
    fr.readAsDataURL(file);
  });
}

async function uploadXlsx() {
  const files = state.selectedFiles.length ? state.selectedFiles : Array.from(el.xlsxFiles.files || []);
  if (!files.length) {
    showToast("Выберите хотя бы один файл xlsx", true);
    return { ok: false, error: "no_files" };
  }
  try {
    state.progressMap = {};
    renderUploadProgress(el.uploadProgress, state.progressMap);
    el.xlsxUploadBtn.disabled = true;
    el.xlsxUploadRunBtn.disabled = true;
    const items = [];
    for (const file of files) {
      setProgress(state.progressMap, file.name, "Чтение", 12, el.uploadProgress);
      const b64 = await toBase64(file);
      setProgress(state.progressMap, file.name, "Подготовлено", 55, el.uploadProgress);
      const item = { file_name: file.name, file_base64: b64 };
      if (el.xlsxProfile.value) item.parser_profile = el.xlsxProfile.value;
      items.push(item);
    }
    for (const file of files) setProgress(state.progressMap, file.name, "Загрузка", 78, el.uploadProgress);
    let result;
    try {
      result = await api(`/api/v1/ingest/xlsx/batch`, {
        method: "POST",
        body: JSON.stringify({ files: items, business_date: el.date.value }),
      });
    } catch (e) {
      if (String(e.message || "").toLowerCase() !== "not_found") throw e;
      if (items.length !== 1) throw e;
      const single = items[0];
      const singleRes = await api(`/api/v1/ingest/xlsx`, {
        method: "POST",
        body: JSON.stringify({ ...single, business_date: el.date.value }),
      });
      result = {
        business_date: singleRes.business_date,
        total_files: 1,
        failed_files: 0,
        imported_records: Number(singleRes.record_count || 0),
        items: [{ file_name: single.file_name, ok: true, result: singleRes }],
      };
    }
    const byName = Object.fromEntries((result.items || []).map((i) => [i.file_name, i]));
    for (const file of files) {
      const r = byName[file.name];
      setProgress(
        state.progressMap,
        file.name,
        r && r.ok ? `Импортировано: ${r.result.record_count}` : `Ошибка: ${(r && r.error) || "Неизвестно"}`,
        100,
        el.uploadProgress
      );
    }
    el.xlsxResult.textContent = pretty(result);
    const hasFailures = Number(result.failed_files || 0) > 0;
    if (hasFailures) {
      showToast(
        `Загрузка завершена с ошибками: ${result.failed_files} из ${result.total_files} файлов не импортированы`,
        true
      );
    } else {
      showToast(`Загружено файлов: ${result.total_files}, записей: ${result.imported_records}`);
    }
    await loadDashboard();
    return { ok: !hasFailures, result };
  } catch (e) {
    el.xlsxResult.textContent = String(e.message || e);
    showToast(e.message, true);
    return { ok: false, error: e };
  } finally {
    el.xlsxUploadBtn.disabled = false;
    el.xlsxUploadRunBtn.disabled = false;
  }
}

async function uploadXlsxAndRun() {
  const uploadResult = await uploadXlsx();
  if (!uploadResult || !uploadResult.ok) {
    showToast("Матчинг не запущен: сначала исправьте ошибки загрузки", true);
    return;
  }
  await runMatching();
}

function wireDropZone(node, onFiles) {
  const prevent = (ev) => {
    ev.preventDefault();
    ev.stopPropagation();
  };
  ["dragenter", "dragover", "dragleave", "drop"].forEach((evt) => node.addEventListener(evt, prevent));
  ["dragenter", "dragover"].forEach((evt) => node.addEventListener(evt, () => node.classList.add("dragover")));
  ["dragleave", "drop"].forEach((evt) => node.addEventListener(evt, () => node.classList.remove("dragover")));
  node.addEventListener("drop", (ev) => onFiles(Array.from(ev.dataTransfer.files || [])));
}

function renderQuickFileLabels() {
  el.liteWay4Name.textContent = state.quick.way4 ? state.quick.way4.name : "Файл не выбран";
  el.liteVisaNames.textContent = state.quick.visa.length
    ? state.quick.visa.map((f) => f.name).join(", ")
    : "Файлы не выбраны";
  el.quickCompareBtn.disabled = !(state.quick.way4 && state.quick.visa.length);
}

function setQuickProgress(step, status, percent) {
  const map = state.quickProgressMap || (state.quickProgressMap = {});
  map[step] = { name: step, state: status, percent };
  renderUploadProgress(el.quickProgress, map, "Шаги не запускались");
}

function clearQuickProgress() {
  state.quickProgressMap = {};
  renderUploadProgress(el.quickProgress, state.quickProgressMap, "Шаги не запускались");
}

function renderQuickValidationErrors(payload) {
  if (!payload || !Array.isArray(payload.errors) || !payload.errors.length) {
    el.quickErrors.textContent = "Нет ошибок";
    return;
  }
  const lines = payload.errors.slice(0, 200).map((e) => `${e.file}: строка ${e.row}, поле ${e.field} - ${e.message}`);
  el.quickErrors.textContent = lines.join("\n");
}

function buildResultsFiltersQuery() {
  const q = new URLSearchParams();
  q.set("page", String(state.quick.resultsPage));
  q.set("page_size", String(state.quick.resultsPageSize));
  if (el.resStatusFilter.value) q.set("status", el.resStatusFilter.value);
  if (el.resSearch.value.trim()) q.set("q", el.resSearch.value.trim());
  if (el.resAmountMin.value !== "") q.set("amount_min", el.resAmountMin.value);
  if (el.resAmountMax.value !== "") q.set("amount_max", el.resAmountMax.value);
  if (el.resCurrency.value.trim()) q.set("currency", el.resCurrency.value.trim().toUpperCase());
  if (el.resSortBy.value) q.set("sort_by", el.resSortBy.value);
  if (el.resSortDir.value) q.set("sort_dir", el.resSortDir.value);
  return q.toString();
}

function renderResultRows(items) {
  el.resRowsBody.innerHTML = "";
  for (const it of items || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${ruUnifiedStatus(it.status)}</td>
      <td>${it.rrn || "-"}</td>
      <td>${it.arn || "-"}</td>
      <td>${it.txn_time ? fmtIso(it.txn_time) : "-"}</td>
      <td>${it.amount_way4 ?? "-"}</td>
      <td>${it.amount_visa ?? "-"}</td>
      <td>${it.delta ?? "-"}</td>
      <td>${it.currency || "-"}</td>
      <td>${it.match_score ?? "-"}</td>
      <td>${ruReason(it.rule_reason)}</td>
    `;
    tr.addEventListener("click", async () => {
      await loadResultDetails(it.row_id);
      Array.from(el.resRowsBody.querySelectorAll("tr")).forEach((x) => x.classList.remove("active"));
      tr.classList.add("active");
    });
    el.resRowsBody.appendChild(tr);
  }
}

function renderResultDetails(d) {
  el.resDetailEmpty.classList.add("hidden");
  el.resDetailWrap.classList.remove("hidden");
  el.resLeftRecord.textContent = pretty(d.left_record || {});
  el.resRightRecord.textContent = pretty(d.right_record || {});
  el.resExplain.textContent = pretty({
    reason_code: ruReason(d.reason_code),
    score: d.score,
    explain_json: d.explain_json || {},
  });

  el.resDiffBody.innerHTML = "";
  for (const diff of d.differences || []) {
    const tr = document.createElement("tr");
    const sev = (diff.severity || "LOW").toUpperCase();
    tr.className = sev === "HIGH" ? "diff-high" : sev === "MEDIUM" ? "diff-medium" : "diff-low";
    tr.innerHTML = `
      <td>${diff.field}</td>
      <td>${diff.left ?? "-"}</td>
      <td>${diff.right ?? "-"}</td>
      <td>${ruSeverity(sev)}</td>
    `;
    el.resDiffBody.appendChild(tr);
  }
}

async function loadResultDetails(rowId) {
  const d = await api(`/api/v1/results/details/${encodeURIComponent(rowId)}`);
  renderResultDetails(d);
}

async function loadResults(runId, businessDate) {
  if (!runId) return;
  const filters = buildResultsFiltersQuery();
  const [results, ex] = await Promise.all([
    api(`/api/v1/results/run/${runId}?${filters}`),
    api(`/api/v1/exceptions?run_id=${encodeURIComponent(runId)}`),
  ]);

  el.resRunId.textContent = `${runId.slice(0, 8)}...`;
  el.resDate.textContent = businessDate || results.run.business_date || "-";
  el.resMatched.textContent = `${results.summary.matched ?? 0}`;
  el.resUnmatchedWay4.textContent = `${results.summary.unmatched_way4 ?? 0}`;
  el.resUnmatchedVisa.textContent = `${results.summary.unmatched_visa ?? 0}`;
  el.resPartial.textContent = `${results.summary.partial ?? 0}`;
  el.resDuplicates.textContent = `${results.summary.duplicates ?? 0}`;
  el.resAmountDelta.textContent = `${results.summary.amount_delta ?? 0}`;

  el.resDetailWrap.classList.add("hidden");
  el.resDetailEmpty.classList.remove("hidden");
  el.resDiffBody.innerHTML = "";
  el.resLeftRecord.textContent = "";
  el.resRightRecord.textContent = "";
  el.resExplain.textContent = "";

  renderResultRows(results.items);
  el.resPageInfo.textContent = `Страница ${results.page} из ${results.total_pages} (всего ${results.total})`;
  el.resPrevPage.disabled = results.page <= 1;
  el.resNextPage.disabled = results.page >= results.total_pages;

  el.resExceptionsBody.innerHTML = "";
  for (const c of (ex.items || []).slice(0, 200)) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${c.case_id.slice(0, 8)}...</td><td>${ruCategory(c.category)}</td><td>${ruStatus(c.status)}</td><td>${ruSeverity(c.severity)}</td>`;
    el.resExceptionsBody.appendChild(tr);
  }
}

async function quickCompare() {
  if (!state.quick.way4 || !state.quick.visa.length) return;
  try {
    el.quickCompareBtn.disabled = true;
    clearQuickProgress();
    el.quickErrors.textContent = "Нет ошибок";

    setQuickProgress("upload", "Подготовка файлов", 15);
    const way4b64 = await toBase64(state.quick.way4);
    const visaB64 = [];
    for (const f of state.quick.visa) visaB64.push({ file_name: f.name, file_base64: await toBase64(f) });

    setQuickProgress("parse_validate", "Парсинг и валидация", 45);
    const payload = {
      business_date: el.date.value,
      way4_file: { file_name: state.quick.way4.name, file_base64: way4b64 },
      visa_files: visaB64,
    };

    setQuickProgress("match", "Запуск матчинга", 75);
    const res = await api(`/api/v1/quick-compare`, { method: "POST", body: JSON.stringify(payload) });

    setQuickProgress("build_report", "Формирование отчета", 95);
    state.quick.runId = res.run_id;
    state.quick.businessDate = res.business_date;
    await loadResults(res.run_id, res.business_date);
    setQuickProgress("done", "Завершено", 100);
    setView("results");
    showToast("Быстрая сверка завершена");
    await loadDashboard();
  } catch (e) {
    try {
      renderQuickValidationErrors(e.payload);
    } catch (_) {}
    setQuickProgress("error", "Ошибка", 100);
    showToast(e.message, true);
  } finally {
    renderQuickFileLabels();
  }
}

async function ensureResultsRun() {
  if (state.quick.runId) return state.quick.runId;
  const q = new URLSearchParams({ business_date: el.date.value, page: "1", page_size: "1" });
  try {
    const latest = await api(`/api/v1/results/latest?${q.toString()}`);
    if (!latest || !latest.has_run || !latest.run || !latest.run.run_id) return null;
    state.quick.runId = latest.run.run_id;
    state.quick.businessDate = latest.run.business_date;
    return state.quick.runId;
  } catch (e) {
    if (String(e.message || "").toLowerCase() !== "not_found") throw e;
  }
  const runs = await safeApi(
    `/api/v1/match/runs?limit=1&business_date=${encodeURIComponent(el.date.value)}`,
    { items: [] }
  );
  const first = (runs.items || [])[0];
  if (!first) return null;
  state.quick.runId = first.run_id;
  state.quick.businessDate = first.business_date;
  return state.quick.runId;
}

function bindEvents() {
  el.refresh.addEventListener("click", () => loadDashboard().catch((e) => showToast(e.message, true)));
  el.run.addEventListener("click", () => runMatching().catch((e) => showToast(e.message, true)));
  el.exportUnmatchedBtn.addEventListener("click", () => exportUnmatched());
  el.statusFilter.addEventListener("change", () => loadDashboard().catch((e) => showToast(e.message, true)));
  el.date.addEventListener("change", () => {
    state.quick.runId = null;
    state.quick.businessDate = null;
    state.quick.resultsPage = 1;
    loadDashboard().catch((e) => showToast(e.message, true));
  });
  el.user.addEventListener("change", () => loadDashboard().catch((e) => showToast(e.message, true)));

  el.tabDashboard.addEventListener("click", () => setView("dashboard"));
  el.tabIngestion.addEventListener("click", () => setView("ingestion"));
  el.tabLite.addEventListener("click", () => setView("lite"));
  el.tabResults.addEventListener("click", async () => {
    setView("results");
    const runId = await ensureResultsRun();
    if (!runId) {
      showToast("Нет запусков для выбранной даты", true);
      return;
    }
    await loadResults(runId, state.quick.businessDate || el.date.value);
  });
  el.goLiteBtn.addEventListener("click", () => setView("lite"));
  el.startOnboardingBtn.addEventListener("click", () => setView("lite"));
  el.openLiteGuideBtn.addEventListener("click", () => setView("lite"));
  el.openLastResultsBtn.addEventListener("click", async () => {
    const runId = await ensureResultsRun();
    if (!runId) {
      showToast("Нет запусков для выбранной даты", true);
      return;
    }
    await loadResults(runId, state.quick.businessDate || el.date.value);
    setView("results");
  });
  el.downloadLastReportBtn.addEventListener("click", () => exportRunReport("report"));

  el.xlsxUploadBtn.addEventListener("click", () => uploadXlsx());
  el.xlsxUploadRunBtn.addEventListener("click", () => uploadXlsxAndRun());
  el.xlsxFiles.addEventListener("change", () => {
    state.selectedFiles = Array.from(el.xlsxFiles.files || []).filter((f) => /\.xlsx$/i.test(f.name));
    syncProfileByFileName();
  });

  el.assignBtn.addEventListener("click", () => act({ action_type: "assign", owner_user_id: el.assigneeSelect.value }));
  el.commentBtn.addEventListener("click", () =>
    act({ action_type: "comment", comment: el.commentInput.value || "Без комментария" })
  );
  el.closeBtn.addEventListener("click", () =>
    act({ action_type: "close", resolution_code: el.resolutionInput.value || "MANUAL_RESOLUTION" })
  );
  for (const btn of el.statusBtns) {
    btn.addEventListener("click", () => act({ action_type: "status_change", status: btn.dataset.status }));
  }

  el.liteWay4File.addEventListener("change", () => {
    state.quick.way4 = (el.liteWay4File.files && el.liteWay4File.files[0]) || null;
    renderQuickFileLabels();
  });
  el.liteVisaFiles.addEventListener("change", () => {
    state.quick.visa = Array.from(el.liteVisaFiles.files || []);
    renderQuickFileLabels();
  });
  el.quickCompareBtn.addEventListener("click", () => quickCompare());
  el.resExportReportBtn.addEventListener("click", () => exportRunReport("report"));
  el.resExportWay4Btn.addEventListener("click", () => exportRunReport("way4"));
  el.resExportVisaBtn.addEventListener("click", () => exportRunReport("visa"));
  el.resExportMismatchBtn.addEventListener("click", () => exportRunReport("mismatch"));

  el.resApplyFilters.addEventListener("click", async () => {
    state.quick.resultsPage = 1;
    const runId = await ensureResultsRun();
    if (!runId) return;
    await loadResults(runId, state.quick.businessDate || el.date.value);
  });
  el.resPrevPage.addEventListener("click", async () => {
    if (state.quick.resultsPage <= 1) return;
    state.quick.resultsPage -= 1;
    const runId = await ensureResultsRun();
    if (!runId) return;
    await loadResults(runId, state.quick.businessDate || el.date.value);
  });
  el.resNextPage.addEventListener("click", async () => {
    state.quick.resultsPage += 1;
    const runId = await ensureResultsRun();
    if (!runId) return;
    await loadResults(runId, state.quick.businessDate || el.date.value);
  });

  wireDropZone(el.dropZone, (files) => {
    state.selectedFiles = files.filter((f) => /\.xlsx$/i.test(f.name));
  });
  wireDropZone(el.dropWay4, (files) => {
    state.quick.way4 = files.find((f) => /\.xlsx$/i.test(f.name)) || null;
    renderQuickFileLabels();
  });
  wireDropZone(el.dropVisa, (files) => {
    state.quick.visa = files.filter((f) => /\.xlsx$/i.test(f.name));
    renderQuickFileLabels();
  });
}

async function init() {
  setView("dashboard");
  renderUploadProgress(el.uploadProgress, state.progressMap);
  clearQuickProgress();
  renderQuickFileLabels();
  bindEvents();
  await loadDashboard();
}

init().catch((e) => showToast(e.message, true));
