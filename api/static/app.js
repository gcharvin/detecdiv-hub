const state = {
  userKey: localStorage.getItem("detecdivHub.userKey") || "localdev",
  sessionToken: localStorage.getItem("detecdivHub.sessionToken") || "",
  authMode: "",
  currentUser: null,
  projects: [],
  groups: [],
  storageRoots: [],
  pipelines: [],
  observedPipelines: [],
  sessions: [],
  users: [],
  indexingJobs: [],
  rawDatasets: [],
  selectedRawDataset: null,
  selectedRawDatasetDetail: null,
  archivePolicyPreview: null,
  automaticArchivePolicyStatus: null,
  micromanagerIngestStatus: null,
  migrationPlans: [],
  selectedMigrationPlan: null,
  selectedProject: null,
  selectedProjectDetail: null,
  notes: [],
  acl: [],
  summary: null,
};

const els = {
  loginPanel: document.querySelector("#login-panel"),
  loginUserKey: document.querySelector("#login-user-key"),
  loginPassword: document.querySelector("#login-password"),
  loginButton: document.querySelector("#login-button"),
  sessionLabel: document.querySelector("#session-label"),
  connectButton: document.querySelector("#connect-button"),
  logoutButton: document.querySelector("#logout-button"),
  projectSearch: document.querySelector("#project-search"),
  ownerFilter: document.querySelector("#owner-filter"),
  storageRootFilter: document.querySelector("#storage-root-filter"),
  projectLimit: document.querySelector("#project-limit"),
  groupFilter: document.querySelector("#group-filter"),
  ownedOnly: document.querySelector("#owned-only"),
  refreshButton: document.querySelector("#refresh-button"),
  newGroupButton: document.querySelector("#new-group-button"),
  projectsTableBody: document.querySelector("#projects-table tbody"),
  projectCountLabel: document.querySelector("#project-count-label"),
  userLabel: document.querySelector("#user-label"),
  summaryTotalProjects: document.querySelector("#summary-total-projects"),
  summaryOwnedProjects: document.querySelector("#summary-owned-projects"),
  summaryTotalBytes: document.querySelector("#summary-total-bytes"),
  summaryGroupCount: document.querySelector("#summary-group-count"),
  detailEmpty: document.querySelector("#detail-empty"),
  detailContent: document.querySelector("#detail-content"),
  detailSubtitle: document.querySelector("#detail-subtitle"),
  detailList: document.querySelector("#detail-list"),
  notesList: document.querySelector("#notes-list"),
  aclList: document.querySelector("#acl-list"),
  addNoteButton: document.querySelector("#add-note-button"),
  shareButton: document.querySelector("#share-button"),
  editProjectButton: document.querySelector("#edit-project-button"),
  addToGroupButton: document.querySelector("#add-to-group-button"),
  previewDeleteButton: document.querySelector("#preview-delete-button"),
  indexSourcePath: document.querySelector("#index-source-path"),
  indexStorageRootName: document.querySelector("#index-storage-root-name"),
  indexVisibility: document.querySelector("#index-visibility"),
  indexClearExisting: document.querySelector("#index-clear-existing"),
  indexButton: document.querySelector("#index-button"),
  indexJobsRefreshButton: document.querySelector("#index-jobs-refresh-button"),
  activeIndexJob: document.querySelector("#active-index-job"),
  indexJobsTableBody: document.querySelector("#index-jobs-table tbody"),
  rawSearch: document.querySelector("#raw-search"),
  rawOwnerFilter: document.querySelector("#raw-owner-filter"),
  rawTierFilter: document.querySelector("#raw-tier-filter"),
  rawArchiveStatusFilter: document.querySelector("#raw-archive-status-filter"),
  rawLimit: document.querySelector("#raw-limit"),
  rawOwnedOnly: document.querySelector("#raw-owned-only"),
  rawDatasetsTableBody: document.querySelector("#raw-datasets-table tbody"),
  rawCountLabel: document.querySelector("#raw-count-label"),
  rawDetailEmpty: document.querySelector("#raw-detail-empty"),
  rawDetailContent: document.querySelector("#raw-detail-content"),
  rawDetailSubtitle: document.querySelector("#raw-detail-subtitle"),
  rawDetailList: document.querySelector("#raw-detail-list"),
  rawLifecycleEvents: document.querySelector("#raw-lifecycle-events"),
  rawPreviewArchiveButton: document.querySelector("#raw-preview-archive-button"),
  rawArchiveButton: document.querySelector("#raw-archive-button"),
  rawRestoreButton: document.querySelector("#raw-restore-button"),
  automaticArchivePolicyPanel: document.querySelector("#automatic-archive-policy-panel"),
  automaticArchivePolicyReportOnly: document.querySelector("#automatic-archive-policy-report-only"),
  automaticArchivePolicyRefreshButton: document.querySelector("#automatic-archive-policy-refresh-button"),
  automaticArchivePolicyRunButton: document.querySelector("#automatic-archive-policy-run-button"),
  automaticArchivePolicySummary: document.querySelector("#automatic-archive-policy-summary"),
  automaticArchivePolicyConfig: document.querySelector("#automatic-archive-policy-config"),
  automaticArchivePolicyRunsTableBody: document.querySelector("#automatic-archive-policy-runs-table tbody"),
  micromanagerIngestPanel: document.querySelector("#micromanager-ingest-panel"),
  micromanagerIngestReportOnly: document.querySelector("#micromanager-ingest-report-only"),
  micromanagerIngestRefreshButton: document.querySelector("#micromanager-ingest-refresh-button"),
  micromanagerIngestRunButton: document.querySelector("#micromanager-ingest-run-button"),
  micromanagerIngestSummary: document.querySelector("#micromanager-ingest-summary"),
  micromanagerIngestConfig: document.querySelector("#micromanager-ingest-config"),
  micromanagerIngestRunsTableBody: document.querySelector("#micromanager-ingest-runs-table tbody"),
  archivePolicyOlderDays: document.querySelector("#archive-policy-older-days"),
  archivePolicyMinGb: document.querySelector("#archive-policy-min-gb"),
  archivePolicyLimit: document.querySelector("#archive-policy-limit"),
  archivePolicyUri: document.querySelector("#archive-policy-uri"),
  archivePolicyCompression: document.querySelector("#archive-policy-compression"),
  archivePolicyDeleteSource: document.querySelector("#archive-policy-delete-source"),
  archivePolicyPreviewButton: document.querySelector("#archive-policy-preview-button"),
  archivePolicyQueueButton: document.querySelector("#archive-policy-queue-button"),
  archivePolicySummary: document.querySelector("#archive-policy-summary"),
  archivePolicyTableBody: document.querySelector("#archive-policy-table tbody"),
  migrationBatchName: document.querySelector("#migration-batch-name"),
  migrationSourcePath: document.querySelector("#migration-source-path"),
  migrationSourceKind: document.querySelector("#migration-source-kind"),
  migrationStrategy: document.querySelector("#migration-strategy"),
  migrationStorageRootName: document.querySelector("#migration-storage-root-name"),
  migrationMaxItems: document.querySelector("#migration-max-items"),
  migrationCreateButton: document.querySelector("#migration-create-button"),
  migrationRefreshButton: document.querySelector("#migration-refresh-button"),
  migrationPlanCountLabel: document.querySelector("#migration-plan-count-label"),
  migrationPlansTableBody: document.querySelector("#migration-plans-table tbody"),
  migrationDetailEmpty: document.querySelector("#migration-detail-empty"),
  migrationDetailContent: document.querySelector("#migration-detail-content"),
  migrationDetailSubtitle: document.querySelector("#migration-detail-subtitle"),
  migrationDetailList: document.querySelector("#migration-detail-list"),
  migrationItemsTableBody: document.querySelector("#migration-items-table tbody"),
  migrationExecutePilotButton: document.querySelector("#migration-execute-pilot-button"),
  pipelineSearch: document.querySelector("#pipeline-search"),
  pipelineRuntimeFilter: document.querySelector("#pipeline-runtime-filter"),
  pipelineSourceFilter: document.querySelector("#pipeline-source-filter"),
  refreshPipelinesButton: document.querySelector("#refresh-pipelines-button"),
  importObservedPipelinesButton: document.querySelector("#import-observed-pipelines-button"),
  newPipelineButton: document.querySelector("#new-pipeline-button"),
  pipelinesTableBody: document.querySelector("#pipelines-table tbody"),
  usersTableBody: document.querySelector("#users-table tbody"),
  newUserButton: document.querySelector("#new-user-button"),
  sessionsTableBody: document.querySelector("#sessions-table tbody"),
  refreshSessionsButton: document.querySelector("#refresh-sessions-button"),
  statusLine: document.querySelector("#status-line"),
};

let dashboardPollHandle = null;
let lastPollSucceededAt = null;

const pageFlags = {
  hasProjectsView: Boolean(els.projectsTableBody),
  hasProjectGroups: Boolean(els.groupFilter),
  hasStorageRootFilter: Boolean(els.storageRootFilter),
  hasProjectDetail: Boolean(els.detailContent),
  hasPipelinesView: Boolean(els.pipelinesTableBody),
  hasUsersView: Boolean(els.usersTableBody),
  hasSessionsView: Boolean(els.sessionsTableBody),
  hasIndexingView: Boolean(els.indexJobsTableBody || els.activeIndexJob),
  hasRawDatasetsView: Boolean(els.rawDatasetsTableBody),
  hasAutomaticArchivePolicy: Boolean(els.automaticArchivePolicyPanel),
  hasMicroManagerIngest: Boolean(els.micromanagerIngestPanel),
  hasRawDatasetDetail: Boolean(els.rawDetailContent),
  hasIndexForm: Boolean(els.indexSourcePath),
  hasMigrationView: Boolean(els.migrationPlansTableBody || els.migrationDetailContent),
};

function setStatus(message) {
  if (els.statusLine) {
    els.statusLine.textContent = message;
  }
}

function clearDashboardState() {
  state.projects = [];
  state.groups = [];
  state.storageRoots = [];
  state.pipelines = [];
  state.observedPipelines = [];
  state.sessions = [];
  state.users = [];
  state.indexingJobs = [];
  state.rawDatasets = [];
  state.selectedRawDataset = null;
  state.selectedRawDatasetDetail = null;
  state.archivePolicyPreview = null;
  state.automaticArchivePolicyStatus = null;
  state.micromanagerIngestStatus = null;
  state.migrationPlans = [];
  state.selectedMigrationPlan = null;
  state.selectedProject = null;
  state.selectedProjectDetail = null;
  state.notes = [];
  state.acl = [];
  state.summary = null;

  if (els.summaryTotalProjects) els.summaryTotalProjects.textContent = "0";
  if (els.summaryOwnedProjects) els.summaryOwnedProjects.textContent = "0";
  if (els.summaryTotalBytes) els.summaryTotalBytes.textContent = "0 B";
  if (els.summaryGroupCount) els.summaryGroupCount.textContent = "0";

  renderGroupFilter();
  renderStorageRootFilter();
  renderProjects();
  renderPipelines();
  renderIndexingJobs();
  renderRawDatasets();
  renderRawDatasetDetail();
  renderAutomaticArchivePolicyStatus();
  renderMicroManagerIngestStatus();
  renderArchivePolicyPreview();
  renderMigrationPlans();
  renderMigrationDetail();
  renderUsers();
  renderSessions();
  renderDetail();
}

function authHeaders(extra = {}) {
  const headers = { ...extra };
  if (state.sessionToken) {
    headers.Authorization = `Bearer ${state.sessionToken}`;
  }
  return headers;
}

function currentQuery() {
  if (!state.sessionToken && state.userKey) {
    return `user_key=${encodeURIComponent(state.userKey)}`;
  }
  return "";
}

function withIdentity(path) {
  const query = currentQuery();
  if (!query) {
    return path;
  }
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}${query}`;
}

async function apiJson(path, options = {}) {
  const response = await fetch(withIdentity(path), {
    credentials: "same-origin",
    headers: authHeaders(options.headers || {}),
    ...options,
  });
  if (!response.ok) {
    if (response.status === 401) {
      state.sessionToken = "";
      state.currentUser = null;
      state.authMode = "";
      localStorage.removeItem("detecdivHub.sessionToken");
      clearDashboardState();
      updateSessionUi();
    }
    throw new Error(await response.text());
  }
  const text = await response.text();
  return text ? JSON.parse(text) : {};
}

function apiGet(path) {
  return apiJson(path);
}

function apiPost(path, payload) {
  return apiJson(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
}

function apiPatch(path, payload) {
  return apiJson(path, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
}

function apiDelete(path) {
  return apiJson(path, {
    method: "DELETE",
  });
}

function humanBytes(value) {
  let num = Number(value || 0);
  const units = ["B", "KB", "MB", "GB", "TB"];
  let idx = 0;
  while (num >= 1024 && idx < units.length - 1) {
    num /= 1024;
    idx += 1;
  }
  return `${num.toFixed(idx === 0 ? 0 : 2)} ${units[idx]}`;
}

function formatTimestamp(value) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return `${value}`;
  }
  return date.toLocaleString();
}

function progressPercent(job) {
  const total = Number(job.total_projects || 0);
  const scanned = Number(job.scanned_projects || 0);
  if (total <= 0) {
    return 0;
  }
  return Math.max(0, Math.min(100, Math.round((100 * scanned) / total)));
}

function isAdmin() {
  return state.currentUser && ["admin", "service"].includes(state.currentUser.role);
}

function updateSessionUi() {
  if (els.loginUserKey) {
    els.loginUserKey.value = state.userKey || "";
  }
  const authenticated = Boolean(state.currentUser);
  if (els.loginPanel) {
    els.loginPanel.classList.toggle("hidden", authenticated);
  }
  if (els.sessionLabel) {
    if (!authenticated) {
      els.sessionLabel.textContent = "Not logged in.";
    } else if (state.authMode === "legacy") {
      els.sessionLabel.textContent = `Legacy identity: ${state.currentUser.display_name} (${state.currentUser.user_key})`;
    } else {
      els.sessionLabel.textContent = `Session: ${state.currentUser.display_name} (${state.currentUser.user_key})`;
    }
  }
  if (els.userLabel) {
    els.userLabel.textContent = authenticated
      ? `User: ${state.currentUser.display_name} (${state.currentUser.user_key})`
      : "User: not connected";
  }
  if (els.logoutButton) {
    els.logoutButton.disabled = !authenticated;
  }
}

function setSummary(summary) {
  state.summary = summary;
  if (els.summaryTotalProjects) els.summaryTotalProjects.textContent = summary.total_projects;
  if (els.summaryOwnedProjects) els.summaryOwnedProjects.textContent = summary.owned_projects;
  if (els.summaryTotalBytes) els.summaryTotalBytes.textContent = humanBytes(summary.total_bytes);
  if (els.summaryGroupCount) els.summaryGroupCount.textContent = summary.group_count;
  state.currentUser = summary.user || null;
  updateSessionUi();
}

function renderGroupFilter() {
  if (!els.groupFilter) {
    return;
  }
  const currentValue = els.groupFilter.value;
  els.groupFilter.innerHTML = `<option value="">All projects</option>`;
  for (const group of state.groups) {
    const option = document.createElement("option");
    option.value = group.id;
    option.textContent = group.display_name;
    els.groupFilter.appendChild(option);
  }
  els.groupFilter.value = state.groups.some((group) => group.id === currentValue) ? currentValue : "";
}

function renderStorageRootFilter() {
  if (!els.storageRootFilter) {
    return;
  }
  const currentValue = els.storageRootFilter.value;
  els.storageRootFilter.innerHTML = `<option value="">All roots</option>`;
  for (const root of state.storageRoots) {
    const option = document.createElement("option");
    option.value = root.name;
    option.textContent = root.name;
    els.storageRootFilter.appendChild(option);
  }
  els.storageRootFilter.value = state.storageRoots.some((root) => root.name === currentValue) ? currentValue : "";
}

function renderProjects() {
  if (!els.projectsTableBody || !els.projectCountLabel) {
    return;
  }
  els.projectsTableBody.innerHTML = "";
  els.projectCountLabel.textContent = `${state.projects.length} visible projects`;
  for (const project of state.projects) {
    const tr = document.createElement("tr");
    if (state.selectedProject && state.selectedProject.id === project.id) {
      tr.classList.add("selected");
    }
    tr.innerHTML = `
      <td>${project.project_name}</td>
      <td>${project.owner ? project.owner.user_key : ""}</td>
      <td>${project.visibility}</td>
      <td>${project.health_status}</td>
      <td>${project.pipeline_run_count}</td>
      <td>${project.h5_count}</td>
      <td>${humanBytes(project.total_bytes)}</td>
    `;
    tr.addEventListener("click", () => selectProject(project.id));
    els.projectsTableBody.appendChild(tr);
  }
}

function pipelineRows() {
  const sourceFilter = els.pipelineSourceFilter ? els.pipelineSourceFilter.value : "";
  if (sourceFilter === "registry") {
    return state.pipelines;
  }
  if (sourceFilter === "observed") {
    return state.observedPipelines;
  }
  return [...state.pipelines, ...state.observedPipelines];
}

function renderPipelines() {
  if (!els.pipelinesTableBody) {
    return;
  }
  els.pipelinesTableBody.innerHTML = "";
  for (const pipeline of pipelineRows()) {
    const tr = document.createElement("tr");
    const source = pipeline.source || "registry";
    const version = pipeline.version || "observed";
    const updated = pipeline.updated_at || pipeline.latest_run_at || pipeline.created_at;
    const projectCount = pipeline.project_count || 0;
    tr.innerHTML = `
      <td>${pipeline.display_name}</td>
      <td>${source}</td>
      <td>${pipeline.pipeline_key || ""}</td>
      <td>${version}</td>
      <td>${pipeline.runtime_kind}</td>
      <td>${projectCount}</td>
      <td>${formatTimestamp(updated)}</td>
    `;
    tr.title = JSON.stringify(pipeline.metadata_json || {});
    if (source === "registry") {
      tr.addEventListener("click", () => editPipeline(pipeline));
    }
    els.pipelinesTableBody.appendChild(tr);
  }
}

function renderIndexingJobs() {
  if (!els.indexJobsTableBody || !els.activeIndexJob) {
    return;
  }
  els.indexJobsTableBody.innerHTML = "";
  const activeJob = state.indexingJobs.find((job) => job.status === "queued" || job.status === "running");
  const latestJob = activeJob || state.indexingJobs[0] || null;

  if (!latestJob) {
    els.activeIndexJob.className = "job-highlight empty-state";
    els.activeIndexJob.textContent = "No indexing jobs yet.";
  } else {
    const pct = progressPercent(latestJob);
    const statusClass = latestJob.status === "running" || latestJob.status === "queued"
      ? "running"
      : latestJob.status.startsWith("completed")
        ? "completed"
        : "failed";
    els.activeIndexJob.className = `job-highlight ${statusClass}`;
    els.activeIndexJob.innerHTML = `
      <div class="job-title">${latestJob.status} | ${latestJob.phase || "queued"} | ${latestJob.source_path}</div>
      <div>${latestJob.message || "No status message."}</div>
      <div class="stack-item-meta">
        mat seen ${latestJob.mat_files_seen || 0} | candidates ${latestJob.total_projects} | scanned ${latestJob.scanned_projects} | indexed ${latestJob.indexed_projects} | failed ${latestJob.failed_projects} | deleted ${latestJob.deleted_projects}
      </div>
      <div class="stack-item-meta">heartbeat ${formatTimestamp(latestJob.heartbeat_at)} | updated ${formatTimestamp(latestJob.updated_at)}</div>
      <div class="progress-bar"><div class="progress-fill" style="width: ${pct}%"></div></div>
    `;
  }

  for (const job of state.indexingJobs) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${job.status}</td>
      <td>${job.phase || ""}</td>
      <td>${job.source_path}</td>
      <td>${job.mat_files_seen || 0}</td>
      <td>${job.scanned_projects}/${job.total_projects} (${progressPercent(job)}%)</td>
      <td>${job.indexed_projects}</td>
      <td>${job.failed_projects}</td>
      <td>${formatTimestamp(job.heartbeat_at)}</td>
      <td>${formatTimestamp(job.updated_at || job.created_at)}</td>
    `;
    tr.title = job.message || "";
    els.indexJobsTableBody.appendChild(tr);
  }
}

function renderRawDatasets() {
  if (!els.rawDatasetsTableBody || !els.rawCountLabel) {
    return;
  }
  els.rawDatasetsTableBody.innerHTML = "";
  els.rawCountLabel.textContent = `${state.rawDatasets.length} visible raw datasets`;
  for (const raw of state.rawDatasets) {
    const tr = document.createElement("tr");
    if (state.selectedRawDataset && state.selectedRawDataset.id === raw.id) {
      tr.classList.add("selected");
    }
    tr.innerHTML = `
      <td>${raw.acquisition_label}</td>
      <td>${raw.owner ? raw.owner.user_key : ""}</td>
      <td>${raw.lifecycle_tier}</td>
      <td>${raw.archive_status}</td>
      <td>${raw.status}</td>
      <td>${humanBytes(raw.total_bytes)}</td>
    `;
    tr.addEventListener("click", () => selectRawDataset(raw.id));
    els.rawDatasetsTableBody.appendChild(tr);
  }
}

function renderRawDatasetDetail() {
  if (!els.rawDetailEmpty || !els.rawDetailContent || !els.rawDetailSubtitle) {
    return;
  }
  if (!state.selectedRawDatasetDetail) {
    els.rawDetailEmpty.classList.remove("hidden");
    els.rawDetailContent.classList.add("hidden");
    els.rawDetailSubtitle.textContent = "Select a raw dataset";
    if (els.rawPreviewArchiveButton) els.rawPreviewArchiveButton.disabled = true;
    if (els.rawArchiveButton) els.rawArchiveButton.disabled = true;
    if (els.rawRestoreButton) els.rawRestoreButton.disabled = true;
    if (els.rawLifecycleEvents) {
      els.rawLifecycleEvents.innerHTML = "";
    }
    return;
  }

  const raw = state.selectedRawDatasetDetail;
  const owner = raw.owner ? `${raw.owner.display_name} (${raw.owner.user_key})` : "unknown";
  const fields = [
    ["Acquisition", raw.acquisition_label],
    ["Owner", owner],
    ["Visibility", raw.visibility],
    ["Status", raw.status],
    ["Completeness", raw.completeness_status],
    ["Tier", raw.lifecycle_tier],
    ["Archive status", raw.archive_status],
    ["Archive URI", raw.archive_uri || ""],
    ["Compression", raw.archive_compression || ""],
    ["Reclaimable", humanBytes(raw.reclaimable_bytes)],
    ["Total size", humanBytes(raw.total_bytes)],
    ["Experiments", (raw.experiment_ids || []).join(", ") || "none"],
    ["Analysis projects", (raw.analysis_project_ids || []).join(", ") || "none"],
    ["Last accessed", formatTimestamp(raw.last_accessed_at)],
  ];

  els.rawDetailList.innerHTML = "";
  for (const [label, value] of fields) {
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.textContent = `${value ?? ""}`;
    els.rawDetailList.append(dt, dd);
  }

  renderRawLifecycleEvents(raw.lifecycle_events || []);
  if (els.rawPreviewArchiveButton) els.rawPreviewArchiveButton.disabled = false;
  if (els.rawArchiveButton) els.rawArchiveButton.disabled = false;
  if (els.rawRestoreButton) els.rawRestoreButton.disabled = false;
  els.rawDetailSubtitle.textContent = raw.acquisition_label;
  els.rawDetailEmpty.classList.add("hidden");
  els.rawDetailContent.classList.remove("hidden");
}

function renderAutomaticArchivePolicyStatus() {
  if (!els.automaticArchivePolicyPanel || !els.automaticArchivePolicySummary) {
    return;
  }
  const canSee = isAdmin();
  els.automaticArchivePolicyPanel.classList.toggle("hidden", !canSee);
  if (!canSee) {
    return;
  }

  const status = state.automaticArchivePolicyStatus;
  if (!status) {
    els.automaticArchivePolicySummary.textContent = "Automatic archive policy status not loaded yet.";
    if (els.automaticArchivePolicyConfig) {
      els.automaticArchivePolicyConfig.innerHTML = "";
    }
    if (els.automaticArchivePolicyRunsTableBody) {
      els.automaticArchivePolicyRunsTableBody.innerHTML = "";
    }
    return;
  }

  const config = status.config || {};
  const lastRun = status.last_run;
  const summaryBits = [
    config.enabled ? "enabled" : "disabled",
    `every ${config.interval_minutes || 0} min`,
    `older than ${config.older_than_days || 0} day(s)`,
    `min ${humanBytes(config.min_total_bytes || 0)}`,
  ];
  if (lastRun) {
    summaryBits.push(
      `last run ${formatTimestamp(lastRun.finished_at || lastRun.started_at || lastRun.created_at)}`,
      `${lastRun.candidate_count} candidate(s)`,
      `${lastRun.queued_count} queued`
    );
  } else {
    summaryBits.push("no runs yet");
  }
  els.automaticArchivePolicySummary.textContent = summaryBits.join(" | ");

  if (els.automaticArchivePolicyConfig) {
    const fields = [
      ["Enabled", config.enabled ? "yes" : "no"],
      ["Interval", `${config.interval_minutes || 0} min`],
      ["Run as", config.run_as_user_key || ""],
      ["Older than", `${config.older_than_days || 0} day(s)`],
      ["Min size", humanBytes(config.min_total_bytes || 0)],
      ["Candidate limit", `${config.limit || 0}`],
      ["Owner filter", config.owner_key || ""],
      ["Search filter", config.search || ""],
      ["Lifecycle tiers", (config.lifecycle_tiers || []).join(", ")],
      ["Archive states", (config.archive_statuses || []).join(", ")],
      ["Archive URI", config.archive_uri || ""],
      ["Compression", config.archive_compression || ""],
      ["Delete hot source", config.delete_hot_source ? "yes" : "no"],
    ];
    els.automaticArchivePolicyConfig.innerHTML = "";
    for (const [label, value] of fields) {
      const dt = document.createElement("dt");
      dt.textContent = label;
      const dd = document.createElement("dd");
      dd.textContent = value;
      els.automaticArchivePolicyConfig.append(dt, dd);
    }
  }

  if (els.automaticArchivePolicyRunsTableBody) {
    els.automaticArchivePolicyRunsTableBody.innerHTML = "";
    for (const run of status.recent_runs || []) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${formatTimestamp(run.finished_at || run.started_at || run.created_at)}</td>
        <td>${run.trigger_mode}</td>
        <td>${run.status}</td>
        <td>${run.report_only ? "yes" : "no"}</td>
        <td>${run.candidate_count}</td>
        <td>${run.queued_count}</td>
        <td>${humanBytes(run.total_reclaimable_bytes)}</td>
        <td>${run.triggered_by ? run.triggered_by.user_key : ""}</td>
      `;
      tr.title = run.error_text || JSON.stringify(run.result_json || {});
      els.automaticArchivePolicyRunsTableBody.appendChild(tr);
    }
  }
}

function renderMicroManagerIngestStatus() {
  if (!els.micromanagerIngestPanel || !els.micromanagerIngestSummary) {
    return;
  }
  const canSee = isAdmin();
  els.micromanagerIngestPanel.classList.toggle("hidden", !canSee);
  if (!canSee) {
    return;
  }

  const status = state.micromanagerIngestStatus;
  if (!status) {
    els.micromanagerIngestSummary.textContent = "Micro-Manager ingestion status not loaded yet.";
    if (els.micromanagerIngestConfig) {
      els.micromanagerIngestConfig.innerHTML = "";
    }
    if (els.micromanagerIngestRunsTableBody) {
      els.micromanagerIngestRunsTableBody.innerHTML = "";
    }
    return;
  }

  const config = status.config || {};
  const lastRun = status.last_run;
  const summaryBits = [
    config.enabled ? "enabled" : "disabled",
    `every ${config.interval_minutes || 0} min`,
    `settle ${config.settle_seconds || 0}s`,
    `limit ${config.max_datasets || 0}`,
  ];
  if (lastRun) {
    summaryBits.push(
      `last run ${formatTimestamp(lastRun.finished_at || lastRun.started_at || lastRun.created_at)}`,
      `${lastRun.candidate_count} candidate(s)`,
      `${lastRun.ingested_count} ingested`
    );
  } else {
    summaryBits.push("no runs yet");
  }
  els.micromanagerIngestSummary.textContent = summaryBits.join(" | ");

  if (els.micromanagerIngestConfig) {
    const fields = [
      ["Enabled", config.enabled ? "yes" : "no"],
      ["Interval", `${config.interval_minutes || 0} min`],
      ["Run as", config.run_as_user_key || ""],
      ["Landing root", config.landing_root || ""],
      ["Storage root name", config.storage_root_name || ""],
      ["Host scope", config.host_scope || ""],
      ["Visibility", config.visibility || ""],
      ["Settle time", `${config.settle_seconds || 0} s`],
      ["Max datasets", `${config.max_datasets || 0}`],
      ["Grouping window", `${config.grouping_window_hours || 0} h`],
      ["Post-ingest pipeline", config.post_ingest_pipeline_key || ""],
      ["Post-ingest mode", config.post_ingest_requested_mode || ""],
    ];
    els.micromanagerIngestConfig.innerHTML = "";
    for (const [label, value] of fields) {
      const dt = document.createElement("dt");
      dt.textContent = label;
      const dd = document.createElement("dd");
      dd.textContent = value;
      els.micromanagerIngestConfig.append(dt, dd);
    }
  }

  if (els.micromanagerIngestRunsTableBody) {
    els.micromanagerIngestRunsTableBody.innerHTML = "";
    for (const run of status.recent_runs || []) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${formatTimestamp(run.finished_at || run.started_at || run.created_at)}</td>
        <td>${run.trigger_mode}</td>
        <td>${run.status}</td>
        <td>${run.report_only ? "yes" : "no"}</td>
        <td>${run.candidate_count}</td>
        <td>${run.ingested_count}</td>
        <td>${run.experiment_count}</td>
        <td>${run.triggered_by ? run.triggered_by.user_key : ""}</td>
      `;
      tr.title = run.error_text || JSON.stringify(run.result_json || {});
      els.micromanagerIngestRunsTableBody.appendChild(tr);
    }
  }
}

function renderArchivePolicyPreview() {
  if (!els.archivePolicySummary || !els.archivePolicyTableBody) {
    return;
  }
  els.archivePolicyTableBody.innerHTML = "";
  const preview = state.archivePolicyPreview;
  if (!preview) {
    els.archivePolicySummary.textContent = "No policy preview yet.";
    return;
  }

  els.archivePolicySummary.textContent =
    `${preview.candidate_count} candidate(s), ${humanBytes(preview.total_candidate_bytes)} total, ` +
    `${humanBytes(preview.total_reclaimable_bytes)} reclaimable, ${preview.skipped_conflicts} skipped conflicts.`;

  for (const candidate of preview.candidates || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${candidate.acquisition_label}</td>
      <td>${candidate.owner ? candidate.owner.user_key : ""}</td>
      <td>${candidate.lifecycle_tier}</td>
      <td>${formatTimestamp(candidate.last_activity_at)}</td>
      <td>${humanBytes(candidate.total_bytes)}</td>
      <td>${humanBytes(candidate.reclaimable_bytes)}</td>
      <td>${candidate.suggested_archive_uri || ""}</td>
    `;
    tr.addEventListener("click", () => selectRawDataset(candidate.raw_dataset_id));
    els.archivePolicyTableBody.appendChild(tr);
  }
}

function renderRawLifecycleEvents(events) {
  if (!els.rawLifecycleEvents) {
    return;
  }
  els.rawLifecycleEvents.innerHTML = "";
  if (!events.length) {
    els.rawLifecycleEvents.innerHTML = `<div class="stack-item">No lifecycle events.</div>`;
    return;
  }
  for (const event of events) {
    const div = document.createElement("div");
    div.className = "stack-item";
    div.innerHTML = `
      <div class="stack-item-meta">${event.event_kind} | ${formatTimestamp(event.created_at)}</div>
      <div>${event.from_tier || ""} -> ${event.to_tier || ""} | archive ${event.archive_status || ""}</div>
      <div class="stack-item-meta">reclaimable ${humanBytes(event.reclaimable_bytes)}</div>
    `;
    els.rawLifecycleEvents.appendChild(div);
  }
}

function renderMigrationPlans() {
  if (!els.migrationPlansTableBody || !els.migrationPlanCountLabel) {
    return;
  }
  els.migrationPlansTableBody.innerHTML = "";
  els.migrationPlanCountLabel.textContent = `${state.migrationPlans.length} plans`;
  for (const plan of state.migrationPlans) {
    const tr = document.createElement("tr");
    if (state.selectedMigrationPlan && state.selectedMigrationPlan.id === plan.id) {
      tr.classList.add("selected");
    }
    tr.innerHTML = `
      <td>${plan.batch_name}</td>
      <td>${plan.source_kind}</td>
      <td>${plan.strategy}</td>
      <td>${plan.status}</td>
      <td>${plan.summary_json?.candidate_count || 0}</td>
      <td>${formatTimestamp(plan.created_at)}</td>
    `;
    tr.title = plan.source_path;
    tr.addEventListener("click", () => selectMigrationPlan(plan.id));
    els.migrationPlansTableBody.appendChild(tr);
  }
}

function renderMigrationDetail() {
  if (!els.migrationDetailEmpty || !els.migrationDetailContent || !els.migrationDetailSubtitle) {
    return;
  }
  if (!state.selectedMigrationPlan) {
    els.migrationDetailEmpty.classList.remove("hidden");
    els.migrationDetailContent.classList.add("hidden");
    els.migrationDetailSubtitle.textContent = "Select a migration plan";
    if (els.migrationExecutePilotButton) {
      els.migrationExecutePilotButton.disabled = true;
    }
    if (els.migrationItemsTableBody) {
      els.migrationItemsTableBody.innerHTML = "";
    }
    return;
  }

  const plan = state.selectedMigrationPlan;
  const fields = [
    ["Batch", plan.batch_name],
    ["Source kind", plan.source_kind],
    ["Strategy", plan.strategy],
    ["Status", plan.status],
    ["Source path", plan.source_path],
    ["Storage root", plan.storage_root_name || ""],
    ["Candidates", plan.summary_json?.candidate_count || 0],
    ["Type counts", JSON.stringify(plan.summary_json?.item_type_counts || {})],
    ["Action counts", JSON.stringify(plan.summary_json?.action_counts || {})],
    ["Created", formatTimestamp(plan.created_at)],
  ];

  els.migrationDetailList.innerHTML = "";
  for (const [label, value] of fields) {
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.textContent = `${value ?? ""}`;
    els.migrationDetailList.append(dt, dd);
  }

  renderMigrationItems(plan.items || []);
  if (els.migrationExecutePilotButton) {
    els.migrationExecutePilotButton.disabled = false;
  }
  els.migrationDetailSubtitle.textContent = plan.batch_name;
  els.migrationDetailEmpty.classList.add("hidden");
  els.migrationDetailContent.classList.remove("hidden");
}

function renderMigrationItems(items) {
  if (!els.migrationItemsTableBody) {
    return;
  }
  els.migrationItemsTableBody.innerHTML = "";
  for (const item of items) {
    const tr = document.createElement("tr");
    const keyLabel = item.proposed_experiment_key || "";
    tr.innerHTML = `
      <td title="${item.legacy_path}">${item.display_name}</td>
      <td>${item.item_type}</td>
      <td>${item.action}</td>
      <td>${item.status}</td>
      <td>${keyLabel}</td>
      <td></td>
    `;
    const actionCell = tr.lastElementChild;
    const controls = document.createElement("div");
    controls.className = "inline-action-row";

    const pilotButton = document.createElement("button");
    pilotButton.textContent = "Pilot";
    pilotButton.addEventListener("click", (event) => {
      event.stopPropagation();
      markMigrationItem(item, { action: "review_for_pilot", status: "planned" }).catch((error) => setStatus(String(error)));
    });
    controls.appendChild(pilotButton);

    const skipButton = document.createElement("button");
    skipButton.textContent = "Skip";
    skipButton.addEventListener("click", (event) => {
      event.stopPropagation();
      markMigrationItem(item, { action: "skip", status: "skipped" }).catch((error) => setStatus(String(error)));
    });
    controls.appendChild(skipButton);

    const attachButton = document.createElement("button");
    attachButton.textContent = "Attach existing";
    attachButton.addEventListener("click", (event) => {
      event.stopPropagation();
      attachMigrationItemToExisting(item).catch((error) => setStatus(String(error)));
    });
    controls.appendChild(attachButton);

    const materializeButton = document.createElement("button");
    materializeButton.textContent = "Create placeholder";
    materializeButton.disabled = item.status === "materialized";
    materializeButton.addEventListener("click", (event) => {
      event.stopPropagation();
      materializeMigrationItem(item).catch((error) => setStatus(String(error)));
    });
    controls.appendChild(materializeButton);

    actionCell.appendChild(controls);
    els.migrationItemsTableBody.appendChild(tr);
  }
}

function renderNotes() {
  if (!els.notesList) {
    return;
  }
  els.notesList.innerHTML = "";
  if (!state.notes.length) {
    els.notesList.innerHTML = `<div class="stack-item">No notes.</div>`;
    return;
  }
  for (const note of state.notes) {
    const div = document.createElement("div");
    div.className = "stack-item";
    div.innerHTML = `
      <div class="stack-item-meta">${note.author ? note.author.user_key : "unknown"} | ${formatTimestamp(note.updated_at || note.created_at)}${note.is_pinned ? " | pinned" : ""}</div>
      <div>${note.note_text}</div>
    `;
    els.notesList.appendChild(div);
  }
}

function renderAcl() {
  if (!els.aclList) {
    return;
  }
  els.aclList.innerHTML = "";
  if (!state.acl.length) {
    els.aclList.innerHTML = `<div class="stack-item">Owner only.</div>`;
    return;
  }
  for (const item of state.acl) {
    const div = document.createElement("div");
    div.className = "stack-item";
    div.innerHTML = `
      <div class="stack-item-meta">${item.user?.display_name || item.user?.user_key || "unknown"}</div>
      <div>${item.access_level}</div>
    `;
    els.aclList.appendChild(div);
  }
}

function renderDetail() {
  if (!els.detailEmpty || !els.detailContent || !els.detailSubtitle) {
    return;
  }
  if (!state.selectedProjectDetail) {
    els.detailEmpty.classList.remove("hidden");
    els.detailContent.classList.add("hidden");
    els.detailSubtitle.textContent = "Select a project";
    return;
  }

  const project = state.selectedProjectDetail;
  const owner = project.owner ? `${project.owner.display_name} (${project.owner.user_key})` : "unknown";
  const inventory = project.metadata_json?.inventory || {};
  const groups = state.groups
    .filter((group) => group.project_ids?.includes?.(project.id))
    .map((group) => group.display_name);

  const fields = [
    ["Project", project.project_name],
    ["Owner", owner],
    ["Visibility", project.visibility],
    ["Health", project.health_status],
    ["Status", project.status],
    ["FOV", project.fov_count],
    ["ROI", project.roi_count],
    ["Classifiers", project.classifier_count],
    ["Processors", project.processor_count],
    ["Pipeline runs", project.pipeline_run_count],
    ["Run JSON", project.run_json_count],
    ["H5 footprint", `${project.h5_count} files / ${humanBytes(project.h5_bytes)}`],
    ["Project size", humanBytes(project.total_bytes)],
    ["Latest run", project.latest_run_status || "none"],
    ["Groups", groups.length ? groups.join(", ") : "none"],
    ["Project MAT", project.metadata_json?.project_mat_abs || ""],
    ["Project dir", project.metadata_json?.project_dir_abs || ""],
    ["Top-level", (inventory.top_level_entries || []).join(", ")],
  ];

  els.detailList.innerHTML = "";
  for (const [label, value] of fields) {
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.textContent = `${value ?? ""}`;
    els.detailList.append(dt, dd);
  }

  renderNotes();
  renderAcl();
  els.detailSubtitle.textContent = project.project_name;
  els.detailEmpty.classList.add("hidden");
  els.detailContent.classList.remove("hidden");
}

function renderUsers() {
  if (!els.usersTableBody) {
    return;
  }
  els.usersTableBody.innerHTML = "";
  const users = state.users.length ? state.users : (state.currentUser ? [state.currentUser] : []);
  for (const user of users) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${user.user_key}</td>
      <td>${user.display_name}</td>
      <td>${user.role}</td>
      <td>${user.is_active ? "yes" : "no"}</td>
    `;
    tr.addEventListener("click", () => editUser(user));
    els.usersTableBody.appendChild(tr);
  }
}

function renderSessions() {
  if (!els.sessionsTableBody) {
    return;
  }
  els.sessionsTableBody.innerHTML = "";
  for (const session of state.sessions) {
    const tr = document.createElement("tr");
    const tdUser = document.createElement("td");
    tdUser.textContent = session.user ? session.user.user_key : "";
    const tdClient = document.createElement("td");
    tdClient.textContent = session.client_label || "";
    const tdLastSeen = document.createElement("td");
    tdLastSeen.textContent = formatTimestamp(session.last_seen_at);
    const tdExpires = document.createElement("td");
    tdExpires.textContent = formatTimestamp(session.expires_at);
    const tdAction = document.createElement("td");
    const button = document.createElement("button");
    button.textContent = "Revoke";
    button.disabled = !isAdmin() && session.user?.id !== state.currentUser?.id;
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      revokeSession(session.id).catch((error) => setStatus(String(error)));
    });
    tdAction.appendChild(button);
    tr.append(tdUser, tdClient, tdLastSeen, tdExpires, tdAction);
    els.sessionsTableBody.appendChild(tr);
  }
}

async function login() {
  const userKey = (els.loginUserKey?.value || "").trim();
  const password = els.loginPassword?.value || "";
  if (!userKey || !password) {
    throw new Error("User key and password are required.");
  }
  const response = await apiPost("/auth/login", {
    user_key: userKey,
    password,
    client_label: "web-ui",
  });
  state.userKey = userKey;
  state.sessionToken = response.session_token;
  state.currentUser = response.user;
  state.authMode = "session";
  localStorage.setItem("detecdivHub.userKey", state.userKey);
  localStorage.setItem("detecdivHub.sessionToken", state.sessionToken);
  if (els.loginPassword) {
    els.loginPassword.value = "";
  }
  updateSessionUi();
  await refreshDashboard();
  setStatus(`Logged in as ${response.user.user_key}.`);
}

async function logout() {
  try {
    if (state.currentUser) {
      await apiPost("/auth/logout", {});
    }
  } catch (error) {
    setStatus(String(error));
  }
  state.sessionToken = "";
  state.currentUser = null;
  state.authMode = "";
  localStorage.removeItem("detecdivHub.sessionToken");
  clearDashboardState();
  updateSessionUi();
  setStatus("Logged out.");
}

async function restoreSession() {
  try {
    const session = await apiGet("/auth/legacy-session");
    if (session.authenticated) {
      state.currentUser = session.user;
      state.authMode = session.auth_mode || "session";
      state.userKey = session.user?.user_key || state.userKey;
      updateSessionUi();
      await refreshDashboard();
      return;
    }
  } catch (error) {
    setStatus(String(error));
  }

  if (state.userKey) {
    try {
      const session = await apiGet("/auth/session");
      if (session.authenticated) {
        state.currentUser = session.user;
        state.authMode = "legacy";
        updateSessionUi();
        await refreshDashboard();
        setStatus(`Connected in legacy mode as ${session.user.user_key}.`);
        return;
      }
    } catch {
      // Ignore legacy fallback errors and show login panel.
    }
  }

  state.currentUser = null;
  clearDashboardState();
  updateSessionUi();
  setStatus("Login required.");
}

async function refreshDashboard() {
  if (!state.currentUser && !state.userKey) {
    updateSessionUi();
    return;
  }

  setStatus("Refreshing dashboard...");
  localStorage.setItem("detecdivHub.userKey", state.userKey || "");

  const bootstrapTasks = [apiGet("/dashboard/summary")];
  if (pageFlags.hasProjectGroups || pageFlags.hasProjectDetail) {
    bootstrapTasks.push(apiGet("/project-groups"));
  } else {
    bootstrapTasks.push(Promise.resolve([]));
  }
  if (pageFlags.hasStorageRootFilter || pageFlags.hasIndexForm) {
    bootstrapTasks.push(apiGet("/storage-roots"));
  } else {
    bootstrapTasks.push(Promise.resolve([]));
  }

  const [summary, groups, storageRoots] = await Promise.all(bootstrapTasks);

  state.groups = groups.map((group) => ({ ...group, project_ids: [] }));
  state.storageRoots = storageRoots;
  if (pageFlags.hasProjectDetail && state.groups.length) {
    const groupDetails = await Promise.all(
      state.groups.map((group) => apiGet(`/project-groups/${group.id}`))
    );
    for (let index = 0; index < state.groups.length; index += 1) {
      state.groups[index].project_ids = (groupDetails[index].projects || []).map((project) => project.id);
    }
  }

  setSummary(summary);
  renderGroupFilter();
  renderStorageRootFilter();

  const refreshTasks = [];
  if (pageFlags.hasProjectsView) {
    refreshTasks.push(refreshProjects());
  }
  if (pageFlags.hasRawDatasetsView) {
    refreshTasks.push(refreshRawDatasets());
  }
  if (pageFlags.hasAutomaticArchivePolicy) {
    refreshTasks.push(refreshAutomaticArchivePolicyStatus());
  }
  if (pageFlags.hasMicroManagerIngest) {
    refreshTasks.push(refreshMicroManagerIngestStatus());
  }
  if (pageFlags.hasPipelinesView) {
    refreshTasks.push(refreshPipelines());
  }
  if (pageFlags.hasUsersView) {
    refreshTasks.push(refreshUsers());
  }
  if (pageFlags.hasSessionsView) {
    refreshTasks.push(refreshSessions());
  }
  if (pageFlags.hasIndexingView) {
    refreshTasks.push(refreshIndexingJobs());
  }
  if (pageFlags.hasMigrationView) {
    refreshTasks.push(refreshMigrationPlans());
  }
  await Promise.all(refreshTasks);

  setStatus(`Connected as ${summary.user.user_key}.`);
}

async function refreshIndexingJobs() {
  if (!els.indexJobsTableBody && !els.activeIndexJob) {
    return;
  }
  state.indexingJobs = await apiGet("/indexing/jobs?limit=25");
  renderIndexingJobs();
}

async function refreshRawDatasets() {
  if (!els.rawDatasetsTableBody) {
    return;
  }
  const params = new URLSearchParams();
  if (els.rawOwnedOnly?.checked) params.set("owned_only", "true");
  if (els.rawSearch?.value.trim()) params.set("search", els.rawSearch.value.trim());
  if (els.rawOwnerFilter?.value.trim()) params.set("owner_key", els.rawOwnerFilter.value.trim());
  if (els.rawTierFilter?.value) params.set("lifecycle_tier", els.rawTierFilter.value);
  if (els.rawArchiveStatusFilter?.value) params.set("archive_status", els.rawArchiveStatusFilter.value);
  if (els.rawLimit?.value) params.set("limit", els.rawLimit.value);

  state.rawDatasets = await apiGet(`/raw-datasets${params.toString() ? `?${params.toString()}` : ""}`);
  renderRawDatasets();
  if (state.selectedRawDataset) {
    const stillExists = state.rawDatasets.find((raw) => raw.id === state.selectedRawDataset.id);
    if (stillExists) {
      await selectRawDataset(stillExists.id);
    } else {
      state.selectedRawDataset = null;
      state.selectedRawDatasetDetail = null;
      renderRawDatasetDetail();
    }
  }
}

async function refreshAutomaticArchivePolicyStatus() {
  if (!pageFlags.hasAutomaticArchivePolicy) {
    return;
  }
  if (!isAdmin()) {
    state.automaticArchivePolicyStatus = null;
    renderAutomaticArchivePolicyStatus();
    return;
  }
  try {
    state.automaticArchivePolicyStatus = await apiGet("/raw-datasets/archive-policy/automatic");
    renderAutomaticArchivePolicyStatus();
  } catch (error) {
    if (`${error}`.includes("403")) {
      state.automaticArchivePolicyStatus = null;
      renderAutomaticArchivePolicyStatus();
      return;
    }
    throw error;
  }
}

async function refreshMicroManagerIngestStatus() {
  if (!pageFlags.hasMicroManagerIngest) {
    return;
  }
  if (!isAdmin()) {
    state.micromanagerIngestStatus = null;
    renderMicroManagerIngestStatus();
    return;
  }
  try {
    state.micromanagerIngestStatus = await apiGet("/micromanager-ingest/status");
    renderMicroManagerIngestStatus();
  } catch (error) {
    if (`${error}`.includes("403")) {
      state.micromanagerIngestStatus = null;
      renderMicroManagerIngestStatus();
      return;
    }
    throw error;
  }
}

function archivePolicyPayload() {
  const olderThanDays = Number(els.archivePolicyOlderDays?.value || 30);
  const minGb = Number(els.archivePolicyMinGb?.value || 0);
  const limit = Number(els.archivePolicyLimit?.value || 25);
  return {
    older_than_days: Number.isFinite(olderThanDays) ? Math.max(0, Math.floor(olderThanDays)) : 30,
    min_total_bytes: Number.isFinite(minGb) ? Math.max(0, Math.round(minGb * 1024 * 1024 * 1024)) : 0,
    limit: Number.isFinite(limit) ? Math.max(1, Math.floor(limit)) : 25,
    owner_key: els.rawOwnerFilter?.value.trim() || null,
    search: els.rawSearch?.value.trim() || null,
    lifecycle_tiers: els.rawTierFilter?.value ? [els.rawTierFilter.value] : ["hot"],
    archive_statuses: els.rawArchiveStatusFilter?.value
      ? [els.rawArchiveStatusFilter.value]
      : ["none", "restored", "archive_failed", "restore_failed"],
    archive_uri: els.archivePolicyUri?.value.trim() || null,
    archive_compression: els.archivePolicyCompression?.value || null,
    mark_archived: Boolean(els.archivePolicyDeleteSource?.checked),
  };
}

async function runAutomaticArchivePolicy() {
  if (!isAdmin()) {
    throw new Error("Admin role required.");
  }
  const reportOnly = Boolean(els.automaticArchivePolicyReportOnly?.checked);
  const action = reportOnly ? "run a report-only preview" : "queue archive jobs";
  const ok = window.confirm(`Run the automatic archive policy now and ${action}?`);
  if (!ok) {
    return;
  }
  const run = await apiPost("/raw-datasets/archive-policy/automatic/run", {
    report_only: reportOnly,
  });
  await refreshAutomaticArchivePolicyStatus();
  if (!reportOnly) {
    await refreshRawDatasets();
  }
  setStatus(
    `Automatic archive policy run completed: ${run.candidate_count} candidate(s), ${run.queued_count} queued.`
  );
}

async function runMicroManagerIngest() {
  if (!isAdmin()) {
    throw new Error("Admin role required.");
  }
  const reportOnly = Boolean(els.micromanagerIngestReportOnly?.checked);
  const action = reportOnly ? "run a detection-only report" : "ingest datasets now";
  const ok = window.confirm(`Run Micro-Manager ingestion now and ${action}?`);
  if (!ok) {
    return;
  }
  const run = await apiPost("/micromanager-ingest/run", {
    report_only: reportOnly,
  });
  await refreshMicroManagerIngestStatus();
  if (!reportOnly) {
    await refreshRawDatasets();
  }
  setStatus(
    `Micro-Manager ingest completed: ${run.candidate_count} candidate(s), ${run.ingested_count} ingested, ${run.experiment_count} experiment(s).`
  );
}

async function previewArchivePolicy() {
  const preview = await apiPost("/raw-datasets/archive-policy/preview", archivePolicyPayload());
  state.archivePolicyPreview = preview;
  renderArchivePolicyPreview();
  setStatus(
    `Archive policy preview ready: ${preview.candidate_count} candidate(s), ${humanBytes(preview.total_reclaimable_bytes)} reclaimable.`
  );
}

async function queueArchivePolicy() {
  const payload = archivePolicyPayload();
  const preview = state.archivePolicyPreview || (await apiPost("/raw-datasets/archive-policy/preview", payload));
  if (!preview.candidate_count) {
    setStatus("No archive candidates for the current policy.");
    return;
  }
  const action = payload.mark_archived ? "archive and delete hot source" : "archive";
  const ok = window.confirm(
    `Queue ${preview.candidate_count} raw dataset(s) for ${action}?\nPotentially reclaimable: ${humanBytes(preview.total_reclaimable_bytes)}`
  );
  if (!ok) {
    return;
  }
  const result = await apiPost("/raw-datasets/archive-policy/queue", payload);
  await refreshRawDatasets();
  await previewArchivePolicy();
  setStatus(result.message);
}

async function refreshMigrationPlans() {
  if (!els.migrationPlansTableBody) {
    return;
  }
  state.migrationPlans = await apiGet("/migrations/plans?limit=50");
  renderMigrationPlans();
  if (state.selectedMigrationPlan) {
    const stillExists = state.migrationPlans.find((plan) => plan.id === state.selectedMigrationPlan.id);
    if (stillExists) {
      await selectMigrationPlan(stillExists.id);
    } else {
      state.selectedMigrationPlan = null;
      renderMigrationDetail();
    }
  }
}

async function refreshProjects() {
  if (!els.projectCountLabel) {
    return;
  }
  const params = new URLSearchParams();
  if (els.groupFilter?.value) params.set("group_id", els.groupFilter.value);
  if (els.ownedOnly?.checked) params.set("owned_only", "true");
  if (els.projectSearch?.value.trim()) params.set("search", els.projectSearch.value.trim());
  if (els.ownerFilter?.value.trim()) params.set("owner_key", els.ownerFilter.value.trim());
  if (els.storageRootFilter?.value) params.set("storage_root_name", els.storageRootFilter.value);
  if (els.projectLimit?.value) params.set("limit", els.projectLimit.value);

  state.projects = await apiGet(`/projects${params.toString() ? `?${params.toString()}` : ""}`);
  renderProjects();
  if (state.selectedProject) {
    const stillExists = state.projects.find((project) => project.id === state.selectedProject.id);
    if (stillExists) {
      await selectProject(stillExists.id);
    } else {
      state.selectedProject = null;
      state.selectedProjectDetail = null;
      state.notes = [];
      state.acl = [];
      renderDetail();
    }
  }
}

async function selectMigrationPlan(planId) {
  const detail = await apiGet(`/migrations/plans/${planId}`);
  state.selectedMigrationPlan = detail;
  renderMigrationPlans();
  renderMigrationDetail();
}

async function selectRawDataset(rawDatasetId) {
  const raw = state.rawDatasets.find((item) => item.id === rawDatasetId);
  if (!raw) {
    return;
  }
  state.selectedRawDataset = raw;
  state.selectedRawDatasetDetail = await apiGet(`/raw-datasets/${rawDatasetId}`);
  renderRawDatasets();
  renderRawDatasetDetail();
}

async function refreshPipelines() {
  if (!els.pipelinesTableBody) {
    return;
  }
  const params = new URLSearchParams();
  if (els.pipelineSearch?.value.trim()) params.set("search", els.pipelineSearch.value.trim());
  if (els.pipelineRuntimeFilter?.value) params.set("runtime_kind", els.pipelineRuntimeFilter.value);
  const [registry, observed] = await Promise.all([
    apiGet(`/pipelines${params.toString() ? `?${params.toString()}` : ""}`),
    apiGet(`/pipelines/observed${params.toString() ? `?${params.toString()}` : ""}`),
  ]);
  state.pipelines = registry.map((item) => ({ ...item, source: "registry", project_count: item.project_count || 0 }));
  state.observedPipelines = observed;
  renderPipelines();
}

async function refreshUsers() {
  if (!els.usersTableBody) {
    return;
  }
  state.users = await apiGet("/users");
  renderUsers();
}

async function refreshSessions() {
  if (!els.sessionsTableBody) {
    return;
  }
  const suffix = isAdmin() ? "?all_users=true" : "";
  state.sessions = await apiGet(`/auth/sessions${suffix}`);
  renderSessions();
}

async function selectProject(projectId) {
  const project = state.projects.find((item) => item.id === projectId);
  if (!project) {
    return;
  }
  state.selectedProject = project;
  const [detail, notes, acl] = await Promise.all([
    apiGet(`/projects/${projectId}`),
    apiGet(`/projects/${projectId}/notes`),
    apiGet(`/projects/${projectId}/acl`),
  ]);
  state.selectedProjectDetail = detail;
  state.notes = notes;
  state.acl = acl;
  renderProjects();
  renderDetail();
}

async function createGroup() {
  const displayName = window.prompt("Group display name");
  if (!displayName) {
    return;
  }
  const groupKeyInput = window.prompt("Group key", displayName.toLowerCase().replace(/[^a-z0-9]+/g, "_"));
  if (!groupKeyInput) {
    return;
  }
  const description = window.prompt("Description", "") || "";
  await apiPost("/project-groups", {
    display_name: displayName,
    group_key: groupKeyInput,
    description,
    metadata_json: {},
  });
  await refreshDashboard();
}

async function addNote() {
  if (!state.selectedProject) {
    return;
  }
  const noteText = window.prompt("New note");
  if (!noteText) {
    return;
  }
  await apiPost(`/projects/${state.selectedProject.id}/notes`, {
    note_text: noteText,
    is_pinned: false,
  });
  await selectProject(state.selectedProject.id);
  setStatus("Note added.");
}

async function editProject() {
  if (!state.selectedProjectDetail) {
    return;
  }
  const currentOwner = state.selectedProjectDetail.owner?.user_key || "";
  const currentVisibility = state.selectedProjectDetail.visibility || "private";
  const ownerUserKey = window.prompt("Owner user key", currentOwner);
  if (ownerUserKey === null) {
    return;
  }
  const visibility = window.prompt("Visibility (private/shared/public)", currentVisibility);
  if (!visibility) {
    return;
  }
  await apiPatch(`/projects/${state.selectedProjectDetail.id}`, {
    owner_user_key: ownerUserKey.trim() || null,
    visibility,
    metadata_json: {},
  });
  await refreshDashboard();
  await selectProject(state.selectedProjectDetail.id);
  setStatus("Project updated.");
}

async function shareProject() {
  if (!state.selectedProject) {
    return;
  }
  const userKey = window.prompt("Share with user key");
  if (!userKey) {
    return;
  }
  const accessLevel = window.prompt("Access level (viewer/editor)", "viewer") || "viewer";
  await apiPost(`/projects/${state.selectedProject.id}/acl`, {
    user_key: userKey,
    access_level: accessLevel,
  });
  await selectProject(state.selectedProject.id);
  setStatus(`Shared with ${userKey}.`);
}

async function addSelectedProjectToGroup() {
  if (!state.selectedProject) {
    return;
  }
  if (!state.groups.length) {
    await createGroup();
    return;
  }
  const labels = state.groups.map((group, index) => `${index + 1}. ${group.display_name}`).join("\n");
  const answer = window.prompt(`Choose group number:\n${labels}`, "1");
  if (!answer) {
    return;
  }
  const selected = state.groups[Number(answer) - 1];
  if (!selected) {
    throw new Error("Invalid group selection.");
  }
  await apiPost(`/project-groups/${selected.id}/projects/${state.selectedProject.id}`, {});
  await refreshDashboard();
  await selectProject(state.selectedProject.id);
  setStatus(`Added to ${selected.display_name}.`);
}

async function previewDelete() {
  if (!state.selectedProject) {
    return;
  }
  const deleteFiles = window.confirm("Also delete project files on disk?");
  const preview = await apiPost(`/projects/${state.selectedProject.id}/deletion-preview`, {
    delete_project_files: deleteFiles,
    delete_linked_raw_data: false,
    confirm: false,
  });
  const ok = window.confirm(
    `Project: ${preview.project_name}\nRecoverable: ${humanBytes(preview.reclaimable_bytes)}\nDelete now?`
  );
  if (!ok) {
    return;
  }
  await apiDelete(
    `/projects/${state.selectedProject.id}?delete_project_files=${deleteFiles ? "true" : "false"}&delete_linked_raw_data=false&confirm=true`
  );
  state.selectedProject = null;
  state.selectedProjectDetail = null;
  await refreshDashboard();
  setStatus("Project deleted.");
}

async function runIndexing() {
  if (!els.indexSourcePath) {
    return;
  }
  const sourcePath = els.indexSourcePath.value.trim();
  if (!sourcePath) {
    throw new Error("Source path is required.");
  }
  const response = await apiPost("/indexing/jobs", {
    source_kind: "project_root",
    source_path: sourcePath,
    storage_root_name: els.indexStorageRootName?.value.trim() || null,
    host_scope: "server",
    root_type: "project_root",
    visibility: els.indexVisibility?.value || "private",
    clear_existing_for_root: Boolean(els.indexClearExisting?.checked),
    metadata_json: {},
  });
  await refreshIndexingJobs();
  setStatus(`Queued indexing job ${response.job.id} for ${response.job.source_path}.`);
}

async function previewRawArchive() {
  if (!state.selectedRawDataset) {
    return;
  }
  const preview = await apiPost(`/raw-datasets/${state.selectedRawDataset.id}/archive-preview`, {});
  window.alert(
    `Dataset: ${preview.acquisition_label}\nCurrent tier: ${preview.current_tier}\nTarget tier: ${preview.target_tier}\nReclaimable: ${humanBytes(preview.reclaimable_bytes)}`
  );
  setStatus(`Archive preview ready for ${preview.acquisition_label}.`);
}

async function requestRawArchive() {
  if (!state.selectedRawDataset) {
    return;
  }
  const archiveUri = window.prompt("Archive URI", state.selectedRawDatasetDetail?.archive_uri || "");
  if (archiveUri === null) {
    return;
  }
  const archiveCompression = window.prompt(
    "Archive compression",
    state.selectedRawDatasetDetail?.archive_compression || "zip"
  );
  if (archiveCompression === null) {
    return;
  }
  const markArchived = window.confirm("Delete the hot source after successful archive?");
  await apiPost(`/raw-datasets/${state.selectedRawDataset.id}/archive`, {
    archive_uri: archiveUri.trim() || null,
    archive_compression: archiveCompression.trim() || null,
    mark_archived: markArchived,
  });
  await refreshRawDatasets();
  await selectRawDataset(state.selectedRawDataset.id);
  setStatus(`Archive transition requested for ${state.selectedRawDataset.acquisition_label}.`);
}

async function requestRawRestore() {
  if (!state.selectedRawDataset) {
    return;
  }
  await apiPost(`/raw-datasets/${state.selectedRawDataset.id}/restore`, {});
  await refreshRawDatasets();
  await selectRawDataset(state.selectedRawDataset.id);
  setStatus(`Restore requested for ${state.selectedRawDataset.acquisition_label}.`);
}

async function createMigrationPlan() {
  if (!els.migrationSourcePath || !els.migrationBatchName) {
    return;
  }
  const batchName = els.migrationBatchName.value.trim();
  const sourcePath = els.migrationSourcePath.value.trim();
  if (!batchName || !sourcePath) {
    throw new Error("Batch name and legacy source path are required.");
  }
  const response = await apiPost("/migrations/plans", {
    batch_name: batchName,
    source_kind: els.migrationSourceKind?.value || "legacy_project_root",
    source_path: sourcePath,
    storage_root_name: els.migrationStorageRootName?.value.trim() || null,
    host_scope: "server",
    root_type: "legacy_root",
    strategy: els.migrationStrategy?.value || "pilot",
    max_items: Number(els.migrationMaxItems?.value || 50),
    metadata_json: {},
  });
  await refreshMigrationPlans();
  await selectMigrationPlan(response.id);
  setStatus(`Created migration plan ${response.batch_name}.`);
}

async function markMigrationItem(item, payload) {
  if (!state.selectedMigrationPlan) {
    return;
  }
  await apiPatch(`/migrations/plans/${state.selectedMigrationPlan.id}/items/${item.id}`, payload);
  await selectMigrationPlan(state.selectedMigrationPlan.id);
  setStatus(`Updated migration item ${item.display_name}.`);
}

async function materializeMigrationItem(item) {
  if (!state.selectedMigrationPlan) {
    return;
  }
  const response = await apiPost(
    `/migrations/plans/${state.selectedMigrationPlan.id}/items/${item.id}/materialize`,
    {}
  );
  await selectMigrationPlan(state.selectedMigrationPlan.id);
  setStatus(`Created placeholder experiment ${response.experiment_key || response.title}.`);
}

async function attachMigrationItemToExisting(item) {
  if (!state.selectedMigrationPlan) {
    return;
  }
  const experimentKey = window.prompt("Existing experiment key", item.proposed_experiment_key || "");
  if (!experimentKey) {
    return;
  }
  const response = await apiPost(
    `/migrations/plans/${state.selectedMigrationPlan.id}/items/${item.id}/attach-existing`,
    { experiment_key: experimentKey.trim() }
  );
  await selectMigrationPlan(state.selectedMigrationPlan.id);
  setStatus(`Attached item ${item.display_name} to ${response.experiment_key || response.title}.`);
}

async function executePilotBatch() {
  if (!state.selectedMigrationPlan) {
    return;
  }
  const rawValue = window.prompt("Max pilot items", "10");
  if (!rawValue) {
    return;
  }
  const maxItems = Number(rawValue);
  if (!Number.isFinite(maxItems) || maxItems <= 0) {
    throw new Error("Max pilot items must be a positive number.");
  }
  const response = await apiPost(
    `/migrations/plans/${state.selectedMigrationPlan.id}/execute-pilot?max_items=${Math.floor(maxItems)}`,
    {}
  );
  await refreshMigrationPlans();
  await selectMigrationPlan(state.selectedMigrationPlan.id);
  setStatus(response.message);
}

async function createPipeline() {
  const displayName = window.prompt("Pipeline display name");
  if (!displayName) {
    return;
  }
  const pipelineKey = window.prompt("Pipeline key", displayName.toLowerCase().replace(/[^a-z0-9]+/g, "_"));
  if (pipelineKey === null) {
    return;
  }
  const version = window.prompt("Version", "1.0") || "1.0";
  const runtimeKind = window.prompt("Runtime kind (matlab/python/hybrid)", "matlab") || "matlab";
  await apiPost("/pipelines", {
    display_name: displayName,
    pipeline_key: pipelineKey.trim() || null,
    version,
    runtime_kind: runtimeKind,
    metadata_json: {},
  });
  await refreshPipelines();
  setStatus(`Created pipeline ${displayName}.`);
}

async function editPipeline(pipeline) {
  const displayName = window.prompt("Pipeline display name", pipeline.display_name);
  if (displayName === null) {
    return;
  }
  const version = window.prompt("Version", pipeline.version);
  if (version === null) {
    return;
  }
  const runtimeKind = window.prompt("Runtime kind (matlab/python/hybrid)", pipeline.runtime_kind);
  if (runtimeKind === null) {
    return;
  }
  await apiPatch(`/pipelines/${pipeline.id}`, {
    display_name: displayName,
    version,
    runtime_kind: runtimeKind,
    metadata_json: {},
  });
  await refreshPipelines();
  setStatus(`Updated pipeline ${displayName}.`);
}

async function importObservedPipelines() {
  const search = els.pipelineSearch?.value.trim() || "";
  const path = `/pipelines/import-observed${search ? `?search=${encodeURIComponent(search)}` : ""}`;
  const imported = await apiPost(path, {});
  await refreshPipelines();
  setStatus(`Imported ${imported.length} observed pipeline(s) into registry.`);
}

async function createUser() {
  if (!isAdmin()) {
    throw new Error("Admin role required.");
  }
  const userKey = window.prompt("User key");
  if (!userKey) {
    return;
  }
  const displayName = window.prompt("Display name", userKey) || userKey;
  const role = window.prompt("Role (user/admin/service)", "user") || "user";
  const password = window.prompt("Temporary password", "");
  await apiPost("/users", {
    user_key: userKey,
    display_name: displayName,
    role,
    is_active: true,
    password: password || null,
    metadata_json: {},
  });
  await refreshUsers();
  setStatus(`Created user ${userKey}.`);
}

async function editUser(user) {
  const displayName = window.prompt("Display name", user.display_name);
  if (displayName === null) {
    return;
  }
  const role = isAdmin() ? (window.prompt("Role (user/admin/service)", user.role) || user.role) : user.role;
  const activeText = isAdmin() ? window.prompt("Active? (yes/no)", user.is_active ? "yes" : "no") : null;
  const password = window.prompt("New password (leave empty to keep unchanged)", "");
  await apiPatch(`/users/${user.id}`, {
    display_name: displayName,
    role,
    is_active: activeText ? /^y/i.test(activeText) : user.is_active,
    password: password || null,
    metadata_json: {},
  });
  await refreshUsers();
  setStatus(`Updated user ${user.user_key}.`);
}

async function revokeSession(sessionId) {
  const ok = window.confirm("Revoke this session?");
  if (!ok) {
    return;
  }
  await apiDelete(`/auth/sessions/${sessionId}`);
  await refreshSessions();
  setStatus("Session revoked.");
}

async function pollDashboard() {
  if (!state.currentUser) {
    return;
  }
  try {
    const pollTasks = [apiGet("/dashboard/summary").then((summary) => setSummary(summary))];
    if (pageFlags.hasIndexingView) {
      pollTasks.push(refreshIndexingJobs());
    }
    if (pageFlags.hasRawDatasetsView) {
      pollTasks.push(refreshRawDatasets());
    }
    if (pageFlags.hasAutomaticArchivePolicy && isAdmin()) {
      pollTasks.push(refreshAutomaticArchivePolicyStatus());
    }
    if (pageFlags.hasMicroManagerIngest && isAdmin()) {
      pollTasks.push(refreshMicroManagerIngestStatus());
    }
    if (pageFlags.hasMigrationView) {
      pollTasks.push(refreshMigrationPlans());
    }
    const hasActiveJob = state.indexingJobs.some((job) => job.status === "queued" || job.status === "running");
    if (pageFlags.hasProjectsView && hasActiveJob) {
      pollTasks.push(refreshProjects());
    }
    await Promise.all(pollTasks);
    lastPollSucceededAt = new Date();
    if (pageFlags.hasIndexingView && state.indexingJobs.length) {
      setStatus(`Auto-refresh OK at ${lastPollSucceededAt.toLocaleTimeString()}.`);
    }
  } catch (error) {
    const suffix = lastPollSucceededAt
      ? ` Last successful refresh at ${lastPollSucceededAt.toLocaleTimeString()}.`
      : "";
    setStatus(`${String(error)}${suffix}`);
  }
}

async function refreshProjectsAndDetail() {
  await refreshProjects();
  if (state.selectedProject && pageFlags.hasProjectDetail) {
    const stillExists = state.projects.find((project) => project.id === state.selectedProject.id);
    if (stillExists) {
      await selectProject(stillExists.id);
    }
  }
}

async function forceRefreshCurrentPage() {
  if (!state.currentUser && !state.userKey) {
    await restoreSession();
    return;
  }
  if (pageFlags.hasProjectsView || pageFlags.hasIndexingView || pageFlags.hasPipelinesView || pageFlags.hasUsersView || pageFlags.hasSessionsView) {
    await refreshDashboard();
  } else if (pageFlags.hasIndexingView) {
    await refreshIndexingJobs();
  }
}

function ensureDashboardPolling() {
  if (dashboardPollHandle !== null) {
    return;
  }
  dashboardPollHandle = window.setInterval(() => {
    pollDashboard().catch((error) => setStatus(String(error)));
  }, 3000);
}

if (els.loginButton) els.loginButton.addEventListener("click", () => login().catch((error) => setStatus(String(error))));
if (els.connectButton) els.connectButton.addEventListener("click", () => forceRefreshCurrentPage().catch((error) => setStatus(String(error))));
if (els.logoutButton) els.logoutButton.addEventListener("click", () => logout().catch((error) => setStatus(String(error))));
if (els.refreshButton) els.refreshButton.addEventListener("click", () => forceRefreshCurrentPage().catch((error) => setStatus(String(error))));
if (els.groupFilter) els.groupFilter.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.projectSearch) els.projectSearch.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.ownerFilter) els.ownerFilter.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.storageRootFilter) els.storageRootFilter.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.projectLimit) els.projectLimit.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.ownedOnly) els.ownedOnly.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.rawSearch) els.rawSearch.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.rawOwnerFilter) els.rawOwnerFilter.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.rawTierFilter) els.rawTierFilter.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.rawArchiveStatusFilter) els.rawArchiveStatusFilter.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.rawLimit) els.rawLimit.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.rawOwnedOnly) els.rawOwnedOnly.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.newGroupButton) els.newGroupButton.addEventListener("click", () => createGroup().catch((error) => setStatus(String(error))));
if (els.addNoteButton) els.addNoteButton.addEventListener("click", () => addNote().catch((error) => setStatus(String(error))));
if (els.shareButton) els.shareButton.addEventListener("click", () => shareProject().catch((error) => setStatus(String(error))));
if (els.editProjectButton) els.editProjectButton.addEventListener("click", () => editProject().catch((error) => setStatus(String(error))));
if (els.addToGroupButton) els.addToGroupButton.addEventListener("click", () => addSelectedProjectToGroup().catch((error) => setStatus(String(error))));
if (els.previewDeleteButton) els.previewDeleteButton.addEventListener("click", () => previewDelete().catch((error) => setStatus(String(error))));
if (els.indexButton) els.indexButton.addEventListener("click", () => runIndexing().catch((error) => setStatus(String(error))));
if (els.indexJobsRefreshButton) els.indexJobsRefreshButton.addEventListener("click", () => refreshIndexingJobs().catch((error) => setStatus(String(error))));
if (els.migrationCreateButton) els.migrationCreateButton.addEventListener("click", () => createMigrationPlan().catch((error) => setStatus(String(error))));
if (els.migrationRefreshButton) els.migrationRefreshButton.addEventListener("click", () => refreshMigrationPlans().catch((error) => setStatus(String(error))));
if (els.migrationExecutePilotButton) els.migrationExecutePilotButton.addEventListener("click", () => executePilotBatch().catch((error) => setStatus(String(error))));
if (els.rawPreviewArchiveButton) els.rawPreviewArchiveButton.addEventListener("click", () => previewRawArchive().catch((error) => setStatus(String(error))));
if (els.rawArchiveButton) els.rawArchiveButton.addEventListener("click", () => requestRawArchive().catch((error) => setStatus(String(error))));
if (els.rawRestoreButton) els.rawRestoreButton.addEventListener("click", () => requestRawRestore().catch((error) => setStatus(String(error))));
if (els.automaticArchivePolicyRefreshButton) els.automaticArchivePolicyRefreshButton.addEventListener("click", () => refreshAutomaticArchivePolicyStatus().catch((error) => setStatus(String(error))));
if (els.automaticArchivePolicyRunButton) els.automaticArchivePolicyRunButton.addEventListener("click", () => runAutomaticArchivePolicy().catch((error) => setStatus(String(error))));
if (els.micromanagerIngestRefreshButton) els.micromanagerIngestRefreshButton.addEventListener("click", () => refreshMicroManagerIngestStatus().catch((error) => setStatus(String(error))));
if (els.micromanagerIngestRunButton) els.micromanagerIngestRunButton.addEventListener("click", () => runMicroManagerIngest().catch((error) => setStatus(String(error))));
if (els.archivePolicyPreviewButton) els.archivePolicyPreviewButton.addEventListener("click", () => previewArchivePolicy().catch((error) => setStatus(String(error))));
if (els.archivePolicyQueueButton) els.archivePolicyQueueButton.addEventListener("click", () => queueArchivePolicy().catch((error) => setStatus(String(error))));
if (els.refreshPipelinesButton) els.refreshPipelinesButton.addEventListener("click", () => refreshPipelines().catch((error) => setStatus(String(error))));
if (els.importObservedPipelinesButton) els.importObservedPipelinesButton.addEventListener("click", () => importObservedPipelines().catch((error) => setStatus(String(error))));
if (els.newPipelineButton) els.newPipelineButton.addEventListener("click", () => createPipeline().catch((error) => setStatus(String(error))));
if (els.pipelineSearch) els.pipelineSearch.addEventListener("change", () => refreshPipelines().catch((error) => setStatus(String(error))));
if (els.pipelineRuntimeFilter) els.pipelineRuntimeFilter.addEventListener("change", () => refreshPipelines().catch((error) => setStatus(String(error))));
if (els.pipelineSourceFilter) els.pipelineSourceFilter.addEventListener("change", () => renderPipelines());
if (els.newUserButton) els.newUserButton.addEventListener("click", () => createUser().catch((error) => setStatus(String(error))));
if (els.refreshSessionsButton) els.refreshSessionsButton.addEventListener("click", () => refreshSessions().catch((error) => setStatus(String(error))));

ensureDashboardPolling();
updateSessionUi();
restoreSession().catch((error) => setStatus(String(error)));
