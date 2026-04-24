const state = {
  userKey: localStorage.getItem("detecdivHub.userKey") || "localdev",
  sessionToken: localStorage.getItem("detecdivHub.sessionToken") || "",
  authMode: "",
  currentUser: null,
  projects: [],
  groups: [],
  storageRoots: [],
  indexBrowse: null,
  pipelines: [],
  observedPipelines: [],
  executionTargets: [],
  pipelineRuns: [],
  sessions: [],
  users: [],
  indexingJobs: [],
  rawDatasets: [],
  selectedRawDataset: null,
  selectedRawDatasetDetail: null,
  selectedRawPositionId: null,
  selectedRawDatasetIds: [],
  pendingRawBulkDelete: null,
  rawBulkDeletePreviewToken: 0,
  rawPreviewQualityStatus: null,
  archivePolicyPreview: null,
  automaticArchivePolicyStatus: null,
  micromanagerIngestStatus: null,
  migrationPlans: [],
  selectedMigrationPlan: null,
  selectedProject: null,
  selectedProjectDetail: null,
  selectedProjectIds: [],
  selectedPipeline: null,
  selectedPipelineRun: null,
  selectedExecutionTarget: null,
  editingExecutionTarget: null,
  pendingBulkDelete: null,
  bulkDeletePreviewToken: 0,
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
  adminNavLinks: document.querySelectorAll(".admin-nav-link"),
  adminContent: document.querySelector("#admin-content"),
  adminDeniedPanel: document.querySelector("#admin-denied-panel"),
  projectPageTitle: document.querySelector("#project-page-title"),
  projectSearch: document.querySelector("#project-search"),
  ownerFilter: document.querySelector("#owner-filter"),
  storageRootFilter: document.querySelector("#storage-root-filter"),
  projectLimit: document.querySelector("#project-limit"),
  groupFilter: document.querySelector("#group-filter"),
  ownedOnly: document.querySelector("#owned-only"),
  refreshButton: document.querySelector("#refresh-button"),
  newGroupButton: document.querySelector("#new-group-button"),
  projectSelectAll: document.querySelector("#project-select-all"),
  projectSelectionSummary: document.querySelector("#project-selection-summary"),
  selectVisibleButton: document.querySelector("#select-visible-button"),
  clearSelectionButton: document.querySelector("#clear-selection-button"),
  bulkQueuePreviewsSelectedButton: document.querySelector("#bulk-queue-previews-selected-button"),
  bulkDeleteSelectedButton: document.querySelector("#bulk-delete-selected-button"),
  bulkDeleteVisibleButton: document.querySelector("#bulk-delete-visible-button"),
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
  projectRawDatasetsList: document.querySelector("#project-raw-datasets-list"),
  projectQueueRawPreviewsButton: document.querySelector("#project-queue-raw-previews-button"),
  addNoteButton: document.querySelector("#add-note-button"),
  shareButton: document.querySelector("#share-button"),
  editProjectButton: document.querySelector("#edit-project-button"),
  updateProjectButton: document.querySelector("#update-project-button"),
  addToGroupButton: document.querySelector("#add-to-group-button"),
  previewDeleteButton: document.querySelector("#preview-delete-button"),
  bulkDeletePanel: document.querySelector("#bulk-delete-panel"),
  bulkDeleteSummary: document.querySelector("#bulk-delete-summary"),
  bulkDeleteMode: document.querySelector("#bulk-delete-mode"),
  bulkDeleteConfirmText: document.querySelector("#bulk-delete-confirm-text"),
  bulkDeleteCancelButton: document.querySelector("#bulk-delete-cancel-button"),
  bulkDeleteConfirmButton: document.querySelector("#bulk-delete-confirm-button"),
  indexSourcePath: document.querySelector("#index-source-path"),
  indexStorageRootName: document.querySelector("#index-storage-root-name"),
  indexOwnerUserKey: document.querySelector("#index-owner-user-key"),
  indexVisibility: document.querySelector("#index-visibility"),
  indexClearExisting: document.querySelector("#index-clear-existing"),
  indexBrowseRoot: document.querySelector("#index-browse-root"),
  indexBrowseOpenButton: document.querySelector("#index-browse-open-button"),
  indexBrowseUseButton: document.querySelector("#index-browse-use-button"),
  indexBrowseCurrent: document.querySelector("#index-browse-current"),
  indexBrowseTableBody: document.querySelector("#index-browse-table tbody"),
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
  rawSelectAll: document.querySelector("#raw-select-all"),
  rawSelectionSummary: document.querySelector("#raw-selection-summary"),
  rawSelectVisibleButton: document.querySelector("#raw-select-visible-button"),
  rawClearSelectionButton: document.querySelector("#raw-clear-selection-button"),
  rawBulkDeleteSelectedButton: document.querySelector("#raw-bulk-delete-selected-button"),
  rawBulkDeleteVisibleButton: document.querySelector("#raw-bulk-delete-visible-button"),
  rawBulkDeletePanel: document.querySelector("#raw-bulk-delete-panel"),
  rawBulkDeleteSummary: document.querySelector("#raw-bulk-delete-summary"),
  rawBulkDeleteMode: document.querySelector("#raw-bulk-delete-mode"),
  rawBulkDeleteLinkedProjects: document.querySelector("#raw-bulk-delete-linked-projects"),
  rawBulkDeleteLinkedProjectFiles: document.querySelector("#raw-bulk-delete-linked-project-files"),
  rawBulkDeleteConfirmText: document.querySelector("#raw-bulk-delete-confirm-text"),
  rawBulkDeleteCancelButton: document.querySelector("#raw-bulk-delete-cancel-button"),
  rawBulkDeleteConfirmButton: document.querySelector("#raw-bulk-delete-confirm-button"),
  rawDatasetsTableBody: document.querySelector("#raw-datasets-table tbody"),
  rawCountLabel: document.querySelector("#raw-count-label"),
  rawDetailEmpty: document.querySelector("#raw-detail-empty"),
  rawDetailContent: document.querySelector("#raw-detail-content"),
  rawDetailSubtitle: document.querySelector("#raw-detail-subtitle"),
  rawDetailList: document.querySelector("#raw-detail-list"),
  rawLocationsList: document.querySelector("#raw-locations-list"),
  rawAnalysisProjectsList: document.querySelector("#raw-analysis-projects-list"),
  rawPositionsTableBody: document.querySelector("#raw-positions-table tbody"),
  rawQueuePreviewButton: document.querySelector("#raw-queue-preview-button"),
  rawRegeneratePreviewButton: document.querySelector("#raw-regenerate-preview-button"),
  rawPreviewProgress: document.querySelector("#raw-preview-progress"),
  rawOpenDatasetPageButton: document.querySelector("#raw-open-dataset-page-button"),
  rawOpenProjectPageButton: document.querySelector("#raw-open-project-page-button"),
  rawPositionViewerEmpty: document.querySelector("#raw-position-viewer-empty"),
  rawPositionViewer: document.querySelector("#raw-position-viewer"),
  rawPositionViewerMeta: document.querySelector("#raw-position-viewer-meta"),
  rawPositionViewerVideo: document.querySelector("#raw-position-viewer-video"),
  rawPositionViewerOpenLink: document.querySelector("#raw-position-viewer-open-link"),
  rawLifecycleEvents: document.querySelector("#raw-lifecycle-events"),
  rawPreviewArchiveButton: document.querySelector("#raw-preview-archive-button"),
  rawArchiveButton: document.querySelector("#raw-archive-button"),
  rawRestoreButton: document.querySelector("#raw-restore-button"),
  rawDatasetPageTitle: document.querySelector("#raw-dataset-page-title"),
  rawPreviewQualityRefreshButton: document.querySelector("#raw-preview-quality-refresh-button"),
  rawPreviewQualitySaveButton: document.querySelector("#raw-preview-quality-save-button"),
  rawPreviewQualityMaxDimension: document.querySelector("#raw-preview-quality-max-dimension"),
  rawPreviewQualityFps: document.querySelector("#raw-preview-quality-fps"),
  rawPreviewQualityFrameMode: document.querySelector("#raw-preview-quality-frame-mode"),
  rawPreviewQualityMaxFrames: document.querySelector("#raw-preview-quality-max-frames"),
  rawPreviewQualityBinningFactor: document.querySelector("#raw-preview-quality-binning-factor"),
  rawPreviewQualityCrf: document.querySelector("#raw-preview-quality-crf"),
  rawPreviewQualityPreset: document.querySelector("#raw-preview-quality-preset"),
  rawPreviewQualityIncludeExisting: document.querySelector("#raw-preview-quality-include-existing"),
  rawPreviewQualityArtifactRoot: document.querySelector("#raw-preview-quality-artifact-root"),
  rawPreviewQualityFfmpegCommand: document.querySelector("#raw-preview-quality-ffmpeg-command"),
  rawPreviewQualityConfig: document.querySelector("#raw-preview-quality-config"),
  rawPreviewQualitySummary: document.querySelector("#raw-preview-quality-summary"),
  rawPreviewQualityTableBody: document.querySelector("#raw-preview-quality-table tbody"),
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
  pipelineRunProjectSelect: document.querySelector("#pipeline-run-project-select"),
  pipelineRunPipelineSelect: document.querySelector("#pipeline-run-pipeline-select"),
  pipelineRunTargetSelect: document.querySelector("#pipeline-run-target-select"),
  pipelineRunModeSelect: document.querySelector("#pipeline-run-mode-select"),
  pipelineRunGpuSelect: document.querySelector("#pipeline-run-gpu-select"),
  pipelineRunPythonModeSelect: document.querySelector("#pipeline-run-python-mode-select"),
  pipelineRunPythonEnv: document.querySelector("#pipeline-run-python-env"),
  pipelineRunId: document.querySelector("#pipeline-run-id"),
  pipelineRunPolicySelect: document.querySelector("#pipeline-run-policy-select"),
  pipelineRunExistingSelect: document.querySelector("#pipeline-run-existing-select"),
  pipelineRunCacheSelect: document.querySelector("#pipeline-run-cache-select"),
  pipelineRunPriority: document.querySelector("#pipeline-run-priority"),
  pipelineRunSelectedNodes: document.querySelector("#pipeline-run-selected-nodes"),
  pipelineRunDescription: document.querySelector("#pipeline-run-description"),
  pipelineRunNodeParams: document.querySelector("#pipeline-run-node-params"),
  pipelineRunSelectionSummary: document.querySelector("#pipeline-run-selection-summary"),
  pipelineRunEditorMode: document.querySelector("#pipeline-run-editor-mode"),
  refreshPipelineRunsButton: document.querySelector("#refresh-pipeline-runs-button"),
  newPipelineRunButton: document.querySelector("#new-pipeline-run-button"),
  cancelPipelineRunButton: document.querySelector("#cancel-pipeline-run-button"),
  submitPipelineRunButton: document.querySelector("#submit-pipeline-run-button"),
  pipelineRunsTableBody: document.querySelector("#pipeline-runs-table tbody"),
  pipelineRunDetail: document.querySelector("#pipeline-run-detail"),
  refreshExecutionTargetsButton: document.querySelector("#refresh-execution-targets-button"),
  newExecutionTargetButton: document.querySelector("#new-execution-target-button"),
  cancelExecutionTargetEditButton: document.querySelector("#cancel-execution-target-edit-button"),
  saveExecutionTargetButton: document.querySelector("#save-execution-target-button"),
  executionTargetEditorMode: document.querySelector("#execution-target-editor-mode"),
  executionTargetName: document.querySelector("#execution-target-name"),
  executionTargetKey: document.querySelector("#execution-target-key"),
  executionTargetKind: document.querySelector("#execution-target-kind"),
  executionTargetHost: document.querySelector("#execution-target-host"),
  executionTargetStatus: document.querySelector("#execution-target-status"),
  executionTargetSupportsMatlab: document.querySelector("#execution-target-supports-matlab"),
  executionTargetSupportsPython: document.querySelector("#execution-target-supports-python"),
  executionTargetSupportsGpu: document.querySelector("#execution-target-supports-gpu"),
  executionTargetMaxConcurrentJobs: document.querySelector("#execution-target-max-concurrent-jobs"),
  executionTargetMatlabMaxThreads: document.querySelector("#execution-target-matlab-max-threads"),
  executionTargetMetadataJson: document.querySelector("#execution-target-metadata-json"),
  executionTargetsTableBody: document.querySelector("#execution-targets-table tbody"),
  executionTargetDetail: document.querySelector("#execution-target-detail"),
  usersTableBody: document.querySelector("#users-table tbody"),
  newUserButton: document.querySelector("#new-user-button"),
  bulkImportUsersText: document.querySelector("#bulk-import-users-text"),
  bulkImportUsersButton: document.querySelector("#bulk-import-users-button"),
  sessionsTableBody: document.querySelector("#sessions-table tbody"),
  refreshSessionsButton: document.querySelector("#refresh-sessions-button"),
  statusLine: document.querySelector("#status-line"),
};

let dashboardPollHandle = null;
let lastPollSucceededAt = null;
const pageKind = document.body?.dataset?.page || "";

const pageFlags = {
  hasAdminView: pageKind === "admin",
  hasProjectPage: pageKind === "project",
  hasProjectsView: Boolean(els.projectsTableBody),
  hasProjectGroups: Boolean(els.groupFilter),
  hasStorageRootFilter: Boolean(els.storageRootFilter),
  hasProjectDetail: Boolean(els.detailContent),
  hasPipelinesView: Boolean(els.pipelinesTableBody),
  hasPipelineRunsView: pageKind === "project" && Boolean(els.pipelineRunsTableBody),
  hasExecutionTargetsView: pageKind === "admin" && Boolean(els.executionTargetsTableBody),
  hasUsersView: pageKind === "admin" && Boolean(els.usersTableBody),
  hasSessionsView: pageKind === "admin" && Boolean(els.sessionsTableBody),
  hasIndexingView: Boolean(els.indexJobsTableBody || els.activeIndexJob),
  hasRawDatasetsView: Boolean(els.rawDatasetsTableBody),
  hasRawDatasetPage: pageKind === "raw-dataset",
  hasAutomaticArchivePolicy: Boolean(els.automaticArchivePolicyPanel),
  hasMicroManagerIngest: Boolean(els.micromanagerIngestPanel),
  hasRawDatasetDetail: Boolean(els.rawDetailContent || els.rawDetailList),
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
  state.indexBrowse = null;
  state.pipelines = [];
  state.observedPipelines = [];
  state.executionTargets = [];
  state.pipelineRuns = [];
  state.sessions = [];
  state.users = [];
  state.indexingJobs = [];
  state.rawDatasets = [];
  state.selectedRawDataset = null;
  state.selectedRawDatasetDetail = null;
  state.selectedRawPositionId = null;
  state.selectedRawDatasetIds = [];
  state.pendingRawBulkDelete = null;
  state.rawBulkDeletePreviewToken = 0;
  state.rawPreviewQualityStatus = null;
  state.archivePolicyPreview = null;
  state.automaticArchivePolicyStatus = null;
  state.micromanagerIngestStatus = null;
  state.migrationPlans = [];
  state.selectedMigrationPlan = null;
  state.selectedProject = null;
  state.selectedProjectDetail = null;
  state.selectedProjectIds = [];
  state.selectedPipeline = null;
  state.selectedPipelineRun = null;
  state.selectedExecutionTarget = null;
  state.pendingBulkDelete = null;
  state.bulkDeletePreviewToken = 0;
  state.notes = [];
  state.acl = [];
  state.summary = null;

  if (els.summaryTotalProjects) els.summaryTotalProjects.textContent = "0";
  if (els.summaryOwnedProjects) els.summaryOwnedProjects.textContent = "0";
  if (els.summaryTotalBytes) els.summaryTotalBytes.textContent = "0 B";
  if (els.summaryGroupCount) els.summaryGroupCount.textContent = "0";

  renderGroupFilter();
  renderStorageRootFilter();
  renderIndexBrowser();
  renderProjects();
  renderProjectSelectionControls();
  renderBulkDeletePanel();
  renderPipelines();
  renderPipelineRunBuilder();
  renderPipelineRuns();
  renderExecutionTargets();
  renderOwnerFilters();
  renderIndexOwnerOptions();
  renderIndexingJobs();
  renderRawDatasets();
  renderRawDatasetDetail();
  renderRawPreviewQualityStatus();
  renderAutomaticArchivePolicyStatus();
  renderMicroManagerIngestStatus();
  renderArchivePolicyPreview();
  renderMigrationPlans();
  renderMigrationDetail();
  renderUsers();
  renderSessions();
  renderDetail();
  renderProjectRawDatasets();
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

function summarizeList(values, limit = 5) {
  const items = (Array.isArray(values) ? values : [])
    .map((value) => String(value || "").trim())
    .filter(Boolean);
  if (!items.length) {
    return "none";
  }
  if (items.length <= limit) {
    return items.join(", ");
  }
  return `${items.slice(0, limit).join(", ")} + ${items.length - limit} more`;
}

function extractRawAcquisitionFacts(raw) {
  const metadata = raw?.metadata_json && typeof raw.metadata_json === "object" ? raw.metadata_json : {};
  const dimensions = metadata.dimensions && typeof metadata.dimensions === "object" ? metadata.dimensions : {};
  const positionsIndexed = Array.isArray(raw?.positions) ? raw.positions.length : 0;

  const channelNamesRaw = Array.isArray(dimensions.channel_names) ? dimensions.channel_names : [];
  const channelNames = channelNamesRaw
    .map((value) => String(value || "").trim())
    .filter(Boolean);
  const channelsLabel = channelNames.length
    ? summarizeList(channelNames, 6)
    : (Number(dimensions.channel_count || 0) > 0 ? `${Number(dimensions.channel_count)} channel(s)` : "unknown");

  const metadataPositionCount = Number(dimensions.position_count || 0);
  const positionsLabel = metadataPositionCount > 0 ? `${metadataPositionCount}` : "unknown";

  const exposureValues = collectExposureMsValues(metadata);
  const exposureLabel = exposureValues.length
    ? exposureValues.map((value) => Number(value.toFixed(3))).join(", ")
    : "unknown";

  return {
    positionsIndexed,
    positionsLabel,
    channelsLabel,
    exposureLabel,
  };
}

function collectExposureMsValues(metadata) {
  const values = [];
  const seen = new Set();
  const stack = [{ value: metadata, depth: 0 }];
  let visited = 0;

  while (stack.length && visited < 5000) {
    const node = stack.pop();
    visited += 1;
    if (!node || node.depth > 7) {
      continue;
    }
    const current = node.value;
    if (Array.isArray(current)) {
      for (const item of current) {
        stack.push({ value: item, depth: node.depth + 1 });
      }
      continue;
    }
    if (!current || typeof current !== "object") {
      continue;
    }
    for (const [key, rawValue] of Object.entries(current)) {
      const normalizedKey = String(key || "").toLowerCase();
      if (normalizedKey.includes("exposure")) {
        const parsed = Number(rawValue);
        if (Number.isFinite(parsed) && parsed >= 0) {
          const token = parsed.toFixed(6);
          if (!seen.has(token)) {
            seen.add(token);
            values.push(parsed);
          }
        }
      }
      if (rawValue && typeof rawValue === "object") {
        stack.push({ value: rawValue, depth: node.depth + 1 });
      }
    }
  }
  values.sort((a, b) => a - b);
  return values.slice(0, 12);
}

function parseBinningFactor(rawValue, fallback = 4) {
  const text = String(rawValue || "").trim().toLowerCase();
  if (!text) {
    return fallback;
  }
  const squareMatch = text.match(/^(\d+)\s*x\s*(\d+)$/);
  if (squareMatch) {
    const first = Number(squareMatch[1]);
    const second = Number(squareMatch[2]);
    if (Number.isFinite(first) && Number.isFinite(second) && first === second && first >= 1) {
      return first;
    }
    throw new Error("Binning factor must be square, for example 2x2 or 4x4.");
  }
  const scalar = Number(text);
  if (Number.isFinite(scalar) && scalar >= 1) {
    return Math.floor(scalar);
  }
  throw new Error("Invalid binning factor. Use 1, 2, 4, or a square form like 4x4.");
}

function updateRawPreviewFrameModeUi() {
  const mode = String(els.rawPreviewQualityFrameMode?.value || "full");
  if (!els.rawPreviewQualityMaxFrames) {
    return;
  }
  if (mode === "full") {
    els.rawPreviewQualityMaxFrames.value = "";
    els.rawPreviewQualityMaxFrames.placeholder = "full (all frames)";
    els.rawPreviewQualityMaxFrames.disabled = true;
    return;
  }
  els.rawPreviewQualityMaxFrames.disabled = false;
  els.rawPreviewQualityMaxFrames.placeholder = "96";
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

function fieldValueFromForm(form, name) {
  const field = form.elements.namedItem(name);
  if (!field) {
    return "";
  }
  if (field.type === "checkbox") {
    return Boolean(field.checked);
  }
  return String(field.value || "");
}

function openFormDialog({ title, description = "", fields = [], submitLabel = "Save" }) {
  return new Promise((resolve) => {
    const overlay = document.createElement("div");
    overlay.className = "modal-backdrop";
    const fieldMarkup = fields.map((field) => {
      const required = field.required ? "required" : "";
      const value = field.value == null ? "" : String(field.value);
      if (field.type === "textarea") {
        return `
          <label class="field dialog-field">
            <span>${escapeHtml(field.label)}</span>
            <textarea name="${field.name}" rows="${field.rows || 4}" ${required}>${escapeHtml(value)}</textarea>
          </label>
        `;
      }
      if (field.type === "select") {
        const options = (field.options || []).map((option) => `
          <option value="${escapeHtml(option.value)}" ${String(option.value) === value ? "selected" : ""}>${escapeHtml(option.label)}</option>
        `).join("");
        return `
          <label class="field dialog-field">
            <span>${escapeHtml(field.label)}</span>
            <select name="${field.name}" ${required}>${options}</select>
          </label>
        `;
      }
      return `
        <label class="field dialog-field">
          <span>${escapeHtml(field.label)}</span>
          <input name="${field.name}" type="${field.type || "text"}" value="${escapeHtml(value)}" ${required} />
        </label>
      `;
    }).join("");
    overlay.innerHTML = `
      <form class="modal-panel">
        <div class="panel-header">
          <div>
            <h2>${escapeHtml(title)}</h2>
            ${description ? `<p class="muted">${escapeHtml(description)}</p>` : ""}
          </div>
        </div>
        <div class="dialog-field-list">${fieldMarkup}</div>
        <div class="dialog-actions">
          <button type="button" data-dialog-cancel>Cancel</button>
          <button type="submit" class="primary">${escapeHtml(submitLabel)}</button>
        </div>
      </form>
    `;
    document.body.appendChild(overlay);

    const form = overlay.querySelector("form");
    const close = (value) => {
      overlay.remove();
      resolve(value);
    };
    overlay.querySelector("[data-dialog-cancel]")?.addEventListener("click", () => close(null));
    overlay.addEventListener("click", (event) => {
      if (event.target === overlay) {
        close(null);
      }
    });
    overlay.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        close(null);
      }
    });
    form?.addEventListener("submit", (event) => {
      event.preventDefault();
      const values = {};
      for (const field of fields) {
        values[field.name] = fieldValueFromForm(form, field.name);
      }
      close(values);
    });
    overlay.querySelector("input, textarea, select")?.focus();
  });
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
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
      els.sessionLabel.textContent = state.userKey
        ? `User key fallback: ${state.userKey} (no session)`
        : "Not logged in.";
    } else if (state.authMode === "session") {
      els.sessionLabel.textContent = `Session: ${state.currentUser.display_name} (${state.currentUser.user_key})`;
    } else if (state.authMode === "user_key") {
      els.sessionLabel.textContent = `User key identity: ${state.currentUser.display_name} (${state.currentUser.user_key})`;
    } else if (state.authMode === "default_user_key") {
      els.sessionLabel.textContent = `Default user identity: ${state.currentUser.display_name} (${state.currentUser.user_key})`;
    } else if (state.authMode === "header") {
      els.sessionLabel.textContent = `Header identity: ${state.currentUser.display_name} (${state.currentUser.user_key})`;
    } else {
      els.sessionLabel.textContent = `Identity: ${state.currentUser.display_name} (${state.currentUser.user_key})`;
    }
  }
  if (els.userLabel) {
    if (authenticated) {
      const modeLabel = state.authMode === "session" ? "session" : (state.authMode || "identity");
      els.userLabel.textContent = `Signed in: ${state.currentUser.display_name} (${state.currentUser.user_key}) via ${modeLabel}`;
    } else {
      els.userLabel.textContent = state.userKey
        ? `Signed in: ${state.userKey} via user_key fallback`
        : "Signed in: not connected";
    }
  }
  if (els.logoutButton) {
    els.logoutButton.disabled = !authenticated;
  }
  for (const link of els.adminNavLinks || []) {
    link.classList.toggle("hidden", !isAdmin());
  }
  if (pageFlags.hasAdminView) {
    const allowed = isAdmin();
    if (els.adminContent) {
      els.adminContent.classList.toggle("hidden", !allowed);
    }
    if (els.adminDeniedPanel) {
      els.adminDeniedPanel.classList.toggle("hidden", allowed || !authenticated);
    }
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

function userOptionLabel(user) {
  return `${user.display_name} (${user.user_key})`;
}

function renderUserSelect(selectElement, users, options = {}) {
  if (!selectElement) {
    return;
  }
  const {
    emptyOptionLabel = "",
    noUsersLabel = "No user accounts available",
    selectedValue = "",
  } = options;
  selectElement.innerHTML = "";
  if (emptyOptionLabel) {
    const blankOption = document.createElement("option");
    blankOption.value = "";
    blankOption.textContent = emptyOptionLabel;
    selectElement.appendChild(blankOption);
  }
  for (const user of users) {
    const option = document.createElement("option");
    option.value = user.user_key;
    option.textContent = userOptionLabel(user);
    selectElement.appendChild(option);
  }
  if (!users.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = noUsersLabel;
    selectElement.appendChild(option);
  }
  if (users.some((user) => user.user_key === selectedValue)) {
    selectElement.value = selectedValue;
  } else if (emptyOptionLabel) {
    selectElement.value = "";
  } else if (users.length) {
    selectElement.value = users[0].user_key;
  }
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

function browseableStorageRoots() {
  return state.storageRoots.filter((root) => root.host_scope === "server");
}

function selectedBrowseRoot() {
  if (!els.indexBrowseRoot) {
    return null;
  }
  const rootId = Number(els.indexBrowseRoot.value || 0);
  if (!rootId) {
    return null;
  }
  return browseableStorageRoots().find((root) => root.id === rootId) || null;
}

function renderIndexBrowser() {
  if (!els.indexBrowseRoot || !els.indexBrowseCurrent || !els.indexBrowseTableBody) {
    return;
  }

  const roots = browseableStorageRoots();
  const currentValue = Number(els.indexBrowseRoot.value || 0);
  els.indexBrowseRoot.innerHTML = "";

  if (!roots.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No server storage roots";
    els.indexBrowseRoot.appendChild(option);
    els.indexBrowseCurrent.textContent = "No browseable server storage roots are registered.";
    els.indexBrowseTableBody.innerHTML = "";
    return;
  }

  for (const root of roots) {
    const option = document.createElement("option");
    option.value = String(root.id);
    option.textContent = `${root.name} (${root.path_prefix})`;
    els.indexBrowseRoot.appendChild(option);
  }

  const hasCurrent = roots.some((root) => root.id === currentValue);
  els.indexBrowseRoot.value = String(hasCurrent ? currentValue : roots[0].id);

  if (!state.indexBrowse || state.indexBrowse.storage_root?.id !== Number(els.indexBrowseRoot.value)) {
    const root = selectedBrowseRoot();
    state.indexBrowse = root
      ? {
          storage_root: root,
          current_relative_path: "",
          current_absolute_path: root.path_prefix,
          parent_relative_path: null,
          directories: [],
        }
      : null;
  }

  const browse = state.indexBrowse;
  if (!browse) {
    els.indexBrowseCurrent.textContent = "No storage root selected.";
    els.indexBrowseTableBody.innerHTML = "";
    return;
  }

  const currentLabel = browse.current_relative_path
    ? `${browse.storage_root.name}: ${browse.current_relative_path}`
    : `${browse.storage_root.name}: /`;
  els.indexBrowseCurrent.textContent = `${currentLabel} -> ${browse.current_absolute_path}`;

  els.indexBrowseTableBody.innerHTML = "";
  if (browse.parent_relative_path !== null) {
    const upRow = document.createElement("tr");
    upRow.innerHTML = `
      <td>..</td>
      <td>${browse.parent_relative_path || "/"}</td>
      <td><button type="button">Up</button></td>
    `;
    upRow.querySelector("button")?.addEventListener("click", () => {
      openIndexBrowserPath(browse.parent_relative_path).catch((error) => setStatus(String(error)));
    });
    els.indexBrowseTableBody.appendChild(upRow);
  }

  for (const directory of browse.directories || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${directory.name}</td>
      <td>${directory.relative_path || "/"}</td>
      <td><button type="button">Enter</button></td>
    `;
    tr.querySelector("button")?.addEventListener("click", () => {
      openIndexBrowserPath(directory.relative_path).catch((error) => setStatus(String(error)));
    });
    els.indexBrowseTableBody.appendChild(tr);
  }

  if (!browse.directories?.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="3" class="muted">No child directories.</td>`;
    els.indexBrowseTableBody.appendChild(tr);
  }
}

function visibleProjectIds() {
  return state.projects.map((project) => `${project.id}`);
}

function selectedProjectIds() {
  const visibleIds = new Set(visibleProjectIds());
  return state.selectedProjectIds.filter((projectId) => visibleIds.has(`${projectId}`));
}

function isProjectSelected(projectId) {
  return state.selectedProjectIds.includes(`${projectId}`);
}

function setSelectedProjectIds(projectIds) {
  state.selectedProjectIds = [...new Set((projectIds || []).map((projectId) => `${projectId}`))];
  if (state.pendingBulkDelete?.scope === "selected") {
    closeBulkDeletePanel();
  }
  renderProjects();
  renderProjectSelectionControls();
}

function toggleProjectSelection(projectId, isSelected) {
  const normalizedId = `${projectId}`;
  const selected = new Set(state.selectedProjectIds);
  if (isSelected) {
    selected.add(normalizedId);
  } else {
    selected.delete(normalizedId);
  }
  setSelectedProjectIds([...selected]);
}

function renderProjectSelectionControls() {
  const visibleIds = visibleProjectIds();
  const selectedIds = selectedProjectIds();
  const visibleCount = visibleIds.length;
  const selectedCount = selectedIds.length;

  if (els.projectSelectAll) {
    const allVisibleSelected = visibleCount > 0 && selectedCount === visibleCount;
    const someVisibleSelected = selectedCount > 0 && selectedCount < visibleCount;
    els.projectSelectAll.checked = allVisibleSelected;
    els.projectSelectAll.indeterminate = someVisibleSelected;
    els.projectSelectAll.disabled = visibleCount === 0;
  }
  if (els.projectSelectionSummary) {
    els.projectSelectionSummary.textContent = `${selectedCount} selected / ${visibleCount} visible`;
  }
  if (els.clearSelectionButton) {
    els.clearSelectionButton.disabled = selectedCount === 0;
  }
  if (els.selectVisibleButton) {
    els.selectVisibleButton.disabled = visibleCount === 0;
  }
  if (els.bulkDeleteSelectedButton) {
    els.bulkDeleteSelectedButton.disabled = selectedCount === 0;
  }
  if (els.bulkDeleteVisibleButton) {
    els.bulkDeleteVisibleButton.disabled = visibleCount === 0;
  }
  if (els.bulkQueuePreviewsSelectedButton) {
    els.bulkQueuePreviewsSelectedButton.disabled = selectedCount === 0;
  }
}

function renderBulkDeletePanel() {
  if (!els.bulkDeletePanel || !els.bulkDeleteSummary || !els.bulkDeleteConfirmButton) {
    return;
  }
  const pending = state.pendingBulkDelete;
  if (!pending || !pending.projectIds?.length) {
    els.bulkDeletePanel.classList.add("hidden");
    return;
  }

  els.bulkDeletePanel.classList.remove("hidden");
  const scopeLabel = pending.scope === "selected" ? "selected" : "visible";
  const mode = els.bulkDeleteMode?.value || "database_only";
  els.bulkDeleteConfirmButton.textContent =
    mode === "project_files" ? "Delete catalog entries and files" : "Delete catalog entries only";

  if (!pending.preview) {
    els.bulkDeleteSummary.textContent = `Preparing deletion preview for ${pending.projectIds.length} ${scopeLabel} project(s)...`;
    return;
  }

  const preview = pending.preview;
  const parts = [`${pending.projectIds.length} ${scopeLabel} project(s)`];
  if (mode === "project_files") {
    parts.push(`project files reclaimable ${humanBytes(preview.reclaimableBytes || 0)}`);
  } else {
    parts.push("catalog entries only, files on disk will be kept");
  }
  if (preview.errorCount) {
    parts.push(`${preview.errorCount} preview error(s)`);
  }
  els.bulkDeleteSummary.textContent = parts.join(" | ");
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
      <td><input class="project-select-checkbox" type="checkbox" ${isProjectSelected(project.id) ? "checked" : ""} /></td>
      <td>${project.project_name}</td>
      <td>${project.owner ? userOptionLabel(project.owner) : ""}</td>
      <td>${project.visibility}</td>
      <td>${project.health_status}</td>
      <td>${project.pipeline_run_count}</td>
      <td>${project.h5_count}</td>
      <td>${humanBytes(project.total_bytes)}</td>
    `;
    tr.querySelector(".project-select-checkbox")?.addEventListener("click", (event) => {
      event.stopPropagation();
    });
    tr.querySelector(".project-select-checkbox")?.addEventListener("change", (event) => {
      toggleProjectSelection(project.id, Boolean(event.target.checked));
    });
    tr.addEventListener("click", () => selectProject(project.id));
    els.projectsTableBody.appendChild(tr);
  }
  renderProjectSelectionControls();
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
    if (state.selectedPipeline && pipelineIdentity(pipeline) === pipelineIdentity(state.selectedPipeline)) {
      tr.classList.add("selected");
    }
    const source = pipeline.source || "registry";
    const version = pipeline.version || "observed";
    const updated = pipeline.updated_at || pipeline.latest_run_at || pipeline.created_at;
    const canDelete = source === "registry" && Boolean(pipeline.id);
    const projectCount = pipeline.project_count || 0;
    tr.innerHTML = `
      <td>${pipeline.display_name}</td>
      <td>${source}</td>
      <td>${pipeline.pipeline_key || ""}</td>
      <td>${version}</td>
      <td>${pipeline.runtime_kind}</td>
      <td>${projectCount}</td>
      <td>${formatTimestamp(updated)}</td>
      <td></td>
    `;
    const actionCell = tr.querySelector("td:last-child");
    if (actionCell) {
      if (canDelete) {
        const deleteButton = document.createElement("button");
        deleteButton.type = "button";
        deleteButton.className = "danger";
        deleteButton.textContent = "Delete";
        deleteButton.addEventListener("click", (event) => {
          event.stopPropagation();
          deletePipeline(pipeline).catch((error) => setStatus(String(error)));
        });
        actionCell.appendChild(deleteButton);
      } else if (source === "project_observed") {
        actionCell.textContent = "Embedded in project";
      }
    }
    tr.title = JSON.stringify(pipeline.metadata_json || {});
    tr.addEventListener("click", () => selectPipelineRow(pipeline));
    if (source === "registry") {
      tr.addEventListener("dblclick", () => editPipeline(pipeline));
    }
    els.pipelinesTableBody.appendChild(tr);
  }
  renderPipelineRunBuilder();
}

function selectPipelineRow(pipeline) {
  state.selectedPipeline = pipeline;
  renderPipelines();
}

function pipelineIdentity(pipeline) {
  if (!pipeline) {
    return "";
  }
  return String(
    pipeline.id
    || pipeline.identity
    || pipeline.pipeline_key
    || pipeline.metadata_json?.pipeline_path
    || pipeline.display_name
  );
}

function pipelineOptionValue(pipeline) {
  return pipelineIdentity(pipeline);
}

function renderPipelineRunBuilder() {
  if (!els.pipelineRunProjectSelect || !els.pipelineRunPipelineSelect || !els.pipelineRunTargetSelect) {
    return;
  }

  const selectedProjectId = String(els.pipelineRunProjectSelect.value || state.selectedProject?.id || "");
  const selectedPipelineId = String(els.pipelineRunPipelineSelect.value || pipelineOptionValue(state.selectedPipeline) || "");
  const selectedTargetId = String(els.pipelineRunTargetSelect.value || "");

  els.pipelineRunProjectSelect.innerHTML = `<option value="">Use selected project</option>`;
  for (const project of state.projects) {
    const option = document.createElement("option");
    option.value = String(project.id);
    option.textContent = project.project_name;
    if (String(project.id) === selectedProjectId) option.selected = true;
    els.pipelineRunProjectSelect.appendChild(option);
  }

  els.pipelineRunPipelineSelect.innerHTML = `<option value="">Use selected pipeline</option>`;
  for (const pipeline of pipelineRows()) {
    const option = document.createElement("option");
    option.value = pipelineOptionValue(pipeline);
    option.textContent = `${pipeline.display_name} [${pipeline.source || "registry"}]`;
    if (pipelineOptionValue(pipeline) === selectedPipelineId) option.selected = true;
    els.pipelineRunPipelineSelect.appendChild(option);
  }

  els.pipelineRunTargetSelect.innerHTML = `<option value="">Auto-select target</option>`;
  for (const target of state.executionTargets) {
    const option = document.createElement("option");
    option.value = String(target.id);
    option.textContent = `${target.display_name} (${target.target_kind}${target.supports_gpu ? ", GPU" : ""})`;
    if (String(target.id) === selectedTargetId) option.selected = true;
    els.pipelineRunTargetSelect.appendChild(option);
  }

  const project = resolvePipelineRunProject();
  const pipeline = resolvePipelineRunPipeline();
  const target = resolvePipelineRunTarget();
  if (els.pipelineRunPythonEnv) {
    const isCustom = String(els.pipelineRunPythonModeSelect?.value || "default") === "custom";
    els.pipelineRunPythonEnv.disabled = !isCustom;
  }
  if (!project && !pipeline) {
    if (els.pipelineRunSelectionSummary) {
      els.pipelineRunSelectionSummary.textContent = "No project or pipeline selected yet.";
    }
    return;
  }
  const projectLabel = project ? project.project_name : "no project";
  const pipelineLabel = pipeline ? pipeline.display_name : "no pipeline";
  const targetLabel = target ? target.display_name : "auto target";
  if (els.pipelineRunSelectionSummary) {
    els.pipelineRunSelectionSummary.textContent = `Project: ${projectLabel} | Pipeline: ${pipelineLabel} | Target: ${targetLabel}`;
  }
  if (els.pipelineRunEditorMode && els.submitPipelineRunButton) {
    if (state.selectedPipelineRun) {
      els.pipelineRunEditorMode.textContent = `Edit mode: ${state.selectedPipelineRun.id}`;
      els.submitPipelineRunButton.textContent = "Save run";
    } else {
      els.pipelineRunEditorMode.textContent = "Create mode.";
      els.submitPipelineRunButton.textContent = "Submit run";
    }
  }
}

function getProjectPageId() {
  const params = new URLSearchParams(window.location.search);
  return params.get("id") || "";
}

function openSelectedProjectPage() {
  if (!state.selectedProject && !state.selectedProjectDetail) {
    return;
  }
  const projectId = state.selectedProjectDetail?.id || state.selectedProject?.id;
  if (!projectId) {
    return;
  }
  window.location.href = `/web/project.html?id=${encodeURIComponent(projectId)}`;
}

function getRawDatasetPageId() {
  const params = new URLSearchParams(window.location.search);
  return params.get("id") || "";
}

function openRawDatasetPage(rawDatasetId) {
  if (!rawDatasetId) {
    return;
  }
  window.location.href = `/web/raw-dataset.html?id=${encodeURIComponent(rawDatasetId)}`;
}

function resetPipelineRunForm() {
  state.selectedPipelineRun = null;
  if (els.pipelineRunProjectSelect) els.pipelineRunProjectSelect.value = "";
  if (els.pipelineRunPipelineSelect) els.pipelineRunPipelineSelect.value = "";
  if (els.pipelineRunTargetSelect) els.pipelineRunTargetSelect.value = "";
  if (els.pipelineRunModeSelect) els.pipelineRunModeSelect.value = "auto";
  if (els.pipelineRunGpuSelect) els.pipelineRunGpuSelect.value = "force_gpu";
  if (els.pipelineRunPythonModeSelect) els.pipelineRunPythonModeSelect.value = "default";
  if (els.pipelineRunPythonEnv) els.pipelineRunPythonEnv.value = "detecdiv_python";
  if (els.pipelineRunId) els.pipelineRunId.value = "";
  if (els.pipelineRunPolicySelect) els.pipelineRunPolicySelect.value = "resume";
  if (els.pipelineRunExistingSelect) els.pipelineRunExistingSelect.value = "replace";
  if (els.pipelineRunCacheSelect) els.pipelineRunCacheSelect.value = "auto";
  if (els.pipelineRunPriority) els.pipelineRunPriority.value = 100;
  if (els.pipelineRunSelectedNodes) els.pipelineRunSelectedNodes.value = "";
  if (els.pipelineRunDescription) els.pipelineRunDescription.value = "";
  if (els.pipelineRunNodeParams) els.pipelineRunNodeParams.value = "[]";
  renderPipelineRuns();
  renderPipelineRunBuilder();
}

function loadPipelineRunIntoForm(run) {
  if (!run) {
    return;
  }
  state.selectedPipelineRun = run;
  const params = run.params_json || {};
  const rr = params.run_request || {};
  const exec = params.execution || {};
  const projectRef = params.project_ref || {};
  const pipelineRef = params.pipeline_ref || {};

  if (els.pipelineRunProjectSelect) {
    els.pipelineRunProjectSelect.value = String(run.project_id || projectRef.project_id || "");
  }

  let selectedPipelineValue = "";
  const candidatePipeline = pipelineRows().find((item) => {
    if (run.pipeline_id && String(item.id) === String(run.pipeline_id)) return true;
    if (pipelineRef.pipeline_key && item.pipeline_key === pipelineRef.pipeline_key) return true;
    const pathHint = pipelineRef.pipeline_json_path || pipelineRef.export_manifest_uri || pipelineRef.pipeline_bundle_uri;
    return Boolean(pathHint) && pipelineRefFromSelection(item).pipeline_json_path === pathHint;
  });
  if (candidatePipeline) {
    state.selectedPipeline = candidatePipeline;
    selectedPipelineValue = pipelineOptionValue(candidatePipeline);
  }
  if (els.pipelineRunPipelineSelect) {
    els.pipelineRunPipelineSelect.value = selectedPipelineValue;
  }
  if (els.pipelineRunTargetSelect) {
    els.pipelineRunTargetSelect.value = String(run.execution_target_id || exec.execution_target_id || "");
  }
  if (els.pipelineRunModeSelect) els.pipelineRunModeSelect.value = String(run.requested_mode || exec.requested_mode || "auto");
  if (els.pipelineRunGpuSelect) els.pipelineRunGpuSelect.value = String(rr.gpu?.mode || "force_gpu");
  if (els.pipelineRunPythonModeSelect) els.pipelineRunPythonModeSelect.value = String(rr.python?.mode || "default");
  if (els.pipelineRunPythonEnv) els.pipelineRunPythonEnv.value = String(rr.python?.env_name || "detecdiv_python");
  if (els.pipelineRunId) els.pipelineRunId.value = String(rr.run_id || "");
  if (els.pipelineRunPolicySelect) els.pipelineRunPolicySelect.value = String(rr.run_policy || "resume");
  if (els.pipelineRunExistingSelect) els.pipelineRunExistingSelect.value = String(rr.existing_data_policy || "replace");
  if (els.pipelineRunCacheSelect) els.pipelineRunCacheSelect.value = String(rr.roi_cache_policy || "auto");
  if (els.pipelineRunPriority) els.pipelineRunPriority.value = Number(run.priority || 100);
  if (els.pipelineRunSelectedNodes) els.pipelineRunSelectedNodes.value = (rr.selected_nodes || []).join(", ");
  if (els.pipelineRunDescription) els.pipelineRunDescription.value = String(rr.description || "");
  if (els.pipelineRunNodeParams) els.pipelineRunNodeParams.value = JSON.stringify(rr.node_params || [], null, 2);
  renderPipelineRuns();
  renderPipelineRunBuilder();
}

function renderPipelineRuns() {
  if (!els.pipelineRunsTableBody) {
    return;
  }
  els.pipelineRunsTableBody.innerHTML = "";
  for (const run of state.pipelineRuns) {
    const tr = document.createElement("tr");
    if (state.selectedPipelineRun && String(state.selectedPipelineRun.id) === String(run.id)) {
      tr.classList.add("selected");
    }
    const rr = run.params_json?.run_request || {};
    const project = state.projects.find((item) => String(item.id) === String(run.project_id));
    const pipeline = [...state.pipelines, ...state.observedPipelines].find((item) => String(item.id || item.identity) === String(run.pipeline_id || ""));
    const target = state.executionTargets.find((item) => String(item.id) === String(run.execution_target_id));
    tr.innerHTML = `
      <td>${run.status}</td>
      <td>${project?.project_name || run.project_id || ""}</td>
      <td>${pipeline?.display_name || run.params_json?.pipeline_ref?.pipeline_key || run.pipeline_id || ""}</td>
      <td>${target?.display_name || run.execution_target_id || "auto"}</td>
      <td>${rr.run_id || ""}</td>
      <td>${formatTimestamp(run.heartbeat_at || run.updated_at || run.created_at)}</td>
    `;
    tr.title = run.error_text || JSON.stringify(run.result_json || {});
    tr.addEventListener("click", () => {
      state.selectedPipelineRun = run;
      renderPipelineRuns();
    });
    tr.addEventListener("dblclick", () => loadPipelineRunIntoForm(run));
    els.pipelineRunsTableBody.appendChild(tr);
  }
  if (els.pipelineRunDetail) {
    if (!state.selectedPipelineRun) {
      els.pipelineRunDetail.textContent = "Select a run to inspect its status.";
    } else {
      renderPipelineRunDetail(state.selectedPipelineRun);
    }
  }
}

function renderPipelineRunDetail(run) {
  if (!els.pipelineRunDetail) {
    return;
  }
  const progress = run.result_json?.progress || {};
  const canCancel = ["queued", "running", "cancelling"].includes(String(run.status || "").toLowerCase());
  const rows = [
    ["Run", run.id],
    ["Status", run.status],
    ["Progress", progress.current_step || progress.phase || run.status],
    ["Heartbeat", formatTimestamp(run.heartbeat_at)],
    ["Updated", formatTimestamp(run.updated_at)],
    ["Started", formatTimestamp(run.started_at)],
    ["Finished", formatTimestamp(run.finished_at)],
  ];
  if (run.error_text) {
    rows.push(["Error", shortText(run.error_text, 700)]);
  }
  const recentLines = [
    ...(progress.recent_stdout || []),
    ...(progress.recent_stderr || []),
  ].slice(-12);
  els.pipelineRunDetail.innerHTML = `
    ${rows.map(([label, value]) => `
      <div class="run-detail-row">
        <span class="run-detail-label">${escapeHtml(label)}</span>
        <span class="run-detail-value">${escapeHtml(value || "")}</span>
      </div>
    `).join("")}
    <div class="toolbar">
      <button type="button" id="open-pipeline-run-json-button">Open JSON</button>
      ${canCancel ? '<button type="button" id="cancel-selected-pipeline-run-button">Cancel run</button>' : ""}
    </div>
    ${recentLines.length ? `
      <div>
        <div class="run-detail-label">Recent MATLAB output</div>
        <pre class="run-progress-lines">${escapeHtml(recentLines.join("\n"))}</pre>
      </div>
    ` : ""}
  `;
  els.pipelineRunDetail.querySelector("#open-pipeline-run-json-button")?.addEventListener("click", () => {
    openJsonInNewTab({
      id: run.id,
      status: run.status,
      requested_mode: run.requested_mode,
      resolved_mode: run.resolved_mode,
      heartbeat_at: run.heartbeat_at,
      updated_at: run.updated_at,
      error_text: run.error_text,
      params_json: run.params_json || {},
      result_json: run.result_json || {},
    }, `pipeline-run-${run.id}.json`);
  });
  els.pipelineRunDetail.querySelector("#cancel-selected-pipeline-run-button")?.addEventListener("click", () => {
    cancelSelectedPipelineRun().catch((error) => {
      setStatus(String(error));
      window.alert(String(error));
    });
  });
}

function shortText(value, maxLength = 300) {
  const text = String(value || "");
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength)}...`;
}

function openJsonInNewTab(value, filename) {
  const blob = new Blob([JSON.stringify(value, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const opened = window.open(url, "_blank", "noopener");
  if (!opened) {
    const link = document.createElement("a");
    link.href = url;
    link.download = filename || "payload.json";
    link.click();
  }
  window.setTimeout(() => URL.revokeObjectURL(url), 60_000);
}

function renderExecutionTargets() {
  if (!els.executionTargetsTableBody) {
    return;
  }
  els.executionTargetsTableBody.innerHTML = "";
  for (const target of state.executionTargets) {
    const tr = document.createElement("tr");
    if (state.selectedExecutionTarget && String(state.selectedExecutionTarget.id) === String(target.id)) {
      tr.classList.add("selected");
    }
    const workerHealth = target.metadata_json?.worker_health || {};
    const maxConcurrentJobs = target.metadata_json?.max_concurrent_jobs || "";
    const matlabMaxThreads = target.metadata_json?.matlab_max_threads || "";
    const healthLabel = workerHealth.current_job_id
      ? `busy (${workerHealth.last_job_status || "running"})`
      : (workerHealth.health || target.status || "unknown");
    tr.innerHTML = `
      <td>${target.display_name}</td>
      <td>${target.target_kind}</td>
      <td>${target.host_name || ""}</td>
      <td>${target.supports_matlab ? "yes" : "no"}</td>
      <td>${target.supports_python ? "yes" : "no"}</td>
      <td>${target.supports_gpu ? "yes" : "no"}</td>
      <td>${maxConcurrentJobs || ""}</td>
      <td>${matlabMaxThreads || ""}</td>
      <td>${target.status}</td>
      <td>${healthLabel}</td>
      <td>${formatTimestamp(workerHealth.last_seen_at || workerHealth.claimed_at || null)}</td>
    `;
    tr.addEventListener("click", () => {
      state.selectedExecutionTarget = target;
      renderExecutionTargets();
    });
    tr.addEventListener("dblclick", () => loadExecutionTargetIntoForm(target));
    els.executionTargetsTableBody.appendChild(tr);
  }
  if (els.executionTargetEditorMode && els.saveExecutionTargetButton) {
    if (state.editingExecutionTarget) {
      els.executionTargetEditorMode.textContent = `Edit mode: ${state.editingExecutionTarget.display_name}`;
      els.saveExecutionTargetButton.textContent = "Save target";
    } else {
      els.executionTargetEditorMode.textContent = "Create mode.";
      els.saveExecutionTargetButton.textContent = "Create target";
    }
  }
  if (els.cancelExecutionTargetEditButton) {
    els.cancelExecutionTargetEditButton.disabled = !state.editingExecutionTarget;
  }
  if (els.executionTargetDetail) {
    if (!state.selectedExecutionTarget) {
      els.executionTargetDetail.textContent = "Select a target to inspect its metadata.";
    } else {
      els.executionTargetDetail.textContent = JSON.stringify(state.selectedExecutionTarget, null, 2);
    }
  }
  renderPipelineRunBuilder();
}

function resolvePipelineRunProject() {
  const projectId = String(els.pipelineRunProjectSelect?.value || state.selectedProject?.id || "");
  return state.projects.find((item) => String(item.id) === projectId) || state.selectedProject || null;
}

function resolvePipelineRunPipeline() {
  const chosen = String(els.pipelineRunPipelineSelect?.value || pipelineOptionValue(state.selectedPipeline) || "");
  const rows = pipelineRows();
  return rows.find((item) => pipelineOptionValue(item) === chosen) || state.selectedPipeline || null;
}

function resolvePipelineRunTarget() {
  const targetId = String(els.pipelineRunTargetSelect?.value || "");
  return state.executionTargets.find((item) => String(item.id) === targetId) || null;
}

function parsePipelineRunSelectedNodes(value) {
  return String(value || "")
    .split(/[\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseOptionalJsonField(rawValue, label, fallbackValue) {
  const text = String(rawValue || "").trim();
  if (!text) {
    return fallbackValue;
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`${label} must be valid JSON. ${error.message}`);
  }
}

function pipelineRefFromSelection(pipeline) {
  const ref = {};
  if (!pipeline) {
    return ref;
  }
  if (pipeline.pipeline_key) {
    ref.pipeline_key = pipeline.pipeline_key;
  }
  const metadata = pipeline.metadata_json || {};
  const observed = metadata.observed || {};
  const pathHints = [
    metadata.pipeline_json_path,
    metadata.pipeline_bundle_uri,
    metadata.export_manifest_uri,
    metadata.pipeline_path,
    observed.pipeline_json_path,
    observed.pipeline_bundle_uri,
    observed.export_manifest_uri,
    observed.pipeline_path,
  ].filter(Boolean);
  const pipelinePath = pathHints[0] || "";
  if (pipelinePath) {
    if (String(pipelinePath).toLowerCase().endsWith("export_manifest.json")) {
      ref.export_manifest_uri = pipelinePath;
    } else {
      ref.pipeline_json_path = pipelinePath;
    }
  }
  return ref;
}

function projectRefFromSelection(project) {
  if (!project) {
    return {};
  }
  const metadata = project.metadata_json || {};
  return {
    project_id: project.id,
    project_key: project.project_key || null,
    project_mat_path: metadata.project_mat_abs || null,
  };
}

function buildPipelineRunPayload() {
  const project = resolvePipelineRunProject();
  if (!project) {
    throw new Error("Select a project before submitting a pipeline run.");
  }
  const pipeline = resolvePipelineRunPipeline();
  if (!pipeline) {
    throw new Error("Select a pipeline before submitting a pipeline run.");
  }

  const requestedMode = String(els.pipelineRunModeSelect?.value || "auto");
  const gpuMode = String(els.pipelineRunGpuSelect?.value || "force_gpu");
  const pythonMode = String(els.pipelineRunPythonModeSelect?.value || "default");
  const pythonEnvName = String(els.pipelineRunPythonEnv?.value || "").trim();
  const priorityValue = Number(els.pipelineRunPriority?.value || 100);
  const nodeParams = parseOptionalJsonField(els.pipelineRunNodeParams?.value, "Node overrides JSON", []);
  if (!Array.isArray(nodeParams)) {
    throw new Error("Node overrides JSON must decode to an array.");
  }

  return {
    project_id: project.id,
    pipeline_id: pipeline.id || null,
    execution_target_id: resolvePipelineRunTarget()?.id || null,
    requested_mode: requestedMode,
    priority: Number.isFinite(priorityValue) ? Math.max(0, Math.floor(priorityValue)) : 100,
    requested_by: state.currentUser?.user_key || state.userKey || null,
    requested_from_host: window.location.hostname || "web-ui",
    project_ref: projectRefFromSelection(project),
    pipeline_ref: pipelineRefFromSelection(pipeline),
    run_request: {
      run_id: String(els.pipelineRunId?.value || "").trim() || null,
      description: String(els.pipelineRunDescription?.value || "").trim() || null,
      selected_nodes: parsePipelineRunSelectedNodes(els.pipelineRunSelectedNodes?.value),
      node_params: nodeParams,
      run_policy: String(els.pipelineRunPolicySelect?.value || "resume"),
      existing_data_policy: String(els.pipelineRunExistingSelect?.value || "replace"),
      roi_cache_policy: String(els.pipelineRunCacheSelect?.value || "auto"),
      python: {
        mode: pythonMode,
        env_name: pythonMode === "custom" ? (pythonEnvName || "detecdiv_python") : null,
      },
      gpu: {
        mode: gpuMode,
      },
    },
    execution: {
      requested_mode: requestedMode,
      execution_target_id: resolvePipelineRunTarget()?.id || null,
      allow_gui: false,
    },
  };
}

async function refreshPipelineRuns() {
  if (!els.pipelineRunsTableBody) {
    return;
  }
  const selectedId = state.selectedPipelineRun?.id || null;
  const projectId = pageFlags.hasProjectPage ? getProjectPageId() : "";
  const path = projectId ? `/pipeline-runs?project_id=${encodeURIComponent(projectId)}` : "/pipeline-runs";
  state.pipelineRuns = await apiGet(path);
  state.selectedPipelineRun = selectedId
    ? state.pipelineRuns.find((item) => String(item.id) === String(selectedId)) || null
    : null;
  renderPipelineRuns();
}

async function refreshExecutionTargets() {
  if (!els.executionTargetsTableBody && !els.pipelineRunTargetSelect) {
    return;
  }
  const selectedId = state.selectedExecutionTarget?.id || null;
  const editingId = state.editingExecutionTarget?.id || null;
  state.executionTargets = await apiGet("/execution-targets");
  state.selectedExecutionTarget = selectedId
    ? state.executionTargets.find((item) => String(item.id) === String(selectedId)) || null
    : null;
  state.editingExecutionTarget = editingId
    ? state.executionTargets.find((item) => String(item.id) === String(editingId)) || null
    : null;
  renderExecutionTargets();
}

async function submitPipelineRun() {
  const payload = buildPipelineRunPayload();
  const project = resolvePipelineRunProject();
  let saved;
  if (state.selectedPipelineRun) {
    saved = await apiPatch(`/pipeline-runs/${state.selectedPipelineRun.id}`, payload);
    setStatus(`Updated pipeline run ${saved.id} for ${project.project_name}.`);
  } else {
    saved = await apiPost("/pipeline-runs", payload);
    setStatus(`Queued pipeline run ${saved.id} for ${project.project_name}.`);
  }
  state.selectedPipelineRun = saved;
  await refreshPipelineRuns();
}

async function cancelSelectedPipelineRun() {
  const run = state.selectedPipelineRun;
  if (!run) {
    throw new Error("Select a pipeline run to cancel.");
  }
  if (!["queued", "running", "cancelling"].includes(String(run.status || "").toLowerCase())) {
    throw new Error(`Cannot cancel a ${run.status} pipeline run.`);
  }
  const confirmed = window.confirm(`Cancel pipeline run ${run.id}?`);
  if (!confirmed) {
    return;
  }
  const saved = await apiPost(`/pipeline-runs/${run.id}/cancel`, {});
  state.selectedPipelineRun = saved;
  setStatus(`Cancellation requested for pipeline run ${saved.id}.`);
  await refreshPipelineRuns();
}

function promptBooleanField(label, currentValue) {
  const answer = window.prompt(`${label} (yes/no)`, currentValue ? "yes" : "no");
  if (answer === null) {
    return null;
  }
  const normalized = answer.trim().toLowerCase();
  if (["yes", "y", "true", "1"].includes(normalized)) return true;
  if (["no", "n", "false", "0"].includes(normalized)) return false;
  throw new Error(`${label} must be yes or no.`);
}

function resetExecutionTargetForm() {
  state.editingExecutionTarget = null;
  if (els.executionTargetName) els.executionTargetName.value = "";
  if (els.executionTargetKey) els.executionTargetKey.value = "";
  if (els.executionTargetKind) els.executionTargetKind.value = "server_gpu";
  if (els.executionTargetHost) els.executionTargetHost.value = "";
  if (els.executionTargetStatus) els.executionTargetStatus.value = "online";
  if (els.executionTargetSupportsMatlab) els.executionTargetSupportsMatlab.value = "true";
  if (els.executionTargetSupportsPython) els.executionTargetSupportsPython.value = "true";
  if (els.executionTargetSupportsGpu) els.executionTargetSupportsGpu.value = "false";
  if (els.executionTargetMaxConcurrentJobs) els.executionTargetMaxConcurrentJobs.value = "1";
  if (els.executionTargetMatlabMaxThreads) els.executionTargetMatlabMaxThreads.value = "";
  if (els.executionTargetMetadataJson) els.executionTargetMetadataJson.value = "{}";
  renderExecutionTargets();
}

function cancelExecutionTargetEdit() {
  state.editingExecutionTarget = null;
  if (state.selectedExecutionTarget) {
    fillExecutionTargetForm(state.selectedExecutionTarget);
  } else {
    resetExecutionTargetForm();
    return;
  }
  renderExecutionTargets();
}

function fillExecutionTargetForm(target) {
  if (!target) {
    return;
  }
  if (els.executionTargetName) els.executionTargetName.value = target.display_name || "";
  if (els.executionTargetKey) els.executionTargetKey.value = target.target_key || "";
  if (els.executionTargetKind) els.executionTargetKind.value = target.target_kind || "";
  if (els.executionTargetHost) els.executionTargetHost.value = target.host_name || "";
  if (els.executionTargetStatus) els.executionTargetStatus.value = target.status || "online";
  if (els.executionTargetSupportsMatlab) els.executionTargetSupportsMatlab.value = target.supports_matlab ? "true" : "false";
  if (els.executionTargetSupportsPython) els.executionTargetSupportsPython.value = target.supports_python ? "true" : "false";
  if (els.executionTargetSupportsGpu) els.executionTargetSupportsGpu.value = target.supports_gpu ? "true" : "false";
  if (els.executionTargetMaxConcurrentJobs) {
    els.executionTargetMaxConcurrentJobs.value = target.metadata_json?.max_concurrent_jobs || "";
  }
  if (els.executionTargetMatlabMaxThreads) {
    els.executionTargetMatlabMaxThreads.value = target.metadata_json?.matlab_max_threads || "";
  }
  if (els.executionTargetMetadataJson) els.executionTargetMetadataJson.value = JSON.stringify(target.metadata_json || {}, null, 2);
}

function loadExecutionTargetIntoForm(target) {
  if (!target) {
    return;
  }
  state.selectedExecutionTarget = target;
  state.editingExecutionTarget = target;
  fillExecutionTargetForm(target);
  renderExecutionTargets();
}

function buildExecutionTargetPayload() {
  const displayName = String(els.executionTargetName?.value || "").trim();
  const targetKey = String(els.executionTargetKey?.value || "").trim();
  const targetKind = String(els.executionTargetKind?.value || "").trim();
  const hostName = String(els.executionTargetHost?.value || "").trim();
  const statusValue = String(els.executionTargetStatus?.value || "").trim();
  if (!displayName) {
    throw new Error("Execution target name is required.");
  }
  if (!targetKind) {
    throw new Error("Execution target kind is required.");
  }
  if (!statusValue) {
    throw new Error("Execution target status is required.");
  }
  const metadata = parseOptionalJsonField(els.executionTargetMetadataJson?.value, "Metadata JSON", {});
  const maxConcurrentJobs = parsePositiveIntegerField(
    els.executionTargetMaxConcurrentJobs?.value,
    "Max concurrent jobs",
  );
  const matlabMaxThreads = parsePositiveIntegerField(
    els.executionTargetMatlabMaxThreads?.value,
    "MATLAB max threads",
  );
  if (maxConcurrentJobs === null) {
    metadata.max_concurrent_jobs = null;
  } else {
    metadata.max_concurrent_jobs = maxConcurrentJobs;
  }
  if (matlabMaxThreads === null) {
    metadata.matlab_max_threads = null;
  } else {
    metadata.matlab_max_threads = matlabMaxThreads;
  }
  return {
    target_key: targetKey || null,
    display_name: displayName,
    target_kind: targetKind,
    host_name: hostName || null,
    supports_matlab: String(els.executionTargetSupportsMatlab?.value || "false") === "true",
    supports_python: String(els.executionTargetSupportsPython?.value || "true") === "true",
    supports_gpu: String(els.executionTargetSupportsGpu?.value || "false") === "true",
    status: statusValue,
    metadata_json: metadata,
  };
}

function parsePositiveIntegerField(rawValue, label) {
  const value = String(rawValue || "").trim();
  if (!value) {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${label} must be a positive integer.`);
  }
  return parsed;
}

async function saveExecutionTarget() {
  const payload = buildExecutionTargetPayload();
  let saved;
  if (state.editingExecutionTarget) {
    saved = await apiPatch(`/execution-targets/${state.editingExecutionTarget.id}`, payload);
    setStatus(`Updated execution target ${saved.display_name}.`);
  } else {
    saved = await apiPost("/execution-targets", payload);
    setStatus(`Created execution target ${saved.display_name}.`);
  }
  state.selectedExecutionTarget = saved;
  state.editingExecutionTarget = saved;
  await refreshExecutionTargets();
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

function visibleRawDatasetIds() {
  return state.rawDatasets.map((raw) => `${raw.id}`);
}

function selectedRawDatasetIds() {
  const visibleIds = new Set(visibleRawDatasetIds());
  return state.selectedRawDatasetIds.filter((rawDatasetId) => visibleIds.has(`${rawDatasetId}`));
}

function isRawDatasetSelected(rawDatasetId) {
  return state.selectedRawDatasetIds.includes(`${rawDatasetId}`);
}

function setSelectedRawDatasetIds(rawDatasetIds) {
  state.selectedRawDatasetIds = [...new Set((rawDatasetIds || []).map((rawDatasetId) => `${rawDatasetId}`))];
  if (state.pendingRawBulkDelete?.scope === "selected") {
    closeRawBulkDeletePanel();
  }
  renderRawDatasets();
  renderRawSelectionControls();
}

function toggleRawDatasetSelection(rawDatasetId, isSelected) {
  const normalizedId = `${rawDatasetId}`;
  const selected = new Set(state.selectedRawDatasetIds);
  if (isSelected) {
    selected.add(normalizedId);
  } else {
    selected.delete(normalizedId);
  }
  setSelectedRawDatasetIds([...selected]);
}

function renderRawSelectionControls() {
  const visibleIds = visibleRawDatasetIds();
  const selectedIds = selectedRawDatasetIds();
  const visibleCount = visibleIds.length;
  const selectedCount = selectedIds.length;

  if (els.rawSelectAll) {
    const allVisibleSelected = visibleCount > 0 && selectedCount === visibleCount;
    const someVisibleSelected = selectedCount > 0 && selectedCount < visibleCount;
    els.rawSelectAll.checked = allVisibleSelected;
    els.rawSelectAll.indeterminate = someVisibleSelected;
    els.rawSelectAll.disabled = visibleCount === 0;
  }
  if (els.rawSelectionSummary) {
    els.rawSelectionSummary.textContent = `${selectedCount} selected / ${visibleCount} visible`;
  }
  if (els.rawClearSelectionButton) {
    els.rawClearSelectionButton.disabled = selectedCount === 0;
  }
  if (els.rawSelectVisibleButton) {
    els.rawSelectVisibleButton.disabled = visibleCount === 0;
  }
  if (els.rawBulkDeleteSelectedButton) {
    els.rawBulkDeleteSelectedButton.disabled = selectedCount === 0;
  }
  if (els.rawBulkDeleteVisibleButton) {
    els.rawBulkDeleteVisibleButton.disabled = visibleCount === 0;
  }
}

function renderRawBulkDeletePanel() {
  if (!els.rawBulkDeletePanel || !els.rawBulkDeleteSummary || !els.rawBulkDeleteConfirmButton) {
    return;
  }
  const pending = state.pendingRawBulkDelete;
  if (!pending || !pending.rawDatasetIds?.length) {
    els.rawBulkDeletePanel.classList.add("hidden");
    return;
  }

  els.rawBulkDeletePanel.classList.remove("hidden");
  const scopeLabel = pending.scope === "selected" ? "selected" : "visible";
  const mode = els.rawBulkDeleteMode?.value || "database_only";
  const deleteLinkedProjects = Boolean(els.rawBulkDeleteLinkedProjects?.checked);
  els.rawBulkDeleteConfirmButton.textContent =
    mode === "source_files" ? "Delete catalog entries and source files" : "Delete catalog entries only";

  if (!pending.preview) {
    els.rawBulkDeleteSummary.textContent = `Preparing deletion preview for ${pending.rawDatasetIds.length} ${scopeLabel} raw dataset(s)...`;
    return;
  }

  const preview = pending.preview;
  const parts = [`${pending.rawDatasetIds.length} ${scopeLabel} raw dataset(s)`];
  if (mode === "source_files") {
    parts.push(`raw source reclaimable ${humanBytes(preview.reclaimableBytes || 0)}`);
  } else {
    parts.push("catalog entries only, source files on disk will be kept");
  }
  if (deleteLinkedProjects) {
    parts.push(`linked projects reclaimable ${humanBytes(preview.linkedProjectBytes || 0)}`);
    if (preview.skippedLinkedProjects) {
      parts.push(`${preview.skippedLinkedProjects} linked project(s) skipped`);
    }
  }
  if (preview.errorCount) {
    parts.push(`${preview.errorCount} preview error(s)`);
  }
  els.rawBulkDeleteSummary.textContent = parts.join(" | ");
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
      <td><input class="raw-select-checkbox" type="checkbox" ${isRawDatasetSelected(raw.id) ? "checked" : ""} /></td>
      <td>${raw.acquisition_label}</td>
      <td>${raw.data_format || "unknown"}</td>
      <td>${raw.owner ? userOptionLabel(raw.owner) : ""}</td>
      <td>${raw.lifecycle_tier}</td>
      <td>${raw.archive_status}</td>
      <td>${raw.status}</td>
      <td>${humanBytes(raw.total_bytes)}</td>
    `;
    tr.querySelector(".raw-select-checkbox")?.addEventListener("click", (event) => {
      event.stopPropagation();
    });
    tr.querySelector(".raw-select-checkbox")?.addEventListener("change", (event) => {
      toggleRawDatasetSelection(raw.id, Boolean(event.target.checked));
    });
    tr.addEventListener("click", () => selectRawDataset(raw.id));
    els.rawDatasetsTableBody.appendChild(tr);
  }
  renderRawSelectionControls();
}

function renderRawDatasetDetail() {
  if (!els.rawDetailEmpty || !els.rawDetailContent || !els.rawDetailSubtitle) {
    return;
  }
  const isDedicatedRawPage = pageFlags.hasRawDatasetPage;
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
    renderRawLocations([]);
    renderRawAnalysisProjects([]);
    renderRawPositions([]);
    renderRawPositionViewer([]);
    return;
  }

  const raw = state.selectedRawDatasetDetail;
  const acquisitionFacts = extractRawAcquisitionFacts(raw);
  const owner = raw.owner ? `${raw.owner.display_name} (${raw.owner.user_key})` : "unknown";
  const linkedProjects = raw.analysis_projects || [];
  const fields = isDedicatedRawPage
    ? [
      ["Acquisition", raw.acquisition_label],
      ["Data type", raw.data_format || "unknown"],
      ["Channels", acquisitionFacts.channelsLabel],
      ["Exposure time (ms)", acquisitionFacts.exposureLabel],
      ["Positions (metadata)", acquisitionFacts.positionsLabel],
      ["Positions (indexed)", `${acquisitionFacts.positionsIndexed}`],
      ["Project owner", owner],
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
    ]
    : [
      ["Acquisition", raw.acquisition_label],
      ["Data type", raw.data_format || "unknown"],
      ["Project owner", owner],
      ["Status", `${raw.status} | ${raw.completeness_status}`],
      ["Storage", humanBytes(raw.total_bytes)],
      ["Projects", `${linkedProjects.length}`],
      ["Positions", `${(raw.positions || []).length}`],
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

  if (els.rawOpenDatasetPageButton) {
    els.rawOpenDatasetPageButton.disabled = false;
    els.rawOpenDatasetPageButton.onclick = () => openRawDatasetPage(raw.id);
  }
  if (els.rawOpenProjectPageButton) {
    const firstProjectId = linkedProjects[0]?.id || "";
    els.rawOpenProjectPageButton.disabled = !firstProjectId;
    els.rawOpenProjectPageButton.onclick = firstProjectId
      ? () => {
          window.location.href = `/web/project.html?id=${encodeURIComponent(firstProjectId)}`;
        }
      : null;
  }

  renderRawLocations(isDedicatedRawPage ? (raw.locations || []) : []);
  renderRawAnalysisProjects(isDedicatedRawPage ? linkedProjects : []);
  renderRawPositions(isDedicatedRawPage ? (raw.positions || []) : []);
  renderRawPositionViewer(isDedicatedRawPage ? (raw.positions || []) : []);
  renderRawLifecycleEvents(isDedicatedRawPage ? (raw.lifecycle_events || []) : []);
  if (els.rawPreviewArchiveButton) els.rawPreviewArchiveButton.disabled = false;
  if (els.rawArchiveButton) els.rawArchiveButton.disabled = false;
  if (els.rawRestoreButton) els.rawRestoreButton.disabled = false;
  els.rawDetailSubtitle.textContent = raw.acquisition_label;
  if (els.rawDatasetPageTitle) {
    els.rawDatasetPageTitle.textContent = raw.acquisition_label;
  }
  els.rawDetailEmpty.classList.add("hidden");
  els.rawDetailContent.classList.remove("hidden");
}

function renderRawLocations(locations) {
  if (!els.rawLocationsList) {
    return;
  }
  els.rawLocationsList.innerHTML = "";
  if (!locations.length) {
    els.rawLocationsList.innerHTML = `<div class="stack-item">No storage location indexed.</div>`;
    return;
  }
  for (const location of locations) {
    const div = document.createElement("div");
    div.className = "stack-item";
    div.innerHTML = `
      <div class="stack-item-meta">${location.storage_root?.name || "unknown root"} | ${location.access_mode}${location.is_preferred ? " | preferred" : ""}</div>
      <div>${location.absolute_path || location.relative_path}</div>
    `;
    els.rawLocationsList.appendChild(div);
  }
}

function renderRawAnalysisProjects(projects) {
  if (!els.rawAnalysisProjectsList) {
    return;
  }
  els.rawAnalysisProjectsList.innerHTML = "";
  if (!projects.length) {
    els.rawAnalysisProjectsList.innerHTML = `<div class="stack-item">No linked analysis project.</div>`;
    return;
  }
  for (const project of projects) {
    const div = document.createElement("div");
    div.className = "stack-item";
    div.innerHTML = `
      <div class="stack-item-meta">${project.health_status || ""} | ${project.owner ? userOptionLabel(project.owner) : "unknown owner"}</div>
      <div>${project.project_name}</div>
      <div class="stack-item-meta">${project.fov_count || 0} FOV | ${humanBytes(project.total_bytes)}</div>
      <div class="stack-actions">
        <button class="open-project-link" data-project-id="${project.id}">Open project</button>
      </div>
    `;
    els.rawAnalysisProjectsList.appendChild(div);
  }
  els.rawAnalysisProjectsList.querySelectorAll(".open-project-link").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      window.location.href = `/web/project.html?id=${encodeURIComponent(button.dataset.projectId || "")}`;
    });
  });
}

function renderRawPositions(positions) {
  if (!els.rawPositionsTableBody) {
    return;
  }
  els.rawPositionsTableBody.innerHTML = "";
  renderRawPreviewProgress(positions);
  if (els.rawQueuePreviewButton) {
    els.rawQueuePreviewButton.disabled = !state.selectedRawDatasetDetail;
  }
  if (!positions.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="4">No positions indexed yet.</td>`;
    els.rawPositionsTableBody.appendChild(tr);
    return;
  }
  syncSelectedRawPosition(positions);
  for (const position of positions) {
    const artifact = position.preview_artifact;
    const tr = document.createElement("tr");
    if (state.selectedRawPositionId === position.id) {
      tr.classList.add("selected");
    }
    const actionCell = artifact?.uri
      ? `<button data-position-id="${position.id}" class="select-position-preview">View</button>`
      : `<button data-position-id="${position.id}" class="queue-position-preview">Queue</button>`;
    tr.innerHTML = `
      <td>${position.display_name || position.position_key}</td>
      <td>${position.status}</td>
      <td>${position.preview_status}</td>
      <td>${actionCell}</td>
    `;
    tr.addEventListener("click", () => {
      state.selectedRawPositionId = position.id;
      renderRawPositions(positions);
      renderRawPositionViewer(positions);
    });
    els.rawPositionsTableBody.appendChild(tr);
  }
  els.rawPositionsTableBody.querySelectorAll(".select-position-preview").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      state.selectedRawPositionId = button.dataset.positionId || null;
      renderRawPositions(positions);
      renderRawPositionViewer(positions);
    });
  });
  els.rawPositionsTableBody.querySelectorAll(".queue-position-preview").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      queueRawPreviewVideo(button.dataset.positionId || "").catch((error) => setStatus(String(error)));
    });
  });
}

function renderRawPreviewProgress(positions) {
  if (!els.rawPreviewProgress) {
    return;
  }
  const counters = {
    queued: 0,
    running: 0,
    ready: 0,
    failed: 0,
    missing: 0,
    other: 0,
  };
  for (const position of positions || []) {
    const status = String(position.preview_status || "").toLowerCase();
    if (status in counters) {
      counters[status] += 1;
    } else {
      counters.other += 1;
    }
  }
  const total = (positions || []).length;
  if (!total) {
    els.rawPreviewProgress.textContent = "No positions indexed yet.";
    return;
  }
  const active = counters.queued + counters.running;
  const text = `Progress: ${counters.ready}/${total} ready | ${counters.running} running | ${counters.queued} queued | ${counters.failed} failed${counters.other ? ` | ${counters.other} other` : ""}`;
  els.rawPreviewProgress.textContent = text;
  els.rawPreviewProgress.classList.toggle("status-running", active > 0);
}

function syncSelectedRawPosition(positions) {
  if (!positions.length) {
    state.selectedRawPositionId = null;
    return;
  }
  const current = positions.find((position) => position.id === state.selectedRawPositionId);
  if (current) {
    return;
  }
  state.selectedRawPositionId = null;
}

function renderRawPositionViewer(positions) {
  if (!els.rawPositionViewerEmpty || !els.rawPositionViewer || !els.rawPositionViewerVideo) {
    return;
  }
  const selected = positions.find((position) => position.id === state.selectedRawPositionId);
  const artifactUrl = selected?.preview_artifact?.uri ? withIdentity(selected.preview_artifact.uri) : "";

  if (!selected) {
    els.rawPositionViewerEmpty.classList.remove("hidden");
    els.rawPositionViewer.classList.add("hidden");
    if (els.rawPositionViewerVideo) {
      els.rawPositionViewerVideo.removeAttribute("src");
      els.rawPositionViewerVideo.load();
    }
    return;
  }

  if (els.rawPositionViewerMeta) {
    els.rawPositionViewerMeta.textContent = `${selected.display_name || selected.position_key} | ${selected.preview_status}`;
  }
  if (artifactUrl) {
    els.rawPositionViewerVideo.src = artifactUrl;
    void els.rawPositionViewerVideo.play().catch(() => {});
    if (els.rawPositionViewerOpenLink) {
      els.rawPositionViewerOpenLink.href = artifactUrl;
      const fileName = `${(selected.position_key || "position").replace(/[^a-zA-Z0-9_-]+/g, "_")}.mp4`;
      els.rawPositionViewerOpenLink.setAttribute("download", fileName);
      els.rawPositionViewerOpenLink.classList.remove("hidden");
    }
  } else {
    els.rawPositionViewerVideo.removeAttribute("src");
    els.rawPositionViewerVideo.load();
    if (els.rawPositionViewerOpenLink) {
      els.rawPositionViewerOpenLink.classList.add("hidden");
    }
  }

  els.rawPositionViewerEmpty.classList.toggle("hidden", Boolean(artifactUrl));
  els.rawPositionViewer.classList.toggle("hidden", !artifactUrl);
}

function renderRawPreviewQualityStatus() {
  if (!els.rawPreviewQualityConfig || !els.rawPreviewQualitySummary || !els.rawPreviewQualityTableBody) {
    return;
  }
  const status = state.rawPreviewQualityStatus;
  els.rawPreviewQualityConfig.innerHTML = "";
  els.rawPreviewQualityTableBody.innerHTML = "";
  if (!status) {
    els.rawPreviewQualitySummary.textContent = "No quality metrics loaded yet.";
    return;
  }

  const config = status.config || {};
  if (els.rawPreviewQualityMaxDimension) els.rawPreviewQualityMaxDimension.value = config.max_dimension ?? "";
  if (els.rawPreviewQualityFps) els.rawPreviewQualityFps.value = config.fps ?? "";
  if (els.rawPreviewQualityFrameMode) els.rawPreviewQualityFrameMode.value = config.frame_mode || "full";
  if (els.rawPreviewQualityMaxFrames) els.rawPreviewQualityMaxFrames.value = config.max_frames ?? "";
  if (els.rawPreviewQualityBinningFactor) {
    const binningFactor = Number(config.binning_factor || 4);
    els.rawPreviewQualityBinningFactor.value = `${binningFactor}x${binningFactor}`;
  }
  if (els.rawPreviewQualityCrf) els.rawPreviewQualityCrf.value = config.crf ?? "";
  if (els.rawPreviewQualityPreset) els.rawPreviewQualityPreset.value = config.preset || "";
  if (els.rawPreviewQualityIncludeExisting) els.rawPreviewQualityIncludeExisting.checked = Boolean(config.include_existing);
  if (els.rawPreviewQualityArtifactRoot) els.rawPreviewQualityArtifactRoot.value = config.artifact_root || "";
  if (els.rawPreviewQualityFfmpegCommand) els.rawPreviewQualityFfmpegCommand.value = config.ffmpeg_command || "";
  updateRawPreviewFrameModeUi();
  const configRows = [
    ["Resolution cap", `${config.max_dimension || 0}px`],
    ["Frames per second", `${config.fps || 0}`],
    ["Frame mode", config.frame_mode || "full"],
    ["Max frames", `${config.max_frames || 0}`],
    ["Binning factor", `${config.binning_factor || 1}x${config.binning_factor || 1}`],
    ["CRF", `${config.crf || 0}`],
    ["Preset", config.preset || ""],
    ["Include existing", config.include_existing ? "yes" : "no"],
    ["Artifact root", config.artifact_root || "dataset-local .detecdiv-previews"],
    ["FFmpeg command", config.ffmpeg_command || "auto-detected"],
  ];
  for (const [label, value] of configRows) {
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.textContent = value;
    els.rawPreviewQualityConfig.append(dt, dd);
  }

  const summary = status.summary || {};
  els.rawPreviewQualitySummary.textContent = [
    `${summary.sample_count || 0} sample(s)`,
    summary.avg_width && summary.avg_height ? `avg ${Math.round(summary.avg_width)}x${Math.round(summary.avg_height)}` : "avg resolution n/a",
    summary.avg_fps ? `avg ${Number(summary.avg_fps).toFixed(2)} fps` : "avg fps n/a",
    summary.avg_bitrate_kbps ? `avg ${Number(summary.avg_bitrate_kbps).toFixed(2)} kbps` : "avg bit rate n/a",
  ].join(" | ");

  for (const sample of status.recent_samples || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${sample.acquisition_label || ""}</td>
      <td>${sample.position_key || ""}</td>
      <td>${sample.width && sample.height ? `${sample.width}x${sample.height}` : ""}</td>
      <td>${sample.fps ?? ""}</td>
      <td>${sample.frame_count ?? ""}</td>
      <td>${sample.duration_seconds != null ? `${Number(sample.duration_seconds).toFixed(2)} s` : ""}</td>
      <td>${sample.file_size_bytes != null ? humanBytes(sample.file_size_bytes) : ""}</td>
      <td>${sample.bitrate_kbps != null ? `${Number(sample.bitrate_kbps).toFixed(2)} kbps` : ""}</td>
      <td>${formatTimestamp(sample.created_at)}</td>
    `;
    els.rawPreviewQualityTableBody.appendChild(tr);
  }
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
      ["Project owner filter", config.owner_key || ""],
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
    els.aclList.innerHTML = `<div class="stack-item">Project owner only.</div>`;
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
    renderProjectRawDatasets();
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
    ["Project owner", owner],
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
    ["Top-level", summarizeList(inventory.top_level_entries, 5)],
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
  renderProjectRawDatasets();
  els.detailSubtitle.textContent = project.project_name;
  els.detailEmpty.classList.add("hidden");
  els.detailContent.classList.remove("hidden");
}

function renderProjectRawDatasets() {
  if (!els.projectRawDatasetsList) {
    return;
  }
  const datasets = state.selectedProjectDetail?.raw_datasets || [];
  if (els.projectQueueRawPreviewsButton) {
    els.projectQueueRawPreviewsButton.disabled = !state.selectedProjectDetail || !datasets.length;
  }
  els.projectRawDatasetsList.innerHTML = "";
  if (!datasets.length) {
    els.projectRawDatasetsList.innerHTML = `<div class="stack-item">No linked raw dataset indexed.</div>`;
    return;
  }
  for (const raw of datasets) {
    const div = document.createElement("div");
    div.className = "stack-item";
    div.innerHTML = `
      <div class="stack-item-meta">${raw.data_format || "unknown"} | ${raw.lifecycle_tier} | ${raw.archive_status} | ${raw.completeness_status}</div>
      <a href="/web/raw-dataset.html?id=${encodeURIComponent(raw.id)}">${raw.acquisition_label}</a>
      <div class="stack-item-meta">${raw.microscope_name || ""} ${humanBytes(raw.total_bytes)}</div>
    `;
    els.projectRawDatasetsList.appendChild(div);
  }
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

function availableOwnerUsers() {
  if (state.users.length) {
    return state.users;
  }
  return state.currentUser ? [state.currentUser] : [];
}

function renderOwnerFilters() {
  const users = availableOwnerUsers();

  if (els.ownerFilter) {
    const currentValue = els.ownerFilter.value || "";
    renderUserSelect(els.ownerFilter, users, {
      emptyOptionLabel: "All project owners",
      selectedValue: currentValue,
    });
  }

  if (els.rawOwnerFilter) {
    const currentValue = els.rawOwnerFilter.value || "";
    renderUserSelect(els.rawOwnerFilter, users, {
      emptyOptionLabel: "All project owners",
      selectedValue: currentValue,
    });
  }
}

function renderIndexOwnerOptions() {
  if (!els.indexOwnerUserKey) {
    return;
  }
  const users = availableOwnerUsers();
  const currentValue = els.indexOwnerUserKey.value || state.currentUser?.user_key || state.userKey || "";
  renderUserSelect(els.indexOwnerUserKey, users, {
    noUsersLabel: "No user accounts available",
    selectedValue: currentValue,
  });
}

function renderUserSelectors() {
  renderOwnerFilters();
  renderIndexOwnerOptions();

  if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
    window.requestAnimationFrame(() => {
      renderOwnerFilters();
      renderIndexOwnerOptions();
    });
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
      state.authMode = session.auth_mode || (state.sessionToken ? "session" : "user_key");
      state.userKey = session.user?.user_key || state.userKey;
      updateSessionUi();
      await refreshDashboard();
      setStatus(`Connected via ${state.authMode} as ${session.user.user_key}.`);
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
  renderUserSelectors();
  renderIndexBrowser();
  if (pageFlags.hasProjectPage) {
    await refreshProjectPage();
  }
  if (pageFlags.hasRawDatasetPage) {
    const rawDatasetId = getRawDatasetPageId();
    if (rawDatasetId) {
      await selectRawDataset(rawDatasetId);
    }
  }

  if (pageFlags.hasIndexForm && browseableStorageRoots().length && !state.indexBrowse?.directories?.length) {
    try {
      await openIndexBrowserPath("");
    } catch (error) {
      setStatus(String(error));
    }
  }

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
  if ((pageFlags.hasExecutionTargetsView && isAdmin()) || els.pipelineRunTargetSelect) {
    refreshTasks.push(refreshExecutionTargets());
  }
  if (pageFlags.hasPipelineRunsView) {
    refreshTasks.push(refreshPipelineRuns());
  }
  if ((pageFlags.hasUsersView && isAdmin()) || els.indexOwnerUserKey || els.ownerFilter || els.rawOwnerFilter || pageFlags.hasProjectPage) {
    refreshTasks.push(refreshUsers());
  }
  if (pageFlags.hasSessionsView && isAdmin()) {
    refreshTasks.push(refreshSessions());
  }
  if (pageFlags.hasIndexingView) {
    refreshTasks.push(refreshIndexingJobs());
  }
  if (pageFlags.hasMigrationView) {
    refreshTasks.push(refreshMigrationPlans());
  }
  if (pageFlags.hasAdminView) {
    refreshTasks.push(refreshRawPreviewQualityStatus());
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
    if (pageFlags.hasRawDatasetPage) {
      const rawDatasetId = getRawDatasetPageId();
      if (rawDatasetId) {
        await selectRawDataset(rawDatasetId);
      }
    }
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
  const visibleIds = new Set(state.rawDatasets.map((raw) => `${raw.id}`));
  state.selectedRawDatasetIds = state.selectedRawDatasetIds.filter((rawDatasetId) => visibleIds.has(`${rawDatasetId}`));
  if (state.pendingRawBulkDelete?.scope === "selected" && !selectedRawDatasetIds().length) {
    closeRawBulkDeletePanel();
  }
  renderRawDatasets();
  renderRawBulkDeletePanel();
  const requestedRawDatasetId = getRawDatasetPageId();
  if (requestedRawDatasetId && (!state.selectedRawDataset || `${state.selectedRawDataset.id}` === requestedRawDatasetId)) {
    await selectRawDataset(requestedRawDatasetId);
  } else if (state.selectedRawDataset) {
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

async function refreshRawPreviewQualityStatus() {
  if (!els.rawPreviewQualityConfig) {
    return;
  }
  if (!isAdmin()) {
    state.rawPreviewQualityStatus = null;
    renderRawPreviewQualityStatus();
    return;
  }
  state.rawPreviewQualityStatus = await apiGet("/raw-datasets/preview-quality");
  renderRawPreviewQualityStatus();
}

async function saveRawPreviewQualitySettings() {
  if (!isAdmin()) {
    throw new Error("Admin role required.");
  }
  const payload = {
    max_dimension: Number(els.rawPreviewQualityMaxDimension?.value || 0),
    fps: Number(els.rawPreviewQualityFps?.value || 0),
    frame_mode: String(els.rawPreviewQualityFrameMode?.value || "full"),
    max_frames: Number(els.rawPreviewQualityMaxFrames?.value || 0),
    binning_factor: parseBinningFactor(els.rawPreviewQualityBinningFactor?.value, 4),
    crf: Number(els.rawPreviewQualityCrf?.value || 0),
    preset: String(els.rawPreviewQualityPreset?.value || "").trim() || null,
    include_existing: Boolean(els.rawPreviewQualityIncludeExisting?.checked),
    artifact_root: String(els.rawPreviewQualityArtifactRoot?.value || "").trim() || null,
    ffmpeg_command: String(els.rawPreviewQualityFfmpegCommand?.value || "").trim() || null,
  };
  if (!Number.isFinite(payload.max_dimension) || payload.max_dimension < 64) {
    throw new Error("Resolution cap must be >= 64.");
  }
  if (!Number.isFinite(payload.fps) || payload.fps < 1) {
    throw new Error("FPS must be >= 1.");
  }
  if (!["full", "limit"].includes(payload.frame_mode)) {
    throw new Error("Frame mode must be full or limit.");
  }
  if (payload.frame_mode === "limit") {
    if (!Number.isFinite(payload.max_frames) || payload.max_frames < 1) {
      throw new Error("Max frames must be >= 1 in limit mode.");
    }
  } else {
    payload.max_frames = 0;
  }
  if (!Number.isFinite(payload.binning_factor) || payload.binning_factor < 1) {
    throw new Error("Binning factor must be >= 1.");
  }
  if (!Number.isFinite(payload.crf) || payload.crf < 16 || payload.crf > 40) {
    throw new Error("CRF must be between 16 and 40.");
  }
  if (!payload.preset) {
    payload.preset = "medium";
  }
  state.rawPreviewQualityStatus = await apiPatch("/raw-datasets/preview-quality", payload);
  renderRawPreviewQualityStatus();
  setStatus("Raw preview quality settings updated.");
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
  state.selectedRawDatasetDetail = await apiGet(`/raw-datasets/${rawDatasetId}`);
  state.selectedRawDataset = raw || state.selectedRawDatasetDetail;
  if ((rawDatasetId === getRawDatasetPageId() || pageFlags.hasRawDatasetPage) && els.rawDetailSubtitle) {
    document.title = `Detecdiv server - ${state.selectedRawDatasetDetail.acquisition_label}`;
  }
  renderRawDatasets();
  renderRawDatasetDetail();
}

async function refreshPipelines() {
  if (!els.pipelinesTableBody) {
    return;
  }
  const selectedId = pipelineIdentity(state.selectedPipeline);
  const params = new URLSearchParams();
  if (els.pipelineSearch?.value.trim()) params.set("search", els.pipelineSearch.value.trim());
  if (els.pipelineRuntimeFilter?.value) params.set("runtime_kind", els.pipelineRuntimeFilter.value);
  const [registry, observed] = await Promise.all([
    apiGet(`/pipelines${params.toString() ? `?${params.toString()}` : ""}`),
    apiGet(`/pipelines/observed${params.toString() ? `?${params.toString()}` : ""}`),
  ]);
  state.pipelines = registry.map((item) => ({ ...item, source: "registry", project_count: item.project_count || 0 }));
  state.observedPipelines = observed;
  state.selectedPipeline = pipelineRows().find((item) => pipelineIdentity(item) === selectedId) || null;
  renderPipelines();
}

async function refreshProjectPage() {
  if (!pageFlags.hasProjectPage) {
    return;
  }
  const projectId = getProjectPageId();
  if (!projectId) {
    state.selectedProject = null;
    state.selectedProjectDetail = null;
    if (els.detailSubtitle) {
      els.detailSubtitle.textContent = "Missing project id";
    }
    renderDetail();
    return;
  }

  const [detail, notes, acl] = await Promise.all([
    apiGet(`/projects/${projectId}`),
    apiGet(`/projects/${projectId}/notes`),
    apiGet(`/projects/${projectId}/acl`),
  ]);
  state.selectedProject = detail;
  state.selectedProjectDetail = detail;
  state.projects = [detail];
  state.notes = notes;
  state.acl = acl;
  if (els.projectPageTitle) {
    els.projectPageTitle.textContent = detail.project_name;
  }
  renderDetail();
  renderPipelineRunBuilder();
}

async function refreshUsers() {
  if (!els.usersTableBody) {
    if (els.indexOwnerUserKey || els.ownerFilter || els.rawOwnerFilter || pageFlags.hasProjectPage) {
      state.users = await apiGet("/users");
      renderUserSelectors();
    }
    return;
  }
  state.users = await apiGet("/users");
  renderUsers();
  renderUserSelectors();
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
  renderPipelineRunBuilder();
}

async function createGroup() {
  const values = await openFormDialog({
    title: "New group",
    description: "Create a project collection for filtering and cleanup.",
    submitLabel: "Create group",
    fields: [
      { name: "displayName", label: "Group name", required: true },
      { name: "groupKey", label: "Group key" },
      { name: "description", label: "Description", type: "textarea", rows: 3 },
    ],
  });
  if (!values?.displayName?.trim()) {
    return;
  }
  const displayName = values.displayName.trim();
  const groupKeyInput = values.groupKey.trim() || displayName.toLowerCase().replace(/[^a-z0-9]+/g, "_");
  await apiPost("/project-groups", {
    display_name: displayName,
    group_key: groupKeyInput,
    description: values.description.trim(),
    metadata_json: {},
  });
  await refreshDashboard();
}

async function addNote() {
  if (!state.selectedProject) {
    return;
  }
  const values = await openFormDialog({
    title: "New note",
    description: state.selectedProject.project_name || "",
    submitLabel: "Add note",
    fields: [
      { name: "noteText", label: "Note", type: "textarea", rows: 5, required: true },
    ],
  });
  const noteText = values?.noteText?.trim();
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
  const ownerOptions = availableOwnerUsers().map((user) => ({
    value: user.user_key,
    label: userOptionLabel(user),
  }));
  if (currentOwner && !ownerOptions.some((option) => option.value === currentOwner)) {
    ownerOptions.unshift({ value: currentOwner, label: currentOwner });
  }
  const values = await openFormDialog({
    title: "Update project",
    description: state.selectedProjectDetail.project_name || "",
    submitLabel: "Save project",
    fields: [
      {
        name: "ownerUserKey",
        label: "Project owner",
        type: ownerOptions.length ? "select" : "text",
        value: currentOwner,
        options: ownerOptions,
      },
      {
        name: "visibility",
        label: "Visibility",
        type: "select",
        value: currentVisibility,
        options: [
          { value: "private", label: "private" },
          { value: "shared", label: "shared" },
          { value: "public", label: "public" },
        ],
      },
    ],
  });
  if (!values) {
    return;
  }
  await apiPatch(`/projects/${state.selectedProjectDetail.id}`, {
    owner_user_key: values.ownerUserKey.trim() || null,
    visibility: values.visibility || currentVisibility,
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
  const currentUserKey = state.currentUser?.user_key || "";
  const userOptions = availableOwnerUsers()
    .filter((user) => user.user_key !== currentUserKey)
    .map((user) => ({ value: user.user_key, label: userOptionLabel(user) }));
  const values = await openFormDialog({
    title: "Share project",
    description: state.selectedProject.project_name || "",
    submitLabel: "Share",
    fields: [
      {
        name: "userKey",
        label: "User",
        type: userOptions.length ? "select" : "text",
        options: userOptions,
        required: true,
      },
      {
        name: "accessLevel",
        label: "Access level",
        type: "select",
        value: "viewer",
        options: [
          { value: "viewer", label: "viewer" },
          { value: "editor", label: "editor" },
        ],
      },
    ],
  });
  const userKey = values?.userKey?.trim();
  if (!userKey) {
    return;
  }
  await apiPost(`/projects/${state.selectedProject.id}/acl`, {
    user_key: userKey,
    access_level: values.accessLevel || "viewer",
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
  const values = await openFormDialog({
    title: "Add to group",
    description: state.selectedProject.project_name || "",
    submitLabel: "Add to group",
    fields: [
      {
        name: "groupId",
        label: "Group",
        type: "select",
        value: state.groups[0]?.id || "",
        options: state.groups.map((group) => ({ value: group.id, label: group.display_name })),
        required: true,
      },
    ],
  });
  if (!values?.groupId) {
    return;
  }
  const selected = state.groups.find((group) => String(group.id) === String(values.groupId));
  if (!selected) {
    throw new Error("Invalid group selection.");
  }
  await apiPost(`/project-groups/${selected.id}/projects/${state.selectedProject.id}`, {});
  await refreshDashboard();
  await selectProject(state.selectedProject.id);
  setStatus(`Added to ${selected.display_name}.`);
}

async function refreshBulkDeletePreview() {
  if (!state.pendingBulkDelete?.projectIds?.length || !els.bulkDeleteMode) {
    return;
  }
  const deleteFiles = els.bulkDeleteMode.value === "project_files";
  const currentToken = state.bulkDeletePreviewToken + 1;
  state.bulkDeletePreviewToken = currentToken;

  if (!deleteFiles) {
    state.pendingBulkDelete.preview = {
      reclaimableBytes: 0,
      errorCount: 0,
    };
    renderBulkDeletePanel();
    return;
  }

  state.pendingBulkDelete.preview = null;
  renderBulkDeletePanel();

  const previews = await Promise.allSettled(
    state.pendingBulkDelete.projectIds.map((projectId) =>
      apiPost(`/projects/${projectId}/deletion-preview`, {
        delete_project_files: true,
        delete_linked_raw_data: false,
        confirm: false,
      })
    )
  );

  if (state.bulkDeletePreviewToken !== currentToken || !state.pendingBulkDelete) {
    return;
  }

  let reclaimableBytes = 0;
  let errorCount = 0;
  for (const result of previews) {
    if (result.status === "fulfilled") {
      reclaimableBytes += Number(result.value.reclaimable_bytes || 0);
    } else {
      errorCount += 1;
    }
  }
  state.pendingBulkDelete.preview = { reclaimableBytes, errorCount };
  renderBulkDeletePanel();
}

async function openBulkDeletePanel(scope) {
  const projectIds = scope === "selected" ? selectedProjectIds() : visibleProjectIds();
  if (!projectIds.length) {
    throw new Error(scope === "selected" ? "Select at least one project first." : "No visible projects to delete.");
  }
  state.pendingBulkDelete = {
    scope,
    projectIds,
    preview: null,
  };
  if (els.bulkDeleteMode) {
    els.bulkDeleteMode.value = "database_only";
  }
  if (els.bulkDeleteConfirmText) {
    els.bulkDeleteConfirmText.value = "";
  }
  renderBulkDeletePanel();
  await refreshBulkDeletePreview();
}

function closeBulkDeletePanel() {
  state.pendingBulkDelete = null;
  state.bulkDeletePreviewToken += 1;
  if (els.bulkDeleteConfirmText) {
    els.bulkDeleteConfirmText.value = "";
  }
  renderBulkDeletePanel();
}

async function executeBulkDelete() {
  if (!state.pendingBulkDelete?.projectIds?.length) {
    return;
  }
  const confirmationText = els.bulkDeleteConfirmText?.value.trim() || "";
  if (confirmationText !== "DELETE") {
    throw new Error("Type DELETE to confirm the bulk deletion.");
  }

  const deleteFiles = els.bulkDeleteMode?.value === "project_files";
  const projectIds = [...state.pendingBulkDelete.projectIds];
  const projectNames = projectIds
    .map((projectId) => state.projects.find((project) => `${project.id}` === `${projectId}`)?.project_name || `${projectId}`);
  const resultSummary = {
    deleted: 0,
    failed: [],
  };

  for (let index = 0; index < projectIds.length; index += 1) {
    const projectId = projectIds[index];
    const projectName = projectNames[index];
    setStatus(`Deleting ${index + 1}/${projectIds.length}: ${projectName}`);
    try {
      await apiDelete(
        `/projects/${projectId}?delete_project_files=${deleteFiles ? "true" : "false"}&delete_linked_raw_data=false&confirm=true`
      );
      resultSummary.deleted += 1;
    } catch (error) {
      resultSummary.failed.push(`${projectName}: ${String(error)}`);
    }
  }

  state.selectedProjectIds = state.selectedProjectIds.filter((projectId) => !projectIds.includes(`${projectId}`));
  if (state.selectedProject && projectIds.includes(`${state.selectedProject.id}`)) {
    state.selectedProject = null;
    state.selectedProjectDetail = null;
  }
  closeBulkDeletePanel();
  await refreshDashboard();

  const suffix = resultSummary.failed.length
    ? ` Failed: ${resultSummary.failed.length}.`
    : "";
  setStatus(`Bulk delete finished. Deleted ${resultSummary.deleted}/${projectIds.length}.${suffix}`);
  if (resultSummary.failed.length) {
    window.alert(`Bulk delete completed with failures:\n${resultSummary.failed.join("\n")}`);
  }
}

async function queueRawPreviewVideosForSelectedProjects() {
  const projectIds = selectedProjectIds();
  if (!projectIds.length) {
    throw new Error("Select at least one project first.");
  }
  const result = await apiPost("/projects/preview-videos/queue-bulk", {
    project_ids: projectIds,
    force: false,
    requested_mode: "auto",
    priority: 100,
    params_json: {},
  });
  setStatus(result.message || `Queued ${result.queued_count} preview job(s).`);
  if (pageFlags.hasRawDatasetsView || pageFlags.hasRawDatasetPage) {
    await refreshRawDatasets();
  }
}

async function refreshRawBulkDeletePreview() {
  if (!state.pendingRawBulkDelete?.rawDatasetIds?.length) {
    return;
  }
  const deleteSourceFiles = els.rawBulkDeleteMode?.value === "source_files";
  const deleteLinkedProjects = Boolean(els.rawBulkDeleteLinkedProjects?.checked);
  const deleteLinkedProjectFiles = Boolean(els.rawBulkDeleteLinkedProjectFiles?.checked);
  const currentToken = state.rawBulkDeletePreviewToken + 1;
  state.rawBulkDeletePreviewToken = currentToken;

  if (!deleteSourceFiles && !deleteLinkedProjects) {
    state.pendingRawBulkDelete.preview = {
      reclaimableBytes: 0,
      linkedProjectBytes: 0,
      skippedLinkedProjects: 0,
      errorCount: 0,
    };
    renderRawBulkDeletePanel();
    return;
  }

  state.pendingRawBulkDelete.preview = null;
  renderRawBulkDeletePanel();

  const previews = await Promise.allSettled(
    state.pendingRawBulkDelete.rawDatasetIds.map((rawDatasetId) =>
      apiPost(`/raw-datasets/${rawDatasetId}/deletion-preview`, {
        delete_source_files: deleteSourceFiles,
        delete_linked_projects: deleteLinkedProjects,
        delete_linked_project_files: deleteLinkedProjectFiles,
        confirm: false,
      })
    )
  );

  if (state.rawBulkDeletePreviewToken !== currentToken || !state.pendingRawBulkDelete) {
    return;
  }

  let reclaimableBytes = 0;
  let linkedProjectBytes = 0;
  let skippedLinkedProjects = 0;
  let errorCount = 0;
  for (const result of previews) {
    if (result.status === "fulfilled") {
      reclaimableBytes += Number(result.value.reclaimable_bytes || 0);
      const linkedProjects = result.value.preview_json?.linked_projects || [];
      for (const linked of linkedProjects) {
        linkedProjectBytes += Number(linked.total_bytes || 0);
      }
      skippedLinkedProjects += Number((result.value.preview_json?.skipped_linked_projects || []).length || 0);
    } else {
      errorCount += 1;
    }
  }
  state.pendingRawBulkDelete.preview = {
    reclaimableBytes,
    linkedProjectBytes,
    skippedLinkedProjects,
    errorCount,
  };
  renderRawBulkDeletePanel();
}

async function openRawBulkDeletePanel(scope) {
  const rawDatasetIds = scope === "selected" ? selectedRawDatasetIds() : visibleRawDatasetIds();
  if (!rawDatasetIds.length) {
    throw new Error(scope === "selected" ? "Select at least one raw dataset first." : "No visible raw datasets to delete.");
  }
  state.pendingRawBulkDelete = {
    scope,
    rawDatasetIds,
    preview: null,
  };
  if (els.rawBulkDeleteMode) {
    els.rawBulkDeleteMode.value = "database_only";
  }
  if (els.rawBulkDeleteLinkedProjects) {
    els.rawBulkDeleteLinkedProjects.checked = false;
  }
  if (els.rawBulkDeleteLinkedProjectFiles) {
    els.rawBulkDeleteLinkedProjectFiles.checked = false;
  }
  if (els.rawBulkDeleteConfirmText) {
    els.rawBulkDeleteConfirmText.value = "";
  }
  renderRawBulkDeletePanel();
  await refreshRawBulkDeletePreview();
}

function closeRawBulkDeletePanel() {
  state.pendingRawBulkDelete = null;
  state.rawBulkDeletePreviewToken += 1;
  if (els.rawBulkDeleteConfirmText) {
    els.rawBulkDeleteConfirmText.value = "";
  }
  renderRawBulkDeletePanel();
}

async function executeRawBulkDelete() {
  if (!state.pendingRawBulkDelete?.rawDatasetIds?.length) {
    return;
  }
  const confirmationText = els.rawBulkDeleteConfirmText?.value.trim() || "";
  if (confirmationText !== "DELETE") {
    throw new Error("Type DELETE to confirm the bulk deletion.");
  }

  const deleteSourceFiles = els.rawBulkDeleteMode?.value === "source_files";
  const deleteLinkedProjects = Boolean(els.rawBulkDeleteLinkedProjects?.checked);
  const deleteLinkedProjectFiles = Boolean(els.rawBulkDeleteLinkedProjectFiles?.checked);
  const rawDatasetIds = [...state.pendingRawBulkDelete.rawDatasetIds];
  const rawDatasetLabels = rawDatasetIds.map(
    (rawDatasetId) =>
      state.rawDatasets.find((raw) => `${raw.id}` === `${rawDatasetId}`)?.acquisition_label || `${rawDatasetId}`
  );
  const resultSummary = {
    deleted: 0,
    failed: [],
  };

  for (let index = 0; index < rawDatasetIds.length; index += 1) {
    const rawDatasetId = rawDatasetIds[index];
    const label = rawDatasetLabels[index];
    setStatus(`Deleting ${index + 1}/${rawDatasetIds.length}: ${label}`);
    try {
      await apiDelete(
        `/raw-datasets/${rawDatasetId}?delete_source_files=${deleteSourceFiles ? "true" : "false"}&delete_linked_projects=${deleteLinkedProjects ? "true" : "false"}&delete_linked_project_files=${deleteLinkedProjectFiles ? "true" : "false"}&confirm=true`
      );
      resultSummary.deleted += 1;
    } catch (error) {
      resultSummary.failed.push(`${label}: ${String(error)}`);
    }
  }

  state.selectedRawDatasetIds = state.selectedRawDatasetIds.filter((rawDatasetId) => !rawDatasetIds.includes(`${rawDatasetId}`));
  if (state.selectedRawDataset && rawDatasetIds.includes(`${state.selectedRawDataset.id}`)) {
    state.selectedRawDataset = null;
    state.selectedRawDatasetDetail = null;
    state.selectedRawPositionId = null;
  }
  closeRawBulkDeletePanel();
  await refreshRawDatasets();

  const suffix = resultSummary.failed.length
    ? ` Failed: ${resultSummary.failed.length}.`
    : "";
  setStatus(`Raw bulk delete finished. Deleted ${resultSummary.deleted}/${rawDatasetIds.length}.${suffix}`);
  if (resultSummary.failed.length) {
    window.alert(`Raw bulk delete completed with failures:\n${resultSummary.failed.join("\n")}`);
  }
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

async function openIndexBrowserPath(relativePath = "") {
  const root = selectedBrowseRoot();
  if (!root) {
    throw new Error("Select a server storage root first.");
  }
  const query = relativePath ? `?relative_path=${encodeURIComponent(relativePath)}` : "";
  state.indexBrowse = await apiGet(`/storage-roots/${root.id}/browse${query}`);
  renderIndexBrowser();
  setStatus(`Opened ${state.indexBrowse.current_absolute_path}.`);
}

function useSelectedIndexFolder() {
  if (!state.indexBrowse || !els.indexSourcePath || !els.indexStorageRootName) {
    return;
  }
  els.indexSourcePath.value = state.indexBrowse.current_absolute_path || "";
  els.indexStorageRootName.value = state.indexBrowse.storage_root?.name || "";
  setStatus(`Selected ${state.indexBrowse.current_absolute_path} for indexing.`);
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
    owner_user_key: els.indexOwnerUserKey?.value || null,
    visibility: els.indexVisibility?.value || "private",
    clear_existing_for_root: Boolean(els.indexClearExisting?.checked),
    metadata_json: {},
  });
  await refreshIndexingJobs();
  setStatus(`Queued indexing job ${response.job.id} for ${response.job.source_path}.`);
}

function parseBulkUserLine(line) {
  const normalized = line.trim();
  if (!normalized || normalized.startsWith("#")) {
    return null;
  }
  let parts = [];
  for (const separator of ["\t", ";", ",", "|"]) {
    if (normalized.includes(separator)) {
      parts = normalized.split(separator).map((value) => value.trim());
      break;
    }
  }
  if (!parts.length) {
    parts = [normalized];
  }
  const [userKey, displayName, email, role] = parts;
  if (!userKey) {
    return null;
  }
  return {
    user_key: userKey,
    display_name: displayName || userKey,
    email: email || null,
    role: role || "user",
    is_active: true,
    metadata_json: {},
  };
}

async function bulkImportUsers() {
  if (!els.bulkImportUsersText) {
    return;
  }
  const rawText = els.bulkImportUsersText.value || "";
  const users = rawText
    .split(/\r?\n/)
    .map((line) => parseBulkUserLine(line))
    .filter((item) => Boolean(item));
  if (!users.length) {
    throw new Error("Paste at least one user line.");
  }
  const response = await apiPost("/users/bulk", { users });
  await refreshUsers();
  if (els.bulkImportUsersText) {
    els.bulkImportUsersText.value = "";
  }
  const message = `Imported accounts: ${response.created_count} created, ${response.updated_count} updated.`;
  setStatus(message);
  window.alert(message);
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

async function queueRawPreviewVideo(positionId = "", force = false) {
  if (!state.selectedRawDatasetDetail) {
    return;
  }
  const payload = {
    position_id: positionId || null,
    force,
    requested_mode: "auto",
    priority: 100,
    params_json: {},
  };
  const result = await apiPost(`/raw-datasets/${state.selectedRawDatasetDetail.id}/preview-videos/queue`, payload);
  await selectRawDataset(state.selectedRawDatasetDetail.id);
  setStatus(result.message || "Raw preview video job queued.");
}

async function regenerateRawPreviewVideos() {
  if (!state.selectedRawDatasetDetail) {
    return;
  }
  const ok = window.confirm("Regenerate all preview movies for this dataset?");
  if (!ok) {
    return;
  }
  await queueRawPreviewVideo("", true);
}

async function queueProjectRawPreviewVideos() {
  if (!state.selectedProjectDetail) {
    return;
  }
  const result = await apiPost(`/projects/${state.selectedProjectDetail.id}/preview-videos/queue`, {
    force: false,
    requested_mode: "auto",
    priority: 100,
    params_json: {},
  });
  await selectProject(state.selectedProjectDetail.id);
  setStatus(result.message || "Project raw preview jobs queued.");
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
  const pipelinePath = window.prompt("Pipeline path (pipeline.json or export_manifest.json)");
  if (pipelinePath === null) {
    return;
  }
  const trimmedPipelinePath = pipelinePath.trim();
  if (!trimmedPipelinePath) {
    throw new Error("Pipeline path is required for import.");
  }
  const pipelineKey = window.prompt("Pipeline key", displayName.toLowerCase().replace(/[^a-z0-9]+/g, "_"));
  if (pipelineKey === null) {
    return;
  }
  const version = window.prompt("Version", "1.0") || "1.0";
  const runtimeKind = window.prompt("Runtime kind (matlab/python/hybrid)", "matlab") || "matlab";
  const metadataJson = String(trimmedPipelinePath).toLowerCase().endsWith("export_manifest.json")
    ? { export_manifest_uri: trimmedPipelinePath }
    : { pipeline_json_path: trimmedPipelinePath };
  await apiPost("/pipelines", {
    display_name: displayName,
    pipeline_key: pipelineKey.trim() || null,
    version,
    runtime_kind: runtimeKind,
    metadata_json: metadataJson,
  });
  await refreshPipelines();
  setStatus(`Imported pipeline ${displayName}.`);
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

async function deletePipeline(pipeline) {
  if (!pipeline?.id) {
    throw new Error("Only registry pipelines can be deleted from the database.");
  }
  const confirmed = window.confirm(
    `Delete pipeline "${pipeline.display_name}" (${pipeline.version || "unknown"}) from the registry database?`
  );
  if (!confirmed) {
    return;
  }
  await apiDelete(`/pipelines/${pipeline.id}`);
  await refreshPipelines();
  setStatus(`Deleted pipeline ${pipeline.display_name} from registry.`);
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
  if (!state.currentUser && !state.userKey) {
    return;
  }
  try {
    const pollTasks = [apiGet("/dashboard/summary").then((summary) => setSummary(summary))];
    if (pageFlags.hasIndexingView) {
      pollTasks.push(refreshIndexingJobs());
    }
    if (pageFlags.hasRawDatasetsView || pageFlags.hasRawDatasetPage) {
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
    if (pageFlags.hasPipelineRunsView) {
      pollTasks.push(refreshPipelineRuns());
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
  if (
    pageFlags.hasProjectsView ||
    pageFlags.hasRawDatasetsView ||
    pageFlags.hasAutomaticArchivePolicy ||
    pageFlags.hasMicroManagerIngest ||
    pageFlags.hasIndexingView ||
    pageFlags.hasPipelinesView ||
    pageFlags.hasUsersView ||
    pageFlags.hasSessionsView ||
    pageFlags.hasProjectPage
  ) {
    await refreshDashboard();
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
if (els.projectSelectAll) els.projectSelectAll.addEventListener("change", () => setSelectedProjectIds(els.projectSelectAll.checked ? visibleProjectIds() : []));
if (els.selectVisibleButton) els.selectVisibleButton.addEventListener("click", () => setSelectedProjectIds(visibleProjectIds()));
if (els.clearSelectionButton) els.clearSelectionButton.addEventListener("click", () => setSelectedProjectIds([]));
if (els.bulkQueuePreviewsSelectedButton) els.bulkQueuePreviewsSelectedButton.addEventListener("click", () => queueRawPreviewVideosForSelectedProjects().catch((error) => setStatus(String(error))));
if (els.bulkDeleteSelectedButton) els.bulkDeleteSelectedButton.addEventListener("click", () => openBulkDeletePanel("selected").catch((error) => setStatus(String(error))));
if (els.bulkDeleteVisibleButton) els.bulkDeleteVisibleButton.addEventListener("click", () => openBulkDeletePanel("visible").catch((error) => setStatus(String(error))));
if (els.bulkDeleteMode) els.bulkDeleteMode.addEventListener("change", () => refreshBulkDeletePreview().catch((error) => setStatus(String(error))));
if (els.bulkDeleteCancelButton) els.bulkDeleteCancelButton.addEventListener("click", () => closeBulkDeletePanel());
if (els.bulkDeleteConfirmButton) els.bulkDeleteConfirmButton.addEventListener("click", () => executeBulkDelete().catch((error) => {
  setStatus(String(error));
  window.alert(String(error));
}));
if (els.rawSearch) els.rawSearch.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.rawOwnerFilter) els.rawOwnerFilter.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.rawTierFilter) els.rawTierFilter.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.rawArchiveStatusFilter) els.rawArchiveStatusFilter.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.rawLimit) els.rawLimit.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.rawOwnedOnly) els.rawOwnedOnly.addEventListener("change", () => refreshRawDatasets().catch((error) => setStatus(String(error))));
if (els.rawSelectAll) els.rawSelectAll.addEventListener("change", () => setSelectedRawDatasetIds(els.rawSelectAll.checked ? visibleRawDatasetIds() : []));
if (els.rawSelectVisibleButton) els.rawSelectVisibleButton.addEventListener("click", () => setSelectedRawDatasetIds(visibleRawDatasetIds()));
if (els.rawClearSelectionButton) els.rawClearSelectionButton.addEventListener("click", () => setSelectedRawDatasetIds([]));
if (els.rawBulkDeleteSelectedButton) els.rawBulkDeleteSelectedButton.addEventListener("click", () => openRawBulkDeletePanel("selected").catch((error) => setStatus(String(error))));
if (els.rawBulkDeleteVisibleButton) els.rawBulkDeleteVisibleButton.addEventListener("click", () => openRawBulkDeletePanel("visible").catch((error) => setStatus(String(error))));
if (els.rawBulkDeleteMode) els.rawBulkDeleteMode.addEventListener("change", () => refreshRawBulkDeletePreview().catch((error) => setStatus(String(error))));
if (els.rawBulkDeleteLinkedProjects) els.rawBulkDeleteLinkedProjects.addEventListener("change", () => refreshRawBulkDeletePreview().catch((error) => setStatus(String(error))));
if (els.rawBulkDeleteLinkedProjectFiles) els.rawBulkDeleteLinkedProjectFiles.addEventListener("change", () => refreshRawBulkDeletePreview().catch((error) => setStatus(String(error))));
if (els.rawBulkDeleteCancelButton) els.rawBulkDeleteCancelButton.addEventListener("click", () => closeRawBulkDeletePanel());
if (els.rawBulkDeleteConfirmButton) els.rawBulkDeleteConfirmButton.addEventListener("click", () => executeRawBulkDelete().catch((error) => {
  setStatus(String(error));
  window.alert(String(error));
}));
if (els.newGroupButton) els.newGroupButton.addEventListener("click", () => createGroup().catch((error) => setStatus(String(error))));
if (els.addNoteButton) els.addNoteButton.addEventListener("click", () => addNote().catch((error) => setStatus(String(error))));
if (els.shareButton) els.shareButton.addEventListener("click", () => shareProject().catch((error) => setStatus(String(error))));
if (els.editProjectButton) els.editProjectButton.addEventListener("click", () => {
  if (pageFlags.hasProjectPage) {
    editProject().catch((error) => setStatus(String(error)));
  } else {
    openSelectedProjectPage();
  }
});
if (els.updateProjectButton) els.updateProjectButton.addEventListener("click", () => editProject().catch((error) => setStatus(String(error))));
if (els.addToGroupButton) els.addToGroupButton.addEventListener("click", () => addSelectedProjectToGroup().catch((error) => setStatus(String(error))));
if (els.projectQueueRawPreviewsButton) els.projectQueueRawPreviewsButton.addEventListener("click", () => queueProjectRawPreviewVideos().catch((error) => setStatus(String(error))));
if (els.previewDeleteButton) els.previewDeleteButton.addEventListener("click", () => previewDelete().catch((error) => setStatus(String(error))));
if (els.indexBrowseRoot) els.indexBrowseRoot.addEventListener("change", () => openIndexBrowserPath("").catch((error) => setStatus(String(error))));
if (els.indexBrowseOpenButton) els.indexBrowseOpenButton.addEventListener("click", () => openIndexBrowserPath(state.indexBrowse?.current_relative_path || "").catch((error) => setStatus(String(error))));
if (els.indexBrowseUseButton) els.indexBrowseUseButton.addEventListener("click", () => useSelectedIndexFolder());
if (els.indexButton) els.indexButton.addEventListener("click", () => runIndexing().catch((error) => setStatus(String(error))));
if (els.indexJobsRefreshButton) els.indexJobsRefreshButton.addEventListener("click", () => refreshIndexingJobs().catch((error) => setStatus(String(error))));
if (els.migrationCreateButton) els.migrationCreateButton.addEventListener("click", () => createMigrationPlan().catch((error) => setStatus(String(error))));
if (els.migrationRefreshButton) els.migrationRefreshButton.addEventListener("click", () => refreshMigrationPlans().catch((error) => setStatus(String(error))));
if (els.migrationExecutePilotButton) els.migrationExecutePilotButton.addEventListener("click", () => executePilotBatch().catch((error) => setStatus(String(error))));
if (els.rawPreviewArchiveButton) els.rawPreviewArchiveButton.addEventListener("click", () => previewRawArchive().catch((error) => setStatus(String(error))));
if (els.rawQueuePreviewButton) els.rawQueuePreviewButton.addEventListener("click", () => queueRawPreviewVideo("").catch((error) => setStatus(String(error))));
if (els.rawRegeneratePreviewButton) els.rawRegeneratePreviewButton.addEventListener("click", () => regenerateRawPreviewVideos().catch((error) => setStatus(String(error))));
if (els.rawPreviewQualityRefreshButton) els.rawPreviewQualityRefreshButton.addEventListener("click", () => refreshRawPreviewQualityStatus().catch((error) => setStatus(String(error))));
if (els.rawPreviewQualitySaveButton) els.rawPreviewQualitySaveButton.addEventListener("click", () => saveRawPreviewQualitySettings().catch((error) => setStatus(String(error))));
if (els.rawPreviewQualityFrameMode) els.rawPreviewQualityFrameMode.addEventListener("change", updateRawPreviewFrameModeUi);
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
if (els.pipelineRunProjectSelect) els.pipelineRunProjectSelect.addEventListener("change", () => renderPipelineRunBuilder());
if (els.pipelineRunPipelineSelect) els.pipelineRunPipelineSelect.addEventListener("change", () => renderPipelineRunBuilder());
if (els.pipelineRunTargetSelect) els.pipelineRunTargetSelect.addEventListener("change", () => renderPipelineRunBuilder());
if (els.pipelineRunPythonModeSelect) els.pipelineRunPythonModeSelect.addEventListener("change", () => renderPipelineRunBuilder());
if (els.refreshPipelineRunsButton) els.refreshPipelineRunsButton.addEventListener("click", () => refreshPipelineRuns().catch((error) => setStatus(String(error))));
if (els.newPipelineRunButton) els.newPipelineRunButton.addEventListener("click", () => resetPipelineRunForm());
if (els.cancelPipelineRunButton) els.cancelPipelineRunButton.addEventListener("click", () => cancelSelectedPipelineRun().catch((error) => {
  setStatus(String(error));
  window.alert(String(error));
}));
if (els.submitPipelineRunButton) els.submitPipelineRunButton.addEventListener("click", () => submitPipelineRun().catch((error) => {
  setStatus(String(error));
  window.alert(String(error));
}));
if (els.refreshExecutionTargetsButton) els.refreshExecutionTargetsButton.addEventListener("click", () => refreshExecutionTargets().catch((error) => setStatus(String(error))));
if (els.newExecutionTargetButton) els.newExecutionTargetButton.addEventListener("click", () => resetExecutionTargetForm());
if (els.cancelExecutionTargetEditButton) els.cancelExecutionTargetEditButton.addEventListener("click", () => cancelExecutionTargetEdit());
if (els.saveExecutionTargetButton) els.saveExecutionTargetButton.addEventListener("click", () => saveExecutionTarget().catch((error) => {
  setStatus(String(error));
  window.alert(String(error));
}));
if (els.newUserButton) els.newUserButton.addEventListener("click", () => createUser().catch((error) => setStatus(String(error))));
if (els.bulkImportUsersButton) els.bulkImportUsersButton.addEventListener("click", () => bulkImportUsers().catch((error) => {
  setStatus(String(error));
  window.alert(String(error));
}));
if (els.refreshSessionsButton) els.refreshSessionsButton.addEventListener("click", () => refreshSessions().catch((error) => setStatus(String(error))));

ensureDashboardPolling();
updateSessionUi();
restoreSession().catch((error) => setStatus(String(error)));
