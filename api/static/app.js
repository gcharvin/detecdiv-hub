const state = {
  userKey: localStorage.getItem("detecdivHub.userKey") || "",
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
  jobs: [],
  pipelineRuns: [],
  sessions: [],
  users: [],
  indexingJobs: [],
  rawDatasets: [],
  rawDatasetsLastRefreshAt: 0,
  selectedRawDataset: null,
  selectedRawDatasetDetail: null,
  selectedRawPositionId: null,
  selectedRawPositionIds: [],
  selectedRawDatasetIds: [],
  pendingRawBulkDelete: null,
  rawBulkDeletePreviewToken: 0,
  pendingRawPositionDelete: null,
  rawPositionDeletePreviewToken: 0,
  rawPreviewQualityStatus: null,
  archiveSettingsStatus: null,
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
  acl: [],
  summary: null,
};

const RAW_DATASETS_POLL_INTERVAL_MS = 15_000;

const els = {
  loginPanel: document.querySelector("#login-panel"),
  loginUserKey: document.querySelector("#login-user-key"),
  loginPassword: document.querySelector("#login-password"),
  loginButton: document.querySelector("#login-button"),
  sessionLabel: document.querySelector("#session-label"),
  sessionBox: document.querySelector(".session-box"),
  logoutButton: document.querySelector("#logout-button"),
  changePasswordButton: document.querySelector("#change-password-button"),
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
  projectNotes: document.querySelector("#project-notes"),
  projectNotesSaveButton: document.querySelector("#project-notes-save-button"),
  aclList: document.querySelector("#acl-list"),
  projectRawDatasetsList: document.querySelector("#project-raw-datasets-list"),
  projectQueueRawPreviewsButton: document.querySelector("#project-queue-raw-previews-button"),
  shareButton: document.querySelector("#share-button"),
  editProjectButton: document.querySelector("#edit-project-button"),
  updateProjectButton: document.querySelector("#update-project-button"),
  changeProjectOwnerButton: document.querySelector("#change-project-owner-button"),
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
  indexBrowseUpButton: document.querySelector("#index-browse-up-button"),
  indexBrowseUseButton: document.querySelector("#index-browse-use-button"),
  indexBrowseCurrent: document.querySelector("#index-browse-current"),
  indexBrowseCurrentText: document.querySelector("#index-browse-current-text"),
  indexBrowseTableBody: document.querySelector("#index-browse-table tbody"),
  indexButton: document.querySelector("#index-button"),
  indexJobsRefreshButton: document.querySelector("#index-jobs-refresh-button"),
  indexJobsClearStaleButton: document.querySelector("#index-jobs-clear-stale-button"),
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
  rawBulkQueuePreviewsSelectedButton: document.querySelector("#raw-bulk-queue-previews-selected-button"),
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
  rawPositionSelectAll: document.querySelector("#raw-position-select-all"),
  rawPositionSelectionSummary: document.querySelector("#raw-position-selection-summary"),
  rawPositionClearSelectionButton: document.querySelector("#raw-position-clear-selection-button"),
  rawPositionDeleteSelectedButton: document.querySelector("#raw-position-delete-selected-button"),
  rawPositionDeletePanel: document.querySelector("#raw-position-delete-panel"),
  rawPositionDeleteSummary: document.querySelector("#raw-position-delete-summary"),
  rawPositionDeleteConfirmText: document.querySelector("#raw-position-delete-confirm-text"),
  rawPositionDeleteCancelButton: document.querySelector("#raw-position-delete-cancel-button"),
  rawPositionDeleteConfirmButton: document.querySelector("#raw-position-delete-confirm-button"),
  rawDatasetsTableBody: document.querySelector("#raw-datasets-table tbody"),
  rawCountLabel: document.querySelector("#raw-count-label"),
  rawDetailEmpty: document.querySelector("#raw-detail-empty"),
  rawDetailContent: document.querySelector("#raw-detail-content"),
  rawDetailSubtitle: document.querySelector("#raw-detail-subtitle"),
  rawDetailList: document.querySelector("#raw-detail-list"),
  rawDatasetNotes: document.querySelector("#raw-dataset-notes"),
  rawDatasetNotesSaveButton: document.querySelector("#raw-dataset-notes-save-button"),
  changeRawOwnerButton: document.querySelector("#change-raw-owner-button"),
  rawLocationsList: document.querySelector("#raw-locations-list"),
  rawAnalysisProjectsList: document.querySelector("#raw-analysis-projects-list"),
  rawPositionsTableBody: document.querySelector("#raw-positions-table tbody"),
  rawQueuePreviewButton: document.querySelector("#raw-queue-preview-button"),
  rawRegeneratePreviewButton: document.querySelector("#raw-regenerate-preview-button"),
  rawPreviewProgress: document.querySelector("#raw-preview-progress"),
  rawOpenDatasetPageButton: document.querySelector("#raw-open-dataset-page-button"),
  rawOpenDisplaySettingsButton: document.querySelector("#raw-open-display-settings-button"),
  rawOpenProjectPageButton: document.querySelector("#raw-open-project-page-button"),
  rawPositionViewerEmpty: document.querySelector("#raw-position-viewer-empty"),
  rawPositionViewer: document.querySelector("#raw-position-viewer"),
  rawPositionViewerMeta: document.querySelector("#raw-position-viewer-meta"),
  rawPositionViewerVideo: document.querySelector("#raw-position-viewer-video"),
  rawPositionViewerOpenLink: document.querySelector("#raw-position-viewer-open-link"),
  rawLifecycleEvents: document.querySelector("#raw-lifecycle-events"),
  rawPreviewArchiveButton: document.querySelector("#raw-preview-archive-button"),
  rawArchiveButton: document.querySelector("#raw-archive-button"),
  rawDeleteArchiveButton: document.querySelector("#raw-delete-archive-button"),
  rawRestoreButton: document.querySelector("#raw-restore-button"),
  rawActionFeedback: document.querySelector("#raw-action-feedback"),
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
  deploymentAwarenessSummary: document.querySelector("#deployment-awareness-summary"),
  deploymentApiStatus: document.querySelector("#deployment-api-status"),
  deploymentDbStatus: document.querySelector("#deployment-db-status"),
  deploymentWorkerStatus: document.querySelector("#deployment-worker-status"),
  deploymentHeartbeatStatus: document.querySelector("#deployment-heartbeat-status"),
  deploymentVersionStatus: document.querySelector("#deployment-version-status"),
  executionTargetHealthWarning: document.querySelector("#execution-target-health-warning"),
  archiveSettingsRefreshButton: document.querySelector("#archive-settings-refresh-button"),
  archiveSettingsSaveButton: document.querySelector("#archive-settings-save-button"),
  archiveSettingsRoot: document.querySelector("#archive-settings-root"),
  archiveSettingsCompression: document.querySelector("#archive-settings-compression"),
  archiveSettingsDeleteSource: document.querySelector("#archive-settings-delete-source"),
  archiveSettingsSummary: document.querySelector("#archive-settings-summary"),
  automaticArchivePolicyPanel: document.querySelector("#automatic-archive-policy-panel"),
  automaticArchivePolicyReportOnly: document.querySelector("#automatic-archive-policy-report-only"),
  automaticArchivePolicyRefreshButton: document.querySelector("#automatic-archive-policy-refresh-button"),
  automaticArchivePolicyRunButton: document.querySelector("#automatic-archive-policy-run-button"),
  automaticArchivePolicySummary: document.querySelector("#automatic-archive-policy-summary"),
  automaticArchivePolicyConfig: document.querySelector("#automatic-archive-policy-config"),
  automaticArchivePolicyRunsTableBody: document.querySelector("#automatic-archive-policy-runs-table tbody"),
  micromanagerIngestPanel: document.querySelector("#micromanager-ingest-panel"),
  micromanagerIngestRootMode: document.querySelector("#micromanager-ingest-root-mode"),
  micromanagerIngestRootPath: document.querySelector("#micromanager-ingest-root-path"),
  micromanagerIngestStorageRootName: document.querySelector("#micromanager-ingest-storage-root-name"),
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
  archivedRawSelectAll: document.querySelector("#archived-raw-select-all"),
  archivedRawSelectVisibleButton: document.querySelector("#archived-raw-select-visible-button"),
  archivedRawClearSelectionButton: document.querySelector("#archived-raw-clear-selection-button"),
  archivedRawDeleteSelectedButton: document.querySelector("#archived-raw-delete-selected-button"),
  archivedRawDatasetsSummary: document.querySelector("#archived-raw-datasets-summary"),
  archivedRawDatasetsTableBody: document.querySelector("#archived-raw-datasets-table tbody"),
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
  openExecutionTargetConfigButton: document.querySelector("#open-execution-target-config-button"),
  applyWorkerInstancesButton: document.querySelector("#apply-worker-instances-button"),
  applyExecutionTargetDrainButton: document.querySelector("#apply-execution-target-drain-button"),
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
  executionTargetWorkerInstances: document.querySelector("#execution-target-worker-instances"),
  executionTargetDrainNewJobs: document.querySelector("#execution-target-drain-new-jobs"),
  executionTargetsTableBody: document.querySelector("#execution-targets-table tbody"),
  executionTargetWorkerSummary: document.querySelector("#execution-target-worker-summary"),
  executionTargetWorkersTableBody: document.querySelector("#execution-target-workers-table tbody"),
  executionTargetJobMixTableBody: document.querySelector("#execution-target-job-mix-table tbody"),
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
const isAdminPage = pageKind === "admin" || pageKind.startsWith("admin-");

const pageFlags = {
  hasAdminView: isAdminPage,
  hasAdminGeneralView: pageKind === "admin" || pageKind === "admin-general",
  hasRawPreviewQualityView: pageKind === "admin-raw-preview-quality",
  hasProjectPage: pageKind === "project",
  hasProjectsView: Boolean(els.projectsTableBody),
  hasProjectGroups: Boolean(els.groupFilter),
  hasStorageRootFilter: Boolean(els.storageRootFilter),
  hasProjectDetail: Boolean(els.detailContent),
  hasPipelinesView: Boolean(els.pipelinesTableBody),
  hasPipelineRunsView: pageKind === "project" && Boolean(els.pipelineRunsTableBody),
  hasExecutionTargetsView: pageKind === "admin-execution-targets" && Boolean(els.executionTargetsTableBody),
  hasUsersView: pageKind === "admin-users" && Boolean(els.usersTableBody),
  hasSessionsView: pageKind === "admin-sessions" && Boolean(els.sessionsTableBody),
  hasIndexingView: Boolean(els.indexJobsTableBody || els.activeIndexJob),
  hasRawDatasetsView: Boolean(els.rawDatasetsTableBody),
  hasRawDatasetPage: pageKind === "raw-dataset",
  hasArchiveSettings: Boolean(els.archiveSettingsSummary),
  hasAutomaticArchivePolicy: Boolean(els.automaticArchivePolicyPanel),
  hasMicroManagerIngest: Boolean(els.micromanagerIngestPanel),
  hasRawDatasetDetail: Boolean(els.rawDetailContent || els.rawDetailList),
  hasIndexForm: Boolean(els.indexSourcePath),
  hasMigrationView: Boolean(els.migrationPlansTableBody || els.migrationDetailContent),
};

let appLayoutInitialized = false;

function getSidebarActiveRoute() {
  if (pageKind === "admin" || pageKind === "admin-general") {
    return "admin-general";
  }
  if (pageKind === "admin-raw-preview-quality") {
    return "admin-raw-preview-quality";
  }
  if (pageKind === "admin-execution-targets") {
    return "admin-execution-targets";
  }
  if (pageKind === "admin-users") {
    return "admin-users";
  }
  if (pageKind === "admin-sessions") {
    return "admin-sessions";
  }
  if (pageKind === "indexing") {
    return "projects-settings";
  }
  if (pageKind === "raw-ops") {
    return "datasets-settings";
  }
  if (pageKind === "raw-datasets" || pageKind === "raw-dataset") {
    return "datasets-catalog";
  }
  return "projects-catalog";
}

function initializeAppLayout() {
  if (appLayoutInitialized) {
    return;
  }
  const pageShell = document.querySelector(".page-shell");
  if (!pageShell) {
    return;
  }

  const existingChildren = Array.from(pageShell.children);
  const existingSidebar = existingChildren.find((child) => child.classList?.contains("sidebar-shell")) || null;
  const existingMainContent = existingChildren.find((child) => child.classList?.contains("main-content")) || null;
  const hero = existingChildren.find((child) => child.classList?.contains("hero")) || null;
  const heroActions = hero?.querySelector(".hero-actions") || null;
  let sidebar = existingSidebar;
  let mainContent = existingMainContent;
  let sidebarMenu = null;
  if (!sidebar || !mainContent) {
    sidebar = document.createElement("aside");
    sidebar.className = "sidebar-shell";
    sidebar.innerHTML = `
      <div class="sidebar-brand">
        <div class="eyebrow">DetecDiv Hub</div>
        <h2>Navigation</h2>
        <p class="muted">Projects, datasets, and admin tools stay available from one place.</p>
      </div>
      <div class="sidebar-auth" data-sidebar-auth></div>
      <nav class="sidebar-menu" aria-label="Primary navigation" data-sidebar-menu></nav>
    `;
    sidebarMenu = sidebar.querySelector("[data-sidebar-menu]") || sidebar.querySelector(".sidebar-menu");

    mainContent = document.createElement("div");
    mainContent.className = "main-content";

    pageShell.replaceChildren(sidebar, mainContent);

    if (els.loginPanel) {
      const sidebarAuth = sidebar.querySelector("[data-sidebar-auth]");
      if (sidebarAuth) {
        sidebarAuth.appendChild(els.loginPanel);
      }
    }

    if (heroActions) {
      const sidebarAuth = sidebar.querySelector("[data-sidebar-auth]");
      if (sidebarAuth) {
        const sessionBox = heroActions.querySelector(".session-box");
        if (sessionBox) {
          sidebarAuth.appendChild(sessionBox);
        }
      }
      heroActions.classList.add("hidden");
    }

    for (const child of existingChildren) {
      if (child === els.loginPanel) {
        continue;
      }
      if (child.classList?.contains("hero-actions")) {
        continue;
      }
      if (child.classList?.contains("hero")) {
        mainContent.appendChild(child);
        continue;
      }
      if (child !== sidebar && child !== mainContent) {
        mainContent.appendChild(child);
      }
    }
  } else {
    const sidebarAuth = sidebar.querySelector("[data-sidebar-auth]") || sidebar.querySelector(".sidebar-auth");
    sidebarMenu = sidebar.querySelector("[data-sidebar-menu]") || sidebar.querySelector(".sidebar-menu");
    if (els.loginPanel && sidebarAuth && els.loginPanel.parentElement !== sidebarAuth) {
      sidebarAuth.appendChild(els.loginPanel);
    }
    if (heroActions && sidebarAuth) {
      const sessionBox = heroActions.querySelector(".session-box");
      if (sessionBox && sessionBox.parentElement !== sidebarAuth) {
        sidebarAuth.appendChild(sessionBox);
      }
      heroActions.classList.add("hidden");
    }
    if (sidebarMenu && !sidebarMenu.childElementCount) {
      sidebarMenu.replaceChildren();
    }
  }

  if (sidebarMenu) {
    const activeRoute = getSidebarActiveRoute();
    const groups = [
      {
        label: "Projects",
        items: [
          { label: "Catalog", href: "/web/", route: "projects-catalog" },
          { label: "Setting", href: "/web/indexing.html", route: "projects-settings" },
        ],
      },
      {
        label: "Datasets",
        items: [
          { label: "Catalog", href: "/web/raw-datasets.html", route: "datasets-catalog" },
          { label: "Settings", href: "/web/raw-ops.html", route: "datasets-settings" },
        ],
      },
    ];

    const fragments = [];
    for (const group of groups) {
      const details = document.createElement("details");
      details.className = "sidebar-group";
      details.open = true;

      const summary = document.createElement("summary");
      summary.textContent = group.label;
      details.appendChild(summary);

      const branch = document.createElement("div");
      branch.className = "sidebar-branch";

      for (const item of group.items) {
        const link = document.createElement("a");
        link.href = item.href;
        link.textContent = item.label;
        link.className = "sidebar-link";
        if (item.route === activeRoute) {
          link.classList.add("active");
          link.setAttribute("aria-current", "page");
        }
        branch.appendChild(link);
      }

      details.appendChild(branch);
      fragments.push(details);
    }

    const adminGroup = document.createElement("details");
    adminGroup.className = "sidebar-group admin-nav-link";
    adminGroup.open = true;
    if (!canAccessAdminPortal()) {
      adminGroup.classList.add("hidden");
    }

    const adminSummary = document.createElement("summary");
    adminSummary.textContent = "Admin";
    adminGroup.appendChild(adminSummary);

    const adminBranch = document.createElement("div");
    adminBranch.className = "sidebar-branch";
    const adminItems = [
      { label: "General", href: "/web/admin.html", route: "admin-general" },
      { label: "Raw Preview Quality", href: "/web/admin-raw-preview-quality.html", route: "admin-raw-preview-quality" },
      { label: "Execution Targets", href: "/web/admin-execution-targets.html", route: "admin-execution-targets" },
      { label: "User Accounts", href: "/web/admin-users.html", route: "admin-users" },
      { label: "Sessions", href: "/web/admin-sessions.html", route: "admin-sessions" },
    ];
    for (const item of adminItems) {
      const link = document.createElement("a");
      link.href = item.href;
      link.textContent = item.label;
      link.className = "sidebar-link";
      if (item.route === activeRoute) {
        link.classList.add("active");
        link.setAttribute("aria-current", "page");
      }
      adminBranch.appendChild(link);
    }
    adminGroup.appendChild(adminBranch);
    fragments.push(adminGroup);

    sidebarMenu.replaceChildren(...fragments);
  }

  appLayoutInitialized = true;
}

function setStatus(message) {
  if (els.statusLine) {
    els.statusLine.textContent = message;
  }
}

function setRawActionFeedback(message, tone = "ok") {
  if (!els.rawActionFeedback) {
    return;
  }
  if (!message) {
    els.rawActionFeedback.textContent = "";
    els.rawActionFeedback.classList.add("hidden");
    els.rawActionFeedback.classList.remove("ok", "warn");
    return;
  }
  els.rawActionFeedback.textContent = message;
  els.rawActionFeedback.classList.remove("hidden");
  els.rawActionFeedback.classList.remove("ok", "warn");
  els.rawActionFeedback.classList.add(tone === "warn" ? "warn" : "ok");
}

function clearDashboardState() {
  state.projects = [];
  state.groups = [];
  state.storageRoots = [];
  state.indexBrowse = null;
  state.pipelines = [];
  state.observedPipelines = [];
  state.executionTargets = [];
  state.jobs = [];
  state.pipelineRuns = [];
  state.sessions = [];
  state.users = [];
  state.indexingJobs = [];
  state.rawDatasets = [];
  state.selectedRawDataset = null;
  state.selectedRawDatasetDetail = null;
  state.selectedRawPositionId = null;
  state.selectedRawPositionIds = [];
  state.selectedRawDatasetIds = [];
  state.pendingRawBulkDelete = null;
  state.rawBulkDeletePreviewToken = 0;
  state.pendingRawPositionDelete = null;
  state.rawPositionDeletePreviewToken = 0;
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
  state.acl = [];
  state.summary = null;
  state.systemHealth = null;
  state.systemHealth = null;

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
  renderDeploymentAwareness();
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

function withCacheBust(path, token) {
  if (!path || !token) {
    return path;
  }
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}v=${encodeURIComponent(token)}`;
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

  const channelSettings = Array.isArray(dimensions.channel_settings) ? dimensions.channel_settings : [];
  const channelSettingsLabel = summarizeChannelSettings(channelSettings);
  const framesLabel = Number(dimensions.frame_count || 0) > 0 ? `${Number(dimensions.frame_count)}` : "unknown";
  const intervalLabel = formatIntervalLabel(dimensions.interval_ms, dimensions.interval_seconds);
  const exposureLabel = channelSettings.length
    ? summarizeChannelMetric(channelSettings, "exposure_ms", "ms")
    : summarizeNumericValues(collectExposureMsValues(metadata), "ms");
  const ledPowerLabel = channelSettings.length
    ? summarizeChannelMetric(channelSettings, "led_power", "")
    : summarizeNumericValues(collectLedPowerValues(metadata), "");

  return {
    positionsIndexed,
    positionsLabel,
    channelsLabel,
    channelSettingsLabel,
    exposureLabel,
    ledPowerLabel,
    framesLabel,
    intervalLabel,
  };
}

function summarizeChannelSettings(channelSettings) {
  if (!Array.isArray(channelSettings) || !channelSettings.length) {
    return "unknown";
  }
  const items = channelSettings
    .map((setting) => {
      const name = String(setting?.channel || "").trim() || `Channel ${Number(setting?.index ?? 0) + 1}`;
      const parts = [];
      if (setting?.exposure_ms != null) parts.push(`Exposure ${Number(setting.exposure_ms).toFixed(3).replace(/\.?0+$/, "")} ms`);
      if (setting?.led_power != null) parts.push(`LED ${Number(setting.led_power).toFixed(3).replace(/\.?0+$/, "")}`);
      if (setting?.interval_ms != null) parts.push(`Interval ${Number(setting.interval_ms).toFixed(3).replace(/\.?0+$/, "")} ms`);
      if (setting?.frames != null) parts.push(`Frames ${Number(setting.frames)}`);
      return parts.length ? `${name}: ${parts.join(" | ")}` : name;
    })
    .filter(Boolean);
  return summarizeList(items, 4);
}

function summarizeChannelMetric(channelSettings, key, suffix) {
  const items = (Array.isArray(channelSettings) ? channelSettings : [])
    .map((setting) => {
      const value = setting?.[key];
      if (value == null || value === "") {
        return "";
      }
      const name = String(setting?.channel || "").trim() || `Channel ${Number(setting?.index ?? 0) + 1}`;
      const numeric = Number(value);
      const text = Number.isFinite(numeric) ? numeric.toFixed(3).replace(/\.?0+$/, "") : String(value);
      return `${name}: ${text}${suffix ? ` ${suffix}` : ""}`;
    })
    .filter(Boolean);
  return items.length ? summarizeList(items, 4) : "unknown";
}

function summarizeNumericValues(values, suffix) {
  const items = (Array.isArray(values) ? values : [])
    .filter((value) => Number.isFinite(Number(value)))
    .map((value) => {
      const numeric = Number(value);
      const text = numeric.toFixed(3).replace(/\.?0+$/, "");
      return `${text}${suffix ? ` ${suffix}` : ""}`;
    });
  return items.length ? summarizeList(items, 4) : "unknown";
}

function formatIntervalLabel(intervalMsValue, intervalSecondsValue) {
  const intervalMs = Number(intervalMsValue);
  if (Number.isFinite(intervalMs) && intervalMs > 0) {
    if (intervalMs >= 1000) {
      return `${Number((intervalMs / 1000)).toFixed(3).replace(/\.?0+$/, "")} s`;
    }
    return `${intervalMs.toFixed(3).replace(/\.?0+$/, "")} ms`;
  }
  const intervalSeconds = Number(intervalSecondsValue);
  if (!Number.isFinite(intervalSeconds) || intervalSeconds <= 0) {
    return "unknown";
  }
  return `${intervalSeconds.toFixed(3).replace(/\.?0+$/, "")} s`;
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

function collectLedPowerValues(metadata) {
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
      if (normalizedKey.includes("led") || normalizedKey.includes("power") || normalizedKey.includes("illumination") || normalizedKey.includes("laser")) {
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

function canAccessAdminPortal() {
  return isAdmin() || Boolean(state.currentUser?.admin_portal_access);
}

function hasAdminPrivileges() {
  return isAdmin();
}

function labStatusLabel(user) {
  return String(user?.lab_status || "yes");
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
  document.body?.classList.toggle("auth-locked", !Boolean(state.currentUser));
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
    } else if (state.authMode === "session") {
      els.sessionLabel.textContent = `Session: ${state.currentUser.display_name} (${state.currentUser.user_key})`;
    } else if (state.authMode === "header") {
      els.sessionLabel.textContent = `Header identity: ${state.currentUser.display_name} (${state.currentUser.user_key})`;
    } else {
      els.sessionLabel.textContent = `Identity: ${state.currentUser.display_name} (${state.currentUser.user_key})`;
    }
  }
  if (els.sessionBox) {
    els.sessionBox.classList.toggle("hidden", !authenticated);
  }
  if (els.userLabel) {
    if (authenticated) {
      const modeLabel = state.authMode === "session" ? "session" : (state.authMode || "identity");
      els.userLabel.textContent = `Signed in: ${state.currentUser.display_name} (${state.currentUser.user_key}) via ${modeLabel}`;
    } else {
      els.userLabel.textContent = "Signed in: not connected";
    }
  }
  if (els.logoutButton) {
    els.logoutButton.disabled = !authenticated;
  }
  if (authenticated) {
    if (!els.changePasswordButton) {
      els.changePasswordButton = document.createElement("button");
      els.changePasswordButton.id = "change-password-button";
      els.changePasswordButton.textContent = "Change password";
      els.changePasswordButton.addEventListener("click", () => {
        changeMyPassword().catch((error) => setStatus(String(error)));
      });
      els.logoutButton?.parentElement?.insertBefore(els.changePasswordButton, els.logoutButton);
    }
    els.changePasswordButton.classList.remove("hidden");
    els.changePasswordButton.disabled = false;
  } else if (els.changePasswordButton) {
    els.changePasswordButton.classList.add("hidden");
    els.changePasswordButton.disabled = true;
  }
  const adminLinks = document.querySelectorAll(".admin-nav-link");
  for (const link of adminLinks) {
    link.classList.toggle("hidden", !canAccessAdminPortal());
  }
  els.adminNavLinks = adminLinks;
  if (pageFlags.hasAdminView) {
    const allowed = canAccessAdminPortal();
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
  if (pageKind !== "raw-ops" && pageKind !== "raw-datasets") {
    if (els.summaryTotalProjects) els.summaryTotalProjects.textContent = summary.total_projects;
    if (els.summaryOwnedProjects) els.summaryOwnedProjects.textContent = summary.owned_projects;
    if (els.summaryTotalBytes) els.summaryTotalBytes.textContent = humanBytes(summary.total_bytes);
    if (els.summaryGroupCount) els.summaryGroupCount.textContent = summary.group_count;
  }
  state.currentUser = summary.user || null;
  updateSessionUi();
}

function renderRawOpsSummary() {
  if (pageKind !== "raw-ops" && pageKind !== "raw-datasets") {
    return;
  }
  const rawItems = Array.isArray(state.rawDatasets) ? state.rawDatasets : [];
  const currentUserKey = state.currentUser?.user_key || state.userKey || "";
  const ownedCount = rawItems.filter((item) => (item.owner || {}).user_key === currentUserKey).length;
  const archivedCount = rawItems.filter((item) => String(item.archive_status || "").toLowerCase() === "archived").length;
  const totalBytes = rawItems.reduce((sum, item) => sum + Number(item.total_bytes || 0), 0);
  if (els.summaryTotalProjects) els.summaryTotalProjects.textContent = `${rawItems.length}`;
  if (els.summaryOwnedProjects) els.summaryOwnedProjects.textContent = `${ownedCount}`;
  if (els.summaryTotalBytes) els.summaryTotalBytes.textContent = humanBytes(totalBytes);
  if (els.summaryGroupCount) els.summaryGroupCount.textContent = `${archivedCount}`;
}

function renderArchiveSettingsStatus() {
  if (!els.archiveSettingsSummary) {
    return;
  }
  const status = state.archiveSettingsStatus;
  if (!status) {
    els.archiveSettingsSummary.textContent = "Archive defaults not loaded yet.";
    return;
  }
  const config = status.config || {};
  if (els.archiveSettingsRoot) {
    els.archiveSettingsRoot.value = config.archive_root || "";
  }
  if (els.archiveSettingsCompression) {
    els.archiveSettingsCompression.value = config.archive_compression || "zip";
  }
  if (els.archiveSettingsDeleteSource) {
    els.archiveSettingsDeleteSource.checked = Boolean(config.delete_hot_source);
  }
  if (els.archivePolicyUri && !els.archivePolicyUri.value) {
    els.archivePolicyUri.placeholder = config.archive_root || "archive root required";
  }
  if (els.archivePolicyCompression) {
    els.archivePolicyCompression.value = config.archive_compression || "zip";
  }
  if (els.archivePolicyDeleteSource) {
    els.archivePolicyDeleteSource.checked = Boolean(config.delete_hot_source);
  }
  const bits = [
    config.archive_root || "no archive root configured",
    config.archive_compression || "zip",
    config.delete_hot_source ? "delete hot source" : "keep hot source",
  ];
  els.archiveSettingsSummary.textContent = bits.join(" | ");
}

function renderArchivedRawDatasets() {
  if (!els.archivedRawDatasetsSummary || !els.archivedRawDatasetsTableBody) {
    return;
  }
  const archivedItems = (state.rawDatasets || []).filter((raw) =>
    ["archived", "restored", "restore_queued"].includes(String(raw.archive_status || "").toLowerCase())
  );
  els.archivedRawDatasetsTableBody.innerHTML = "";
  if (!archivedItems.length) {
    els.archivedRawDatasetsSummary.textContent = "No archived raw datasets in the current selection.";
    renderArchivedRawSelectionControls([]);
    return;
  }
  const archivedBytes = archivedItems.reduce((sum, item) => sum + Number(item.archive_file_bytes || 0), 0);
  els.archivedRawDatasetsSummary.textContent =
    `${archivedItems.length} archived dataset(s) visible | ${humanBytes(archivedBytes)} total`;
  renderArchivedRawSelectionControls(archivedItems);
  for (const raw of archivedItems) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input class="archived-raw-select-checkbox" type="checkbox" ${isRawDatasetSelected(raw.id) ? "checked" : ""} /></td>
      <td>${raw.acquisition_label}</td>
      <td>${raw.owner ? userOptionLabel(raw.owner) : ""}</td>
      <td>${raw.lifecycle_tier}</td>
      <td>${raw.archive_status}</td>
      <td>${raw.archive_compression || ""}</td>
      <td title="${raw.archive_uri || ""}">${raw.archive_uri || ""}</td>
      <td>${humanBytes(raw.archive_file_bytes || 0)}</td>
    `;
    tr.querySelector(".archived-raw-select-checkbox")?.addEventListener("click", (event) => {
      event.stopPropagation();
    });
    tr.querySelector(".archived-raw-select-checkbox")?.addEventListener("change", (event) => {
      toggleRawDatasetSelection(raw.id, Boolean(event.target.checked));
    });
    tr.addEventListener("click", () => openRawDatasetPage(raw.id));
    els.archivedRawDatasetsTableBody.appendChild(tr);
  }
}

function visibleArchivedRawDatasetIds() {
  return (state.rawDatasets || [])
    .filter((raw) => ["archived", "restored", "restore_queued"].includes(String(raw.archive_status || "").toLowerCase()))
    .map((raw) => `${raw.id}`);
}

function renderArchivedRawSelectionControls(archivedItems) {
  const visibleIds = (archivedItems || []).map((raw) => `${raw.id}`);
  const selectedIds = state.selectedRawDatasetIds.filter((rawDatasetId) => visibleIds.includes(`${rawDatasetId}`));
  const visibleCount = visibleIds.length;
  const selectedCount = selectedIds.length;
  if (els.archivedRawSelectAll) {
    const allVisibleSelected = visibleCount > 0 && selectedCount === visibleCount;
    const someVisibleSelected = selectedCount > 0 && selectedCount < visibleCount;
    els.archivedRawSelectAll.checked = allVisibleSelected;
    els.archivedRawSelectAll.indeterminate = someVisibleSelected;
    els.archivedRawSelectAll.disabled = visibleCount === 0;
  }
  if (els.archivedRawSelectVisibleButton) {
    els.archivedRawSelectVisibleButton.disabled = visibleCount === 0;
  }
  if (els.archivedRawClearSelectionButton) {
    els.archivedRawClearSelectionButton.disabled = selectedCount === 0;
  }
  if (els.archivedRawDeleteSelectedButton) {
    els.archivedRawDeleteSelectedButton.disabled = selectedCount === 0;
  }
}

function userOptionLabel(user) {
  return `${user.display_name} (${user.user_key})`;
}

function ownerUserOptions(currentOwnerKey) {
  const options = availableOwnerUsers().map((user) => ({
    value: user.user_key,
    label: userOptionLabel(user),
  }));
  if (currentOwnerKey && !options.some((option) => option.value === currentOwnerKey)) {
    options.unshift({ value: currentOwnerKey, label: currentOwnerKey });
  }
  return options;
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
    if (els.indexBrowseCurrentText) {
      els.indexBrowseCurrentText.textContent = "No browseable server storage roots are registered.";
    } else {
      els.indexBrowseCurrent.textContent = "No browseable server storage roots are registered.";
    }
    els.indexBrowseTableBody.innerHTML = "";
    if (els.indexBrowseUpButton) {
      els.indexBrowseUpButton.disabled = true;
    }
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
    if (els.indexBrowseCurrentText) {
      els.indexBrowseCurrentText.textContent = "No storage root selected.";
    } else {
      els.indexBrowseCurrent.textContent = "No storage root selected.";
    }
    els.indexBrowseTableBody.innerHTML = "";
    if (els.indexBrowseUpButton) {
      els.indexBrowseUpButton.disabled = true;
    }
    return;
  }

  const currentLabel = browse.current_relative_path
    ? `${browse.storage_root.name}: ${browse.current_relative_path}`
    : `${browse.storage_root.name}: /`;
  if (els.indexBrowseCurrentText) {
    els.indexBrowseCurrentText.textContent = `${currentLabel} -> ${browse.current_absolute_path}`;
  } else {
    els.indexBrowseCurrent.textContent = `${currentLabel} -> ${browse.current_absolute_path}`;
  }
  if (els.indexBrowseUpButton) {
    els.indexBrowseUpButton.disabled = browse.parent_relative_path === null;
  }

  els.indexBrowseTableBody.innerHTML = "";
  const canGoUp = browse.parent_relative_path !== null;
  const upRow = document.createElement("tr");
  if (!canGoUp) {
    upRow.classList.add("muted");
  }
  upRow.innerHTML = `
    <td>..</td>
    <td>${escapeHtml(browse.parent_relative_path || "/")}</td>
    <td><button type="button" ${canGoUp ? "" : "disabled"}>${canGoUp ? "Up" : "Root"}</button></td>
  `;
  upRow.querySelector("button")?.addEventListener("click", () => {
    if (!canGoUp) {
      return;
    }
    openIndexBrowserPath(browse.parent_relative_path).catch((error) => setStatus(String(error)));
  });
  els.indexBrowseTableBody.appendChild(upRow);

  for (const directory of browse.directories || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(directory.name)}</td>
      <td>${escapeHtml(directory.relative_path || "/")}</td>
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
    const projectCount = pipeline.project_count || 0;
    const canDelete = source === "registry" && Boolean(pipeline.id);
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

function openRawDisplaySettings(rawDatasetId) {
  if (!rawDatasetId) {
    return;
  }
  window.open(`/raw-datasets/${encodeURIComponent(rawDatasetId)}/display-settings`, "_blank", "noopener,noreferrer");
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
    const workerSnapshot = executionTargetWorkerSnapshot(target);
    const workerHealth = workerSnapshot.workerHealthSummary || {};
    const maxConcurrentJobs = workerSnapshot.maxConcurrentJobs || "";
    const matlabMaxThreads = target.metadata_json?.matlab_max_threads || "";
    const drainNewJobs = Boolean(target.metadata_json?.drain_new_jobs);
    const workerSlots = maxConcurrentJobs || "";
    let healthLabel = workerHealth.health || target.status || "unknown";
    if (drainNewJobs && workerSnapshot.busyWorkerCount > 0) {
      healthLabel = `draining (${workerSnapshot.busyWorkerCount}${workerSlots ? `/${workerSlots}` : ""} slots used)`;
    } else if (drainNewJobs) {
      healthLabel = "drained";
    } else if (workerSnapshot.busyWorkerCount > 0) {
      healthLabel = `busy (${workerSnapshot.busyWorkerCount}${workerSlots ? `/${workerSlots}` : ""} slots used)`;
    } else if (workerHealth.capacity_full) {
      healthLabel = "capacity full";
    }
    tr.innerHTML = `
      <td>${target.display_name}</td>
      <td>${target.target_kind}</td>
      <td>${target.host_name || ""}</td>
      <td>${target.supports_matlab ? "yes" : "no"}</td>
      <td>${target.supports_python ? "yes" : "no"}</td>
      <td>${target.supports_gpu ? "yes" : "no"}</td>
      <td>${maxConcurrentJobs || ""}</td>
      <td>${workerSnapshot.activeWorkerCount}/${workerSnapshot.registeredWorkerCount}</td>
      <td>${matlabMaxThreads || ""}</td>
      <td>${target.status}</td>
      <td>${healthLabel}</td>
      <td>${formatTimestamp(workerSnapshot.heartbeatLabel || workerHealth.last_seen_at || workerHealth.claimed_at || null)}</td>
    `;
    tr.addEventListener("click", () => loadExecutionTargetIntoForm(target));
    els.executionTargetsTableBody.appendChild(tr);
  }
  if (els.executionTargetEditorMode && els.saveExecutionTargetButton) {
    if (state.editingExecutionTarget) {
      els.executionTargetEditorMode.textContent = `Edit mode: ${state.editingExecutionTarget.display_name}`;
      els.saveExecutionTargetButton.textContent = "Save settings";
    } else {
      els.executionTargetEditorMode.textContent = "Create mode.";
      els.saveExecutionTargetButton.textContent = "Save settings";
    }
  }
  if (els.cancelExecutionTargetEditButton) {
    els.cancelExecutionTargetEditButton.disabled = !state.editingExecutionTarget;
  }
  if (els.applyWorkerInstancesButton) {
    els.applyWorkerInstancesButton.disabled = !state.selectedExecutionTarget;
  }
  if (els.applyExecutionTargetDrainButton) {
    els.applyExecutionTargetDrainButton.disabled = !state.selectedExecutionTarget;
    els.applyExecutionTargetDrainButton.textContent = (state.selectedExecutionTarget?.metadata_json?.drain_new_jobs)
      ? "Disable drain"
      : "Apply drain";
  }
  if (els.openExecutionTargetConfigButton) {
    els.openExecutionTargetConfigButton.disabled = !state.selectedExecutionTarget;
  }
  if (!state.selectedExecutionTarget) {
    if (els.executionTargetWorkerSummary) {
      els.executionTargetWorkerSummary.textContent = "Select a target to inspect worker activity.";
    }
    if (els.executionTargetWorkersTableBody) {
      els.executionTargetWorkersTableBody.innerHTML = "";
    }
    if (els.executionTargetJobMixTableBody) {
      els.executionTargetJobMixTableBody.innerHTML = "";
    }
    if (els.executionTargetHealthWarning) {
      els.executionTargetHealthWarning.classList.add("hidden");
      els.executionTargetHealthWarning.textContent = "";
    }
  } else {
    renderExecutionTargetWorkerPanels(state.selectedExecutionTarget);
    const snapshot = executionTargetWorkerSnapshot(state.selectedExecutionTarget);
    if (els.executionTargetHealthWarning) {
      if (snapshot.mismatch.length) {
        els.executionTargetHealthWarning.textContent = `Capacity mismatch: ${snapshot.mismatch.join("; ")}.`;
        els.executionTargetHealthWarning.classList.remove("hidden");
        els.executionTargetHealthWarning.classList.remove("ok");
        els.executionTargetHealthWarning.classList.add("warn");
      } else {
        els.executionTargetHealthWarning.classList.add("hidden");
        els.executionTargetHealthWarning.textContent = "";
        els.executionTargetHealthWarning.classList.remove("warn", "ok");
      }
    }
  }
  renderDeploymentAwareness();
  renderPipelineRunBuilder();
}

function jobKindLabel(job) {
  const raw = typeof job === "string"
    ? job
    : (job?.params_json?.job_kind || job?.job_kind || "generic");
  return String(raw).replaceAll("_", " ");
}

function normalizedJobStatus(job) {
  return String(job?.status || "").trim().toLowerCase();
}

function executionTargetJobs(target) {
  if (!target) {
    return [];
  }
  return state.jobs.filter((job) => String(job.execution_target_id || "") === String(target.id));
}

function latestTimestampValue(...values) {
  let best = null;
  let bestMs = -Infinity;
  for (const value of values) {
    if (!value) {
      continue;
    }
    const ms = Date.parse(value);
    if (Number.isNaN(ms)) {
      continue;
    }
    if (ms > bestMs) {
      bestMs = ms;
      best = value;
    }
  }
  return best;
}

function userLabelForKey(userKey) {
  const normalized = String(userKey || "").trim();
  if (!normalized) {
    return "";
  }
  const users = state.users.length ? state.users : (state.currentUser ? [state.currentUser] : []);
  const user = users.find((item) => item.user_key === normalized);
  return user ? userOptionLabel(user) : normalized;
}

function executionTargetWorkerEntries(target) {
  const workerHealths = target?.metadata_json?.worker_healths || {};
  return Object.entries(workerHealths)
    .map(([workerId, workerHealth]) => ({ workerId, workerHealth: workerHealth || {} }))
    .sort((a, b) => String(a.workerId).localeCompare(String(b.workerId)));
}

function executionTargetWorkerSnapshot(target) {
  const workerEntries = executionTargetWorkerEntries(target);
  const maxConcurrentJobs = target?.metadata_json?.max_concurrent_jobs || "";
  const desiredWorkers = target?.metadata_json?.worker_instances_desired
    ?? target?.metadata_json?.worker_health_summary?.registered_workers
    ?? workerEntries.length
    ?? 0;
  const workerHealthSummary = target?.metadata_json?.worker_health_summary || {};
  const activeWorkerEntries = workerEntries.filter((entry) => {
    const lastSeen = entry.workerHealth.last_seen_at ? Date.parse(entry.workerHealth.last_seen_at) : NaN;
    if (Number.isNaN(lastSeen)) {
      return true;
    }
    return (Date.now() - lastSeen) <= 60000;
  });
  const activeWorkerCount = Number(workerHealthSummary.worker_count || activeWorkerEntries.length || 0);
  const registeredWorkerCount = Number(workerHealthSummary.registered_workers || workerEntries.length || 0);
  const busyWorkerCount = Number(workerHealthSummary.busy_workers || 0);
  const errorWorkerCount = Number(workerHealthSummary.error_workers || 0);
  const staleWorkerCount = Number(workerHealthSummary.stale_workers || Math.max(registeredWorkerCount - activeWorkerCount, 0));
  const heartbeatLabel = latestTimestampValue(
    workerHealthSummary.last_seen_at,
    workerHealthSummary.claimed_at,
    ...workerEntries.map((entry) => entry.workerHealth.last_seen_at),
    ...workerEntries.map((entry) => entry.workerHealth.claimed_at)
  );
  const lastClaimLabel = latestTimestampValue(
    workerHealthSummary.claimed_at,
    ...workerEntries.map((entry) => entry.workerHealth.claimed_at)
  );
  const mismatch = [];
  if (Number(desiredWorkers) && Number(desiredWorkers) !== registeredWorkerCount) {
    mismatch.push(`desired ${desiredWorkers} vs registered ${registeredWorkerCount}`);
  }
  if (registeredWorkerCount !== activeWorkerCount) {
    mismatch.push(`active ${activeWorkerCount}/${registeredWorkerCount}`);
  }
  if (maxConcurrentJobs && busyWorkerCount > Number(maxConcurrentJobs)) {
    mismatch.push(`busy ${busyWorkerCount}/${maxConcurrentJobs}`);
  }
  return {
    workerEntries,
    workerHealthSummary,
    activeWorkerEntries,
    activeWorkerCount,
    registeredWorkerCount,
    desiredWorkers: Number(desiredWorkers) || 0,
    busyWorkerCount,
    errorWorkerCount,
    staleWorkerCount,
    maxConcurrentJobs: maxConcurrentJobs || "",
    heartbeatLabel,
    lastClaimLabel,
    mismatch,
    deploymentVersions: workerHealthSummary.deployment_versions || workerEntries.map((entry) => entry.workerHealth.deployment_version).filter(Boolean),
    codeFingerprints: workerHealthSummary.code_fingerprints || workerEntries.map((entry) => entry.workerHealth.code_fingerprint).filter(Boolean),
    versionDrift: Boolean(workerHealthSummary.version_drift),
  };
}

function deploymentVersionSnapshot() {
  const apiHealth = state.systemHealth || {};
  const targetSnapshots = state.executionTargets.map((target) => ({ target, snapshot: executionTargetWorkerSnapshot(target) }));
  const workerVersions = [];
  const workerFingerprints = [];
  for (const item of targetSnapshots) {
    for (const version of item.snapshot.deploymentVersions || []) {
      if (version && !workerVersions.includes(version)) workerVersions.push(version);
    }
    for (const fingerprint of item.snapshot.codeFingerprints || []) {
      if (fingerprint && !workerFingerprints.includes(fingerprint)) workerFingerprints.push(fingerprint);
    }
  }
  const apiVersion = String(apiHealth.deployment_version || "").trim();
  const apiFingerprint = String(apiHealth.code_fingerprint || "").trim();
  const workerVersionDrift = workerVersions.length > 1 || workerFingerprints.length > 1;
  const apiWorkerMismatch = Boolean(
    (apiVersion && workerVersions.length && !workerVersions.includes(apiVersion))
    || (apiFingerprint && workerFingerprints.length && !workerFingerprints.includes(apiFingerprint))
  );
  let status = "unknown";
  if (!apiVersion && !workerVersions.length) {
    status = "no version metadata yet";
  } else if (workerVersionDrift || apiWorkerMismatch) {
    status = "drift detected";
  } else if (apiVersion || workerVersions.length) {
    status = "in sync";
  }
  const apiLabel = apiVersion || (apiFingerprint ? `fp-${apiFingerprint}` : "unknown");
  const workerLabel = workerVersions.length
    ? workerVersions.join(" / ")
    : workerFingerprints.length
      ? workerFingerprints.map((item) => `fp-${item}`).join(" / ")
      : "unknown";
  return {
    status,
    apiLabel,
    workerLabel,
    workerVersionDrift,
    apiWorkerMismatch,
  };
}

function renderExecutionTargetWorkerPanels(target) {
  const targetJobs = executionTargetJobs(target);
  const snapshot = executionTargetWorkerSnapshot(target);
  const workerEntries = snapshot.workerEntries;
  const queuedJobs = targetJobs.filter((job) => job.status === "queued");
  const cancellingJobs = targetJobs.filter((job) => job.status === "cancelling");

  if (els.executionTargetWorkerSummary) {
    els.executionTargetWorkerSummary.textContent =
      `${snapshot.activeWorkerCount}/${snapshot.registeredWorkerCount} worker records active on ${target.display_name}. `
      + `${snapshot.busyWorkerCount} busy, ${snapshot.staleWorkerCount} stale, ${queuedJobs.length} queued, ${cancellingJobs.length} cancelling.`
      + (snapshot.desiredWorkers ? ` Desired ${snapshot.desiredWorkers}.` : "")
      + (snapshot.maxConcurrentJobs ? ` Max concurrent jobs ${snapshot.maxConcurrentJobs}.` : "");
  }

  if (els.executionTargetWorkersTableBody) {
    els.executionTargetWorkersTableBody.innerHTML = "";
    for (const { workerId, workerHealth } of workerEntries) {
      const tr = document.createElement("tr");
      const currentJobId = String(workerHealth.current_job_id || "");
      const currentJob = currentJobId
        ? targetJobs.find((job) => String(job.id) === currentJobId) || state.jobs.find((job) => String(job.id) === currentJobId) || null
        : null;
      const currentJobKind = currentJob ? jobKindLabel(currentJob) : (workerHealth.current_job_kind || "");
      const currentJobStatus = currentJob?.status || workerHealth.current_job_status || "";
      const currentJobStartedAt = currentJob?.started_at || workerHealth.current_job_started_at || null;
      const currentUserKey = currentJob?.requested_by || "";
      tr.innerHTML = `
        <td>${workerId}</td>
        <td>${workerHealth.health || "unknown"}</td>
        <td>${currentJobId ? `${shortText(currentJobId, 10)}${currentJob ? ` (${shortText(currentJobKind, 18)})` : ""}` : ""}</td>
        <td>${currentUserKey ? userLabelForKey(currentUserKey) : ""}</td>
        <td>${currentJobKind}</td>
        <td>${currentJobStatus}</td>
        <td>${formatTimestamp(currentJobStartedAt)}</td>
        <td>${formatTimestamp(workerHealth.last_seen_at || workerHealth.claimed_at || null)}</td>
        <td>${shortText(workerHealth.last_error || "", 120)}</td>
      `;
      els.executionTargetWorkersTableBody.appendChild(tr);
    }
  }

  if (els.executionTargetJobMixTableBody) {
    els.executionTargetJobMixTableBody.innerHTML = "";
    const grouped = new Map();
    for (const job of targetJobs) {
      const kind = jobKindLabel(job);
      if (!grouped.has(kind)) {
        grouped.set(kind, {
          kind,
          running: 0,
          queued: 0,
          cancelling: 0,
          done: 0,
          failed: 0,
          lastUpdated: null,
        });
      }
      const entry = grouped.get(kind);
      const status = normalizedJobStatus(job);
      if (status === "running") entry.running += 1;
      if (status === "queued") entry.queued += 1;
      if (status === "cancelling") entry.cancelling += 1;
      if (status === "done") entry.done += 1;
      if (status === "failed") entry.failed += 1;
      const updatedAt = latestTimestampValue(job.heartbeat_at, job.updated_at, job.started_at, job.created_at);
      if (!entry.lastUpdated || latestTimestampValue(updatedAt, entry.lastUpdated) === updatedAt) {
        entry.lastUpdated = updatedAt;
      }
    }
    for (const { workerHealth } of workerEntries) {
      const kind = jobKindLabel(workerHealth.current_job_kind || "");
      if (!kind) {
        continue;
      }
      if (!grouped.has(kind)) {
        grouped.set(kind, {
          kind,
          running: 0,
          queued: 0,
          cancelling: 0,
          done: 0,
          failed: 0,
          lastUpdated: null,
        });
      }
      const entry = grouped.get(kind);
      const lastSeenAt = latestTimestampValue(workerHealth.last_seen_at, workerHealth.claimed_at);
      if (!entry.lastUpdated || latestTimestampValue(lastSeenAt, entry.lastUpdated) === lastSeenAt) {
        entry.lastUpdated = lastSeenAt;
      }
    }
    const rows = [...grouped.values()].sort((a, b) => a.kind.localeCompare(b.kind));
    for (const row of rows) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${row.kind}</td>
        <td>${row.running}</td>
        <td>${row.queued}</td>
        <td>${row.cancelling}</td>
        <td>${row.done}</td>
        <td>${row.failed}</td>
        <td>${formatTimestamp(row.lastUpdated)}</td>
      `;
      els.executionTargetJobMixTableBody.appendChild(tr);
    }
  }
}

function deploymentAwarenessSnapshot() {
  const targetSnapshots = state.executionTargets.map((target) => ({
    target,
    snapshot: executionTargetWorkerSnapshot(target),
  }));
  const totalRegistered = targetSnapshots.reduce((sum, item) => sum + item.snapshot.registeredWorkerCount, 0);
  const totalActive = targetSnapshots.reduce((sum, item) => sum + item.snapshot.activeWorkerCount, 0);
  const totalBusy = targetSnapshots.reduce((sum, item) => sum + item.snapshot.busyWorkerCount, 0);
  const totalStale = targetSnapshots.reduce((sum, item) => sum + item.snapshot.staleWorkerCount, 0);
  const latestHeartbeat = latestTimestampValue(...targetSnapshots.map((item) => item.snapshot.heartbeatLabel));
  const latestClaim = latestTimestampValue(...targetSnapshots.map((item) => item.snapshot.lastClaimLabel));
  const deploymentTargets = [
    "webserver-labo",
    "detecdiv-server",
  ];
  return {
    apiStatus: state.systemHealth || null,
    deploymentTargets,
    totalRegistered,
    totalActive,
    totalBusy,
    totalStale,
    latestHeartbeat,
    latestClaim,
  };
}

function renderDeploymentAwareness() {
  if (!els.deploymentAwarenessSummary && !els.deploymentApiStatus) {
    return;
  }
  const snapshot = deploymentAwarenessSnapshot();
  const apiHealth = state.systemHealth || {};
  const dbStatus = apiHealth.database_status || (state.summary ? "reachable" : "unknown");
  const apiStatus = apiHealth.status || "unknown";
  const apiHost = apiHealth.hostname || "unknown host";
  const apiLabel = `${apiStatus} on ${apiHost}`;
  const dbLabel = dbStatus === "ok"
    ? `ok via ${apiHealth.service || "detecdiv-hub"}`
    : dbStatus === "error"
      ? `error${apiHealth.database_message ? `: ${shortText(apiHealth.database_message, 70)}` : ""}`
      : dbStatus;
  const workerLabel = snapshot.totalRegistered
    ? `${snapshot.totalActive}/${snapshot.totalRegistered} active on detecdiv-server`
    : "no worker metadata yet";
  const heartbeatLabel = snapshot.latestHeartbeat
    ? `latest ${formatTimestamp(snapshot.latestHeartbeat)}`
    : "no heartbeat yet";

  if (els.deploymentAwarenessSummary) {
    els.deploymentAwarenessSummary.textContent =
      `Primary API/DB: webserver-labo. Compute workers: detecdiv-server. `
      + `Targets: ${snapshot.deploymentTargets.join(" / ")}.`;
  }
  if (els.deploymentApiStatus) els.deploymentApiStatus.textContent = apiLabel;
  if (els.deploymentDbStatus) els.deploymentDbStatus.textContent = dbLabel;
  if (els.deploymentWorkerStatus) {
    els.deploymentWorkerStatus.textContent = snapshot.totalRegistered
      ? `${workerLabel} | ${snapshot.totalBusy} busy | ${snapshot.totalStale} stale`
      : workerLabel;
  }
  if (els.deploymentHeartbeatStatus) {
    els.deploymentHeartbeatStatus.textContent = `${heartbeatLabel}${snapshot.latestClaim ? ` | last claim ${formatTimestamp(snapshot.latestClaim)}` : ""}`;
  }
  if (els.deploymentVersionStatus) {
    const versionSnapshot = deploymentVersionSnapshot();
    els.deploymentVersionStatus.textContent = `${versionSnapshot.status} | api ${versionSnapshot.apiLabel} | workers ${versionSnapshot.workerLabel}`;
  }
}

async function refreshDeploymentAwareness() {
  if (!els.deploymentAwarenessSummary && !els.deploymentApiStatus) {
    return;
  }
  try {
    state.systemHealth = await apiGet("/health");
  } catch (error) {
    state.systemHealth = {
      status: "error",
      service: "detecdiv-hub",
      database_status: "error",
      database_message: String(error),
      hostname: window.location.hostname || null,
    };
  }
  renderDeploymentAwareness();
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
  const requests = [apiGet("/execution-targets")];
  if (pageFlags.hasExecutionTargetsView) {
    requests.push(apiGet("/jobs"));
  }
  const [targets, jobs = state.jobs] = await Promise.all(requests);
  state.executionTargets = targets;
  state.jobs = jobs;
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
  if (els.executionTargetWorkerInstances) els.executionTargetWorkerInstances.value = "1";
  if (els.executionTargetDrainNewJobs) els.executionTargetDrainNewJobs.checked = false;
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
  if (els.executionTargetWorkerInstances) {
    els.executionTargetWorkerInstances.value = target.metadata_json?.worker_instances_desired
      || target.metadata_json?.worker_health_summary?.worker_count
      || target.metadata_json?.worker_health?.worker_count
      || "";
  }
  if (els.executionTargetDrainNewJobs) {
    els.executionTargetDrainNewJobs.checked = Boolean(target.metadata_json?.drain_new_jobs);
  }
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
  const metadata = {};
  const maxConcurrentJobs = parsePositiveIntegerField(
    els.executionTargetMaxConcurrentJobs?.value,
    "Max concurrent jobs",
  );
  const matlabMaxThreads = parsePositiveIntegerField(
    els.executionTargetMatlabMaxThreads?.value,
    "MATLAB max threads",
  );
  const workerInstances = parsePositiveIntegerField(
    els.executionTargetWorkerInstances?.value,
    "Worker instances",
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
  if (workerInstances === null) {
    delete metadata.worker_instances_desired;
  } else {
    metadata.worker_instances_desired = workerInstances;
  }
  metadata.drain_new_jobs = Boolean(els.executionTargetDrainNewJobs?.checked);
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

async function applyWorkerInstances() {
  if (!state.selectedExecutionTarget) {
    throw new Error("Select an execution target first.");
  }
  const workerInstances = parsePositiveIntegerField(
    els.executionTargetWorkerInstances?.value,
    "Worker instances",
  );
  if (workerInstances === null) {
    throw new Error("Worker instances is required.");
  }
  const response = await apiPost(
    `/execution-targets/${state.selectedExecutionTarget.id}/worker-scale`,
    { worker_instances: workerInstances },
  );
  setStatus(response.message || `Configured ${workerInstances} worker instance(s).`);
  await refreshExecutionTargets();
}

async function applyExecutionTargetDrain() {
  if (!state.selectedExecutionTarget) {
    throw new Error("Select an execution target first.");
  }
  const nextDrainValue = Boolean(els.executionTargetDrainNewJobs?.checked);
  const saved = await apiPatch(`/execution-targets/${state.selectedExecutionTarget.id}`, {
    metadata_json: {
      drain_new_jobs: nextDrainValue,
    },
  });
  setStatus(
    nextDrainValue
      ? `Drain enabled for ${saved.display_name}. Running jobs will finish; no new jobs will be claimed.`
      : `Drain disabled for ${saved.display_name}. New jobs may be claimed again.`
  );
  state.selectedExecutionTarget = saved;
  state.editingExecutionTarget = saved;
  await refreshExecutionTargets();
}

function openExecutionTargetConfig() {
  if (!state.selectedExecutionTarget) {
    throw new Error("Select an execution target first.");
  }
  const url = withIdentity(`/execution-targets/${state.selectedExecutionTarget.id}`);
  window.open(url, "_blank", "noopener");
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
  renderArchivedRawDatasets();
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

function currentRawPositions() {
  return state.selectedRawDatasetDetail?.positions || [];
}

function visibleRawPositionIds() {
  return currentRawPositions().map((position) => `${position.id}`);
}

function selectedRawPositionIds() {
  const visibleIds = new Set(visibleRawPositionIds());
  return state.selectedRawPositionIds.filter((positionId) => visibleIds.has(`${positionId}`));
}

function isRawPositionSelected(positionId) {
  return state.selectedRawPositionIds.includes(`${positionId}`);
}

function setSelectedRawPositionIds(positionIds) {
  state.selectedRawPositionIds = [...new Set((positionIds || []).map((positionId) => `${positionId}`))];
  if (state.pendingRawPositionDelete?.scope === "selected") {
    closeRawPositionDeletePanel();
  }
  renderRawPositions(currentRawPositions());
  renderRawPositionDeletePanel();
}

function toggleRawPositionSelection(positionId, isSelected) {
  const normalizedId = `${positionId}`;
  const selected = new Set(state.selectedRawPositionIds);
  if (isSelected) {
    selected.add(normalizedId);
  } else {
    selected.delete(normalizedId);
  }
  setSelectedRawPositionIds([...selected]);
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
  if (els.rawBulkQueuePreviewsSelectedButton) {
    els.rawBulkQueuePreviewsSelectedButton.disabled = selectedCount === 0;
  }
  if (els.rawBulkDeleteSelectedButton) {
    els.rawBulkDeleteSelectedButton.disabled = selectedCount === 0;
  }
  if (els.rawBulkDeleteVisibleButton) {
    els.rawBulkDeleteVisibleButton.disabled = visibleCount === 0;
  }
}

function renderRawPositionSelectionControls(positions) {
  if (!els.rawPositionSelectAll && !els.rawPositionSelectionSummary && !els.rawPositionClearSelectionButton && !els.rawPositionDeleteSelectedButton) {
    return;
  }
  const visibleCount = positions.length;
  const selectedIds = selectedRawPositionIds();
  const selectedCount = selectedIds.length;

  if (els.rawPositionSelectAll) {
    const allVisibleSelected = visibleCount > 0 && selectedCount === visibleCount;
    const someVisibleSelected = selectedCount > 0 && selectedCount < visibleCount;
    els.rawPositionSelectAll.checked = allVisibleSelected;
    els.rawPositionSelectAll.indeterminate = someVisibleSelected;
    els.rawPositionSelectAll.disabled = visibleCount === 0;
  }
  if (els.rawPositionSelectionSummary) {
    els.rawPositionSelectionSummary.textContent = `${selectedCount} selected / ${visibleCount} positions`;
  }
  if (els.rawPositionClearSelectionButton) {
    els.rawPositionClearSelectionButton.disabled = selectedCount === 0;
  }
  if (els.rawPositionDeleteSelectedButton) {
    els.rawPositionDeleteSelectedButton.disabled = selectedCount === 0;
  }
}

function renderRawPositionDeletePanel() {
  if (!els.rawPositionDeletePanel || !els.rawPositionDeleteSummary || !els.rawPositionDeleteConfirmButton) {
    return;
  }
  const pending = state.pendingRawPositionDelete;
  if (!pending || !pending.positionIds?.length) {
    els.rawPositionDeletePanel.classList.add("hidden");
    return;
  }

  els.rawPositionDeletePanel.classList.remove("hidden");
  els.rawPositionDeleteConfirmButton.textContent = "Delete selected positions";
  if (!pending.preview) {
    els.rawPositionDeleteSummary.textContent = `Preparing deletion preview for ${pending.positionIds.length} selected position(s)...`;
    return;
  }

  const preview = pending.preview;
  const positionList = (preview.positions || [])
    .map((position) => position.display_name || position.position_key || position.position_id)
    .slice(0, 4);
  const linkedProjects = preview.linked_projects || [];
  const warningCount = (preview.warnings || []).length;
  const parts = [`${pending.positionIds.length} selected position(s)`];
  parts.push(`source reclaimable ${humanBytes(preview.reclaimable_bytes || 0)}`);
  if (positionList.length) {
    parts.push(`positions: ${positionList.join(", ")}${preview.positions?.length > positionList.length ? " ..." : ""}`);
  }
  if (linkedProjects.length) {
    const projectNames = linkedProjects
      .map((project) => project.project_name)
      .slice(0, 4);
    parts.push(`linked projects: ${projectNames.join(", ")}${linkedProjects.length > projectNames.length ? " ..." : ""}`);
  }
  if (warningCount) {
    parts.push(`${warningCount} warning(s)`);
  }
  els.rawPositionDeleteSummary.textContent = parts.join(" | ");
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
  renderRawOpsSummary();
  renderArchivedRawDatasets();
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
    renderRawDatasetNotes();
    if (els.changeRawOwnerButton) {
      els.changeRawOwnerButton.classList.add("hidden");
      els.changeRawOwnerButton.disabled = true;
    }
    if (els.rawPreviewArchiveButton) els.rawPreviewArchiveButton.disabled = true;
    if (els.rawArchiveButton) els.rawArchiveButton.disabled = true;
    if (els.rawDeleteArchiveButton) els.rawDeleteArchiveButton.disabled = true;
    if (els.rawRestoreButton) els.rawRestoreButton.disabled = true;
    if (els.rawOpenDisplaySettingsButton) els.rawOpenDisplaySettingsButton.disabled = true;
    setRawActionFeedback("");
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
      ["Channel settings", acquisitionFacts.channelSettingsLabel],
      ["Exposure time (per channel)", acquisitionFacts.exposureLabel],
      ["LED power (per channel)", acquisitionFacts.ledPowerLabel],
      ["Frames", acquisitionFacts.framesLabel],
      ["Interval", acquisitionFacts.intervalLabel],
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
      ["Archive size", humanBytes(raw.archive_file_bytes || 0)],
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

  if (els.changeRawOwnerButton) {
    const canChangeOwner = isAdmin();
    els.changeRawOwnerButton.classList.toggle("hidden", !canChangeOwner);
    els.changeRawOwnerButton.disabled = !canChangeOwner;
  }

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
  if (els.rawOpenDisplaySettingsButton) {
    const hasDisplaySettings = Boolean(raw.display_settings_uri);
    els.rawOpenDisplaySettingsButton.disabled = !hasDisplaySettings;
    els.rawOpenDisplaySettingsButton.onclick = hasDisplaySettings ? () => openRawDisplaySettings(raw.id) : null;
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
  renderRawDatasetNotes();
  renderRawAnalysisProjects(isDedicatedRawPage ? linkedProjects : []);
  renderRawPositions(isDedicatedRawPage ? (raw.positions || []) : []);
  renderRawPositionViewer(isDedicatedRawPage ? (raw.positions || []) : []);
  renderRawLifecycleEvents(isDedicatedRawPage ? (raw.lifecycle_events || []) : []);
  if (els.rawPreviewArchiveButton) els.rawPreviewArchiveButton.disabled = false;
  if (els.rawArchiveButton) els.rawArchiveButton.disabled = false;
  if (els.rawDeleteArchiveButton) els.rawDeleteArchiveButton.disabled = !raw.archive_uri;
  if (els.rawRestoreButton) els.rawRestoreButton.disabled = false;
  if (raw.archive_status === "archive_queued") {
    setRawActionFeedback("Archive request queued. Waiting for worker execution.", "ok");
  } else if (raw.archive_status === "restore_queued") {
    setRawActionFeedback("Restore request queued. Waiting for worker execution.", "ok");
  } else if (raw.archive_status === "archive_failed" || raw.archive_status === "restore_failed") {
    setRawActionFeedback(`Latest lifecycle action failed: ${raw.archive_status}.`, "warn");
  } else {
    setRawActionFeedback("");
  }
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
  if (els.rawRegeneratePreviewButton) {
    els.rawRegeneratePreviewButton.disabled = !state.selectedRawDatasetDetail;
  }
  if (!positions.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="5">No positions indexed yet.</td>`;
    els.rawPositionsTableBody.appendChild(tr);
    renderRawPositionSelectionControls(positions);
    renderRawPositionDeletePanel();
    return;
  }
  syncSelectedRawPosition(positions);
  for (const position of positions) {
    const artifact = position.preview_artifact;
    const tr = document.createElement("tr");
    if (state.selectedRawPositionId === position.id) {
      tr.classList.add("selected");
    }
    const actionLabel = artifact?.uri ? "Regenerate MP4" : "Queue MP4";
    tr.innerHTML = `
      <td><input class="raw-position-select-checkbox" type="checkbox" ${isRawPositionSelected(position.id) ? "checked" : ""} /></td>
      <td>${position.display_name || position.position_key}</td>
      <td>${position.status}</td>
      <td>${position.preview_status}</td>
      <td><button data-position-id="${position.id}" class="queue-position-preview">${actionLabel}</button></td>
    `;
    tr.addEventListener("click", () => {
      state.selectedRawPositionId = position.id;
      renderRawPositions(positions);
      renderRawPositionViewer(positions);
    });
    tr.querySelector(".raw-position-select-checkbox")?.addEventListener("click", (event) => {
      event.stopPropagation();
    });
    tr.querySelector(".raw-position-select-checkbox")?.addEventListener("change", (event) => {
      toggleRawPositionSelection(position.id, Boolean(event.target.checked));
    });
    els.rawPositionsTableBody.appendChild(tr);
  }
  const selectedRow = els.rawPositionsTableBody.querySelector("tr.selected");
  if (selectedRow) {
    selectedRow.scrollIntoView({ block: "nearest", inline: "nearest" });
  }
  renderRawPositionSelectionControls(positions);
  renderRawPositionDeletePanel();
  if (els.rawPositionSelectAll) {
    els.rawPositionSelectAll.disabled = positions.length === 0;
  }
  els.rawPositionsTableBody.querySelectorAll(".queue-position-preview").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const positionId = button.dataset.positionId || "";
      const selectedPosition = positions.find((position) => `${position.id}` === positionId);
      queueRawPreviewVideo(positionId, Boolean(selectedPosition?.preview_artifact?.uri)).catch((error) => setStatus(String(error)));
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
  const artifact = selected?.preview_artifact || null;
  const artifactVersion = artifact?.job_finished_at || artifact?.created_at || "";
  const artifactUrl = artifact?.uri ? withCacheBust(withIdentity(artifact.uri), artifactVersion) : "";
  const currentVideoSrc = els.rawPositionViewerVideo.getAttribute("src") || "";

  if (!selected) {
    els.rawPositionViewerEmpty.classList.remove("hidden");
    els.rawPositionViewerEmpty.textContent = "Click a position to load its preview movie.";
    els.rawPositionViewer.classList.add("hidden");
    if (els.rawPositionViewerVideo) {
      els.rawPositionViewerVideo.removeAttribute("src");
      els.rawPositionViewerVideo.load();
    }
    return;
  }

  if (els.rawPositionViewerMeta) {
    const indexLabel = selected.position_index != null ? `#${selected.position_index}` : "";
    els.rawPositionViewerMeta.textContent = `${selected.display_name || selected.position_key}${indexLabel ? ` (${indexLabel})` : ""} | ${selected.preview_status}`;
  }
  if (artifactUrl) {
    if (currentVideoSrc !== artifactUrl) {
      els.rawPositionViewerVideo.src = artifactUrl;
      els.rawPositionViewerVideo.load();
      void els.rawPositionViewerVideo.play().catch(() => {});
    }
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
    els.rawPositionViewerEmpty.textContent = `No preview movie available for ${selected.display_name || selected.position_key}.`;
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
    ["Artifact root", config.artifact_root || "dataset/.detecdiv-previews"],
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
  if (els.micromanagerIngestRootMode && !els.micromanagerIngestRootMode.value) {
    els.micromanagerIngestRootMode.value = "auto";
  }
  if (els.micromanagerIngestRootPath && !els.micromanagerIngestRootPath.value && config.landing_root) {
    els.micromanagerIngestRootPath.placeholder = config.landing_root;
  }
  if (els.micromanagerIngestStorageRootName && !els.micromanagerIngestStorageRootName.value && config.storage_root_name) {
    els.micromanagerIngestStorageRootName.placeholder = config.storage_root_name;
  }
  updateMicroManagerIngestModeUi();
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
      ["Configured landing root", config.landing_root || ""],
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
      <td>${candidate.archive_status || ""}</td>
      <td>${formatTimestamp(candidate.last_activity_at)}</td>
      <td>${humanBytes(candidate.total_bytes)}</td>
      <td>${humanBytes(candidate.reclaimable_bytes)}</td>
      <td>${candidate.suggested_archive_compression || ""}</td>
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

function renderProjectNotes() {
  if (!els.projectNotes) {
    return;
  }
  const notes = state.selectedProjectDetail?.notes || "";
  if (document.activeElement !== els.projectNotes || els.projectNotes.value === notes) {
    els.projectNotes.value = notes;
  }
  if (els.projectNotesSaveButton) {
    els.projectNotesSaveButton.disabled = !state.selectedProjectDetail;
  }
}

function renderRawDatasetNotes() {
  if (!els.rawDatasetNotes) {
    return;
  }
  const notes = state.selectedRawDatasetDetail?.notes || "";
  if (document.activeElement !== els.rawDatasetNotes || els.rawDatasetNotes.value === notes) {
    els.rawDatasetNotes.value = notes;
  }
  if (els.rawDatasetNotesSaveButton) {
    els.rawDatasetNotesSaveButton.disabled = !state.selectedRawDatasetDetail;
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
    renderProjectNotes();
    if (els.changeProjectOwnerButton) {
      els.changeProjectOwnerButton.classList.add("hidden");
      els.changeProjectOwnerButton.disabled = true;
    }
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

  if (els.changeProjectOwnerButton) {
    const canChangeOwner = isAdmin();
    els.changeProjectOwnerButton.classList.toggle("hidden", !canChangeOwner);
    els.changeProjectOwnerButton.disabled = !canChangeOwner;
  }

  renderProjectNotes();
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
      <td>${escapeHtml(labStatusLabel(user))}</td>
      <td>${user.admin_portal_access ? "yes" : "no"}</td>
      <td title="${escapeHtml(user.default_path || "")}">${escapeHtml(user.default_path || "")}</td>
      <td>${user.is_active ? "yes" : "no"}</td>
      <td><button type="button" class="user-edit-button">Settings</button></td>
    `;
    tr.addEventListener("click", () => editUser(user));
    tr.querySelector(".user-edit-button")?.addEventListener("click", (event) => {
      event.stopPropagation();
      editUser(user).catch((error) => setStatus(String(error)));
    });
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
  if (state.sessionToken) {
  try {
    const session = await apiGet("/auth/session");
    if (session.authenticated) {
      state.currentUser = session.user;
      state.authMode = session.auth_mode || "session";
      state.userKey = session.user?.user_key || state.userKey;
      updateSessionUi();
      await refreshDashboard();
      setStatus(`Connected via ${state.authMode} as ${session.user.user_key}.`);
      return;
    }
  } catch {
    // Session expired or invalid; fall through to login.
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
  if (pageFlags.hasRawDatasetsView || pageKind === "raw-ops") {
    refreshTasks.push(refreshRawDatasets());
  }
  if (pageFlags.hasArchiveSettings && isAdmin()) {
    refreshTasks.push(refreshArchiveSettingsStatus());
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
  if (
    (pageFlags.hasUsersView && isAdmin()) ||
    els.indexOwnerUserKey ||
    els.ownerFilter ||
    els.rawOwnerFilter ||
    pageFlags.hasProjectPage ||
    pageFlags.hasRawDatasetPage
  ) {
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
    refreshTasks.push(refreshDeploymentAwareness());
  }
  await Promise.all(refreshTasks);

  setStatus(`Connected as ${summary.user.user_key}.`);
}

async function refreshIndexingJobs() {
  if (!els.indexJobsTableBody && !els.activeIndexJob) {
    return;
  }
  state.indexingJobs = await apiGet("/indexing/jobs?limit=25");
  if (els.indexJobsClearStaleButton) {
    els.indexJobsClearStaleButton.disabled = !state.indexingJobs.some((job) => job.status === "stale");
  }
  renderIndexingJobs();
}

async function clearStaleIndexingJobs() {
  const staleCount = state.indexingJobs.filter((job) => job.status === "stale").length;
  if (!staleCount) {
    throw new Error("No stale indexing jobs to clear.");
  }
  const confirmed = window.confirm(`Delete ${staleCount} stale indexing job(s)?`);
  if (!confirmed) {
    return;
  }
  const result = await apiPost("/indexing/jobs/clear-stale", {});
  setStatus(result.message || `Deleted ${result.deleted_count} stale indexing job(s).`);
  await refreshIndexingJobs();
}

async function refreshRawDatasets() {
  if (!els.rawDatasetsTableBody && pageKind !== "raw-ops") {
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
  state.rawDatasetsLastRefreshAt = Date.now();
  const visibleIds = new Set(state.rawDatasets.map((raw) => `${raw.id}`));
  state.selectedRawDatasetIds = state.selectedRawDatasetIds.filter((rawDatasetId) => visibleIds.has(`${rawDatasetId}`));
  if (state.pendingRawBulkDelete?.scope === "selected" && !selectedRawDatasetIds().length) {
    closeRawBulkDeletePanel();
  }
  if (els.rawDatasetsTableBody) {
    renderRawDatasets();
    renderRawBulkDeletePanel();
  } else {
    renderRawOpsSummary();
    renderArchivedRawDatasets();
  }
  const requestedRawDatasetId = getRawDatasetPageId();
  if (requestedRawDatasetId && pageFlags.hasRawDatasetPage) {
    if (!state.selectedRawDataset || `${state.selectedRawDataset.id}` === requestedRawDatasetId) {
      await selectRawDataset(requestedRawDatasetId, { hydrateDetail: true });
    }
  } else if (state.selectedRawDataset) {
    const stillExists = state.rawDatasets.find((raw) => raw.id === state.selectedRawDataset.id);
    if (stillExists) {
      state.selectedRawDataset = stillExists;
      state.selectedRawDatasetDetail = stillExists;
      renderRawDatasetDetail();
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

async function refreshArchiveSettingsStatus() {
  if (!isAdmin()) {
    state.archiveSettingsStatus = null;
    if (pageFlags.hasArchiveSettings) {
      renderArchiveSettingsStatus();
    }
    return;
  }
  state.archiveSettingsStatus = await apiGet("/raw-datasets/settings/archive");
  if (pageFlags.hasArchiveSettings) {
    renderArchiveSettingsStatus();
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
  const archiveConfig = state.archiveSettingsStatus?.config || {};
  const explicitCompression = els.archivePolicyCompression?.value || "";
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
    archive_uri: els.archivePolicyUri?.value.trim() || archiveConfig.archive_root || null,
    archive_compression: explicitCompression || archiveConfig.archive_compression || null,
    mark_archived: els.archivePolicyDeleteSource
      ? Boolean(els.archivePolicyDeleteSource.checked)
      : Boolean(archiveConfig.delete_hot_source),
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
  const rootMode = String(els.micromanagerIngestRootMode?.value || "auto");
  const landingRootOverride = rootMode === "manual"
    ? String(els.micromanagerIngestRootPath?.value || "").trim()
    : "";
  const storageRootNameOverride = String(els.micromanagerIngestStorageRootName?.value || "").trim();
  if (rootMode === "manual" && !landingRootOverride) {
    throw new Error("Manual raw root is required in manual mode.");
  }
  const action = reportOnly ? "run a detection-only report" : "ingest datasets now";
  const ok = window.confirm(`Run Micro-Manager ingestion now and ${action}?`);
  if (!ok) {
    return;
  }
  const run = await apiPost("/micromanager-ingest/run", {
    report_only: reportOnly,
    landing_root_override: landingRootOverride || null,
    storage_root_name_override: storageRootNameOverride || null,
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

async function selectRawDataset(rawDatasetId, options = {}) {
  const hydrateDetail = options.hydrateDetail ?? pageFlags.hasRawDatasetPage;
  if (!state.selectedRawDatasetDetail || `${state.selectedRawDatasetDetail.id}` !== `${rawDatasetId}`) {
    state.selectedRawPositionIds = [];
    closeRawPositionDeletePanel();
  }
  const raw = state.rawDatasets.find((item) => item.id === rawDatasetId);
  if (!hydrateDetail) {
    if (!raw) {
      return;
    }
    state.selectedRawDatasetDetail = raw;
    state.selectedRawDataset = raw;
    if ((rawDatasetId === getRawDatasetPageId() || pageFlags.hasRawDatasetPage) && els.rawDetailSubtitle) {
      document.title = `Detecdiv server - ${raw.acquisition_label}`;
    }
    renderRawDatasets();
    renderRawDatasetDetail();
    return;
  }
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

  const [detail, acl] = await Promise.all([
    apiGet(`/projects/${projectId}`),
    apiGet(`/projects/${projectId}/acl`),
  ]);
  state.selectedProject = detail;
  state.selectedProjectDetail = detail;
  state.projects = [detail];
  state.acl = acl;
  if (els.projectPageTitle) {
    els.projectPageTitle.textContent = detail.project_name;
  }
  renderDetail();
  renderPipelineRunBuilder();
}

async function refreshUsers() {
  if (!els.usersTableBody) {
    if (els.indexOwnerUserKey || els.ownerFilter || els.rawOwnerFilter || pageFlags.hasProjectPage || pageFlags.hasRawDatasetPage) {
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
  const [detail, acl] = await Promise.all([
    apiGet(`/projects/${projectId}`),
    apiGet(`/projects/${projectId}/acl`),
  ]);
  state.selectedProjectDetail = detail;
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

async function saveProjectNotes() {
  if (!state.selectedProjectDetail || !els.projectNotes) {
    return;
  }
  const notes = els.projectNotes.value;
  const payload = {
    notes: notes.trim() ? notes : null,
  };
  const saved = await apiPatch(`/projects/${state.selectedProjectDetail.id}`, payload);
  state.selectedProjectDetail = saved;
  state.selectedProject = saved;
  state.projects = state.projects.map((project) => (project.id === saved.id ? saved : project));
  renderProjects();
  renderDetail();
  renderPipelineRunBuilder();
  setStatus("Project notes saved.");
}

async function saveRawDatasetNotes() {
  if (!state.selectedRawDatasetDetail || !els.rawDatasetNotes) {
    return;
  }
  const notes = els.rawDatasetNotes.value;
  const payload = {
    notes: notes.trim() ? notes : null,
  };
  const saved = await apiPatch(`/raw-datasets/${state.selectedRawDatasetDetail.id}`, payload);
  state.selectedRawDatasetDetail = saved;
  state.selectedRawDataset = saved;
  state.rawDatasets = state.rawDatasets.map((raw) => (raw.id === saved.id ? saved : raw));
  renderRawDatasets();
  renderRawDatasetDetail();
  setStatus("Raw dataset notes saved.");
}

async function changeRawDatasetOwner() {
  if (!state.selectedRawDatasetDetail) {
    return;
  }
  if (!isAdmin()) {
    throw new Error("Admin role required.");
  }
  const currentOwner = state.selectedRawDatasetDetail.owner?.user_key || "";
  const ownerOptions = ownerUserOptions(currentOwner);
  const values = await openFormDialog({
    title: "Change raw dataset owner",
    description: state.selectedRawDatasetDetail.acquisition_label || "",
    submitLabel: "Change owner",
    fields: [
      {
        name: "ownerUserKey",
        label: "New owner",
        type: ownerOptions.length ? "select" : "text",
        value: currentOwner,
        options: ownerOptions,
      },
    ],
  });
  if (!values) {
    return;
  }
  const ownerUserKey = String(values.ownerUserKey || "").trim();
  if (!ownerUserKey) {
    return;
  }
  if (ownerUserKey === currentOwner) {
    setStatus("Raw dataset owner unchanged.");
    return;
  }
  const saved = await apiPatch(`/raw-datasets/${state.selectedRawDatasetDetail.id}`, {
    owner_user_key: ownerUserKey,
    metadata_json: {},
  });
  state.selectedRawDatasetDetail = saved;
  state.selectedRawDataset = saved;
  state.rawDatasets = state.rawDatasets.map((raw) => (raw.id === saved.id ? saved : raw));
  await refreshDashboard();
  await refreshUsers();
  renderRawDatasets();
  renderRawDatasetDetail();
  setStatus(`Raw dataset owner changed to ${ownerUserKey}.`);
}

async function editProject() {
  if (!state.selectedProjectDetail) {
    return;
  }
  const currentOwner = state.selectedProjectDetail.owner?.user_key || "";
  const currentVisibility = state.selectedProjectDetail.visibility || "private";
  const ownerOptions = ownerUserOptions(currentOwner);
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

async function changeProjectOwner() {
  if (!state.selectedProjectDetail) {
    return;
  }
  if (!isAdmin()) {
    throw new Error("Admin role required.");
  }
  const currentOwner = state.selectedProjectDetail.owner?.user_key || "";
  const ownerOptions = ownerUserOptions(currentOwner);
  const values = await openFormDialog({
    title: "Change project owner",
    description: state.selectedProjectDetail.project_name || "",
    submitLabel: "Change owner",
    fields: [
      {
        name: "ownerUserKey",
        label: "New owner",
        type: ownerOptions.length ? "select" : "text",
        value: currentOwner,
        options: ownerOptions,
      },
    ],
  });
  if (!values) {
    return;
  }
  const ownerUserKey = String(values.ownerUserKey || "").trim();
  if (!ownerUserKey) {
    return;
  }
  if (ownerUserKey === currentOwner) {
    setStatus("Project owner unchanged.");
    return;
  }
  const saved = await apiPatch(`/projects/${state.selectedProjectDetail.id}`, {
    owner_user_key: ownerUserKey,
    metadata_json: {},
  });
  state.selectedProjectDetail = saved;
  state.selectedProject = saved;
  state.projects = state.projects.map((project) => (project.id === saved.id ? saved : project));
  await refreshDashboard();
  await refreshUsers();
  renderProjects();
  renderDetail();
  renderPipelineRunBuilder();
  setStatus(`Project owner changed to ${ownerUserKey}.`);
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

async function saveArchiveSettings() {
  if (!isAdmin()) {
    throw new Error("Admin role required.");
  }
  const archiveRoot = String(els.archiveSettingsRoot?.value || "").trim();
  const archiveCompression = String(els.archiveSettingsCompression?.value || "zip").trim() || "zip";
  const deleteHotSource = Boolean(els.archiveSettingsDeleteSource?.checked);
  state.archiveSettingsStatus = await apiPatch("/raw-datasets/settings/archive", {
    archive_root: archiveRoot || null,
    archive_compression: archiveCompression,
    delete_hot_source: deleteHotSource,
  });
  renderArchiveSettingsStatus();
  state.archivePolicyPreview = null;
  renderArchivePolicyPreview();
  setStatus("Archive defaults updated.");
}

function updateMicroManagerIngestModeUi() {
  const manualMode = String(els.micromanagerIngestRootMode?.value || "auto") === "manual";
  if (els.micromanagerIngestRootPath) {
    els.micromanagerIngestRootPath.disabled = !manualMode;
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

async function queueRawPreviewVideosForSelectedRawDatasets() {
  const rawDatasetIds = selectedRawDatasetIds();
  if (!rawDatasetIds.length) {
    throw new Error("Select at least one raw dataset first.");
  }
  const result = await apiPost("/raw-datasets/preview-videos/queue-bulk", {
    raw_dataset_ids: rawDatasetIds,
    force: false,
    requested_mode: "auto",
    priority: 100,
    params_json: {},
  });
  setStatus(result.message || `Queued ${result.queued_count} raw preview job(s).`);
  await refreshRawDatasets();
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

async function refreshRawPositionDeletePreview() {
  if (!state.pendingRawPositionDelete?.positionIds?.length || !state.selectedRawDatasetDetail) {
    return;
  }
  const currentToken = state.rawPositionDeletePreviewToken + 1;
  state.rawPositionDeletePreviewToken = currentToken;

  state.pendingRawPositionDelete.preview = null;
  renderRawPositionDeletePanel();

  const preview = await apiPost(`/raw-datasets/${state.selectedRawDatasetDetail.id}/positions/deletion-preview`, {
    position_ids: state.pendingRawPositionDelete.positionIds,
    confirm: false,
  });

  if (state.rawPositionDeletePreviewToken !== currentToken || !state.pendingRawPositionDelete) {
    return;
  }
  state.pendingRawPositionDelete.preview = preview.preview_json || {};
  renderRawPositionDeletePanel();
}

async function openRawPositionDeletePanel() {
  const positionIds = selectedRawPositionIds();
  if (!positionIds.length) {
    throw new Error("Select at least one position first.");
  }
  if (!state.selectedRawDatasetDetail) {
    throw new Error("Select a raw dataset first.");
  }
  state.pendingRawPositionDelete = {
    scope: "selected",
    positionIds,
    preview: null,
  };
  if (els.rawPositionDeleteConfirmText) {
    els.rawPositionDeleteConfirmText.value = "";
  }
  renderRawPositionDeletePanel();
  await refreshRawPositionDeletePreview();
}

function closeRawPositionDeletePanel() {
  state.pendingRawPositionDelete = null;
  state.rawPositionDeletePreviewToken += 1;
  if (els.rawPositionDeleteConfirmText) {
    els.rawPositionDeleteConfirmText.value = "";
  }
  renderRawPositionDeletePanel();
}

async function executeRawPositionDelete() {
  if (!state.pendingRawPositionDelete?.positionIds?.length || !state.selectedRawDatasetDetail) {
    return;
  }
  const confirmationText = els.rawPositionDeleteConfirmText?.value.trim() || "";
  if (confirmationText !== "DELETE") {
    throw new Error("Type DELETE to confirm the position deletion.");
  }

  const positionIds = [...state.pendingRawPositionDelete.positionIds];
  const positionNames = positionIds.map((positionId) => {
    const position = currentRawPositions().find((item) => `${item.id}` === `${positionId}`);
    return position?.display_name || position?.position_key || `${positionId}`;
  });
  setStatus(`Deleting ${positionIds.length} selected position(s)...`);
  const result = await apiPost(`/raw-datasets/${state.selectedRawDatasetDetail.id}/positions/delete`, {
    position_ids: positionIds,
    confirm: true,
  });

  state.selectedRawPositionIds = state.selectedRawPositionIds.filter((positionId) => !positionIds.includes(`${positionId}`));
  if (state.selectedRawPositionId && positionIds.includes(`${state.selectedRawPositionId}`)) {
    state.selectedRawPositionId = null;
  }
  closeRawPositionDeletePanel();
  await selectRawDataset(state.selectedRawDatasetDetail.id);
  setStatus(result.message || `Deleted ${result.position_count || positionIds.length} position(s).`);
  if (result.result_json?.errors?.length) {
    window.alert(`Position delete completed with failures:\n${result.result_json.errors.join("\n")}`);
  } else if (positionNames.length) {
    setStatus(`Deleted ${positionNames.join(", ")}.`);
  }
}

async function deleteProject() {
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
  const [userKey, displayName, email, role, labStatus] = parts;
  if (!userKey) {
    return null;
  }
  return {
    user_key: userKey,
    display_name: displayName || userKey,
    email: email || null,
    role: role || "user",
    is_active: true,
    lab_status: labStatus || "yes",
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
  const selectedPositionId = positionId || state.selectedRawPositionId || null;
  const payload = {
    position_id: positionId || null,
    force,
    requested_mode: "auto",
    priority: 100,
    params_json: {},
  };
  const result = await apiPost(`/raw-datasets/${state.selectedRawDatasetDetail.id}/preview-videos/queue`, payload);
  await selectRawDataset(state.selectedRawDatasetDetail.id);
  if (selectedPositionId) {
    state.selectedRawPositionId = selectedPositionId;
    renderRawPositions(currentRawPositions());
    renderRawPositionViewer(currentRawPositions());
  }
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
  if (!state.archiveSettingsStatus && isAdmin()) {
    await refreshArchiveSettingsStatus();
  }
  const config = state.archiveSettingsStatus?.config || {};
  const archiveRoot = String(config.archive_root || state.selectedRawDatasetDetail?.archive_uri || "").trim();
  if (!archiveRoot) {
    throw new Error("No archive root configured. Set it in Raw datasets Ops > Archive Defaults.");
  }
  const archiveCompression = String(config.archive_compression || state.selectedRawDatasetDetail?.archive_compression || "zip");
  const markArchived = Boolean(config.delete_hot_source);
  const action = markArchived ? "archive and delete hot source" : "archive";
  const ok = window.confirm(
    `Request ${action} for ${state.selectedRawDataset.acquisition_label}?\nRoot: ${archiveRoot}\nCompression: ${archiveCompression}`
  );
  if (!ok) {
    return;
  }
  await apiPost(`/raw-datasets/${state.selectedRawDataset.id}/archive`, {
    archive_uri: archiveRoot,
    archive_compression: archiveCompression,
    mark_archived: markArchived,
  });
  await refreshRawDatasets();
  await selectRawDataset(state.selectedRawDataset.id);
  setRawActionFeedback(`Archive request queued for ${state.selectedRawDataset.acquisition_label}.`, "ok");
  setStatus(`Archive transition requested for ${state.selectedRawDataset.acquisition_label}.`);
}

async function deleteRawArchive() {
  if (!state.selectedRawDataset) {
    return;
  }
  const archiveUri = String(state.selectedRawDatasetDetail?.archive_uri || "").trim();
  if (!archiveUri) {
    throw new Error("No archive file is registered for this dataset.");
  }
  const ok = window.confirm(
    `Delete archive for ${state.selectedRawDataset.acquisition_label}?\nArchive: ${archiveUri}`
  );
  if (!ok) {
    return;
  }
  const result = await apiDelete(`/raw-datasets/${state.selectedRawDataset.id}/archive-file`);
  await refreshRawDatasets();
  await selectRawDataset(state.selectedRawDataset.id);
  setRawActionFeedback(`Archive deleted for ${state.selectedRawDataset.acquisition_label}.`, "ok");
  setStatus(result.message || `Archive deleted for ${state.selectedRawDataset.acquisition_label}.`);
}

async function deleteSelectedRawArchives() {
  const rawDatasetIds = selectedRawDatasetIds().filter((rawDatasetId) => visibleArchivedRawDatasetIds().includes(`${rawDatasetId}`));
  if (!rawDatasetIds.length) {
    throw new Error("Select at least one archived dataset first.");
  }
  const ok = window.confirm(`Delete archive files for ${rawDatasetIds.length} dataset(s)?`);
  if (!ok) {
    return;
  }
  const result = await apiPost("/raw-datasets/archive-files/delete-bulk", {
    raw_dataset_ids: rawDatasetIds,
  });
  await refreshRawDatasets();
  setStatus(result.message || `Deleted ${result.deleted_count} archive file(s).`);
}

async function requestRawRestore() {
  if (!state.selectedRawDataset) {
    return;
  }
  await apiPost(`/raw-datasets/${state.selectedRawDataset.id}/restore`, {});
  await refreshRawDatasets();
  await selectRawDataset(state.selectedRawDataset.id);
  setRawActionFeedback(`Restore request queued for ${state.selectedRawDataset.acquisition_label}.`, "ok");
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
  const labStatus = window.prompt("Lab status (yes/alumni)", "yes") || "yes";
  const defaultPath = window.prompt("Default path", "") || null;
  const adminPortalAccess = window.confirm("Allow admin portal access for this account?");
  const password = window.prompt("Temporary password", "");
  await apiPost("/users", {
    user_key: userKey,
    display_name: displayName,
    role,
    is_active: true,
    admin_portal_access: adminPortalAccess,
    lab_status: labStatus,
    default_path: defaultPath,
    password: password || null,
    metadata_json: {},
  });
  await refreshUsers();
  setStatus(`Created user ${userKey}.`);
}

async function editUser(user) {
  const adminFields = hasAdminPrivileges()
    ? [
        {
          name: "labStatus",
          label: "Lab status",
          type: "select",
          value: user.lab_status || "yes",
          options: [
            { value: "yes", label: "yes" },
            { value: "alumni", label: "alumni" },
          ],
        },
        {
          name: "defaultPath",
          label: "Default path",
          value: user.default_path || "",
        },
        {
          name: "role",
          label: "Role",
          type: "select",
          value: user.role,
          options: [
            { value: "user", label: "user" },
            { value: "admin", label: "admin" },
            { value: "service", label: "service" },
          ],
        },
        {
          name: "adminPortalAccess",
          label: "Admin portal access",
          type: "select",
          value: user.admin_portal_access ? "true" : "false",
          options: [
            { value: "true", label: "yes" },
            { value: "false", label: "no" },
          ],
        },
        {
          name: "accountActive",
          label: "Account active",
          type: "select",
          value: user.is_active ? "true" : "false",
          options: [
            { value: "true", label: "yes" },
            { value: "false", label: "no" },
          ],
        },
        {
          name: "password",
          label: "Reset password",
          type: "password",
        },
      ]
    : [];
  const values = await openFormDialog({
    title: `Edit user ${user.user_key}`,
    description: "Update the account profile and access settings.",
    submitLabel: "Save user",
    fields: [
      { name: "displayName", label: "Display name", value: user.display_name, required: true },
      { name: "email", label: "Email", value: user.email || "" },
      ...adminFields,
    ],
  });
  if (!values) {
    return;
  }
  const payload = {
    display_name: values.displayName.trim(),
    email: values.email.trim() || null,
    metadata_json: {},
  };
  if (hasAdminPrivileges()) {
    payload.role = values.role || user.role;
    payload.is_active = String(values.accountActive) === "true";
    payload.admin_portal_access = String(values.adminPortalAccess) === "true";
    payload.lab_status = values.labStatus || "yes";
    payload.default_path = values.defaultPath.trim() || null;
  }
  if (values.password && values.password.trim()) {
    payload.password = values.password.trim();
  }
  await apiPatch(`/users/${user.id}`, payload);
  await refreshUsers();
  setStatus(`Updated user ${user.user_key}.`);
}

async function changeMyPassword() {
  if (!state.currentUser) {
    return;
  }
  const values = await openFormDialog({
    title: "Change password",
    description: `Account: ${state.currentUser.user_key}`,
    submitLabel: "Update password",
    fields: [
      { name: "currentPassword", label: "Current password", type: "password", required: true },
      { name: "newPassword", label: "New password", type: "password", required: true },
      { name: "confirmPassword", label: "Confirm password", type: "password", required: true },
    ],
  });
  if (!values) {
    return;
  }
  const currentPassword = String(values.currentPassword || "").trim();
  const newPassword = String(values.newPassword || "").trim();
  const confirmPassword = String(values.confirmPassword || "").trim();
  if (!currentPassword || !newPassword) {
    throw new Error("Current and new passwords are required.");
  }
  if (newPassword !== confirmPassword) {
    throw new Error("Password confirmation does not match.");
  }
  await apiPost("/auth/me/password", {
    current_password: currentPassword,
    new_password: newPassword,
  });
  setStatus("Password updated.");
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
    if (pageFlags.hasRawDatasetsView || pageKind === "raw-ops") {
      if (Date.now() - state.rawDatasetsLastRefreshAt >= RAW_DATASETS_POLL_INTERVAL_MS) {
        pollTasks.push(refreshRawDatasets());
      }
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
    if (pageFlags.hasExecutionTargetsView && isAdmin()) {
      pollTasks.push(refreshExecutionTargets());
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
  if (!state.currentUser) {
    await restoreSession();
    return;
  }
  if (
    pageFlags.hasProjectsView ||
    pageFlags.hasRawDatasetsView ||
    pageFlags.hasRawDatasetPage ||
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
if (els.rawBulkQueuePreviewsSelectedButton) els.rawBulkQueuePreviewsSelectedButton.addEventListener("click", () => queueRawPreviewVideosForSelectedRawDatasets().catch((error) => setStatus(String(error))));
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
if (els.rawPositionSelectAll) els.rawPositionSelectAll.addEventListener("change", () => setSelectedRawPositionIds(els.rawPositionSelectAll.checked ? visibleRawPositionIds() : []));
if (els.rawPositionClearSelectionButton) els.rawPositionClearSelectionButton.addEventListener("click", () => setSelectedRawPositionIds([]));
if (els.rawPositionDeleteSelectedButton) els.rawPositionDeleteSelectedButton.addEventListener("click", () => openRawPositionDeletePanel().catch((error) => setStatus(String(error))));
if (els.rawPositionDeleteCancelButton) els.rawPositionDeleteCancelButton.addEventListener("click", () => closeRawPositionDeletePanel());
if (els.rawPositionDeleteConfirmButton) els.rawPositionDeleteConfirmButton.addEventListener("click", () => executeRawPositionDelete().catch((error) => {
  setStatus(String(error));
  window.alert(String(error));
}));
if (els.rawPositionDeleteConfirmText) els.rawPositionDeleteConfirmText.addEventListener("input", () => renderRawPositionDeletePanel());
if (els.newGroupButton) els.newGroupButton.addEventListener("click", () => createGroup().catch((error) => setStatus(String(error))));
if (els.projectNotesSaveButton) els.projectNotesSaveButton.addEventListener("click", () => saveProjectNotes().catch((error) => setStatus(String(error))));
if (els.rawDatasetNotesSaveButton) els.rawDatasetNotesSaveButton.addEventListener("click", () => saveRawDatasetNotes().catch((error) => setStatus(String(error))));
if (els.shareButton) els.shareButton.addEventListener("click", () => shareProject().catch((error) => setStatus(String(error))));
if (els.editProjectButton) els.editProjectButton.addEventListener("click", () => {
  if (pageFlags.hasProjectPage) {
    editProject().catch((error) => setStatus(String(error)));
  } else {
    openSelectedProjectPage();
  }
});
if (els.updateProjectButton) els.updateProjectButton.addEventListener("click", () => editProject().catch((error) => setStatus(String(error))));
if (els.changeProjectOwnerButton) els.changeProjectOwnerButton.addEventListener("click", () => changeProjectOwner().catch((error) => setStatus(String(error))));
if (els.addToGroupButton) els.addToGroupButton.addEventListener("click", () => addSelectedProjectToGroup().catch((error) => setStatus(String(error))));
if (els.projectQueueRawPreviewsButton) els.projectQueueRawPreviewsButton.addEventListener("click", () => queueProjectRawPreviewVideos().catch((error) => setStatus(String(error))));
if (els.previewDeleteButton) els.previewDeleteButton.addEventListener("click", () => deleteProject().catch((error) => setStatus(String(error))));
if (els.indexBrowseRoot) els.indexBrowseRoot.addEventListener("change", () => openIndexBrowserPath("").catch((error) => setStatus(String(error))));
if (els.indexBrowseOpenButton) els.indexBrowseOpenButton.addEventListener("click", () => openIndexBrowserPath(state.indexBrowse?.current_relative_path || "").catch((error) => setStatus(String(error))));
if (els.indexBrowseUpButton) els.indexBrowseUpButton.addEventListener("click", () => {
  const parentPath = state.indexBrowse?.parent_relative_path;
  if (parentPath === null || parentPath === undefined) {
    return;
  }
  openIndexBrowserPath(parentPath).catch((error) => setStatus(String(error)));
});
if (els.indexBrowseUseButton) els.indexBrowseUseButton.addEventListener("click", () => useSelectedIndexFolder());
if (els.indexButton) els.indexButton.addEventListener("click", () => runIndexing().catch((error) => setStatus(String(error))));
if (els.indexJobsRefreshButton) els.indexJobsRefreshButton.addEventListener("click", () => refreshIndexingJobs().catch((error) => setStatus(String(error))));
if (els.indexJobsClearStaleButton) els.indexJobsClearStaleButton.addEventListener("click", () => clearStaleIndexingJobs().catch((error) => {
  setStatus(String(error));
  window.alert(String(error));
}));
if (els.migrationCreateButton) els.migrationCreateButton.addEventListener("click", () => createMigrationPlan().catch((error) => setStatus(String(error))));
if (els.migrationRefreshButton) els.migrationRefreshButton.addEventListener("click", () => refreshMigrationPlans().catch((error) => setStatus(String(error))));
if (els.migrationExecutePilotButton) els.migrationExecutePilotButton.addEventListener("click", () => executePilotBatch().catch((error) => setStatus(String(error))));
if (els.rawPreviewArchiveButton) els.rawPreviewArchiveButton.addEventListener("click", () => previewRawArchive().catch((error) => setStatus(String(error))));
if (els.rawQueuePreviewButton) els.rawQueuePreviewButton.addEventListener("click", () => queueRawPreviewVideo("").catch((error) => setStatus(String(error))));
if (els.rawRegeneratePreviewButton) els.rawRegeneratePreviewButton.addEventListener("click", () => regenerateRawPreviewVideos().catch((error) => setStatus(String(error))));
if (els.rawPreviewQualityRefreshButton) els.rawPreviewQualityRefreshButton.addEventListener("click", () => refreshRawPreviewQualityStatus().catch((error) => setStatus(String(error))));
if (els.rawPreviewQualitySaveButton) els.rawPreviewQualitySaveButton.addEventListener("click", () => saveRawPreviewQualitySettings().catch((error) => setStatus(String(error))));
if (els.rawPreviewQualityFrameMode) els.rawPreviewQualityFrameMode.addEventListener("change", updateRawPreviewFrameModeUi);
if (els.changeRawOwnerButton) els.changeRawOwnerButton.addEventListener("click", () => changeRawDatasetOwner().catch((error) => setStatus(String(error))));
if (els.rawArchiveButton) els.rawArchiveButton.addEventListener("click", () => requestRawArchive().catch((error) => setStatus(String(error))));
if (els.rawDeleteArchiveButton) els.rawDeleteArchiveButton.addEventListener("click", () => deleteRawArchive().catch((error) => setStatus(String(error))));
if (els.rawRestoreButton) els.rawRestoreButton.addEventListener("click", () => requestRawRestore().catch((error) => setStatus(String(error))));
if (els.archiveSettingsRefreshButton) els.archiveSettingsRefreshButton.addEventListener("click", () => refreshArchiveSettingsStatus().catch((error) => setStatus(String(error))));
if (els.archiveSettingsSaveButton) els.archiveSettingsSaveButton.addEventListener("click", () => saveArchiveSettings().catch((error) => setStatus(String(error))));
if (els.automaticArchivePolicyRefreshButton) els.automaticArchivePolicyRefreshButton.addEventListener("click", () => refreshAutomaticArchivePolicyStatus().catch((error) => setStatus(String(error))));
if (els.automaticArchivePolicyRunButton) els.automaticArchivePolicyRunButton.addEventListener("click", () => runAutomaticArchivePolicy().catch((error) => setStatus(String(error))));
if (els.micromanagerIngestRefreshButton) els.micromanagerIngestRefreshButton.addEventListener("click", () => refreshMicroManagerIngestStatus().catch((error) => setStatus(String(error))));
if (els.micromanagerIngestRunButton) els.micromanagerIngestRunButton.addEventListener("click", () => runMicroManagerIngest().catch((error) => setStatus(String(error))));
if (els.micromanagerIngestRootMode) els.micromanagerIngestRootMode.addEventListener("change", updateMicroManagerIngestModeUi);
if (els.archivePolicyPreviewButton) els.archivePolicyPreviewButton.addEventListener("click", () => previewArchivePolicy().catch((error) => setStatus(String(error))));
if (els.archivePolicyQueueButton) els.archivePolicyQueueButton.addEventListener("click", () => queueArchivePolicy().catch((error) => setStatus(String(error))));
if (els.archivedRawSelectAll) els.archivedRawSelectAll.addEventListener("change", () => setSelectedRawDatasetIds(els.archivedRawSelectAll.checked ? visibleArchivedRawDatasetIds() : []));
if (els.archivedRawSelectVisibleButton) els.archivedRawSelectVisibleButton.addEventListener("click", () => setSelectedRawDatasetIds(visibleArchivedRawDatasetIds()));
if (els.archivedRawClearSelectionButton) els.archivedRawClearSelectionButton.addEventListener("click", () => setSelectedRawDatasetIds([]));
if (els.archivedRawDeleteSelectedButton) els.archivedRawDeleteSelectedButton.addEventListener("click", () => deleteSelectedRawArchives().catch((error) => setStatus(String(error))));
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
if (els.openExecutionTargetConfigButton) els.openExecutionTargetConfigButton.addEventListener("click", () => {
  try {
    openExecutionTargetConfig();
  } catch (error) {
    setStatus(String(error));
    window.alert(String(error));
  }
});
if (els.applyWorkerInstancesButton) els.applyWorkerInstancesButton.addEventListener("click", () => applyWorkerInstances().catch((error) => {
  setStatus(String(error));
  window.alert(String(error));
}));
if (els.applyExecutionTargetDrainButton) els.applyExecutionTargetDrainButton.addEventListener("click", () => applyExecutionTargetDrain().catch((error) => {
  setStatus(String(error));
  window.alert(String(error));
}));
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
initializeAppLayout();
updateSessionUi();
restoreSession().catch((error) => setStatus(String(error)));
