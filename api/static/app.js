const state = {
  userKey: localStorage.getItem("detecdivHub.userKey") || "localdev",
  projects: [],
  groups: [],
  storageRoots: [],
  pipelines: [],
  indexingJobs: [],
  selectedProject: null,
  selectedProjectDetail: null,
  notes: [],
  acl: [],
  summary: null,
};

const els = {
  userKey: document.querySelector("#user-key"),
  connectButton: document.querySelector("#connect-button"),
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
  pipelineSearch: document.querySelector("#pipeline-search"),
  pipelineRuntimeFilter: document.querySelector("#pipeline-runtime-filter"),
  refreshPipelinesButton: document.querySelector("#refresh-pipelines-button"),
  newPipelineButton: document.querySelector("#new-pipeline-button"),
  pipelinesTableBody: document.querySelector("#pipelines-table tbody"),
  statusLine: document.querySelector("#status-line"),
};

let dashboardPollHandle = null;

function setStatus(message) {
  if (els.statusLine) {
    els.statusLine.textContent = message;
  }
}

function currentQuery() {
  return `user_key=${encodeURIComponent(state.userKey)}`;
}

async function apiGet(path) {
  const separator = path.includes("?") ? "&" : "?";
  const response = await fetch(`${path}${separator}${currentQuery()}`);
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
}

async function apiPost(path, payload) {
  const separator = path.includes("?") ? "&" : "?";
  const response = await fetch(`${path}${separator}${currentQuery()}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
}

async function apiDelete(path) {
  const separator = path.includes("?") ? "&" : "?";
  const response = await fetch(`${path}${separator}${currentQuery()}`, {
    method: "DELETE",
  });
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
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

function setSummary(summary) {
  state.summary = summary;
  if (els.summaryTotalProjects) els.summaryTotalProjects.textContent = summary.total_projects;
  if (els.summaryOwnedProjects) els.summaryOwnedProjects.textContent = summary.owned_projects;
  if (els.summaryTotalBytes) els.summaryTotalBytes.textContent = humanBytes(summary.total_bytes);
  if (els.summaryGroupCount) els.summaryGroupCount.textContent = summary.group_count;
  if (els.userLabel) els.userLabel.textContent = `User: ${summary.user.display_name} (${summary.user.user_key})`;
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

function renderPipelines() {
  if (!els.pipelinesTableBody) {
    return;
  }
  els.pipelinesTableBody.innerHTML = "";
  for (const pipeline of state.pipelines) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${pipeline.display_name}</td>
      <td>${pipeline.pipeline_key || ""}</td>
      <td>${pipeline.version}</td>
      <td>${pipeline.runtime_kind}</td>
      <td>${formatTimestamp(pipeline.updated_at || pipeline.created_at)}</td>
    `;
    tr.title = JSON.stringify(pipeline.metadata_json || {});
    tr.addEventListener("click", () => editPipeline(pipeline));
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
      <div class="job-title">${latestJob.status} | ${latestJob.source_path}</div>
      <div>${latestJob.message || "No status message."}</div>
      <div class="stack-item-meta">
        scanned ${latestJob.scanned_projects}/${latestJob.total_projects} | indexed ${latestJob.indexed_projects} | failed ${latestJob.failed_projects} | deleted ${latestJob.deleted_projects}
      </div>
      <div class="progress-bar"><div class="progress-fill" style="width: ${pct}%"></div></div>
    `;
  }

  for (const job of state.indexingJobs) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${job.status}</td>
      <td>${job.source_path}</td>
      <td>${job.scanned_projects}/${job.total_projects} (${progressPercent(job)}%)</td>
      <td>${job.indexed_projects}</td>
      <td>${job.failed_projects}</td>
      <td>${formatTimestamp(job.updated_at || job.created_at)}</td>
    `;
    tr.title = job.message || "";
    els.indexJobsTableBody.appendChild(tr);
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
      <div class="stack-item-meta">${note.author ? note.author.user_key : "unknown"} | ${note.updated_at || note.created_at || ""}${note.is_pinned ? " | pinned" : ""}</div>
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

async function refreshDashboard() {
  state.userKey = els.userKey.value.trim() || "localdev";
  localStorage.setItem("detecdivHub.userKey", state.userKey);

  setStatus("Refreshing dashboard...");
  const [summary, groups, storageRoots] = await Promise.all([
    apiGet("/dashboard/summary"),
    apiGet("/project-groups"),
    apiGet("/storage-roots"),
  ]);

  state.groups = groups.map((group) => ({ ...group, project_ids: [] }));
  state.storageRoots = storageRoots;
  for (const group of state.groups) {
    const detail = await apiGet(`/project-groups/${group.id}`);
    group.project_ids = (detail.projects || []).map((project) => project.id);
  }

  setSummary(summary);
  renderGroupFilter();
  renderStorageRootFilter();
  await refreshProjects();
  if (els.indexJobsTableBody) {
    await refreshIndexingJobs();
  }
  if (els.pipelinesTableBody) {
    await refreshPipelines();
  }
  setStatus(`Connected as ${summary.user.user_key}.`);
}

async function refreshIndexingJobs() {
  state.indexingJobs = await apiGet("/indexing/jobs?limit=25");
  renderIndexingJobs();
}

async function refreshProjects() {
  if (!els.projectCountLabel) {
    return;
  }
  const groupId = els.groupFilter.value;
  const ownedOnly = els.ownedOnly.checked;
  const params = new URLSearchParams();
  const search = els.projectSearch ? els.projectSearch.value.trim() : "";
  const ownerKey = els.ownerFilter ? els.ownerFilter.value.trim() : "";
  const storageRootName = els.storageRootFilter ? els.storageRootFilter.value : "";
  const limit = els.projectLimit ? els.projectLimit.value : "50";
  if (groupId) {
    params.set("group_id", groupId);
  }
  if (ownedOnly) {
    params.set("owned_only", "true");
  }
  if (search) {
    params.set("search", search);
  }
  if (ownerKey) {
    params.set("owner_key", ownerKey);
  }
  if (storageRootName) {
    params.set("storage_root_name", storageRootName);
  }
  if (limit) {
    params.set("limit", limit);
  }
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

async function refreshPipelines() {
  if (!els.pipelinesTableBody) {
    return;
  }
  const params = new URLSearchParams();
  const search = els.pipelineSearch ? els.pipelineSearch.value.trim() : "";
  const runtime = els.pipelineRuntimeFilter ? els.pipelineRuntimeFilter.value : "";
  if (search) {
    params.set("search", search);
  }
  if (runtime) {
    params.set("runtime_kind", runtime);
  }
  state.pipelines = await apiGet(`/pipelines${params.toString() ? `?${params.toString()}` : ""}`);
  renderPipelines();
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
  await fetch(`/projects/${state.selectedProjectDetail.id}?${currentQuery()}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      owner_user_key: ownerUserKey.trim() || null,
      visibility,
      metadata_json: {},
    }),
  }).then(async (response) => {
    if (!response.ok) {
      throw new Error(await response.text());
    }
    return response.json();
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
  const payload = {
    source_kind: "project_root",
    source_path: sourcePath,
    storage_root_name: els.indexStorageRootName.value.trim() || null,
    host_scope: "server",
    root_type: "project_root",
    visibility: els.indexVisibility.value,
    clear_existing_for_root: els.indexClearExisting.checked,
    metadata_json: {},
  };
  const response = await apiPost("/indexing/jobs", payload);
  await refreshIndexingJobs();
  setStatus(`Queued indexing job ${response.job.id} for ${response.job.source_path}.`);
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
  const separator = "?";
  const response = await fetch(`/pipelines/${pipeline.id}${separator}${currentQuery()}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      display_name: displayName,
      version,
      runtime_kind: runtimeKind,
      metadata_json: {},
    }),
  });
  if (!response.ok) {
    throw new Error(await response.text());
  }
  await refreshPipelines();
  setStatus(`Updated pipeline ${displayName}.`);
}

async function pollDashboard() {
  if (!state.userKey) {
    return;
  }
  const hasActiveJob = state.indexingJobs.some((job) => job.status === "queued" || job.status === "running");
  if (!hasActiveJob) {
    return;
  }
  try {
    await refreshDashboard();
  } catch (error) {
    setStatus(String(error));
  }
}

function ensureDashboardPolling() {
  if (dashboardPollHandle !== null) {
    return;
  }
  dashboardPollHandle = window.setInterval(() => {
    pollDashboard().catch((error) => setStatus(String(error)));
  }, 5000);
}

els.userKey.value = state.userKey;
if (els.connectButton) els.connectButton.addEventListener("click", () => refreshDashboard().catch((error) => setStatus(String(error))));
if (els.refreshButton) els.refreshButton.addEventListener("click", () => refreshDashboard().catch((error) => setStatus(String(error))));
if (els.groupFilter) els.groupFilter.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.projectSearch) els.projectSearch.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.ownerFilter) els.ownerFilter.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.storageRootFilter) els.storageRootFilter.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.projectLimit) els.projectLimit.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.ownedOnly) els.ownedOnly.addEventListener("change", () => refreshProjects().catch((error) => setStatus(String(error))));
if (els.newGroupButton) els.newGroupButton.addEventListener("click", () => createGroup().catch((error) => setStatus(String(error))));
if (els.addNoteButton) els.addNoteButton.addEventListener("click", () => addNote().catch((error) => setStatus(String(error))));
if (els.shareButton) els.shareButton.addEventListener("click", () => shareProject().catch((error) => setStatus(String(error))));
if (els.editProjectButton) els.editProjectButton.addEventListener("click", () => editProject().catch((error) => setStatus(String(error))));
if (els.addToGroupButton) els.addToGroupButton.addEventListener("click", () => addSelectedProjectToGroup().catch((error) => setStatus(String(error))));
if (els.previewDeleteButton) els.previewDeleteButton.addEventListener("click", () => previewDelete().catch((error) => setStatus(String(error))));
if (els.indexButton) els.indexButton.addEventListener("click", () => runIndexing().catch((error) => setStatus(String(error))));
if (els.indexJobsRefreshButton) els.indexJobsRefreshButton.addEventListener("click", () => refreshIndexingJobs().catch((error) => setStatus(String(error))));
if (els.refreshPipelinesButton) els.refreshPipelinesButton.addEventListener("click", () => refreshPipelines().catch((error) => setStatus(String(error))));
if (els.newPipelineButton) els.newPipelineButton.addEventListener("click", () => createPipeline().catch((error) => setStatus(String(error))));
if (els.pipelineSearch) els.pipelineSearch.addEventListener("change", () => refreshPipelines().catch((error) => setStatus(String(error))));
if (els.pipelineRuntimeFilter) els.pipelineRuntimeFilter.addEventListener("change", () => refreshPipelines().catch((error) => setStatus(String(error))));

ensureDashboardPolling();
refreshDashboard().catch((error) => setStatus(String(error)));
