const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

// Exercise the shipped functions without booting the catalog or accessing a DB.
const source = readFileSync(path.join(__dirname, "../api/static/app.js"), "utf8");
function shippedFunction(name, next) {
  return source.slice(source.indexOf(`function ${name}(`), source.indexOf(`function ${next}(`));
}
function setup(run, confirm = true) {
  const posts = [], listeners = {};
  const detail = {
    innerHTML: "",
    querySelector(selector) { return { addEventListener(type, fn) { listeners[selector] = fn; } }; },
  };
  const context = {
    state: { selectedPipelineRun: run }, els: { pipelineRunDetail: detail },
    window: { confirm(text) { context.confirmation = text; return confirm; } },
    formatTimestamp: (value) => value || "", escapeHtml: (v) => String(v),
    shortText: (v) => v, setStatus: (v) => { context.status = v; },
    refreshPipelineRuns: async () => {},
    apiPost: async (url, body) => { posts.push([url, body]); return { ...run, force_stop_requested_at: "now" }; },
  };
  vm.createContext(context);
  vm.runInContext(shippedFunction("renderPipelineRunDetail", "shortText"), context);
  vm.runInContext("async " + shippedFunction("forceStopSelectedPipelineRun", "promptBooleanField"), context);
  return { context, detail, posts, listeners };
}
const running = () => ({ id: "job-123", status: "running", result_json: { worker_runtime: { force_stop_supported: true } } });

test("force stop requires confirmation and sends only the selected job ID", async () => {
  const { context, detail, posts } = setup(running());
  context.renderPipelineRunDetail(context.state.selectedPipelineRun);
  assert.match(detail.innerHTML, /Force stop/);
  const button = { disabled: false };
  await context.forceStopSelectedPipelineRun(button);
  assert.equal(posts.length, 1);
  assert.equal(posts[0][0], "/pipeline-runs/job-123/force-stop");
  assert.match(context.confirmation, /incomplete files/);
  assert.match(context.confirmation, /Unsaved results/);
});

test("declining confirmation never calls the API", async () => {
  const { context, posts } = setup(running(), false);
  await context.forceStopSelectedPipelineRun({ disabled: false });
  assert.equal(posts.length, 0);
});

test("old workers and pending requests disable force stop; finished jobs hide it", () => {
  const oldRun = running();
  oldRun.result_json.worker_runtime.force_stop_supported = false;
  const old = setup(oldRun);
  old.context.renderPipelineRunDetail(oldRun);
  assert.match(old.detail.innerHTML, /id="force-stop-selected-pipeline-run-button"\s+disabled/);
  assert.match(old.detail.innerHTML, /updated worker/);
  const pending = running();
  pending.force_stop_requested_at = "now";
  const { context, detail } = setup(pending);
  context.renderPipelineRunDetail(pending);
  assert.match(detail.innerHTML, /Force stop requested…/);
  assert.match(detail.innerHTML, /resources remain reserved/);
  pending.status = "cancelled";
  pending.force_stopped_at = "later";
  context.renderPipelineRunDetail(pending);
  assert.doesNotMatch(detail.innerHTML, /force-stop-selected-pipeline-run-button/);
  assert.match(detail.innerHTML, /Confirmed at later/);
});
