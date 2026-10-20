(() => {
  const status = document.querySelector("#assistant-status");
  const form = document.querySelector("#assistant-form");
  const input = document.querySelector("#assistant-message");
  const mode = document.querySelector("#assistant-mode");
  const messages = document.querySelector("#assistant-messages");
  const feedback = document.querySelector("#assistant-feedback");
  const send = document.querySelector("#assistant-send");
  const adminControls = document.querySelector("#assistant-admin-controls");
  const startModel = document.querySelector("#assistant-start-model");
  const stopModel = document.querySelector("#assistant-stop-model");
  const controlFeedback = document.querySelector("#assistant-control-feedback");
  let controlBusy = false;

  function headers() {
    const token = localStorage.getItem("detecdivHub.sessionToken");
    return { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}) };
  }

  function addMessage(role, text) {
    const item = document.createElement("article");
    item.className = `assistant-message ${role}`;
    const label = document.createElement("strong");
    label.textContent = role === "assistant" ? "Qwen" : "You";
    const content = document.createElement("div");
    content.textContent = text;
    item.append(label, content);
    const wasNearBottom = messages.scrollHeight - messages.scrollTop - messages.clientHeight < 96;
    messages.append(item);
    if (role === "user" || wasNearBottom) {
      window.requestAnimationFrame(() => messages.scrollTo({ top: messages.scrollHeight, behavior: "smooth" }));
    }
    return item;
  }

  function finalAnswerOnly(text) {
    return String(text || "")
      .replace(/<think\b[^>]*>[\s\S]*?<\/think\s*>/gi, "")
      .replace(/<think\b[^>]*>[\s\S]*$/gi, "")
      .replace(/<\/think\s*>/gi, "")
      .trim();
  }

  async function getStatus() {
    try {
      const response = await fetch("/assistant/status", { credentials: "same-origin", headers: headers() });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || "Hub sign-in required.");
      status.textContent = payload.message;
      send.disabled = !payload.enabled || !payload.configured || !payload.running;
      if (adminControls) adminControls.classList.toggle("hidden", !payload.can_control);
      if (startModel) startModel.disabled = controlBusy || payload.control_pending || !payload.configured || payload.running;
      if (stopModel) stopModel.disabled = controlBusy || payload.control_pending || !payload.running;
      return payload;
    } catch (error) {
      status.textContent = error.message || "Sign in to the Hub to use the assistant.";
      send.disabled = true;
      if (adminControls) adminControls.classList.add("hidden");
      return null;
    }
  }

  async function controlModel(action) {
    if (controlBusy) return;
    controlBusy = true;
    if (startModel) startModel.disabled = true;
    if (stopModel) stopModel.disabled = true;
    if (controlFeedback) controlFeedback.textContent = action === "start" ? "Sending start request…" : "Sending stop request…";
    try {
      const response = await fetch("/assistant/control", {
        method: "POST",
        credentials: "same-origin",
        headers: headers(),
        body: JSON.stringify({ action }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || "Unable to control the Qwen service.");

      const deadline = Date.now() + 180000;
      let job;
      while (Date.now() < deadline) {
        const jobResponse = await fetch(`/jobs/${payload.id}`, { credentials: "same-origin", headers: headers() });
        job = await jobResponse.json();
        if (!jobResponse.ok) throw new Error(job.detail || "Unable to track the command sent to the worker.");
        if (["done", "failed", "cancelled"].includes(job.status)) break;
        if (controlFeedback) controlFeedback.textContent = "The worker is applying the command…";
        await new Promise((resolve) => window.setTimeout(resolve, 1500));
      }
      if (!job || !["done", "failed", "cancelled"].includes(job.status)) {
        throw new Error("The worker did not finish within the expected time.");
      }
      if (job.status !== "done") throw new Error(job.error_text || "The worker could not apply the command.");

      if (controlFeedback) controlFeedback.textContent = action === "start" ? "Qwen is starting…" : "Qwen is stopping…";
      const targetRunning = action === "start";
      while (Date.now() < deadline) {
        const current = await getStatus();
        if (current && current.running === targetRunning) {
          if (controlFeedback) controlFeedback.textContent = targetRunning ? "Qwen is ready." : "Qwen is stopped.";
          return;
        }
        await new Promise((resolve) => window.setTimeout(resolve, 2000));
      }
      throw new Error(action === "start" ? "Qwen is not ready yet. Check its status again shortly." : "The Qwen service has not confirmed its stop yet.");
    } catch (error) {
      if (controlFeedback) controlFeedback.textContent = error.message;
    } finally {
      controlBusy = false;
      await getStatus();
    }
  }

  startModel?.addEventListener("click", () => controlModel("start"));
  stopModel?.addEventListener("click", () => controlModel("stop"));

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const message = input.value.trim();
    if (!message) return;
    addMessage("user", message);
    input.value = "";
    send.disabled = true;
    feedback.textContent = "Qwen is responding…";
    const thinking = addMessage("assistant", "Qwen is thinking");
    thinking.classList.add("assistant-thinking");
    try {
      const response = await fetch("/assistant/chat", {
        method: "POST", credentials: "same-origin", headers: headers(),
        body: JSON.stringify({ message, mode: mode.value }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || "Assistant service error.");
      thinking.remove();
      addMessage("assistant", finalAnswerOnly(payload.answer) || "No final response could be generated.");
      feedback.textContent = `Response generated by ${payload.model}.`;
    } catch (error) {
      thinking.remove();
      feedback.textContent = error.message;
    } finally {
      await getStatus();
    }
  });

  getStatus();
})();
