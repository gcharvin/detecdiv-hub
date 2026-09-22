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
    label.textContent = role === "assistant" ? "Qwen" : "Vous";
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
      if (!response.ok) throw new Error(payload.detail || "Connexion au Hub requise.");
      status.textContent = payload.message;
      send.disabled = !payload.enabled || !payload.configured || !payload.running;
      if (adminControls) adminControls.classList.toggle("hidden", !payload.can_control);
      if (startModel) startModel.disabled = controlBusy || payload.control_pending || !payload.configured || payload.running;
      if (stopModel) stopModel.disabled = controlBusy || payload.control_pending || !payload.running;
      return payload;
    } catch (error) {
      status.textContent = error.message || "Connexion au Hub requise pour utiliser l’assistant.";
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
    if (controlFeedback) controlFeedback.textContent = action === "start" ? "Envoi de l’ordre de démarrage…" : "Envoi de l’ordre d’arrêt…";
    try {
      const response = await fetch("/assistant/control", {
        method: "POST",
        credentials: "same-origin",
        headers: headers(),
        body: JSON.stringify({ action }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || "Impossible de contrôler le service Qwen.");

      const deadline = Date.now() + 180000;
      let job;
      while (Date.now() < deadline) {
        const jobResponse = await fetch(`/jobs/${payload.id}`, { credentials: "same-origin", headers: headers() });
        job = await jobResponse.json();
        if (!jobResponse.ok) throw new Error(job.detail || "Impossible de suivre l’ordre envoyé au worker.");
        if (["done", "failed", "cancelled"].includes(job.status)) break;
        if (controlFeedback) controlFeedback.textContent = "Le worker applique la commande…";
        await new Promise((resolve) => window.setTimeout(resolve, 1500));
      }
      if (!job || !["done", "failed", "cancelled"].includes(job.status)) {
        throw new Error("Le worker n’a pas terminé dans le délai prévu.");
      }
      if (job.status !== "done") throw new Error(job.error_text || "Le worker n’a pas pu appliquer la commande.");

      if (controlFeedback) controlFeedback.textContent = action === "start" ? "Qwen démarre…" : "Qwen s’arrête…";
      const targetRunning = action === "start";
      while (Date.now() < deadline) {
        const current = await getStatus();
        if (current && current.running === targetRunning) {
          if (controlFeedback) controlFeedback.textContent = targetRunning ? "Qwen est prêt." : "Qwen est arrêté.";
          return;
        }
        await new Promise((resolve) => window.setTimeout(resolve, 2000));
      }
      throw new Error(action === "start" ? "Qwen n’est pas encore prêt. Consultez son état dans quelques instants." : "Le service Qwen ne confirme pas encore son arrêt.");
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
    feedback.textContent = "Qwen répond…";
    const thinking = addMessage("assistant", "Qwen réfléchit");
    thinking.classList.add("assistant-thinking");
    try {
      const response = await fetch("/assistant/chat", {
        method: "POST", credentials: "same-origin", headers: headers(),
        body: JSON.stringify({ message, mode: mode.value }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || "Erreur du service assistant.");
      thinking.remove();
      addMessage("assistant", finalAnswerOnly(payload.answer) || "Je n’ai pas pu produire de réponse finale.");
      feedback.textContent = `Réponse générée par ${payload.model}.`;
    } catch (error) {
      thinking.remove();
      feedback.textContent = error.message;
    } finally {
      await getStatus();
    }
  });

  getStatus();
})();
