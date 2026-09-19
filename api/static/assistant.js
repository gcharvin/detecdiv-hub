(() => {
  const status = document.querySelector("#assistant-status");
  const form = document.querySelector("#assistant-form");
  const input = document.querySelector("#assistant-message");
  const mode = document.querySelector("#assistant-mode");
  const messages = document.querySelector("#assistant-messages");
  const feedback = document.querySelector("#assistant-feedback");
  const send = document.querySelector("#assistant-send");

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
    messages.append(item);
    messages.scrollTop = messages.scrollHeight;
  }

  async function getStatus() {
    try {
      const response = await fetch("/assistant/status", { credentials: "same-origin", headers: headers() });
      const payload = await response.json();
      status.textContent = payload.message;
      send.disabled = !payload.enabled || !payload.configured;
    } catch (_) {
      status.textContent = "Connexion au Hub requise pour utiliser l’assistant.";
      send.disabled = true;
    }
  }

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const message = input.value.trim();
    if (!message) return;
    addMessage("user", message);
    input.value = "";
    send.disabled = true;
    feedback.textContent = "Qwen répond…";
    try {
      const response = await fetch("/assistant/chat", {
        method: "POST", credentials: "same-origin", headers: headers(),
        body: JSON.stringify({ message, mode: mode.value }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || "Erreur du service assistant.");
      addMessage("assistant", payload.answer);
      feedback.textContent = `Réponse générée par ${payload.model}.`;
    } catch (error) {
      feedback.textContent = error.message;
    } finally {
      await getStatus();
    }
  });

  getStatus();
})();
