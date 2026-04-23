from __future__ import annotations


def build_browser_client_script() -> str:
    """Shared browser helpers for OCP's local operator pages."""
    return """
    const OCP_OPERATOR_TOKEN_KEY = "ocp_operator_token";

    function consumeOperatorToken() {
      const hash = String(window.location.hash || "");
      let token = "";
      if (hash.indexOf("#ocp_operator_token=") === 0) {
        token = decodeURIComponent(hash.slice("#ocp_operator_token=".length));
      } else if (hash.indexOf("ocp_operator_token=") !== -1) {
        token = new URLSearchParams(hash.replace(/^#/, "")).get("ocp_operator_token") || "";
      }
      if (token) {
        try {
          window.localStorage.setItem(OCP_OPERATOR_TOKEN_KEY, token);
        } catch (error) {
        }
        history.replaceState(null, "", window.location.pathname + window.location.search);
      }
    }

    function operatorToken() {
      try {
        return String(window.localStorage.getItem(OCP_OPERATOR_TOKEN_KEY) || "").trim();
      } catch (error) {
        return "";
      }
    }

    function withOperatorAuth(options) {
      const next = Object.assign({}, options || {});
      const headers = new Headers(next.headers || {});
      const token = operatorToken();
      if (token && !headers.has("X-OCP-Operator-Token")) {
        headers.set("X-OCP-Operator-Token", token);
      }
      next.headers = headers;
      return next;
    }

    function withOperatorFragment(url) {
      const token = operatorToken();
      const target = String(url || "");
      if (!token || !target) {
        return target;
      }
      return target.replace(/#.*$/, "") + "#ocp_operator_token=" + encodeURIComponent(token);
    }

    async function fetchJson(url, options) {
      const response = await fetch(url, withOperatorAuth(options));
      if (!response.ok) {
        let message = response.status + " " + response.statusText;
        try {
          const payload = await response.json();
          message = payload.error || payload.message || message;
        } catch (error) {
        }
        throw new Error(message);
      }
      return response.json();
    }

    async function copyText(text) {
      const token = String(text || "");
      if (!token) {
        throw new Error("nothing to copy");
      }
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(token);
        return;
      }
      const input = document.createElement("textarea");
      input.value = token;
      input.setAttribute("readonly", "readonly");
      input.style.position = "absolute";
      input.style.left = "-9999px";
      document.body.appendChild(input);
      input.select();
      document.execCommand("copy");
      document.body.removeChild(input);
    }

    consumeOperatorToken();
""".strip()


__all__ = ["build_browser_client_script"]
