from __future__ import annotations

import argparse
import os
import secrets
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ocp_startup

APP_NAME = "OCP"
LOCAL_MODE = "local"
MESH_MODE = "mesh"

DEFAULT_CONFIG: dict[str, Any] = {
    "port": 8421,
    "node_id": "",
    "display_name": "OCP Node",
    "device_class": "full",
    "form_factor": "workstation",
    "operator_token": "",
}


@dataclass(frozen=True)
class LaunchPlan:
    mode: str
    profile: ocp_startup.StartupProfile
    command: list[str]
    app_url: str
    manifest_url: str
    share_urls: list[str]
    config_path: Path

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "host": self.profile.host,
            "port": self.profile.port,
            "node_id": self.profile.node_id,
            "display_name": self.profile.display_name,
            "command": list(self.command),
            "app_url": self.app_url,
            "manifest_url": self.manifest_url,
            "share_urls": list(self.share_urls),
            "config_path": str(self.config_path),
            "db_path": str(self.profile.db_path),
            "identity_dir": str(self.profile.identity_dir),
            "workspace_root": str(self.profile.workspace_root),
            "operator_auth_required": self.mode == MESH_MODE,
        }


def launcher_config_path(*, home: Path | None = None) -> Path:
    return ocp_startup.default_launcher_config_path(home=home)


def load_launcher_config(path: Path | None = None) -> dict[str, Any]:
    config = dict(DEFAULT_CONFIG)
    config.update(ocp_startup.read_json_file(path or launcher_config_path(), default={}))
    return normalize_launcher_config(config)


def save_launcher_config(config: dict[str, Any], path: Path | None = None) -> dict[str, Any]:
    normalized = normalize_launcher_config(config)
    ocp_startup.write_json_file(path or launcher_config_path(), normalized)
    return normalized


def normalize_launcher_config(config: dict[str, Any]) -> dict[str, Any]:
    payload = dict(DEFAULT_CONFIG)
    payload.update(dict(config or {}))
    payload["port"] = int(payload.get("port") or 8421)
    payload["node_id"] = str(payload.get("node_id") or "").strip() or ocp_startup.default_node_id()
    payload["display_name"] = str(payload.get("display_name") or "OCP Node").strip() or "OCP Node"
    payload["device_class"] = str(payload.get("device_class") or "full").strip() or "full"
    payload["form_factor"] = str(payload.get("form_factor") or "workstation").strip() or "workstation"
    payload["operator_token"] = str(payload.get("operator_token") or "").strip()
    return payload


def operator_app_url(base_url: str, operator_token: str = "") -> str:
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        return ""
    app_url = base if base.endswith("/app") else f"{base}/app"
    token = str(operator_token or "").strip()
    if not token:
        return app_url
    return f"{app_url}#ocp_operator_token={urllib.parse.quote(token, safe='')}"


def build_launch_plan(
    mode: str,
    config: dict[str, Any] | None,
    repo_root: Path,
    *,
    config_path: Path | None = None,
    home: Path | None = None,
    create_paths: bool = False,
) -> LaunchPlan:
    mode_token = MESH_MODE if str(mode or "").strip().lower() == MESH_MODE else LOCAL_MODE
    normalized = normalize_launcher_config(config or {})
    host = "0.0.0.0" if mode_token == MESH_MODE else "127.0.0.1"
    state_dir = ocp_startup.default_launcher_state_dir(home=home)
    profile = ocp_startup.profile_from_values(
        repo_root,
        host=host,
        port=int(normalized["port"]),
        node_id=str(normalized["node_id"]),
        display_name=str(normalized["display_name"]),
        device_class=str(normalized["device_class"]),
        form_factor=str(normalized["form_factor"]),
        state_dir=state_dir,
        create_paths=create_paths,
    )
    return LaunchPlan(
        mode=mode_token,
        profile=profile,
        command=ocp_startup.server_command(profile, repo_root),
        app_url=ocp_startup.build_open_url(host, profile.port, "/"),
        manifest_url=ocp_startup.health_url(host, profile.port),
        share_urls=ocp_startup.share_urls_for_host(host, profile.port),
        config_path=config_path or launcher_config_path(home=home),
    )


def server_is_alive(plan: LaunchPlan, *, timeout: float = 0.75) -> bool:
    try:
        with urllib.request.urlopen(plan.manifest_url, timeout=timeout) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


class OCPLauncherApp:
    def __init__(self, root, *, repo_root: Path, config_path: Path | None = None):
        import tkinter as tk
        from tkinter import ttk

        self.tk = tk
        self.ttk = ttk
        self.root = root
        self.repo_root = Path(repo_root)
        self.config_path = config_path or launcher_config_path()
        self.config = load_launcher_config(self.config_path)
        self.process: subprocess.Popen | None = None
        self.current_plan: LaunchPlan | None = None
        self.closing = False

        root.title("OCP Launcher")
        root.geometry("720x560")
        root.minsize(620, 500)
        root.protocol("WM_DELETE_WINDOW", self.close)

        self.display_name = tk.StringVar(value=str(self.config["display_name"]))
        self.node_id = tk.StringVar(value=str(self.config["node_id"]))
        self.port = tk.StringVar(value=str(self.config["port"]))
        self.status = tk.StringVar(value="OCP is stopped.")
        self.urls = tk.StringVar(value="Start Mesh Mode to get a phone/LAN link.")
        self.firewall_hint = tk.StringVar(
            value="Mesh Mode binds to your LAN. If another device cannot connect, allow Python/OCP through the macOS firewall."
        )

        self._build_ui()
        self._poll_status()

    def _build_ui(self) -> None:
        tk = self.tk
        ttk = self.ttk
        frame = ttk.Frame(self.root, padding=18)
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text="Open Compute Protocol", font=("Helvetica", 22, "bold")).pack(anchor="w")
        ttk.Label(
            frame,
            text="Start a local-first OCP node, share it with your phone, and activate the Autonomic Mesh.",
            wraplength=640,
        ).pack(anchor="w", pady=(4, 18))

        fields = ttk.LabelFrame(frame, text="Node profile", padding=12)
        fields.pack(fill="x")
        self._field(fields, "Display name", self.display_name, 0)
        self._field(fields, "Node id", self.node_id, 1)
        self._field(fields, "Port", self.port, 2)

        actions = ttk.Frame(frame)
        actions.pack(fill="x", pady=16)
        ttk.Button(actions, text="Start Mesh Mode", command=self.start_mesh_mode).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Start Local Only", command=self.start_local_mode).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Restart", command=self.restart).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Stop", command=self.stop).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Open App", command=self.open_app).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Copy Phone Link", command=self.copy_phone_link).pack(side="left")

        status_box = ttk.LabelFrame(frame, text="Status", padding=12)
        status_box.pack(fill="both", expand=True)
        ttk.Label(status_box, textvariable=self.status, wraplength=640).pack(anchor="w")
        ttk.Separator(status_box).pack(fill="x", pady=10)
        ttk.Label(status_box, text="Links", font=("Helvetica", 13, "bold")).pack(anchor="w")
        ttk.Label(status_box, textvariable=self.urls, wraplength=640, justify="left").pack(anchor="w", pady=(4, 10))
        ttk.Label(status_box, textvariable=self.firewall_hint, wraplength=640, foreground="#8a5a00").pack(anchor="w")

    def _field(self, parent, label: str, variable, row: int) -> None:
        ttk = self.ttk
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=4)
        entry = ttk.Entry(parent, textvariable=variable)
        entry.grid(row=row, column=1, sticky="ew", pady=4, padx=(12, 0))
        parent.columnconfigure(1, weight=1)

    def _current_config(self) -> dict[str, Any]:
        config = dict(self.config)
        config.update(
            {
                "display_name": self.display_name.get(),
                "node_id": self.node_id.get(),
                "port": self.port.get(),
            }
        )
        self.config = save_launcher_config(config, self.config_path)
        return self.config

    def _operator_token_for_mode(self, mode: str) -> str:
        if mode != MESH_MODE:
            return ""
        config = dict(self.config)
        token = str(config.get("operator_token") or "").strip()
        if not token:
            token = secrets.token_urlsafe(24)
            config["operator_token"] = token
            self.config = save_launcher_config(config, self.config_path)
        return token

    def _start(self, mode: str) -> None:
        if self.process and self.process.poll() is None:
            self.status.set("OCP is already running.")
            return
        config = self._current_config()
        plan = build_launch_plan(mode, config, self.repo_root, config_path=self.config_path, create_paths=True)
        ocp_startup.ensure_state_paths(plan.profile)
        self.current_plan = plan
        env = os.environ.copy()
        operator_token = self._operator_token_for_mode(plan.mode)
        if operator_token:
            env["OCP_OPERATOR_TOKEN"] = operator_token
        self.process = subprocess.Popen(plan.command, cwd=str(self.repo_root), env=env)
        self.status.set(f"Starting OCP in {plan.mode} mode...")
        self._render_links(plan)

    def start_mesh_mode(self) -> None:
        self._start(MESH_MODE)

    def start_local_mode(self) -> None:
        self._start(LOCAL_MODE)

    def stop(self) -> None:
        if not self.process or self.process.poll() is not None:
            self.status.set("OCP is already stopped.")
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        self.status.set("OCP stopped.")

    def close(self) -> None:
        self.closing = True
        self.stop()
        self.root.destroy()

    def restart(self) -> None:
        mode = self.current_plan.mode if self.current_plan else MESH_MODE
        self.stop()
        self._start(mode)

    def open_app(self) -> None:
        plan = self.current_plan or build_launch_plan(LOCAL_MODE, self._current_config(), self.repo_root)
        webbrowser.open(self._app_link(plan, plan.app_url))

    def copy_phone_link(self) -> None:
        plan = self.current_plan or build_launch_plan(MESH_MODE, self._current_config(), self.repo_root)
        link = self._app_link(plan, (plan.share_urls or [plan.app_url])[0])
        self.root.clipboard_clear()
        self.root.clipboard_append(link)
        self.status.set(f"Copied phone link: {link}")

    def _app_link(self, plan: LaunchPlan, base_url: str) -> str:
        token = self._operator_token_for_mode(plan.mode)
        return operator_app_url(base_url, token)

    def _render_links(self, plan: LaunchPlan) -> None:
        rows = [f"App: {self._app_link(plan, plan.app_url)}"]
        if plan.share_urls:
            rows.append("Phone/LAN:")
            rows.extend(f"  {self._app_link(plan, url)}" for url in plan.share_urls)
        else:
            rows.append("Phone/LAN: start Mesh Mode on Wi-Fi to expose a LAN link.")
        self.urls.set("\n".join(rows))

    def _poll_status(self) -> None:
        if self.closing:
            return
        plan = self.current_plan
        if plan and self.process and self.process.poll() is None:
            if server_is_alive(plan):
                self.status.set(f"OCP is running. Open {plan.app_url} or use the phone link below.")
            else:
                self.status.set("OCP is starting...")
        elif self.process and self.process.poll() is not None:
            self.status.set(f"OCP stopped with exit code {self.process.returncode}.")
        self.root.after(1500, self._poll_status)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Launch the OCP desktop app.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--config-path", default="")
    parser.add_argument("--plan", choices=[LOCAL_MODE, MESH_MODE], default="", help="Print a launch plan and exit.")
    args = parser.parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    config_path = Path(args.config_path).expanduser() if args.config_path else launcher_config_path()
    if args.plan:
        plan = build_launch_plan(args.plan, load_launcher_config(config_path), repo_root, config_path=config_path)
        print(plan.as_dict())
        return 0

    try:
        import tkinter as tk
    except Exception as exc:
        print(f"tkinter is required for the OCP desktop launcher: {exc}", file=sys.stderr)
        return 2

    root = tk.Tk()
    OCPLauncherApp(root, repo_root=repo_root, config_path=config_path)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
