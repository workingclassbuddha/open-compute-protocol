import AppKit
import Foundation
import OCPDesktopCore

@MainActor
final class OCPDesktopModel: ObservableObject {
    @Published var config: LauncherConfig
    @Published var statusText = "OCP is stopped."
    @Published var appURL = "http://127.0.0.1:8421/"
    @Published var phoneURL = "Start Mesh Mode to create a phone link."
    @Published var snapshot: AppStatusSnapshot?
    @Published var history = AppStatusHistory.empty
    @Published var isRunning = false
    @Published var isActivating = false
    @Published var isProofAssistantRunning = false
    @Published var proofAssistant = ProofAssistantStatus.idle

    let repoRoot: URL
    let paths: LaunchPaths

    private var process: Process?
    private var timer: Timer?
    private var currentMode: LaunchMode = .local
    private var lastSampleAt = Date.distantPast
    private var isSampling = false

    init(repoRoot: URL = RepoLocator.defaultRepoRoot()) {
        self.repoRoot = repoRoot
        self.paths = LauncherCore.paths()
        self.config = LauncherCore.loadConfig(from: paths.configPath)
        refreshStaticLinks(mode: .local)
        startPolling()
    }

    deinit {
        timer?.invalidate()
        process?.terminate()
    }

    var chartPoints: [MissionControlChartPoint] {
        MissionControlMetrics.chartPoints(from: history)
    }

    var meshScore: Int {
        chartPoints.last?.meshScore ?? MissionControlMetrics.meshScore(from: snapshot)
    }

    var launchMode: LaunchMode {
        currentMode
    }

    var topology: TopologyGraph {
        MissionControlDeriver.topology(from: snapshot)
    }

    var demoState: DemoStripState {
        MissionControlDeriver.demoState(snapshot: snapshot, mode: currentMode, phoneURL: phoneURL)
    }

    var deviceRoles: [DeviceRoleSummary] {
        MissionControlDeriver.deviceRoles(from: snapshot)
    }

    var setupGuideSteps: [SetupGuideStep] {
        MissionControlDeriver.setupGuideSteps(snapshot: snapshot, mode: currentMode, phoneURL: phoneURL)
    }

    var setupLabel: String {
        snapshot?.setup?.label ?? snapshot?.setup?.status ?? "Local node ready"
    }

    var setupSummary: String {
        snapshot?.setup?.operatorSummary ?? snapshot?.setup?.nextFix ?? "Start OCP, open the app, then press Activate Mesh."
    }

    var nextFix: String {
        snapshot?.setup?.nextFix ?? "Press Activate Mesh to discover nearby devices and prove the mesh."
    }

    var executionSummary: String {
        snapshot?.executionReadiness?.operatorSummary ?? "No execution readiness yet."
    }

    var artifactSummary: String {
        snapshot?.artifactSync?.operatorSummary ?? "No artifact sync yet."
    }

    var protocolSummary: String {
        snapshot?.protocolStatus?.operatorSummary ?? "The live protocol contract is available after OCP starts."
    }

    var routeSummary: String {
        snapshot?.routeHealth?.operatorSummary ?? "No peer routes have been proven yet."
    }

    var serverBaseURL: String {
        let host = LauncherCore.displayHostForBrowser(currentMode.host)
        return "http://\(host):\(config.port)"
    }

    func startLocal() {
        start(mode: .local)
    }

    func startMesh() {
        if config.operatorToken.isEmpty {
            config.operatorToken = Self.generateToken()
        }
        start(mode: .mesh)
    }

    func restart() {
        let mode = currentMode
        stop()
        start(mode: mode)
    }

    func stop() {
        process?.terminate()
        process = nil
        isRunning = false
        statusText = "OCP stopped."
    }

    func openApp() {
        guard let url = URL(string: appLink(for: currentMode)) else { return }
        NSWorkspace.shared.open(url)
    }

    func copyPhoneLink() {
        let value = phoneURL.hasPrefix("http") ? phoneURL : appLink(for: currentMode)
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(value, forType: .string)
        statusText = "Copied phone link."
    }

    func refreshNow() {
        Task {
            await pollStatus(forceHistory: true)
        }
    }

    func activateMesh() {
        guard !isActivating else { return }
        isActivating = true
        statusText = "Activating Mesh: probing routes, planning helpers, and running proof..."
        Task {
            defer { Task { @MainActor in self.isActivating = false } }
            do {
                try await client().activateMesh()
                statusText = "Activate Mesh completed. Refreshing Mission Control..."
                await pollStatus(forceHistory: true)
            } catch {
                statusText = "Activate Mesh failed: \(error.localizedDescription)"
            }
        }
    }

    func runProofAssistant() {
        guard !isProofAssistantRunning else { return }
        isProofAssistantRunning = true
        Task {
            await runProofAssistantFlow()
        }
    }

    func saveConfig() {
        let normalized = config.normalized(defaultNodeID: LauncherCore.defaultNodeID())
        config = normalized
        do {
            try LauncherCore.saveConfig(normalized, to: paths.configPath)
            refreshStaticLinks(mode: currentMode)
            statusText = "Saved launcher settings."
        } catch {
            statusText = "Could not save settings: \(error.localizedDescription)"
        }
    }

    func pollStatus(forceHistory: Bool = false) async {
        do {
            let next = try await client().fetchStatus()
            apply(next)
            try await refreshHistory()
            if forceHistory || Date().timeIntervalSince(lastSampleAt) >= 15 {
                await recordHistorySample()
            }
        } catch {
            if isRunning {
                statusText = "OCP is starting or not reachable yet..."
            }
        }
    }

    private func runProofAssistantFlow() async {
        var copiedPhoneLink = false
        defer {
            isProofAssistantRunning = false
            isActivating = false
        }

        ensureOperatorToken()
        proofAssistant = ProofAssistantReducer.initial(mode: currentMode, phoneURL: phoneURL)
        statusText = proofAssistant.message

        if currentMode != .mesh || !isRunning {
            startMesh()
        } else {
            refreshStaticLinks(mode: .mesh)
        }

        proofAssistant = ProofAssistantReducer.waitingForServer(phoneURL: phoneURL)
        statusText = proofAssistant.message

        let firstSnapshot: AppStatusSnapshot
        do {
            firstSnapshot = try await waitForReachableServer(timeout: 20)
        } catch ProofAssistantRunError.startupTimeout {
            proofAssistant = ProofAssistantReducer.startupTimeout(seconds: 20)
            statusText = proofAssistant.message
            await recordHistorySample(preserveStatus: true)
            return
        } catch {
            proofAssistant = ProofAssistantReducer.failure(
                "Could not reach OCP: \(error.localizedDescription)",
                detail: "Start Mesh Mode manually or check the configured port, then run the assistant again.",
                phoneURL: phoneURL
            )
            statusText = proofAssistant.message
            await recordHistorySample(preserveStatus: true)
            return
        }

        apply(firstSnapshot)
        copiedPhoneLink = copyProofAssistantPhoneLinkIfReady()
        proofAssistant = ProofAssistantReducer.phoneLinkReady(phoneURL: phoneURL, copiedPhoneLink: copiedPhoneLink)
        statusText = proofAssistant.message

        proofAssistant = ProofAssistantReducer.activating(phoneURL: phoneURL, copiedPhoneLink: copiedPhoneLink)
        statusText = proofAssistant.message
        isActivating = true

        do {
            try await client().activateMesh()
        } catch {
            isActivating = false
            proofAssistant = ProofAssistantReducer.failure(
                "Activate Mesh failed: \(error.localizedDescription)",
                detail: nextFix,
                phoneURL: phoneURL,
                copiedPhoneLink: copiedPhoneLink
            )
            statusText = proofAssistant.message
            await recordHistorySample(preserveStatus: true)
            return
        }

        isActivating = false
        proofAssistant = ProofAssistantStatus(
            phase: .pollingProof,
            title: "Polling proof",
            message: "Activation completed. The assistant is watching for strong mesh status.",
            phoneURL: phoneURL,
            copiedPhoneLink: copiedPhoneLink
        )
        statusText = proofAssistant.message

        let finalStatus = await pollProofUntilDone(timeout: 90, copiedPhoneLink: copiedPhoneLink)
        proofAssistant = finalStatus
        statusText = finalStatus.message
        await recordHistorySample(preserveStatus: true)
    }

    private func waitForReachableServer(timeout: TimeInterval) async throws -> AppStatusSnapshot {
        let deadline = Date().addingTimeInterval(timeout)
        var lastError: Error?

        while Date() < deadline {
            do {
                let next = try await client().fetchStatus()
                apply(next)
                try? await refreshHistory()
                return next
            } catch {
                lastError = error
                try? await Task.sleep(nanoseconds: 500_000_000)
            }
        }

        if lastError != nil {
            throw ProofAssistantRunError.startupTimeout
        }
        throw ProofAssistantRunError.startupTimeout
    }

    private func pollProofUntilDone(timeout: TimeInterval, copiedPhoneLink: Bool) async -> ProofAssistantStatus {
        let deadline = Date().addingTimeInterval(timeout)
        var latestSnapshot = snapshot

        while Date() < deadline {
            do {
                let next = try await client().fetchStatus()
                latestSnapshot = next
                apply(next)
                try? await refreshHistory()
                let reduced = ProofAssistantReducer.status(
                    for: next,
                    mode: currentMode,
                    phoneURL: phoneURL,
                    currentPhase: .pollingProof,
                    copiedPhoneLink: copiedPhoneLink
                )

                switch reduced.phase {
                case .completed, .needsAttention, .failed:
                    return reduced
                default:
                    proofAssistant = reduced.phase == .pollingProof ? reduced : ProofAssistantStatus(
                        phase: .pollingProof,
                        title: "Polling proof",
                        message: reduced.message,
                        detail: reduced.detail.isEmpty ? "Waiting for OCP to report strong status or a concrete fix." : reduced.detail,
                        phoneURL: reduced.phoneURL,
                        copiedPhoneLink: copiedPhoneLink
                    )
                    statusText = proofAssistant.message
                }
            } catch {
                proofAssistant = ProofAssistantStatus(
                    phase: .pollingProof,
                    title: "Polling proof",
                    message: "Status polling is retrying: \(error.localizedDescription)",
                    detail: "The assistant will keep trying until the proof timeout.",
                    phoneURL: phoneURL,
                    copiedPhoneLink: copiedPhoneLink
                )
                statusText = proofAssistant.message
            }

            try? await Task.sleep(nanoseconds: 2_000_000_000)
        }

        return ProofAssistantReducer.proofTimeout(
            snapshot: latestSnapshot,
            phoneURL: phoneURL,
            copiedPhoneLink: copiedPhoneLink
        )
    }

    private func ensureOperatorToken() {
        guard config.operatorToken.isEmpty else { return }
        config.operatorToken = Self.generateToken()
        let normalized = config.normalized(defaultNodeID: LauncherCore.defaultNodeID())
        config = normalized
        try? LauncherCore.saveConfig(normalized, to: paths.configPath)
    }

    private func copyProofAssistantPhoneLinkIfReady() -> Bool {
        let value = phoneURL.hasPrefix("http") ? phoneURL : appLink(for: .mesh)
        guard value.hasPrefix("http") else { return false }
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(value, forType: .string)
        phoneURL = value
        return true
    }

    private func start(mode: LaunchMode) {
        stop()
        currentMode = mode
        let normalized = config.normalized(defaultNodeID: LauncherCore.defaultNodeID())
        config = normalized
        do {
            try LauncherCore.saveConfig(normalized, to: paths.configPath)
            try LauncherCore.ensurePaths(paths)
            let plan = LauncherCore.buildPlan(mode: mode, config: normalized, repoRoot: repoRoot)
            refreshStaticLinks(mode: mode)
            let process = Process()
            process.executableURL = URL(fileURLWithPath: plan.command[0])
            process.arguments = Array(plan.command.dropFirst())
            process.currentDirectoryURL = repoRoot
            var env = ProcessInfo.processInfo.environment
            for (key, value) in plan.environment {
                env[key] = value
            }
            process.environment = env
            process.terminationHandler = { [weak self] process in
                Task { @MainActor in
                    self?.isRunning = false
                    self?.statusText = "OCP stopped with exit code \(process.terminationStatus)."
                }
            }
            try process.run()
            self.process = process
            isRunning = true
            statusText = "Starting OCP in \(mode.rawValue) mode..."
        } catch {
            statusText = "Could not start OCP: \(error.localizedDescription)"
        }
    }

    private func apply(_ snapshot: AppStatusSnapshot) {
        self.snapshot = snapshot
        if let phone = snapshot.setup?.phoneURL, !phone.isEmpty {
            phoneURL = tokened(url: phone)
        }
        statusText = setupSummary
    }

    private func refreshHistory() async throws {
        history = try await client().fetchHistory(limit: 240)
    }

    private func recordHistorySample(preserveStatus: Bool = false) async {
        guard !isSampling else { return }
        isSampling = true
        let previousStatus = statusText
        defer { isSampling = false }
        do {
            _ = try await client().recordHistorySample()
            lastSampleAt = Date()
            try await refreshHistory()
        } catch {
            statusText = preserveStatus ? previousStatus : "Status is live, but history sampling failed: \(error.localizedDescription)"
        }
    }

    private func refreshStaticLinks(mode: LaunchMode) {
        let plan = LauncherCore.buildPlan(mode: mode, config: config, repoRoot: repoRoot)
        appURL = tokened(url: plan.appURL)
        phoneURL = plan.phoneURLs.first ?? (mode == .mesh ? "No LAN IP found yet. Check Wi-Fi." : "Start Mesh Mode to create a phone link.")
    }

    private func startPolling() {
        timer = Timer.scheduledTimer(withTimeInterval: 2.0, repeats: true) { [weak self] _ in
            Task { @MainActor in
                await self?.pollStatus()
            }
        }
    }

    private func client() -> OCPServerClient {
        OCPServerClient(baseURL: serverBaseURL, operatorToken: config.operatorToken)
    }

    private func appLink(for mode: LaunchMode) -> String {
        let base = LauncherCore.buildOpenURL(host: mode.host, port: config.port, path: "/")
        return tokened(url: base)
    }

    private func tokened(url: String) -> String {
        LauncherCore.operatorAppURL(baseURL: url, operatorToken: currentMode == .mesh ? config.operatorToken : "")
    }

    private static func generateToken() -> String {
        "\(UUID().uuidString.lowercased())-\(UUID().uuidString.lowercased())"
    }
}

private enum ProofAssistantRunError: Error {
    case startupTimeout
}
