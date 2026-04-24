import Foundation

public enum ProofAssistantPhase: String, Codable, Equatable, CaseIterable, Sendable {
    case idle
    case startingMesh
    case waitingForServer
    case phoneLinkReady
    case activating
    case pollingProof
    case completed
    case needsAttention
    case failed

    public var label: String {
        switch self {
        case .idle:
            return "Idle"
        case .startingMesh:
            return "Starting Mesh"
        case .waitingForServer:
            return "Waiting for Server"
        case .phoneLinkReady:
            return "Phone Link Ready"
        case .activating:
            return "Activating"
        case .pollingProof:
            return "Polling Proof"
        case .completed:
            return "Completed"
        case .needsAttention:
            return "Needs Attention"
        case .failed:
            return "Failed"
        }
    }

    public var statusToken: String {
        switch self {
        case .idle:
            return "ready"
        case .startingMesh, .waitingForServer, .activating, .pollingProof:
            return "running"
        case .phoneLinkReady:
            return "ready"
        case .completed:
            return "completed"
        case .needsAttention:
            return "needs_attention"
        case .failed:
            return "failed"
        }
    }

    public var isRunning: Bool {
        switch self {
        case .startingMesh, .waitingForServer, .phoneLinkReady, .activating, .pollingProof:
            return true
        case .idle, .completed, .needsAttention, .failed:
            return false
        }
    }
}

public struct ProofAssistantStatus: Equatable, Sendable {
    public var phase: ProofAssistantPhase
    public var title: String
    public var message: String
    public var detail: String
    public var phoneURL: String
    public var copiedPhoneLink: Bool

    public init(
        phase: ProofAssistantPhase,
        title: String,
        message: String,
        detail: String = "",
        phoneURL: String = "",
        copiedPhoneLink: Bool = false
    ) {
        self.phase = phase
        self.title = title
        self.message = message
        self.detail = detail
        self.phoneURL = phoneURL
        self.copiedPhoneLink = copiedPhoneLink
    }

    public static let idle = ProofAssistantStatus(
        phase: .idle,
        title: "Ready to run proof",
        message: "Run Proof Assistant to start Mesh Mode, copy the phone link, activate the mesh, and watch for a strong proof."
    )

    public var isRunning: Bool {
        phase.isRunning
    }

    public var canRun: Bool {
        !phase.isRunning
    }

    public var phaseLabel: String {
        phase.label
    }

    public var statusToken: String {
        phase.statusToken
    }
}

public enum ProofAssistantReducer {
    public static func initial(mode: LaunchMode, phoneURL: String) -> ProofAssistantStatus {
        if mode == .mesh {
            return waitingForServer(phoneURL: phoneURL)
        }
        return ProofAssistantStatus(
            phase: .startingMesh,
            title: "Starting Mesh Mode",
            message: "The assistant is preparing this Mac to accept a second device.",
            phoneURL: clean(phoneURL) ?? ""
        )
    }

    public static func waitingForServer(phoneURL: String) -> ProofAssistantStatus {
        ProofAssistantStatus(
            phase: .waitingForServer,
            title: "Waiting for OCP server",
            message: "Mesh Mode is starting. The assistant will continue as soon as /mesh/app/status responds.",
            phoneURL: clean(phoneURL) ?? ""
        )
    }

    public static func phoneLinkReady(phoneURL: String, copiedPhoneLink: Bool) -> ProofAssistantStatus {
        ProofAssistantStatus(
            phase: .phoneLinkReady,
            title: copiedPhoneLink ? "Phone link copied" : "Phone link ready",
            message: copiedPhoneLink
                ? "Open the copied link on the second device. The assistant is ready to activate the mesh."
                : "Copy this link to the second device, then activate the mesh.",
            detail: copiedPhoneLink ? "The link was copied once for this run." : "",
            phoneURL: clean(phoneURL) ?? "",
            copiedPhoneLink: copiedPhoneLink
        )
    }

    public static func activating(phoneURL: String, copiedPhoneLink: Bool) -> ProofAssistantStatus {
        ProofAssistantStatus(
            phase: .activating,
            title: "Activating mesh",
            message: "OCP is probing routes, repairing what it can, and launching the proof run.",
            phoneURL: clean(phoneURL) ?? "",
            copiedPhoneLink: copiedPhoneLink
        )
    }

    public static func status(
        for snapshot: AppStatusSnapshot?,
        mode: LaunchMode,
        phoneURL: String,
        currentPhase: ProofAssistantPhase = .idle,
        copiedPhoneLink: Bool = false
    ) -> ProofAssistantStatus {
        guard let snapshot else {
            if mode == .mesh {
                return waitingForServer(phoneURL: phoneURL)
            }
            return initial(mode: mode, phoneURL: phoneURL)
        }

        let setupStatus = normalized(snapshot.setup?.status)
        let proofStatus = normalized(snapshot.latestProof?.status ?? snapshot.setup?.latestProofStatus)
        let recoveryState = normalized(snapshot.setup?.recoveryState)
        let nextFix = clean(snapshot.setup?.nextFix) ?? clean(snapshot.nextActions?.first)
        let proofSummary = clean(snapshot.latestProof?.summary)
        let operatorSummary = clean(snapshot.setup?.operatorSummary)
        let blockingIssue = clean(snapshot.setup?.blockingIssue)
        let link = bestPhoneURL(snapshot: snapshot, fallback: phoneURL)

        if setupStatus == "strong" {
            return ProofAssistantStatus(
                phase: .completed,
                title: "Proof completed",
                message: operatorSummary ?? proofSummary ?? "OCP reports a strong two-device proof.",
                detail: nextFix ?? "Mesh setup is strong.",
                phoneURL: link,
                copiedPhoneLink: copiedPhoneLink
            )
        }

        if ["needs_attention", "blocked"].contains(setupStatus)
            || ["needs_attention", "blocked"].contains(recoveryState)
            || ["failed", "needs_attention", "cancelled"].contains(proofStatus) {
            return ProofAssistantStatus(
                phase: .needsAttention,
                title: "Needs attention",
                message: nextFix ?? proofSummary ?? "OCP needs one concrete fix before the proof can finish.",
                detail: blockingIssue ?? proofSummary ?? "Keep the Mac app open and connect the second device with the copied phone link.",
                phoneURL: link,
                copiedPhoneLink: copiedPhoneLink
            )
        }

        if ["planned", "queued", "running", "accepted"].contains(proofStatus)
            || ["proving", "activating"].contains(setupStatus)
            || recoveryState == "repairing" {
            return ProofAssistantStatus(
                phase: .pollingProof,
                title: "Proof running",
                message: proofSummary ?? operatorSummary ?? nextFix ?? "The assistant is waiting for OCP to complete the whole-mesh proof.",
                detail: nextFix ?? "This can take a little while after the phone joins.",
                phoneURL: link,
                copiedPhoneLink: copiedPhoneLink
            )
        }

        if currentPhase == .activating {
            return activating(phoneURL: link, copiedPhoneLink: copiedPhoneLink)
        }

        if hasPhoneURL(link) {
            return phoneLinkReady(phoneURL: link, copiedPhoneLink: copiedPhoneLink)
        }

        return waitingForServer(phoneURL: link)
    }

    public static func startupTimeout(seconds: Int) -> ProofAssistantStatus {
        ProofAssistantStatus(
            phase: .failed,
            title: "Server did not start",
            message: "OCP did not answer /mesh/app/status within \(seconds) seconds.",
            detail: "Check whether another process owns the configured port, then run Proof Assistant again."
        )
    }

    public static func proofTimeout(snapshot: AppStatusSnapshot?, phoneURL: String, copiedPhoneLink: Bool) -> ProofAssistantStatus {
        if let snapshot {
            let nextFix = clean(snapshot.setup?.nextFix) ?? clean(snapshot.nextActions?.first)
            return ProofAssistantStatus(
                phase: .needsAttention,
                title: "Proof needs more time",
                message: nextFix ?? "The proof did not reach strong status before the timeout.",
                detail: clean(snapshot.setup?.blockingIssue) ?? "Make sure the second device opened the copied phone link and is still reachable.",
                phoneURL: bestPhoneURL(snapshot: snapshot, fallback: phoneURL),
                copiedPhoneLink: copiedPhoneLink
            )
        }

        return ProofAssistantStatus(
            phase: .failed,
            title: "Proof timed out",
            message: "OCP did not provide proof status before the timeout.",
            detail: "Run Proof Assistant again after confirming the server is reachable.",
            phoneURL: clean(phoneURL) ?? "",
            copiedPhoneLink: copiedPhoneLink
        )
    }

    public static func failure(_ message: String, detail: String = "", phoneURL: String = "", copiedPhoneLink: Bool = false) -> ProofAssistantStatus {
        ProofAssistantStatus(
            phase: .failed,
            title: "Proof Assistant failed",
            message: message,
            detail: detail,
            phoneURL: clean(phoneURL) ?? "",
            copiedPhoneLink: copiedPhoneLink
        )
    }

    private static func bestPhoneURL(snapshot: AppStatusSnapshot, fallback: String) -> String {
        if let fallback = clean(fallback), hasPhoneURL(fallback) {
            return fallback
        }
        return clean(snapshot.setup?.phoneURL) ?? clean(fallback) ?? ""
    }

    private static func hasPhoneURL(_ value: String) -> Bool {
        value.hasPrefix("http://") || value.hasPrefix("https://")
    }

    private static func normalized(_ value: String?) -> String {
        (clean(value) ?? "").lowercased().replacingOccurrences(of: " ", with: "_")
    }

    private static func clean(_ value: String?) -> String? {
        guard let value else { return nil }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }
}
