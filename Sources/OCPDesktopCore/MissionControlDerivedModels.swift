import Foundation

public struct TopologyNode: Equatable, Identifiable, Sendable {
    public var id: String
    public var label: String
    public var role: String
    public var status: String
    public var subtitle: String

    public init(id: String, label: String, role: String, status: String, subtitle: String = "") {
        self.id = id
        self.label = label
        self.role = role
        self.status = status
        self.subtitle = subtitle
    }
}

public struct TopologyEdge: Equatable, Identifiable, Sendable {
    public var source: String
    public var target: String
    public var status: String
    public var freshness: String
    public var label: String

    public var id: String { "\(source)->\(target):\(label)" }

    public init(source: String, target: String, status: String, freshness: String = "", label: String = "") {
        self.source = source
        self.target = target
        self.status = status
        self.freshness = freshness
        self.label = label
    }
}

public struct TopologyGraph: Equatable, Sendable {
    public var nodes: [TopologyNode]
    public var edges: [TopologyEdge]

    public static let empty = TopologyGraph(nodes: [], edges: [])
}

public struct SetupGuideStep: Equatable, Identifiable, Sendable {
    public var id: String
    public var title: String
    public var summary: String
    public var status: String
    public var action: String

    public init(id: String, title: String, summary: String, status: String, action: String) {
        self.id = id
        self.title = title
        self.summary = summary
        self.status = status
        self.action = action
    }
}

public struct DeviceRoleSummary: Equatable, Identifiable, Sendable {
    public var id: String
    public var label: String
    public var role: String
    public var status: String
    public var summary: String

    public init(id: String, label: String, role: String, status: String, summary: String) {
        self.id = id
        self.label = label
        self.role = role
        self.status = status
        self.summary = summary
    }
}

public struct DemoStripState: Equatable, Sendable {
    public var phoneLabel: String
    public var phoneSummary: String
    public var primaryPeerLabel: String
    public var primaryPeerSummary: String
    public var proofLabel: String
    public var proofSummary: String
    public var recoveryLabel: String
    public var recoverySummary: String
    public var story: [String]

    public init(
        phoneLabel: String,
        phoneSummary: String,
        primaryPeerLabel: String,
        primaryPeerSummary: String,
        proofLabel: String,
        proofSummary: String,
        recoveryLabel: String,
        recoverySummary: String,
        story: [String]
    ) {
        self.phoneLabel = phoneLabel
        self.phoneSummary = phoneSummary
        self.primaryPeerLabel = primaryPeerLabel
        self.primaryPeerSummary = primaryPeerSummary
        self.proofLabel = proofLabel
        self.proofSummary = proofSummary
        self.recoveryLabel = recoveryLabel
        self.recoverySummary = recoverySummary
        self.story = story
    }
}

public enum MissionControlDeriver {
    public static func topology(from snapshot: AppStatusSnapshot?) -> TopologyGraph {
        guard let snapshot else {
            return TopologyGraph(
                nodes: [TopologyNode(id: "local", label: "This Mac", role: "local", status: "local_only", subtitle: "Start OCP to map the mesh.")],
                edges: []
            )
        }
        let localID = clean(snapshot.node?.nodeID) ?? "local"
        let setupRoles: [String: AppStatusSnapshot.Setup.DeviceRole] = Dictionary(
            uniqueKeysWithValues: (snapshot.setup?.deviceRoles ?? []).compactMap { role in
                guard let peerID = clean(role.peerID ?? role.displayName) else { return nil }
                return (peerID, role)
            }
        )
        var nodes: [String: TopologyNode] = [
            localID: TopologyNode(
                id: localID,
                label: clean(snapshot.node?.displayName) ?? clean(snapshot.node?.nodeID) ?? "This Mac",
                role: topologyRole(from: setupRoles[localID]?.role, fallback: "local"),
                status: snapshot.setup?.status ?? "ready",
                subtitle: snapshot.node?.formFactor ?? "OCP node"
            )
        ]
        var edges: [TopologyEdge] = []

        for route in snapshot.routeHealth?.routes ?? [] {
            guard let peerID = clean(route.peerID ?? route.displayName) else { continue }
            let setupRole = setupRoles[peerID]
            nodes[peerID] = TopologyNode(
                id: peerID,
                label: clean(route.displayName) ?? peerID,
                role: topologyRole(from: setupRole?.role, fallback: "peer"),
                status: setupRole?.status ?? route.status ?? "unknown",
                subtitle: clean(setupRole?.summary) ?? route.freshness ?? "route"
            )
            edges.append(
                TopologyEdge(
                    source: localID,
                    target: peerID,
                    status: route.status ?? "unknown",
                    freshness: route.freshness ?? "",
                    label: route.bestRoute ?? "route"
                )
            )
        }

        for target in snapshot.executionReadiness?.targets ?? [] {
            guard let peerID = clean(target.peerID ?? target.displayName) else { continue }
            let existing = nodes[peerID]
            let setupRole = setupRoles[peerID]
            nodes[peerID] = TopologyNode(
                id: peerID,
                label: clean(target.displayName) ?? existing?.label ?? peerID,
                role: topologyRole(from: setupRole?.role, fallback: target.role == "local" ? "local" : "worker"),
                status: setupRole?.status ?? target.status ?? existing?.status ?? "unknown",
                subtitle: clean(setupRole?.summary) ?? "\(target.workerCount ?? 0) worker(s)"
            )
            if peerID != localID && !edges.contains(where: { $0.source == localID && $0.target == peerID }) {
                edges.append(TopologyEdge(source: localID, target: peerID, status: target.status ?? "unknown", label: "execution"))
            }
        }

        for item in snapshot.artifactSync?.items ?? [] {
            guard let sourcePeer = clean(item.sourcePeerID), sourcePeer != localID else { continue }
            let existing = nodes[sourcePeer]
            let setupRole = setupRoles[sourcePeer]
            nodes[sourcePeer] = TopologyNode(
                id: sourcePeer,
                label: existing?.label ?? sourcePeer,
                role: topologyRole(from: setupRole?.role, fallback: existing?.role == "worker" ? "worker" : "artifact"),
                status: setupRole?.status ?? item.verificationStatus ?? existing?.status ?? "unknown",
                subtitle: clean(setupRole?.summary) ?? "artifact source"
            )
            edges.append(
                TopologyEdge(
                    source: sourcePeer,
                    target: localID,
                    status: item.verificationStatus ?? "unknown",
                    label: "artifact"
                )
            )
        }

        return TopologyGraph(
            nodes: nodes.values.sorted { lhs, rhs in
                if lhs.id == localID { return true }
                if rhs.id == localID { return false }
                return lhs.label.localizedCaseInsensitiveCompare(rhs.label) == .orderedAscending
            },
            edges: edges
        )
    }

    public static func deviceRoles(from snapshot: AppStatusSnapshot?) -> [DeviceRoleSummary] {
        guard let snapshot else {
            return [
                DeviceRoleSummary(
                    id: "local",
                    label: "This Mac",
                    role: "local_command",
                    status: "local_only",
                    summary: "Start OCP and switch to Mesh Mode to add trusted devices."
                )
            ]
        }

        let serverRoles = snapshot.setup?.deviceRoles ?? []
        if !serverRoles.isEmpty {
            return serverRoles.map {
                DeviceRoleSummary(
                    id: clean($0.peerID ?? $0.displayName) ?? UUID().uuidString,
                    label: clean($0.displayName) ?? clean($0.peerID) ?? "Peer",
                    role: clean($0.role) ?? "peer",
                    status: clean($0.status) ?? "unknown",
                    summary: clean($0.summary) ?? "Role available."
                )
            }
        }

        var roles: [DeviceRoleSummary] = [
            DeviceRoleSummary(
                id: clean(snapshot.node?.nodeID) ?? "local",
                label: clean(snapshot.node?.displayName) ?? "This Mac",
                role: "local_command",
                status: clean(snapshot.setup?.status) ?? "ready",
                summary: "This Mac is the local command node."
            )
        ]

        for target in snapshot.executionReadiness?.targets ?? [] {
            guard let peerID = clean(target.peerID ?? target.displayName),
                  clean(target.role)?.lowercased() != "local",
                  clean(target.status)?.lowercased() == "ready"
            else { continue }
            roles.append(
                DeviceRoleSummary(
                    id: peerID,
                    label: clean(target.displayName) ?? peerID,
                    role: "compute",
                    status: clean(target.status) ?? "ready",
                    summary: "\(clean(target.displayName) ?? peerID) is ready for compute work."
                )
            )
        }

        return roles
    }

    public static func demoState(snapshot: AppStatusSnapshot?, mode: LaunchMode, phoneURL: String) -> DemoStripState {
        let setup = snapshot?.setup
        let latestProof = snapshot?.latestProof
        let phoneReady = mode == .mesh && phoneURL.hasPrefix("http")
        let primaryPeer = setup?.primaryPeer
        let story = (setup?.story ?? []).filter { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }
        let recoveryState = clean(setup?.recoveryState) ?? clean(setup?.status) ?? "ready"
        let blockerCode = clean(setup?.blockerCode) ?? ""
        let proofStatus = clean(latestProof?.status ?? setup?.latestProofStatus) ?? "none"

        let phoneLabel: String
        let phoneSummary: String
        if phoneReady {
            phoneLabel = "Phone link ready"
            phoneSummary = "Your same-Wi-Fi phone link is live."
        } else if mode == .mesh {
            phoneLabel = "Mesh mode"
            phoneSummary = "LAN mode is on, but OCP still needs a phone link."
        } else {
            phoneLabel = "Local only"
            phoneSummary = "Start Mesh Mode to make a phone link."
        }

        let primaryPeerLabel = clean(primaryPeer?.displayName) ?? "No remote peer yet"
        let primaryPeerSummary = clean(primaryPeer?.summary) ?? "Connect another trusted device to build the mesh."

        let proofLabel = humanize(proofStatus)
        let proofSummary = clean(latestProof?.summary)
            ?? (proofStatus == "none" ? "No whole-mesh proof has run yet." : "OCP is tracking the latest whole-mesh proof.")

        let recoveryLabel = humanize(recoveryState)
        let recoverySummary: String
        switch recoveryState {
        case "repairing":
            recoverySummary = "OCP is repairing routes and retrying proof work."
        case "repaired":
            recoverySummary = "A route repair succeeded and the mesh recovered."
        case "needs_attention":
            recoverySummary = blockerCode.isEmpty ? (clean(setup?.nextFix) ?? "One concrete fix is waiting.") : "\(humanize(blockerCode)) detected."
        default:
            recoverySummary = clean(setup?.nextFix) ?? "The current mesh path is healthy."
        }

        return DemoStripState(
            phoneLabel: phoneLabel,
            phoneSummary: phoneSummary,
            primaryPeerLabel: primaryPeerLabel,
            primaryPeerSummary: primaryPeerSummary,
            proofLabel: proofLabel,
            proofSummary: proofSummary,
            recoveryLabel: recoveryLabel,
            recoverySummary: recoverySummary,
            story: story.isEmpty ? [clean(setup?.operatorSummary) ?? "Activate Mesh to start the demo."] : story
        )
    }

    public static func setupGuideSteps(snapshot: AppStatusSnapshot?, mode: LaunchMode, phoneURL: String) -> [SetupGuideStep] {
        let setupStatus = (snapshot?.setup?.status ?? "").lowercased()
        let recoveryState = (snapshot?.setup?.recoveryState ?? "").lowercased()
        let routeCount = snapshot?.setup?.routeCount ?? snapshot?.meshQuality?.routeCount ?? 0
        let healthyRoutes = snapshot?.setup?.healthyRouteCount ?? snapshot?.meshQuality?.healthyRoutes ?? 0
        let proofStatus = (snapshot?.setup?.latestProofStatus ?? snapshot?.latestProof?.status ?? "").lowercased()
        let phoneReady = mode == .mesh && phoneURL.hasPrefix("http")
        let strong = setupStatus == "strong"

        return [
            SetupGuideStep(
                id: "start_mesh",
                title: "Start Mesh Mode",
                summary: mode == .mesh ? "This Mac is listening for trusted devices on your LAN." : "Bind OCP to the LAN so your phone and spare laptop can reach it.",
                status: mode == .mesh ? "complete" : "active",
                action: "Start Mesh Mode"
            ),
            SetupGuideStep(
                id: "copy_phone_link",
                title: "Copy Phone Link",
                summary: phoneReady ? "A tokened phone link is ready for the same Wi-Fi." : "Mesh Mode creates the LAN phone link.",
                status: phoneReady ? "complete" : (mode == .mesh ? "active" : "blocked"),
                action: "Copy Phone Link"
            ),
            SetupGuideStep(
                id: "connect_device",
                title: "Connect Device",
                summary: routeCount > 0 ? "\(healthyRoutes)/\(routeCount) peer route(s) are fresh." : "Open the phone or laptop link and connect a nearby OCP node.",
                status: routeCount > 0 ? (healthyRoutes == routeCount ? "complete" : "attention") : (phoneReady ? "active" : "blocked"),
                action: "Open Setup Doctor"
            ),
            SetupGuideStep(
                id: "activate_mesh",
                title: "Activate Mesh",
                summary: proofStatus.isEmpty || proofStatus == "none" ? "Run discovery, repair, helper planning, and proof." : "Latest proof: \(proofStatus.replacingOccurrences(of: "_", with: " ")).",
                status: ["completed"].contains(proofStatus) ? "complete" : ((["planned", "queued", "running", "accepted"].contains(proofStatus) || recoveryState == "repairing") ? "active" : (routeCount > 0 ? "active" : "blocked")),
                action: "Activate Mesh"
            ),
            SetupGuideStep(
                id: "verify_strong",
                title: "Verify Strong",
                summary: strong ? "The mesh has proven routes and a completed proof." : "OCP will mark the mesh strong after route proof and whole-mesh proof complete.",
                status: strong ? "complete" : ((["failed", "needs_attention", "cancelled"].contains(proofStatus) || setupStatus == "needs_attention" || recoveryState == "needs_attention") ? "attention" : "blocked"),
                action: "Review Next Fix"
            ),
        ]
    }

    private static func topologyRole(from setupRole: String?, fallback: String) -> String {
        switch clean(setupRole)?.lowercased() {
        case "local_command":
            return "local"
        case "approval_only":
            return "approval"
        case "artifact_source":
            return "artifact"
        case let role?:
            return role
        default:
            return fallback
        }
    }

    private static func humanize(_ value: String) -> String {
        let token = clean(value) ?? "unknown"
        return token.replacingOccurrences(of: "_", with: " ").capitalized
    }

    private static func clean(_ value: String?) -> String? {
        let trimmed = (value ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }
}
