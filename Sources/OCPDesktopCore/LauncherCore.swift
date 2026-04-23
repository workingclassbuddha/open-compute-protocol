import Foundation
import Darwin

public enum LaunchMode: String, Codable, CaseIterable, Sendable {
    case local
    case mesh

    public var host: String {
        switch self {
        case .local:
            return "127.0.0.1"
        case .mesh:
            return "0.0.0.0"
        }
    }
}

public struct LauncherConfig: Codable, Equatable, Sendable {
    public var port: Int
    public var nodeID: String
    public var displayName: String
    public var deviceClass: String
    public var formFactor: String
    public var operatorToken: String

    public init(
        port: Int = 8421,
        nodeID: String = "",
        displayName: String = "OCP Node",
        deviceClass: String = "full",
        formFactor: String = "workstation",
        operatorToken: String = ""
    ) {
        self.port = port
        self.nodeID = nodeID
        self.displayName = displayName
        self.deviceClass = deviceClass
        self.formFactor = formFactor
        self.operatorToken = operatorToken
    }

    public func normalized(defaultNodeID: String) -> LauncherConfig {
        LauncherConfig(
            port: max(1, port),
            nodeID: nodeID.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? defaultNodeID : nodeID.trimmingCharacters(in: .whitespacesAndNewlines),
            displayName: displayName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "OCP Node" : displayName.trimmingCharacters(in: .whitespacesAndNewlines),
            deviceClass: deviceClass.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "full" : deviceClass.trimmingCharacters(in: .whitespacesAndNewlines),
            formFactor: formFactor.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "workstation" : formFactor.trimmingCharacters(in: .whitespacesAndNewlines),
            operatorToken: operatorToken.trimmingCharacters(in: .whitespacesAndNewlines)
        )
    }
}

public struct LaunchPaths: Equatable, Sendable {
    public var supportDirectory: URL
    public var configPath: URL
    public var stateDirectory: URL
    public var databasePath: URL
    public var identityDirectory: URL
    public var workspaceDirectory: URL
}

public struct LaunchPlan: Equatable, Sendable {
    public var mode: LaunchMode
    public var host: String
    public var port: Int
    public var command: [String]
    public var environment: [String: String]
    public var appURL: String
    public var manifestURL: String
    public var phoneURLs: [String]
    public var repoRoot: URL
    public var paths: LaunchPaths
}

public struct AppStatusSnapshot: Decodable, Equatable, Sendable {
    public struct Node: Decodable, Equatable, Sendable {
        public var nodeID: String?
        public var displayName: String?
        public var deviceClass: String?
        public var formFactor: String?

        enum CodingKeys: String, CodingKey {
            case nodeID = "node_id"
            case displayName = "display_name"
            case deviceClass = "device_class"
            case formFactor = "form_factor"
        }
    }

    public struct MeshQuality: Decodable, Equatable, Sendable {
        public var status: String?
        public var label: String?
        public var peerCount: Int?
        public var routeCount: Int?
        public var healthyRoutes: Int?
        public var operatorSummary: String?

        enum CodingKeys: String, CodingKey {
            case status
            case label
            case peerCount = "peer_count"
            case routeCount = "route_count"
            case healthyRoutes = "healthy_routes"
            case operatorSummary = "operator_summary"
        }
    }

    public struct TimelineEvent: Decodable, Equatable, Identifiable, Sendable {
        public var kind: String
        public var status: String?
        public var summary: String?
        public var peerID: String?
        public var createdAt: String?

        public var id: String {
            "\(createdAt ?? "")-\(kind)-\(peerID ?? "")-\(summary ?? "")"
        }

        enum CodingKeys: String, CodingKey {
            case kind
            case status
            case summary
            case peerID = "peer_id"
            case createdAt = "created_at"
        }
    }

    public struct Setup: Decodable, Equatable, Sendable {
        public struct PrimaryPeer: Decodable, Equatable, Sendable {
            public var peerID: String?
            public var displayName: String?
            public var role: String?
            public var status: String?
            public var route: String?
            public var summary: String?

            enum CodingKeys: String, CodingKey {
                case peerID = "peer_id"
                case displayName = "display_name"
                case role
                case status
                case route
                case summary
            }
        }

        public struct DeviceRole: Decodable, Equatable, Identifiable, Sendable {
            public var peerID: String?
            public var displayName: String?
            public var role: String?
            public var status: String?
            public var summary: String?

            public var id: String { peerID ?? displayName ?? role ?? "device-role" }

            enum CodingKeys: String, CodingKey {
                case peerID = "peer_id"
                case displayName = "display_name"
                case role
                case status
                case summary
            }
        }

        public var status: String?
        public var label: String?
        public var phoneURL: String?
        public var knownPeerCount: Int?
        public var healthyRouteCount: Int?
        public var routeCount: Int?
        public var latestProofStatus: String?
        public var recoveryState: String?
        public var primaryPeer: PrimaryPeer?
        public var deviceRoles: [DeviceRole]?
        public var blockingIssue: String?
        public var blockerCode: String?
        public var nextFix: String?
        public var operatorSummary: String?
        public var story: [String]?
        public var timeline: [TimelineEvent]?

        enum CodingKeys: String, CodingKey {
            case status
            case label
            case phoneURL = "phone_url"
            case knownPeerCount = "known_peer_count"
            case healthyRouteCount = "healthy_route_count"
            case routeCount = "route_count"
            case latestProofStatus = "latest_proof_status"
            case recoveryState = "recovery_state"
            case primaryPeer = "primary_peer"
            case deviceRoles = "device_roles"
            case blockingIssue = "blocking_issue"
            case blockerCode = "blocker_code"
            case nextFix = "next_fix"
            case operatorSummary = "operator_summary"
            case story
            case timeline
        }
    }

    public struct Route: Decodable, Equatable, Identifiable, Sendable {
        public var peerID: String?
        public var displayName: String?
        public var status: String?
        public var freshness: String?
        public var bestRoute: String?
        public var operatorSummary: String?

        public var id: String { peerID ?? displayName ?? bestRoute ?? "route" }

        enum CodingKeys: String, CodingKey {
            case peerID = "peer_id"
            case displayName = "display_name"
            case status
            case freshness
            case bestRoute = "best_route"
            case operatorSummary = "operator_summary"
        }
    }

    public struct RouteHealth: Decodable, Equatable, Sendable {
        public var count: Int?
        public var healthy: Int?
        public var routes: [Route]?
        public var operatorSummary: String?

        enum CodingKeys: String, CodingKey {
            case count
            case healthy
            case routes
            case operatorSummary = "operator_summary"
        }
    }

    public struct ExecutionTarget: Decodable, Equatable, Identifiable, Sendable {
        public var peerID: String?
        public var displayName: String?
        public var role: String?
        public var status: String?
        public var workerCount: Int?
        public var routeStatus: String?
        public var routeFreshness: String?
        public var reasons: [String]?

        public var id: String { peerID ?? displayName ?? "target" }

        enum CodingKeys: String, CodingKey {
            case peerID = "peer_id"
            case displayName = "display_name"
            case role
            case status
            case workerCount = "worker_count"
            case routeStatus = "route_status"
            case routeFreshness = "route_freshness"
            case reasons
        }
    }

    public struct LocalExecution: Decodable, Equatable, Sendable {
        public var workerCount: Int?
        public var readyWorkerCount: Int?

        enum CodingKeys: String, CodingKey {
            case workerCount = "worker_count"
            case readyWorkerCount = "ready_worker_count"
        }
    }

    public struct ExecutionReadiness: Decodable, Equatable, Sendable {
        public var status: String?
        public var local: LocalExecution?
        public var targets: [ExecutionTarget]?
        public var operatorSummary: String?

        enum CodingKeys: String, CodingKey {
            case status
            case local
            case targets
            case operatorSummary = "operator_summary"
        }
    }

    public struct ArtifactSync: Decodable, Equatable, Sendable {
        public var status: String?
        public var replicatedCount: Int?
        public var verifiedCount: Int?
        public var latestSyncedAt: String?
        public var items: [ArtifactSyncItem]?
        public var operatorSummary: String?

        enum CodingKeys: String, CodingKey {
            case status
            case replicatedCount = "replicated_count"
            case verifiedCount = "verified_count"
            case latestSyncedAt = "latest_synced_at"
            case items
            case operatorSummary = "operator_summary"
        }
    }

    public struct ArtifactSyncItem: Decodable, Equatable, Identifiable, Sendable {
        public var artifactID: String?
        public var digest: String?
        public var sourcePeerID: String?
        public var verificationStatus: String?
        public var pinned: Bool?
        public var syncedAt: String?

        public var id: String { artifactID ?? digest ?? "artifact" }

        enum CodingKeys: String, CodingKey {
            case artifactID = "artifact_id"
            case digest
            case sourcePeerID = "source_peer_id"
            case verificationStatus = "verification_status"
            case pinned
            case syncedAt = "synced_at"
        }
    }

    public struct ProtocolStatus: Decodable, Equatable, Sendable {
        public var release: String?
        public var version: String?
        public var schemaVersion: String?
        public var contractURL: String?
        public var operatorSummary: String?

        enum CodingKeys: String, CodingKey {
            case release
            case version
            case schemaVersion = "schema_version"
            case contractURL = "contract_url"
            case operatorSummary = "operator_summary"
        }
    }

    public struct LatestProof: Decodable, Equatable, Sendable {
        public var status: String?
        public var title: String?
        public var summary: String?
        public var missionID: String?

        enum CodingKeys: String, CodingKey {
            case status
            case title
            case summary
            case missionID = "mission_id"
        }
    }

    public struct Approvals: Decodable, Equatable, Sendable {
        public var pendingCount: Int?

        enum CodingKeys: String, CodingKey {
            case pendingCount = "pending_count"
        }
    }

    public var status: String?
    public var node: Node?
    public var meshQuality: MeshQuality?
    public var setup: Setup?
    public var routeHealth: RouteHealth?
    public var executionReadiness: ExecutionReadiness?
    public var artifactSync: ArtifactSync?
    public var protocolStatus: ProtocolStatus?
    public var latestProof: LatestProof?
    public var approvals: Approvals?
    public var nextActions: [String]?
    public var generatedAt: String?

    enum CodingKeys: String, CodingKey {
        case status
        case node
        case meshQuality = "mesh_quality"
        case setup
        case routeHealth = "route_health"
        case executionReadiness = "execution_readiness"
        case artifactSync = "artifact_sync"
        case protocolStatus = "protocol"
        case latestProof = "latest_proof"
        case approvals
        case nextActions = "next_actions"
        case generatedAt = "generated_at"
    }
}

public struct AppStatusSample: Decodable, Equatable, Identifiable, Sendable {
    public var id: String
    public var sampledAt: String
    public var nodeID: String
    public var setupStatus: String
    public var meshScore: Int
    public var knownPeerCount: Int
    public var routeCount: Int
    public var healthyRouteCount: Int
    public var latestProofStatus: String
    public var executionReadyTargets: Int
    public var localReadyWorkers: Int
    public var artifactVerifiedCount: Int
    public var pendingApprovals: Int

    enum CodingKeys: String, CodingKey {
        case id
        case sampledAt = "sampled_at"
        case nodeID = "node_id"
        case setupStatus = "setup_status"
        case meshScore = "mesh_score"
        case knownPeerCount = "known_peer_count"
        case routeCount = "route_count"
        case healthyRouteCount = "healthy_route_count"
        case latestProofStatus = "latest_proof_status"
        case executionReadyTargets = "execution_ready_targets"
        case localReadyWorkers = "local_ready_workers"
        case artifactVerifiedCount = "artifact_verified_count"
        case pendingApprovals = "pending_approvals"
    }
}

public struct AppStatusHistory: Decodable, Equatable, Sendable {
    public var status: String
    public var count: Int
    public var limit: Int?
    public var samples: [AppStatusSample]
    public var generatedAt: String?

    public static let empty = AppStatusHistory(status: "ok", count: 0, limit: 0, samples: [], generatedAt: nil)

    enum CodingKeys: String, CodingKey {
        case status
        case count
        case limit
        case samples
        case generatedAt = "generated_at"
    }
}

public struct AppHistorySampleResponse: Decodable, Equatable, Sendable {
    public var status: String
    public var sample: AppStatusSample
    public var retentionLimit: Int?

    enum CodingKeys: String, CodingKey {
        case status
        case sample
        case retentionLimit = "retention_limit"
    }
}

public struct MissionControlChartPoint: Equatable, Identifiable, Sendable {
    public var id: String
    public var sampledAt: String
    public var meshScore: Int
    public var healthyRouteCount: Int
    public var routeCount: Int
    public var executionReadyTargets: Int
    public var artifactVerifiedCount: Int
    public var pendingApprovals: Int
}

public enum MissionControlMetrics {
    public static func chartPoints(from history: AppStatusHistory) -> [MissionControlChartPoint] {
        history.samples.map {
            MissionControlChartPoint(
                id: $0.id,
                sampledAt: $0.sampledAt,
                meshScore: $0.meshScore,
                healthyRouteCount: $0.healthyRouteCount,
                routeCount: $0.routeCount,
                executionReadyTargets: $0.executionReadyTargets,
                artifactVerifiedCount: $0.artifactVerifiedCount,
                pendingApprovals: $0.pendingApprovals
            )
        }
    }

    public static func meshScore(from snapshot: AppStatusSnapshot?) -> Int {
        guard let snapshot else { return 0 }
        let routeCount = snapshot.meshQuality?.routeCount ?? snapshot.setup?.routeCount ?? 0
        let healthyRoutes = snapshot.meshQuality?.healthyRoutes ?? snapshot.setup?.healthyRouteCount ?? 0
        let peerCount = snapshot.meshQuality?.peerCount ?? snapshot.setup?.knownPeerCount ?? 0
        let proofStatus = (snapshot.latestProof?.status ?? snapshot.setup?.latestProofStatus ?? "").lowercased()
        let setupStatus = (snapshot.setup?.status ?? "").lowercased()

        var score = routeCount > 0 ? Int(round((Double(healthyRoutes) / Double(max(1, routeCount))) * 70)) : (peerCount > 0 ? 30 : 12)
        if proofStatus == "completed" {
            score += 20
        } else if ["planned", "queued", "running", "accepted"].contains(proofStatus) {
            score += 8
        } else if ["failed", "needs_attention", "cancelled"].contains(proofStatus) {
            score -= 12
        }

        if setupStatus == "strong" {
            score += 10
        } else if setupStatus == "ready" {
            score += 5
        } else if ["needs_attention", "local_only"].contains(setupStatus) {
            score -= 5
        }
        return min(100, max(0, score))
    }
}

public enum LauncherCore {
    public static func supportDirectory(home: URL = FileManager.default.homeDirectoryForCurrentUser) -> URL {
        home
            .appendingPathComponent("Library", isDirectory: true)
            .appendingPathComponent("Application Support", isDirectory: true)
            .appendingPathComponent("OCP", isDirectory: true)
    }

    public static func paths(home: URL = FileManager.default.homeDirectoryForCurrentUser) -> LaunchPaths {
        let support = supportDirectory(home: home)
        let state = support.appendingPathComponent("state", isDirectory: true)
        return LaunchPaths(
            supportDirectory: support,
            configPath: support.appendingPathComponent("launcher.json"),
            stateDirectory: state,
            databasePath: state.appendingPathComponent("ocp.db"),
            identityDirectory: state.appendingPathComponent("identity", isDirectory: true),
            workspaceDirectory: state.appendingPathComponent("workspace", isDirectory: true)
        )
    }

    public static func ensurePaths(_ paths: LaunchPaths) throws {
        try FileManager.default.createDirectory(at: paths.supportDirectory, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: paths.stateDirectory, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: paths.databasePath.deletingLastPathComponent(), withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: paths.identityDirectory, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: paths.workspaceDirectory, withIntermediateDirectories: true)
    }

    public static func slugify(_ value: String) -> String {
        var result = ""
        var lastDash = false
        for scalar in value.lowercased().unicodeScalars {
            if CharacterSet.alphanumerics.contains(scalar) {
                result.unicodeScalars.append(scalar)
                lastDash = false
            } else if !lastDash {
                result.append("-")
                lastDash = true
            }
        }
        return result.trimmingCharacters(in: CharacterSet(charactersIn: "-"))
    }

    public static func defaultNodeID(hostname: String = Host.current().localizedName ?? "ocp") -> String {
        let token = slugify(hostname).isEmpty ? "ocp" : slugify(hostname)
        return "\(token)-node"
    }

    public static func defaultWorkerID(nodeID: String) -> String {
        let token = slugify(nodeID).isEmpty ? "ocp" : slugify(nodeID)
        return "\(token)-default-worker"
    }

    public static func autoWorkerEnabled(deviceClass: String, formFactor: String) -> Bool {
        let device = deviceClass.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let form = formFactor.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        return device == "full" && !["phone", "watch", "tablet"].contains(form)
    }

    public static func displayHostForBrowser(_ host: String) -> String {
        ["", "0.0.0.0", "::", "[::]"].contains(host.trimmingCharacters(in: .whitespacesAndNewlines)) ? "127.0.0.1" : host
    }

    public static func buildOpenURL(host: String, port: Int, path: String = "/") -> String {
        let route = path.hasPrefix("/") ? path : "/\(path)"
        return "http://\(displayHostForBrowser(host)):\(port)\(route.isEmpty ? "/" : route)"
    }

    public static func operatorAppURL(baseURL: String, operatorToken: String, path: String = "/app") -> String {
        let trimmed = baseURL.trimmingCharacters(in: .whitespacesAndNewlines).trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        guard !trimmed.isEmpty else { return "" }
        let route = path.hasPrefix("/") ? path : "/\(path)"
        let url = trimmed.hasSuffix(route) ? trimmed : "\(trimmed)\(route)"
        guard !operatorToken.isEmpty else { return url }
        let encoded = operatorToken.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? operatorToken
        return "\(url)#ocp_operator_token=\(encoded)"
    }

    public static func localIPv4Addresses() -> [String] {
        var addresses: Set<String> = []
        var ifaddr: UnsafeMutablePointer<ifaddrs>?
        guard getifaddrs(&ifaddr) == 0, let first = ifaddr else { return [] }
        defer { freeifaddrs(ifaddr) }

        var cursor: UnsafeMutablePointer<ifaddrs>? = first
        while let interface = cursor?.pointee {
            defer { cursor = interface.ifa_next }
            guard interface.ifa_addr.pointee.sa_family == UInt8(AF_INET) else { continue }
            var hostname = [CChar](repeating: 0, count: Int(NI_MAXHOST))
            let result = getnameinfo(
                interface.ifa_addr,
                socklen_t(interface.ifa_addr.pointee.sa_len),
                &hostname,
                socklen_t(hostname.count),
                nil,
                0,
                NI_NUMERICHOST
            )
            guard result == 0 else { continue }
            let address = String(cString: hostname)
            if !address.hasPrefix("127.") && address != "0.0.0.0" {
                addresses.insert(address)
            }
        }
        return addresses.sorted()
    }

    public static func shareURLs(host: String, port: Int, addresses: [String]? = nil) -> [String] {
        let token = host.trimmingCharacters(in: .whitespacesAndNewlines)
        if token == "0.0.0.0" || token.isEmpty {
            return (addresses ?? localIPv4Addresses()).map { "http://\($0):\(port)/" }
        }
        if token == "localhost" || token.hasPrefix("127.") {
            return []
        }
        return ["http://\(token):\(port)/"]
    }

    public static func loadConfig(from url: URL, hostname: String = Host.current().localizedName ?? "ocp") -> LauncherConfig {
        let fallback = LauncherConfig().normalized(defaultNodeID: defaultNodeID(hostname: hostname))
        guard let data = try? Data(contentsOf: url) else { return fallback }
        let decoded = (try? JSONDecoder().decode(LauncherConfig.self, from: data)) ?? fallback
        return decoded.normalized(defaultNodeID: defaultNodeID(hostname: hostname))
    }

    public static func saveConfig(_ config: LauncherConfig, to url: URL) throws {
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        let data = try JSONEncoder.pretty.encode(config)
        try data.write(to: url, options: .atomic)
    }

    public static func buildPlan(
        mode: LaunchMode,
        config rawConfig: LauncherConfig,
        repoRoot: URL,
        pythonExecutable: String = "/usr/bin/env",
        home: URL = FileManager.default.homeDirectoryForCurrentUser,
        addresses: [String]? = nil
    ) -> LaunchPlan {
        let launchPaths = paths(home: home)
        let config = rawConfig.normalized(defaultNodeID: defaultNodeID())
        let host = mode.host
        let command = [
            pythonExecutable,
            "python3",
            repoRoot.appendingPathComponent("server.py").path,
            "--host", host,
            "--port", String(config.port),
            "--db-path", launchPaths.databasePath.path,
            "--workspace-root", launchPaths.workspaceDirectory.path,
            "--identity-dir", launchPaths.identityDirectory.path,
            "--node-id", config.nodeID,
            "--display-name", config.displayName,
            "--device-class", config.deviceClass,
            "--form-factor", config.formFactor
        ]
        let shares = shareURLs(host: host, port: config.port, addresses: addresses)
        var environment: [String: String] = [:]
        if mode == .mesh && !config.operatorToken.isEmpty {
            environment["OCP_OPERATOR_TOKEN"] = config.operatorToken
        }
        if autoWorkerEnabled(deviceClass: config.deviceClass, formFactor: config.formFactor) {
            environment["OCP_AUTO_REGISTER_WORKER"] = "1"
            environment["OCP_AUTO_WORKER_ID"] = defaultWorkerID(nodeID: config.nodeID)
        }
        return LaunchPlan(
            mode: mode,
            host: host,
            port: config.port,
            command: command,
            environment: environment,
            appURL: buildOpenURL(host: host, port: config.port, path: "/"),
            manifestURL: buildOpenURL(host: host, port: config.port, path: "/mesh/manifest"),
            phoneURLs: shares.map { operatorAppURL(baseURL: $0, operatorToken: mode == .mesh ? config.operatorToken : "") },
            repoRoot: repoRoot,
            paths: launchPaths
        )
    }
}

private extension JSONEncoder {
    static var pretty: JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        return encoder
    }
}
