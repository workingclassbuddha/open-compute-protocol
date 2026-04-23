import Testing
@testable import OCPDesktopCore
import Foundation

@Test func launchPlanUsesApplicationSupportStateAndLocalHost() {
    let home = URL(fileURLWithPath: "/tmp/ocp-home", isDirectory: true)
    let repo = URL(fileURLWithPath: "/tmp/open-compute-protocol", isDirectory: true)
    let config = LauncherConfig(
        port: 8555,
        nodeID: "alpha-node",
        displayName: "Alpha",
        deviceClass: "full",
        formFactor: "laptop",
        operatorToken: "secret token"
    )

    let plan = LauncherCore.buildPlan(
        mode: .local,
        config: config,
        repoRoot: repo,
        pythonExecutable: "/usr/bin/env",
        home: home,
        addresses: ["192.168.1.44"]
    )

    #expect(plan.host == "127.0.0.1")
    #expect(plan.appURL == "http://127.0.0.1:8555/")
    #expect(plan.phoneURLs == [])
    #expect(plan.paths.databasePath.path.hasSuffix("Library/Application Support/OCP/state/ocp.db"))
    #expect(plan.environment["OCP_AUTO_REGISTER_WORKER"] == "1")
    #expect(plan.environment["OCP_AUTO_WORKER_ID"] == "alpha-node-default-worker")
    #expect(plan.environment["OCP_OPERATOR_TOKEN"] == nil)
    #expect(plan.command.contains("--display-name"))
    #expect(plan.command.contains("Alpha"))
}

@Test func meshPlanBuildsTokenedPhoneLinks() {
    let home = URL(fileURLWithPath: "/tmp/ocp-home", isDirectory: true)
    let repo = URL(fileURLWithPath: "/tmp/open-compute-protocol", isDirectory: true)
    let config = LauncherConfig(
        port: 8421,
        nodeID: "beta-node",
        displayName: "Beta",
        deviceClass: "full",
        formFactor: "workstation",
        operatorToken: "secret token"
    )

    let plan = LauncherCore.buildPlan(
        mode: .mesh,
        config: config,
        repoRoot: repo,
        home: home,
        addresses: ["192.168.1.44"]
    )

    #expect(plan.host == "0.0.0.0")
    #expect(plan.appURL == "http://127.0.0.1:8421/")
    #expect(plan.phoneURLs == ["http://192.168.1.44:8421/app#ocp_operator_token=secret%20token"])
    #expect(plan.environment["OCP_OPERATOR_TOKEN"] == "secret token")
}

@Test func deviceHelpersMatchPythonLauncherDefaults() {
    #expect(LauncherCore.slugify("Alpha Node!") == "alpha-node")
    #expect(LauncherCore.defaultWorkerID(nodeID: "Alpha Node!") == "alpha-node-default-worker")
    #expect(LauncherCore.autoWorkerEnabled(deviceClass: "full", formFactor: "laptop"))
    #expect(!LauncherCore.autoWorkerEnabled(deviceClass: "light", formFactor: "phone"))
}

@Test func appStatusSnapshotDecodesMissionControlFields() throws {
    let data = """
    {
      "status": "ok",
      "node": {"node_id": "alpha-node", "display_name": "Alpha"},
      "mesh_quality": {"peer_count": 1, "route_count": 2, "healthy_routes": 1},
      "setup": {
        "status": "needs_attention",
        "label": "Route needs repair",
        "known_peer_count": 1,
        "healthy_route_count": 1,
        "route_count": 2,
        "latest_proof_status": "failed",
        "recovery_state": "needs_attention",
        "primary_peer": {
          "peer_id": "beta-node",
          "display_name": "Beta Laptop",
          "role": "compute",
          "status": "ready",
          "summary": "Beta Laptop is best for compute right now."
        },
        "device_roles": [
          {
            "peer_id": "alpha-node",
            "display_name": "Alpha",
            "role": "local_command",
            "status": "ready",
            "summary": "This Mac is the local command node."
          },
          {
            "peer_id": "beta-node",
            "display_name": "Beta Laptop",
            "role": "compute",
            "status": "ready",
            "summary": "Beta Laptop is ready for compute work."
          }
        ],
        "blocker_code": "proof_failed",
        "next_fix": "Press Activate Mesh again.",
        "story": [
          "The latest whole-mesh proof did not complete.",
          "Beta Laptop is best for compute right now."
        ],
        "timeline": [{"kind": "route_verified", "status": "ok", "summary": "Beta is reachable."}]
      },
      "route_health": {"count": 2, "healthy": 1, "routes": [{"peer_id": "beta-node", "status": "reachable", "freshness": "fresh"}]},
      "execution_readiness": {"status": "ready", "local": {"ready_worker_count": 1}, "targets": [{"peer_id": "alpha-node", "status": "ready", "worker_count": 1}]},
      "artifact_sync": {"status": "verified", "verified_count": 1, "replicated_count": 1},
      "protocol": {"release": "0.1", "version": "sovereign-mesh/v1"},
      "latest_proof": {"status": "failed"},
      "approvals": {"pending_count": 0},
      "next_actions": ["Repair route"]
    }
    """.data(using: .utf8)!

    let snapshot = try JSONDecoder().decode(AppStatusSnapshot.self, from: data)

    #expect(snapshot.node?.nodeID == "alpha-node")
    #expect(snapshot.setup?.timeline?.first?.kind == "route_verified")
    #expect(snapshot.setup?.recoveryState == "needs_attention")
    #expect(snapshot.setup?.primaryPeer?.peerID == "beta-node")
    #expect(snapshot.setup?.deviceRoles?.count == 2)
    #expect(snapshot.setup?.blockerCode == "proof_failed")
    #expect(snapshot.setup?.story?.first == "The latest whole-mesh proof did not complete.")
    #expect(snapshot.routeHealth?.routes?.first?.peerID == "beta-node")
    #expect(snapshot.executionReadiness?.targets?.first?.status == "ready")
    #expect(snapshot.artifactSync?.verifiedCount == 1)
    #expect(MissionControlMetrics.meshScore(from: snapshot) == 18)
}

@Test func appStatusHistoryBuildsChartPoints() throws {
    let data = """
    {
      "status": "ok",
      "count": 2,
      "limit": 240,
      "samples": [
        {
          "id": "s1",
          "sampled_at": "2026-04-23T01:00:00Z",
          "node_id": "alpha-node",
          "setup_status": "ready",
          "mesh_score": 55,
          "known_peer_count": 1,
          "route_count": 1,
          "healthy_route_count": 1,
          "latest_proof_status": "planned",
          "execution_ready_targets": 1,
          "local_ready_workers": 1,
          "artifact_verified_count": 0,
          "pending_approvals": 0
        },
        {
          "id": "s2",
          "sampled_at": "2026-04-23T01:01:00Z",
          "node_id": "alpha-node",
          "setup_status": "strong",
          "mesh_score": 100,
          "known_peer_count": 2,
          "route_count": 2,
          "healthy_route_count": 2,
          "latest_proof_status": "completed",
          "execution_ready_targets": 2,
          "local_ready_workers": 1,
          "artifact_verified_count": 1,
          "pending_approvals": 0
        }
      ]
    }
    """.data(using: .utf8)!

    let history = try JSONDecoder().decode(AppStatusHistory.self, from: data)
    let points = MissionControlMetrics.chartPoints(from: history)

    #expect(points.count == 2)
    #expect(points[0].meshScore == 55)
    #expect(points[1].healthyRouteCount == 2)
    #expect(points[1].artifactVerifiedCount == 1)
}

@Test func topologyDerivesLocalOnlyAndHealthyPeerGraph() throws {
    let empty = MissionControlDeriver.topology(from: nil)
    #expect(empty.nodes.count == 1)
    #expect(empty.edges.isEmpty)
    #expect(empty.nodes[0].role == "local")

    let data = """
    {
      "status": "ok",
      "node": {"node_id": "alpha-node", "display_name": "Alpha", "form_factor": "laptop"},
      "setup": {"status": "ready"},
      "route_health": {
        "count": 1,
        "healthy": 1,
        "routes": [
          {"peer_id": "beta-node", "display_name": "Beta", "status": "reachable", "freshness": "fresh", "best_route": "http://beta:8421"}
        ]
      },
      "execution_readiness": {
        "status": "ready",
        "targets": [
          {"peer_id": "beta-node", "display_name": "Beta", "role": "remote", "status": "ready", "worker_count": 1}
        ]
      }
    }
    """.data(using: .utf8)!
    let snapshot = try JSONDecoder().decode(AppStatusSnapshot.self, from: data)
    let graph = MissionControlDeriver.topology(from: snapshot)

    #expect(graph.nodes.map(\.id).contains("alpha-node"))
    #expect(graph.nodes.map(\.id).contains("beta-node"))
    #expect(graph.edges.contains { $0.source == "alpha-node" && $0.target == "beta-node" && $0.status == "reachable" })
    #expect(graph.nodes.first { $0.id == "beta-node" }?.role == "worker")
}

@Test func topologyMarksStaleFailedAndArtifactEdges() throws {
    let data = """
    {
      "status": "ok",
      "node": {"node_id": "alpha-node", "display_name": "Alpha"},
      "route_health": {
        "count": 2,
        "healthy": 0,
        "routes": [
          {"peer_id": "stale-node", "status": "reachable", "freshness": "stale"},
          {"peer_id": "failed-node", "status": "unreachable", "freshness": "failed"}
        ]
      },
      "artifact_sync": {
        "status": "verified",
        "items": [
          {"artifact_id": "a1", "source_peer_id": "stale-node", "verification_status": "verified"}
        ]
      }
    }
    """.data(using: .utf8)!
    let snapshot = try JSONDecoder().decode(AppStatusSnapshot.self, from: data)
    let graph = MissionControlDeriver.topology(from: snapshot)

    #expect(graph.edges.contains { $0.target == "stale-node" && $0.freshness == "stale" })
    #expect(graph.edges.contains { $0.target == "failed-node" && $0.status == "unreachable" })
    #expect(graph.edges.contains { $0.source == "stale-node" && $0.target == "alpha-node" && $0.label == "artifact" })
}

@Test func demoStateAndDeviceRolesUseSetupProjectionWhenAvailable() throws {
    let data = """
    {
      "status": "ok",
      "node": {"node_id": "alpha-node", "display_name": "Alpha"},
      "setup": {
        "status": "proving",
        "label": "Proof running",
        "phone_url": "http://192.168.1.4:8421/app",
        "latest_proof_status": "running",
        "recovery_state": "repairing",
        "blocker_code": "",
        "next_fix": "Keep this page open while OCP finishes route checks and proof execution.",
        "story": [
          "OCP is repairing routes and proving the mesh.",
          "Beta Laptop is best for compute right now."
        ],
        "primary_peer": {
          "peer_id": "beta-node",
          "display_name": "Beta Laptop",
          "role": "compute",
          "status": "ready",
          "summary": "Beta Laptop is best for compute right now."
        },
        "device_roles": [
          {
            "peer_id": "alpha-node",
            "display_name": "Alpha",
            "role": "local_command",
            "status": "ready",
            "summary": "This Mac is the local command node."
          },
          {
            "peer_id": "beta-node",
            "display_name": "Beta Laptop",
            "role": "compute",
            "status": "ready",
            "summary": "Beta Laptop is ready for compute work."
          }
        ]
      },
      "latest_proof": {"status": "running", "summary": "Whole-mesh proof is in flight."}
    }
    """.data(using: .utf8)!

    let snapshot = try JSONDecoder().decode(AppStatusSnapshot.self, from: data)
    let demo = MissionControlDeriver.demoState(snapshot: snapshot, mode: .mesh, phoneURL: "http://192.168.1.4:8421/app")
    let roles = MissionControlDeriver.deviceRoles(from: snapshot)

    #expect(demo.phoneLabel == "Phone link ready")
    #expect(demo.recoveryLabel == "Repairing")
    #expect(demo.primaryPeerLabel == "Beta Laptop")
    #expect(demo.proofLabel == "Running")
    #expect(demo.story.first == "OCP is repairing routes and proving the mesh.")
    #expect(roles.count == 2)
    #expect(roles.first?.role == "local_command")
    #expect(roles.last?.role == "compute")
}

@Test func setupGuideStepsTrackLocalReadyProofFailureAndStrongStates() throws {
    let localSteps = MissionControlDeriver.setupGuideSteps(snapshot: nil, mode: .local, phoneURL: "Start Mesh Mode")
    #expect(localSteps[0].status == "active")
    #expect(localSteps[1].status == "blocked")

    let readyData = """
    {
      "status": "ok",
      "node": {"node_id": "alpha-node"},
      "setup": {"status": "ready", "route_count": 1, "healthy_route_count": 1, "latest_proof_status": "running"}
    }
    """.data(using: .utf8)!
    let ready = try JSONDecoder().decode(AppStatusSnapshot.self, from: readyData)
    let readySteps = MissionControlDeriver.setupGuideSteps(snapshot: ready, mode: .mesh, phoneURL: "http://192.168.1.4:8421/app")
    #expect(readySteps[0].status == "complete")
    #expect(readySteps[1].status == "complete")
    #expect(readySteps[2].status == "complete")
    #expect(readySteps[3].status == "active")

    let repairingData = """
    {
      "status": "ok",
      "setup": {
        "status": "proving",
        "route_count": 1,
        "healthy_route_count": 1,
        "latest_proof_status": "running",
        "recovery_state": "repairing"
      }
    }
    """.data(using: .utf8)!
    let repairing = try JSONDecoder().decode(AppStatusSnapshot.self, from: repairingData)
    let repairingSteps = MissionControlDeriver.setupGuideSteps(snapshot: repairing, mode: .mesh, phoneURL: "http://192.168.1.4:8421/app")
    #expect(repairingSteps[3].status == "active")

    let failedData = """
    {"status": "ok", "setup": {"status": "needs_attention", "route_count": 1, "healthy_route_count": 0, "latest_proof_status": "failed"}}
    """.data(using: .utf8)!
    let failed = try JSONDecoder().decode(AppStatusSnapshot.self, from: failedData)
    let failedSteps = MissionControlDeriver.setupGuideSteps(snapshot: failed, mode: .mesh, phoneURL: "http://192.168.1.4:8421/app")
    #expect(failedSteps[2].status == "attention")
    #expect(failedSteps[4].status == "attention")

    let strongData = """
    {"status": "ok", "setup": {"status": "strong", "route_count": 1, "healthy_route_count": 1, "latest_proof_status": "completed"}}
    """.data(using: .utf8)!
    let strong = try JSONDecoder().decode(AppStatusSnapshot.self, from: strongData)
    let strongSteps = MissionControlDeriver.setupGuideSteps(snapshot: strong, mode: .mesh, phoneURL: "http://192.168.1.4:8421/app")
    #expect(strongSteps.allSatisfy { $0.status == "complete" })
}
