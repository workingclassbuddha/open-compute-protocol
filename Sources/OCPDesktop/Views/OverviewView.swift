import Charts
import SwiftUI
import OCPDesktopCore

struct OverviewView: View {
    @ObservedObject var model: OCPDesktopModel
    @Binding var showGuide: Bool
    var allowMotion: Bool
    var openSetup: () -> Void

    var body: some View {
        let demo = model.demoState
        MissionScroll(allowMotion: allowMotion) {
            CinematicOverviewHero(
                summary: model.setupSummary,
                setupLabel: model.setupLabel,
                setupStatus: model.snapshot?.setup?.status ?? "ready",
                nextFix: model.nextFix,
                meshScore: model.meshScore,
                phoneURL: model.phoneURL,
                isActivating: model.isActivating,
                recoveryLabel: demo.recoveryLabel,
                recoverySummary: demo.recoverySummary,
                proofLabel: demo.proofLabel,
                proofSummary: demo.proofSummary,
                primaryPeerLabel: demo.primaryPeerLabel,
                primaryPeerSummary: demo.primaryPeerSummary,
                story: demo.story,
                allowMotion: allowMotion,
                startMesh: { model.startMesh() },
                activateMesh: { model.activateMesh() },
                copyPhoneLink: { model.copyPhoneLink() },
                openApp: { model.openApp() }
            )

            if showGuide {
                CompactSetupGuideCard(
                    steps: model.setupGuideSteps,
                    allowMotion: allowMotion,
                    startMesh: { model.startMesh() },
                    copyPhoneLink: { model.copyPhoneLink() },
                    activateMesh: { model.activateMesh() },
                    openSetup: openSetup
                )
                .transition(.move(edge: .top).combined(with: .opacity))
            }

            DemoStatusStrip(state: demo, roles: model.deviceRoles)

            HStack(alignment: .top, spacing: 18) {
                MissionCard(tint: MissionTheme.signal) {
                    TopologyGraphView(graph: model.topology, compact: true, allowMotion: allowMotion)
                }

                MissionCard(tint: MissionTheme.mint) {
                    VStack(alignment: .leading, spacing: 12) {
                        HStack {
                            Text("Mesh Score History").sectionLabel()
                            Spacer()
                            Text("\(model.history.count) samples")
                                .font(.caption.monospaced())
                                .foregroundStyle(.secondary)
                        }
                        MeshScoreChart(points: model.chartPoints)
                    }
                }
            }

            HStack(alignment: .top, spacing: 18) {
                MissionCard(tint: MissionTheme.copper) {
                    HStack(alignment: .center, spacing: 20) {
                        MeshGauge(score: model.meshScore, allowMotion: allowMotion)
                            .scaleEffect(0.72)
                            .frame(width: 170, height: 170)
                        VStack(alignment: .leading, spacing: 10) {
                            Text("Mission Status").sectionLabel()
                            Text(model.nextFix)
                                .font(.system(size: 25, weight: .black, design: .rounded))
                                .lineLimit(3)
                            Text(model.statusText)
                                .foregroundStyle(.secondary)
                                .lineLimit(3)
                        }
                    }
                }
                SovereignPledgeCard()
                    .frame(maxWidth: 430)
            }

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible()), GridItem(.flexible())], spacing: 14) {
                MetricCard(
                    title: "Routes",
                    value: "\(model.snapshot?.meshQuality?.healthyRoutes ?? 0)/\(model.snapshot?.meshQuality?.routeCount ?? 0)",
                    detail: model.routeSummary,
                    tint: .green
                )
                MetricCard(
                    title: "Execution",
                    value: "\(model.snapshot?.executionReadiness?.targets?.filter { $0.status == "ready" }.count ?? 0) ready",
                    detail: model.executionSummary,
                    tint: .cyan
                )
                MetricCard(
                    title: "Artifacts",
                    value: "\(model.snapshot?.artifactSync?.verifiedCount ?? 0) verified",
                    detail: model.artifactSummary,
                    tint: .orange
                )
            }

            MissionCard(tint: MissionTheme.copper) {
                VStack(alignment: .leading, spacing: 12) {
                    Text("Next Actions").sectionLabel()
                    ForEach(Array((model.snapshot?.nextActions ?? ["Start OCP and press Activate Mesh."]).enumerated()), id: \.offset) { _, action in
                        Label(action, systemImage: "arrow.right.circle")
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
    }
}
