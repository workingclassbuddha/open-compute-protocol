import SwiftUI

struct SetupDoctorView: View {
    @ObservedObject var model: OCPDesktopModel
    var allowMotion: Bool

    var body: some View {
        let demo = model.demoState

        MissionScroll(allowMotion: allowMotion) {
            PageHeader(
                eyebrow: "Setup Doctor",
                title: model.setupLabel,
                summary: model.setupSummary
            )

            DemoStatusStrip(state: demo, roles: model.deviceRoles)

            SetupGuideCard(
                steps: model.setupGuideSteps,
                allowMotion: allowMotion,
                startMesh: { model.startMesh() },
                copyPhoneLink: { model.copyPhoneLink() },
                activateMesh: { model.activateMesh() },
                openSetup: { model.refreshNow() }
            )

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 14) {
                MetricCard(title: "Known Peers", value: "\(model.snapshot?.setup?.knownPeerCount ?? 0)", detail: "Devices OCP can currently reason about.", tint: .cyan)
                MetricCard(title: "Healthy Routes", value: "\(model.snapshot?.setup?.healthyRouteCount ?? 0)", detail: "Fresh route proofs available for dispatch.", tint: .green)
                MetricCard(title: "Latest Proof", value: model.snapshot?.setup?.latestProofStatus ?? "none", detail: model.snapshot?.latestProof?.summary ?? "No whole-mesh proof summary yet.", tint: .purple)
                MetricCard(title: "Approvals", value: "\(model.snapshot?.approvals?.pendingCount ?? 0)", detail: "Pending operator attention.", tint: .orange)
            }

            MissionCard {
                VStack(alignment: .leading, spacing: 12) {
                    Text("One Concrete Fix").sectionLabel()
                    Text(model.nextFix)
                        .font(.title3.weight(.semibold))
                    if let blocking = model.snapshot?.setup?.blockingIssue, !blocking.isEmpty {
                        Label(blocking, systemImage: "exclamationmark.triangle")
                            .foregroundStyle(.orange)
                    }
                    Text("Phone: \(model.phoneURL)")
                        .font(.callout)
                        .foregroundStyle(.secondary)
                        .textSelection(.enabled)
                    HStack {
                        Button("Activate Mesh") { model.activateMesh() }
                            .buttonStyle(.borderedProminent)
                        Button("Copy Phone Link") { model.copyPhoneLink() }
                    }
                }
            }

            MissionCard {
                VStack(alignment: .leading, spacing: 14) {
                    Text("Proof Timeline").sectionLabel()
                    TimelineList(events: model.snapshot?.setup?.timeline ?? [])
                }
            }
        }
    }
}
