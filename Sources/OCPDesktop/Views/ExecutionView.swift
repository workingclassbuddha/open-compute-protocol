import Charts
import SwiftUI

struct ExecutionView: View {
    @ObservedObject var model: OCPDesktopModel
    var allowMotion: Bool = true

    var body: some View {
        let readiness = model.snapshot?.executionReadiness
        let targets = readiness?.targets ?? []
        let demo = model.demoState

        MissionScroll(allowMotion: allowMotion) {
            PageHeader(
                eyebrow: "Execution",
                title: readiness?.status?.replacingOccurrences(of: "_", with: " ").capitalized ?? "No readiness yet",
                summary: readiness?.operatorSummary ?? "Worker readiness appears after OCP starts and advertises local or remote capacity."
            )

            DemoStatusStrip(state: demo, roles: model.deviceRoles)

            MissionCard {
                VStack(alignment: .leading, spacing: 12) {
                    Text("Ready Targets").sectionLabel()
                    if targets.isEmpty {
                        EmptyState(text: "No execution targets are visible yet.")
                    } else {
                        Chart(targets) { target in
                            BarMark(
                                x: .value("Target", target.displayName ?? target.peerID ?? "Peer"),
                                y: .value("Workers", target.workerCount ?? 0)
                            )
                            .foregroundStyle((target.status ?? "") == "ready" ? .green : .orange)
                        }
                        .frame(height: 190)
                    }
                }
            }

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 14) {
                ForEach(targets) { target in
                    MissionCard {
                        VStack(alignment: .leading, spacing: 10) {
                            HStack {
                                Text(target.displayName ?? target.peerID ?? "Target")
                                    .font(.headline)
                                Spacer()
                                StatusPill(text: target.status ?? "unknown", status: target.status ?? "unknown")
                            }
                            Text("\(target.workerCount ?? 0) worker(s)")
                                .foregroundStyle(.secondary)
                            ForEach(target.reasons ?? [], id: \.self) { reason in
                                Label(reason, systemImage: "checkmark.circle")
                                    .font(.callout)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }
                }
            }
        }
    }
}
