import SwiftUI

struct RoutesView: View {
    @ObservedObject var model: OCPDesktopModel
    var allowMotion: Bool

    var body: some View {
        let health = model.snapshot?.routeHealth
        let routes = health?.routes ?? []
        let demo = model.demoState

        MissionScroll(allowMotion: allowMotion) {
            PageHeader(
                eyebrow: "Route Health",
                title: "\(health?.healthy ?? 0) fresh route(s)",
                summary: health?.operatorSummary ?? "Route health appears after peer discovery and Autonomic Mesh activation."
            )

            DemoStatusStrip(state: demo, roles: model.deviceRoles)

            MissionCard(tint: MissionTheme.signal) {
                TopologyGraphView(graph: model.topology, allowMotion: allowMotion)
            }

            MissionCard {
                VStack(alignment: .leading, spacing: 12) {
                    Text("Reachability").sectionLabel()
                    RouteHealthChart(healthy: health?.healthy ?? 0, total: health?.count ?? 0)
                }
            }

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 14) {
                ForEach(routes) { route in
                    MissionCard {
                        VStack(alignment: .leading, spacing: 10) {
                            HStack {
                                Text(route.displayName ?? route.peerID ?? "Peer")
                                    .font(.headline)
                                Spacer()
                                StatusPill(text: route.status ?? "unknown", status: route.status ?? "unknown")
                            }
                            Text(route.bestRoute ?? "No working route recorded.")
                                .font(.callout.monospaced())
                                .foregroundStyle(.secondary)
                                .textSelection(.enabled)
                            Text(route.operatorSummary ?? "Press Activate Mesh to probe this route.")
                                .foregroundStyle(.secondary)
                            StatusPill(text: route.freshness ?? "unknown", status: route.freshness ?? "unknown")
                        }
                    }
                }
            }

            if routes.isEmpty {
                MissionCard {
                    EmptyState(text: "No peer routes yet. Start Mesh Mode, connect another device, then press Activate Mesh.")
                }
            }
        }
    }
}
