import SwiftUI

struct ContentView: View {
    @ObservedObject var model: OCPDesktopModel
    @SceneStorage("ocp.desktop.selectedSection") private var selectedSectionID = DesktopSection.overview.rawValue
    @AppStorage("ocp.desktop.hasSeenGuide") private var hasSeenGuide = false
    @AppStorage("ocp.desktop.showGuide") private var showGuide = true
    @AppStorage("ocp.desktop.prefersReducedMissionMotion") private var prefersReducedMissionMotion = false
    @Environment(\.accessibilityReduceMotion) private var accessibilityReduceMotion

    private var selection: Binding<DesktopSection?> {
        Binding {
            DesktopSection(rawValue: selectedSectionID) ?? .overview
        } set: { next in
            selectedSectionID = (next ?? .overview).rawValue
        }
    }

    var body: some View {
        NavigationSplitView {
            SidebarView(selection: selection)
        } detail: {
            detailView
                .navigationTitle((selection.wrappedValue ?? .overview).title)
                .toolbar {
                    ToolbarItemGroup {
                        Button {
                            model.startLocal()
                        } label: {
                            Label("Local", systemImage: "desktopcomputer")
                        }
                        Button {
                            model.startMesh()
                        } label: {
                            Label("Mesh", systemImage: "network")
                        }
                        Button {
                            model.activateMesh()
                        } label: {
                            Label("Activate", systemImage: "bolt.circle")
                        }
                            .disabled(model.isActivating)
                        Button {
                            model.refreshNow()
                        } label: {
                            Label("Refresh", systemImage: "arrow.clockwise")
                        }
                        Button {
                            showGuide.toggle()
                        } label: {
                            Label(showGuide ? "Hide Guide" : "Show Guide", systemImage: "map")
                        }
                        Button {
                            model.stop()
                        } label: {
                            Label("Stop", systemImage: "stop.circle")
                        }
                    }
                    ToolbarItemGroup {
                        Button {
                            model.openApp()
                        } label: {
                            Label("Open App", systemImage: "safari")
                        }
                        Button {
                            model.copyPhoneLink()
                        } label: {
                            Label("Copy Phone Link", systemImage: "iphone")
                        }
                    }
                }
        }
        .onAppear {
            if !hasSeenGuide {
                hasSeenGuide = true
                showGuide = true
            }
        }
        .preferredColorScheme(.dark)
        .tint(MissionTheme.mint)
    }

    @ViewBuilder
    private var detailView: some View {
        switch selection.wrappedValue ?? .overview {
        case .overview:
            OverviewView(model: model, showGuide: $showGuide, allowMotion: allowMotion) {
                selection.wrappedValue = .setup
            }
        case .setup:
            SetupDoctorView(model: model, allowMotion: allowMotion)
        case .routes:
            RoutesView(model: model, allowMotion: allowMotion)
        case .execution:
            ExecutionView(model: model, allowMotion: allowMotion)
        case .artifacts:
            ArtifactsView(model: model, allowMotion: allowMotion)
        case .protocolStatus:
            ProtocolView(model: model, allowMotion: allowMotion)
        case .settings:
            SettingsView(model: model, allowMotion: allowMotion)
        }
    }

    private var allowMotion: Bool {
        !prefersReducedMissionMotion && !accessibilityReduceMotion
    }
}
