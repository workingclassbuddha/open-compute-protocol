import SwiftUI

@main
struct OCPDesktopApp: App {
    @StateObject private var model = OCPDesktopModel()

    var body: some Scene {
        WindowGroup {
            ContentView(model: model)
                .frame(minWidth: 1060, minHeight: 720)
        }
        .commands {
            CommandGroup(replacing: .newItem) {}
            CommandMenu("Mesh") {
                Button("Run Proof Assistant") { model.runProofAssistant() }
                    .keyboardShortcut("p", modifiers: [.command, .shift])
                    .disabled(model.isProofAssistantRunning)
                Button("Activate Mesh") { model.activateMesh() }
                    .keyboardShortcut("a", modifiers: [.command, .shift])
                    .disabled(model.isActivating)
                Button("Refresh Status") { model.refreshNow() }
                    .keyboardShortcut("r")
                Button("Copy Phone Link") { model.copyPhoneLink() }
                    .keyboardShortcut("c", modifiers: [.command, .shift])
                Divider()
                Button("Start Mesh Mode") { model.startMesh() }
                Button("Start Local Only") { model.startLocal() }
                Button("Stop Server") { model.stop() }
                    .keyboardShortcut(".", modifiers: [.command])
            }
        }
    }
}
