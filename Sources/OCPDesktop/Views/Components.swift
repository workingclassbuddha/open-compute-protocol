import Charts
import SwiftUI
import OCPDesktopCore

enum MissionTheme {
    static let ink = Color(red: 0.018, green: 0.026, blue: 0.028)
    static let deepSea = Color(red: 0.030, green: 0.082, blue: 0.078)
    static let desk = Color(red: 0.18, green: 0.10, blue: 0.045)
    static let cream = Color(red: 0.95, green: 0.91, blue: 0.78)
    static let lamp = Color(red: 1.00, green: 0.70, blue: 0.34)
    static let copper = Color(red: 0.96, green: 0.58, blue: 0.24)
    static let signal = Color(red: 0.36, green: 0.86, blue: 0.72)
    static let mint = Color(red: 0.36, green: 0.96, blue: 0.55)
    static let ember = Color(red: 1.00, green: 0.36, blue: 0.30)
}

struct MissionScroll<Content: View>: View {
    var allowMotion: Bool = true
    @ViewBuilder var content: () -> Content
    @State private var drift = false

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                content()
            }
            .padding(26)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .background(MissionBackground(allowMotion: allowMotion))
        .onAppear {
            guard allowMotion else { return }
            withAnimation(.easeInOut(duration: 8).repeatForever(autoreverses: true)) {
                drift = true
            }
        }
    }
}

struct MissionCard<Content: View>: View {
    var tint: Color = MissionTheme.signal
    @ViewBuilder var content: () -> Content

    var body: some View {
        content()
            .padding(20)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(
                ZStack(alignment: .topLeading) {
                    RoundedRectangle(cornerRadius: 26, style: .continuous)
                        .fill(
                            LinearGradient(
                                colors: [
                                    MissionTheme.cream.opacity(0.11),
                                    MissionTheme.deepSea.opacity(0.55),
                                    Color.black.opacity(0.42)
                                ],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            )
                        )
                    RoundedRectangle(cornerRadius: 26, style: .continuous)
                        .fill(Color.black.opacity(0.18))
                    LinearGradient(
                        colors: [tint.opacity(0.18), .clear, MissionTheme.lamp.opacity(0.06)],
                        startPoint: .topLeading,
                        endPoint: .bottomTrailing
                    )
                    .clipShape(RoundedRectangle(cornerRadius: 26, style: .continuous))
                }
            )
            .overlay(
                RoundedRectangle(cornerRadius: 26, style: .continuous)
                    .stroke(
                        LinearGradient(
                            colors: [tint.opacity(0.32), .white.opacity(0.10), .clear],
                            startPoint: .topLeading,
                            endPoint: .bottomTrailing
                        ),
                        lineWidth: 1
                    )
            )
            .shadow(color: tint.opacity(0.10), radius: 22, x: 0, y: 12)
    }
}

struct MissionBackground: View {
    var allowMotion: Bool = true
    @State private var phase = false

    var body: some View {
        ZStack {
            LinearGradient(
                colors: [MissionTheme.ink, MissionTheme.deepSea, MissionTheme.ink],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            RadialGradient(
                colors: [MissionTheme.signal.opacity(0.34), .clear],
                center: .topTrailing,
                startRadius: 80,
                endRadius: phase && allowMotion ? 640 : 560
            )
            .scaleEffect(phase && allowMotion ? 1.08 : 1.0, anchor: .topTrailing)
            RadialGradient(
                colors: [MissionTheme.lamp.opacity(0.23), .clear],
                center: .bottomLeading,
                startRadius: 80,
                endRadius: phase && allowMotion ? 590 : 520
            )
            .scaleEffect(phase && allowMotion ? 1.05 : 1.0, anchor: .bottomLeading)
            LinearGradient(
                colors: [.clear, MissionTheme.desk.opacity(0.48)],
                startPoint: .center,
                endPoint: .bottom
            )
            CinematicConstellation()
                .opacity(0.62)
            MeshPattern()
                .opacity(0.11)
        }
        .ignoresSafeArea()
        .onAppear {
            guard allowMotion else { return }
            withAnimation(.easeInOut(duration: 7).repeatForever(autoreverses: true)) {
                phase = true
            }
        }
    }
}

struct CinematicConstellation: View {
    var body: some View {
        Canvas { context, size in
            let points = [
                CGPoint(x: size.width * 0.10, y: size.height * 0.58),
                CGPoint(x: size.width * 0.24, y: size.height * 0.42),
                CGPoint(x: size.width * 0.46, y: size.height * 0.25),
                CGPoint(x: size.width * 0.70, y: size.height * 0.31),
                CGPoint(x: size.width * 0.88, y: size.height * 0.52),
                CGPoint(x: size.width * 0.58, y: size.height * 0.66),
            ]

            for index in points.indices {
                let start = points[index]
                let end = points[(index + 2) % points.count]
                var curve = Path()
                curve.move(to: start)
                curve.addQuadCurve(
                    to: end,
                    control: CGPoint(x: (start.x + end.x) / 2, y: min(start.y, end.y) - size.height * 0.22)
                )
                context.stroke(curve, with: .color(MissionTheme.signal.opacity(0.30)), lineWidth: 1.1)
            }

            for point in points {
                context.fill(
                    Path(ellipseIn: CGRect(x: point.x - 3, y: point.y - 3, width: 6, height: 6)),
                    with: .color(MissionTheme.mint.opacity(0.88))
                )
                context.fill(
                    Path(ellipseIn: CGRect(x: point.x - 13, y: point.y - 13, width: 26, height: 26)),
                    with: .color(MissionTheme.mint.opacity(0.08))
                )
            }
        }
    }
}

struct MeshPattern: View {
    var body: some View {
        Canvas { context, size in
            var path = Path()
            let spacing: CGFloat = 42
            var x: CGFloat = 0
            while x <= size.width {
                path.move(to: CGPoint(x: x, y: 0))
                path.addLine(to: CGPoint(x: x - size.height * 0.45, y: size.height))
                x += spacing
            }
            var y: CGFloat = 0
            while y <= size.height {
                path.move(to: CGPoint(x: 0, y: y))
                path.addLine(to: CGPoint(x: size.width, y: y + size.width * 0.16))
                y += spacing
            }
            context.stroke(path, with: .color(.white.opacity(0.20)), lineWidth: 0.6)
        }
    }
}

struct PageHeader: View {
    var eyebrow: String
    var title: String
    var summary: String

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(eyebrow.uppercased())
                .font(.system(size: 11, weight: .bold, design: .monospaced))
                .foregroundStyle(MissionTheme.signal)
                .tracking(2.2)
            Text(title)
                .font(.system(size: 42, weight: .black, design: .rounded))
                .foregroundStyle(
                    LinearGradient(
                        colors: [.primary, MissionTheme.signal.opacity(0.82)],
                        startPoint: .leading,
                        endPoint: .trailing
                    )
                )
            Text(summary)
                .font(.title3)
                .foregroundStyle(.secondary)
                .frame(maxWidth: 820, alignment: .leading)
        }
    }
}

struct SovereignHeroHeader: View {
    var version: String = "OCP v0.1.6"
    var subtitle: String = "Autonomic Mesh Alpha"
    var summary: String

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text(version)
                .font(.system(size: 76, weight: .black, design: .rounded))
                .minimumScaleFactor(0.55)
                .lineLimit(1)
                .foregroundStyle(
                    LinearGradient(
                        colors: [MissionTheme.cream, .white.opacity(0.92)],
                        startPoint: .leading,
                        endPoint: .trailing
                    )
                )
                .shadow(color: MissionTheme.lamp.opacity(0.20), radius: 18, x: 0, y: 8)
            VStack(alignment: .leading, spacing: 8) {
                Text(subtitle)
                    .font(.system(size: 26, weight: .bold, design: .monospaced))
                    .tracking(1.6)
                    .foregroundStyle(MissionTheme.mint)
                Capsule()
                    .fill(
                        LinearGradient(
                            colors: [MissionTheme.mint, MissionTheme.signal.opacity(0.12), .clear],
                            startPoint: .leading,
                            endPoint: .trailing
                        )
                    )
                    .frame(width: 340, height: 2)
            }
            Text(summary)
                .font(.title3)
                .foregroundStyle(MissionTheme.cream.opacity(0.74))
                .frame(maxWidth: 880, alignment: .leading)
        }
        .padding(.top, 8)
    }
}

struct CinematicOverviewHero: View {
    var summary: String
    var setupLabel: String
    var setupStatus: String
    var nextFix: String
    var meshScore: Int
    var phoneURL: String
    var isActivating: Bool
    var proofAssistant: ProofAssistantStatus
    var isProofAssistantRunning: Bool
    var recoveryLabel: String
    var recoverySummary: String
    var proofLabel: String
    var proofSummary: String
    var primaryPeerLabel: String
    var primaryPeerSummary: String
    var story: [String]
    var allowMotion: Bool
    var runProofAssistant: () -> Void
    var startMesh: () -> Void
    var activateMesh: () -> Void
    var copyPhoneLink: () -> Void
    var openApp: () -> Void
    @State private var glow = false

    var body: some View {
        ZStack(alignment: .bottom) {
            RoundedRectangle(cornerRadius: 34, style: .continuous)
                .fill(
                    LinearGradient(
                        colors: [
                            Color.black.opacity(0.78),
                            MissionTheme.deepSea.opacity(0.90),
                            MissionTheme.ink
                        ],
                        startPoint: .topLeading,
                        endPoint: .bottomTrailing
                    )
                )
            MeshPattern()
                .opacity(0.16)
                .clipShape(RoundedRectangle(cornerRadius: 34, style: .continuous))
            CinematicConstellation()
                .opacity(glow && allowMotion ? 0.82 : 0.52)
                .blur(radius: glow && allowMotion ? 0 : 0.4)
            LinearGradient(
                colors: [.clear, MissionTheme.desk.opacity(0.78)],
                startPoint: .center,
                endPoint: .bottom
            )
            .clipShape(RoundedRectangle(cornerRadius: 34, style: .continuous))

            VStack(alignment: .leading, spacing: 24) {
                HStack(alignment: .top, spacing: 24) {
                    VStack(alignment: .leading, spacing: 15) {
                        Text("OCP v0.1.6")
                            .font(.system(size: 76, weight: .black, design: .rounded))
                            .minimumScaleFactor(0.55)
                            .lineLimit(1)
                            .foregroundStyle(MissionTheme.cream)
                            .shadow(color: MissionTheme.lamp.opacity(0.28), radius: 24, x: 0, y: 10)
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Autonomic Mesh Alpha")
                                .font(.system(size: 25, weight: .bold, design: .monospaced))
                                .tracking(1.7)
                                .foregroundStyle(MissionTheme.mint)
                            Capsule()
                                .fill(
                                    LinearGradient(
                                        colors: [MissionTheme.mint, MissionTheme.signal.opacity(0.28), .clear],
                                        startPoint: .leading,
                                        endPoint: .trailing
                                    )
                                )
                                .frame(width: 360, height: 2)
                        }
                        Text(summary)
                            .font(.title3.weight(.semibold))
                            .foregroundStyle(MissionTheme.cream.opacity(0.76))
                            .frame(maxWidth: 720, alignment: .leading)
                        if !story.isEmpty {
                            VStack(alignment: .leading, spacing: 8) {
                                ForEach(Array(story.prefix(4).enumerated()), id: \.offset) { _, line in
                                    HStack(alignment: .top, spacing: 8) {
                                        Circle()
                                            .fill(MissionTheme.mint.opacity(0.9))
                                            .frame(width: 6, height: 6)
                                            .padding(.top, 7)
                                        Text(line)
                                            .font(.callout.weight(.medium))
                                            .foregroundStyle(MissionTheme.cream.opacity(0.74))
                                    }
                                }
                            }
                            .padding(.top, 4)
                        }
                    }

                    Spacer(minLength: 18)

                    VStack(alignment: .trailing, spacing: 16) {
                        StatusPill(text: setupLabel, status: setupStatus)
                        VStack(alignment: .trailing, spacing: 2) {
                            Text("\(meshScore)")
                                .font(.system(size: 58, weight: .black, design: .rounded))
                                .foregroundStyle(scoreColor)
                            Text("mesh score")
                                .font(.caption.bold())
                                .foregroundStyle(MissionTheme.cream.opacity(0.62))
                                .textCase(.uppercase)
                        }
                        VStack(alignment: .leading, spacing: 12) {
                            HeroDetailBlock(title: "Assistant", value: proofAssistant.phaseLabel, summary: proofAssistant.message)
                            HeroDetailBlock(title: "Recovery", value: recoveryLabel, summary: recoverySummary)
                            HeroDetailBlock(title: "Proof", value: proofLabel, summary: proofSummary)
                            HeroDetailBlock(title: "Primary Peer", value: primaryPeerLabel, summary: primaryPeerSummary)
                        }
                        Text(nextFix)
                            .font(.callout.weight(.semibold))
                            .foregroundStyle(MissionTheme.cream.opacity(0.72))
                            .multilineTextAlignment(.trailing)
                            .lineLimit(3)
                            .frame(maxWidth: 300, alignment: .trailing)
                    }
                    .padding(18)
                    .frame(maxWidth: 320)
                    .background(Color.black.opacity(0.34), in: RoundedRectangle(cornerRadius: 24, style: .continuous))
                    .overlay(RoundedRectangle(cornerRadius: 24, style: .continuous).stroke(MissionTheme.mint.opacity(0.18), lineWidth: 1))
                }

                HeroDeviceScene(allowMotion: allowMotion)

                HStack(spacing: 12) {
                    Button("Run Proof Assistant") {
                        runProofAssistant()
                    }
                    .buttonStyle(.borderedProminent)
                    .controlSize(.large)
                    .disabled(isProofAssistantRunning)

                    Button("Start Mesh Mode") {
                        startMesh()
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.large)

                    Button("Activate Mesh") {
                        activateMesh()
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.large)
                    .disabled(isActivating)

                    Button("Copy Phone Link") {
                        copyPhoneLink()
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.large)

                    Button("Open App") {
                        openApp()
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.large)

                    Spacer()

                    Text(phoneLinkCaption)
                        .font(.caption.monospaced())
                        .foregroundStyle(MissionTheme.cream.opacity(0.62))
                        .lineLimit(1)
                }
            }
            .padding(34)
        }
        .frame(minHeight: 520)
        .overlay(
            RoundedRectangle(cornerRadius: 34, style: .continuous)
                .stroke(
                    LinearGradient(
                        colors: [MissionTheme.mint.opacity(0.36), MissionTheme.lamp.opacity(0.18), .white.opacity(0.06)],
                        startPoint: .topLeading,
                        endPoint: .bottomTrailing
                    ),
                    lineWidth: 1
                )
        )
        .shadow(color: MissionTheme.mint.opacity(glow && allowMotion ? 0.22 : 0.12), radius: 34, x: 0, y: 20)
        .onAppear {
            guard allowMotion else { return }
            withAnimation(.easeInOut(duration: 2.8).repeatForever(autoreverses: true)) {
                glow = true
            }
        }
    }

    private var phoneLinkCaption: String {
        phoneURL.hasPrefix("http") ? phoneURL : "Start Mesh Mode to create the phone link."
    }

    private var scoreColor: Color {
        if meshScore >= 80 { return MissionTheme.mint }
        if meshScore >= 50 { return MissionTheme.copper }
        return MissionTheme.ember
    }
}

struct ProofAssistantCard: View {
    var status: ProofAssistantStatus
    var phoneURL: String
    var timeline: [AppStatusSnapshot.TimelineEvent]
    var isActivating: Bool
    var runProofAssistant: () -> Void
    var startMesh: () -> Void
    var copyPhoneLink: () -> Void
    var activateMesh: () -> Void
    var openApp: () -> Void

    var body: some View {
        MissionCard(tint: tint) {
            VStack(alignment: .leading, spacing: 16) {
                HStack(alignment: .firstTextBaseline) {
                    VStack(alignment: .leading, spacing: 5) {
                        Text("Proof Assistant").sectionLabel()
                        Text(status.title)
                            .font(.title3.weight(.bold))
                    }
                    Spacer()
                    StatusPill(text: status.phaseLabel, status: status.statusToken)
                }

                Text(status.message)
                    .font(.headline)
                    .foregroundStyle(MissionTheme.cream.opacity(0.82))
                    .fixedSize(horizontal: false, vertical: true)

                if !status.detail.isEmpty {
                    Label(status.detail, systemImage: detailIcon)
                        .font(.callout)
                        .foregroundStyle(detailColor)
                        .fixedSize(horizontal: false, vertical: true)
                }

                VStack(alignment: .leading, spacing: 6) {
                    HStack(spacing: 8) {
                        Image(systemName: status.copiedPhoneLink ? "checkmark.square" : "iphone")
                            .foregroundStyle(status.copiedPhoneLink ? MissionTheme.mint : MissionTheme.signal)
                        Text(status.copiedPhoneLink ? "Phone link copied" : "Phone link")
                            .font(.caption.bold())
                            .foregroundStyle(.secondary)
                    }
                    Text(linkText)
                        .font(.callout.monospaced())
                        .lineLimit(2)
                        .textSelection(.enabled)
                        .foregroundStyle(MissionTheme.cream.opacity(linkText.hasPrefix("http") ? 0.82 : 0.58))
                }
                .padding(12)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(Color.black.opacity(0.22), in: RoundedRectangle(cornerRadius: 14, style: .continuous))

                HStack(spacing: 10) {
                    Button {
                        runProofAssistant()
                    } label: {
                        Label(status.isRunning ? "Running" : "Run Proof Assistant", systemImage: "checkmark.seal")
                    }
                    .buttonStyle(.borderedProminent)
                    .controlSize(.large)
                    .disabled(status.isRunning)

                    Button {
                        startMesh()
                    } label: {
                        Label("Start Mesh", systemImage: "network")
                    }
                    .buttonStyle(.bordered)

                    Button {
                        activateMesh()
                    } label: {
                        Label("Activate", systemImage: "bolt.circle")
                    }
                    .buttonStyle(.bordered)
                    .disabled(isActivating)

                    Button {
                        copyPhoneLink()
                    } label: {
                        Label("Copy Link", systemImage: "doc.on.doc")
                    }
                    .buttonStyle(.bordered)

                    Button {
                        openApp()
                    } label: {
                        Label("Open", systemImage: "safari")
                    }
                    .buttonStyle(.bordered)
                }

                if !timeline.isEmpty {
                    Divider().opacity(0.36)
                    VStack(alignment: .leading, spacing: 10) {
                        Text("Live Proof Timeline").sectionLabel()
                        TimelineList(events: Array(timeline.prefix(5)))
                    }
                }
            }
        }
    }

    private var linkText: String {
        if !status.phoneURL.isEmpty { return status.phoneURL }
        return phoneURL
    }

    private var tint: Color {
        switch status.phase {
        case .completed:
            return MissionTheme.mint
        case .needsAttention:
            return MissionTheme.copper
        case .failed:
            return MissionTheme.ember
        default:
            return MissionTheme.signal
        }
    }

    private var detailIcon: String {
        switch status.phase {
        case .completed:
            return "checkmark.circle"
        case .needsAttention, .failed:
            return "exclamationmark.triangle"
        default:
            return "info.circle"
        }
    }

    private var detailColor: Color {
        switch status.phase {
        case .completed:
            return MissionTheme.mint
        case .needsAttention:
            return MissionTheme.copper
        case .failed:
            return MissionTheme.ember
        default:
            return .secondary
        }
    }
}

struct HeroDetailBlock: View {
    var title: String
    var value: String
    var summary: String

    var body: some View {
        VStack(alignment: .trailing, spacing: 2) {
            Text(title.uppercased())
                .font(.system(size: 10, weight: .bold, design: .monospaced))
                .tracking(1.3)
                .foregroundStyle(MissionTheme.signal.opacity(0.85))
            Text(value)
                .font(.headline.weight(.bold))
                .foregroundStyle(MissionTheme.cream)
                .lineLimit(1)
            Text(summary)
                .font(.caption)
                .foregroundStyle(MissionTheme.cream.opacity(0.68))
                .multilineTextAlignment(.trailing)
                .lineLimit(2)
        }
        .frame(maxWidth: .infinity, alignment: .trailing)
    }
}

struct HeroDeviceScene: View {
    var allowMotion: Bool
    @State private var pulse = false

    var body: some View {
        ZStack {
            RoundedRectangle(cornerRadius: 28, style: .continuous)
                .fill(
                    LinearGradient(
                        colors: [Color.black.opacity(0.30), MissionTheme.desk.opacity(0.46)],
                        startPoint: .top,
                        endPoint: .bottom
                    )
                )
            RouteArc(from: CGPoint(x: 0.18, y: 0.62), to: CGPoint(x: 0.50, y: 0.36), glow: pulse && allowMotion)
            RouteArc(from: CGPoint(x: 0.50, y: 0.36), to: CGPoint(x: 0.82, y: 0.60), glow: pulse && allowMotion)
            RouteArc(from: CGPoint(x: 0.22, y: 0.74), to: CGPoint(x: 0.78, y: 0.74), glow: pulse && allowMotion)
            HStack(alignment: .bottom, spacing: 34) {
                HeroDevice(title: "Phone", subtitle: "govern", icon: "iphone", tint: MissionTheme.mint)
                HeroDevice(title: "Alpha Mac", subtitle: "command", icon: "laptopcomputer", tint: MissionTheme.signal, scale: 1.28)
                HeroDevice(title: "Beta Laptop", subtitle: "compute", icon: "macbook", tint: MissionTheme.lamp)
            }
            .padding(.horizontal, 42)
            .padding(.top, 34)
            .frame(maxHeight: .infinity, alignment: .center)

            VStack(spacing: 4) {
                TrustShieldMark(size: 52, allowMotion: allowMotion)
                Text("trusted personal fabric")
                    .font(.caption.bold())
                    .foregroundStyle(MissionTheme.mint.opacity(0.86))
                    .tracking(1.2)
                    .textCase(.uppercase)
            }
            .offset(y: 66)
        }
        .frame(height: 245)
        .clipShape(RoundedRectangle(cornerRadius: 28, style: .continuous))
        .overlay(RoundedRectangle(cornerRadius: 28, style: .continuous).stroke(MissionTheme.mint.opacity(0.14), lineWidth: 1))
        .onAppear {
            guard allowMotion else { return }
            withAnimation(.easeInOut(duration: 2.2).repeatForever(autoreverses: true)) {
                pulse = true
            }
        }
    }
}

struct HeroDevice: View {
    var title: String
    var subtitle: String
    var icon: String
    var tint: Color
    var scale: CGFloat = 1

    var body: some View {
        VStack(spacing: 8) {
            Image(systemName: icon)
                .font(.system(size: 50 * scale, weight: .semibold))
                .foregroundStyle(tint)
                .shadow(color: tint.opacity(0.48), radius: 18)
            Text(title)
                .font(.headline.weight(.bold))
                .foregroundStyle(MissionTheme.cream)
            Text(subtitle)
                .font(.caption.bold())
                .foregroundStyle(tint.opacity(0.84))
                .tracking(1.2)
                .textCase(.uppercase)
        }
        .frame(maxWidth: .infinity)
    }
}

struct CompactSetupGuideCard: View {
    var steps: [SetupGuideStep]
    var allowMotion: Bool
    var startMesh: () -> Void
    var copyPhoneLink: () -> Void
    var activateMesh: () -> Void
    var openSetup: () -> Void
    @State private var reveal = false

    var body: some View {
        MissionCard(tint: MissionTheme.signal) {
            VStack(alignment: .leading, spacing: 14) {
                HStack(alignment: .firstTextBaseline) {
                    VStack(alignment: .leading, spacing: 4) {
                        Text("Guided Path").sectionLabel()
                        Text("Five small checks from app launch to mesh proof.")
                            .font(.headline)
                    }
                    Spacer()
                    if let step = activeStep {
                        Button(step.action) {
                            perform(step)
                        }
                        .buttonStyle(.borderedProminent)
                        .disabled(step.status == "blocked" || step.status == "complete")
                    }
                }
                HStack(spacing: 10) {
                    ForEach(Array(steps.prefix(5).enumerated()), id: \.element.id) { index, step in
                        HStack(spacing: 8) {
                            StatusRing(status: step.status, index: index + 1, allowMotion: allowMotion)
                            VStack(alignment: .leading, spacing: 2) {
                                Text(step.title)
                                    .font(.subheadline.bold())
                                    .lineLimit(1)
                                Text(step.summary)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(1)
                            }
                        }
                        .padding(10)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .background(Color.black.opacity(step.status == "active" ? 0.34 : 0.18), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
                        .overlay(RoundedRectangle(cornerRadius: 18, style: .continuous).stroke(stepColor(step.status).opacity(0.25), lineWidth: 1))
                        .opacity(reveal || !allowMotion ? 1 : 0)
                        .offset(y: reveal || !allowMotion ? 0 : 8)
                        .animation(.easeOut(duration: 0.32).delay(Double(index) * 0.04), value: reveal)
                    }
                }
            }
        }
        .onAppear { reveal = true }
    }

    private var activeStep: SetupGuideStep? {
        steps.first { ["active", "attention"].contains($0.status) } ?? steps.first { $0.status != "complete" }
    }

    private func perform(_ step: SetupGuideStep) {
        switch step.id {
        case "start_mesh":
            startMesh()
        case "copy_phone_link":
            copyPhoneLink()
        case "activate_mesh":
            activateMesh()
        default:
            openSetup()
        }
    }

    private func stepColor(_ status: String) -> Color {
        switch status {
        case "complete": return MissionTheme.mint
        case "active": return MissionTheme.signal
        case "attention": return MissionTheme.copper
        default: return .secondary
        }
    }
}

struct DemoStatusStrip: View {
    var state: DemoStripState
    var roles: [DeviceRoleSummary]

    var body: some View {
        MissionCard(tint: MissionTheme.signal) {
            VStack(alignment: .leading, spacing: 16) {
                HStack {
                    Text("Demo Strip").sectionLabel()
                    Spacer()
                    StatusPill(text: state.recoveryLabel, status: state.recoveryLabel)
                }

                LazyVGrid(columns: [GridItem(.adaptive(minimum: 190, maximum: 320))], spacing: 12) {
                    DemoStatusTile(title: "Phone", value: state.phoneLabel, detail: state.phoneSummary, tint: MissionTheme.signal)
                    DemoStatusTile(title: "Primary Peer", value: state.primaryPeerLabel, detail: state.primaryPeerSummary, tint: MissionTheme.mint)
                    DemoStatusTile(title: "Proof", value: state.proofLabel, detail: state.proofSummary, tint: MissionTheme.copper)
                    DemoStatusTile(title: "Recovery", value: state.recoveryLabel, detail: state.recoverySummary, tint: MissionTheme.signal)
                }

                if !roles.isEmpty {
                    VStack(alignment: .leading, spacing: 10) {
                        Text("Device Roles").sectionLabel()
                        RoleBadgeWall(roles: roles)
                    }
                }

                if !state.story.isEmpty {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Mesh Story").sectionLabel()
                        ForEach(Array(state.story.enumerated()), id: \.offset) { _, line in
                            Label(line, systemImage: "sparkles")
                                .foregroundStyle(.secondary)
                        }
                    }
                }
            }
        }
    }
}

struct DemoStatusTile: View {
    var title: String
    var value: String
    var detail: String
    var tint: Color

    var body: some View {
        VStack(alignment: .leading, spacing: 7) {
            Text(title).sectionLabel()
            Text(value)
                .font(.system(size: 24, weight: .black, design: .rounded))
                .foregroundStyle(MissionTheme.cream)
                .lineLimit(2)
            Text(detail)
                .font(.callout)
                .foregroundStyle(.secondary)
                .lineLimit(3)
        }
        .padding(14)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.black.opacity(0.18), in: RoundedRectangle(cornerRadius: 20, style: .continuous))
        .overlay(RoundedRectangle(cornerRadius: 20, style: .continuous).stroke(tint.opacity(0.22), lineWidth: 1))
    }
}

struct RoleBadgeWall: View {
    var roles: [DeviceRoleSummary]

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            ForEach(roles) { role in
                HStack(alignment: .top, spacing: 12) {
                    StatusPill(text: roleLabel(role.role), status: role.status)
                    VStack(alignment: .leading, spacing: 3) {
                        Text(role.label)
                            .font(.headline)
                        Text(role.summary)
                            .font(.callout)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                }
                .padding(12)
                .background(Color.black.opacity(0.14), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
            }
        }
    }

    private func roleLabel(_ value: String) -> String {
        value.replacingOccurrences(of: "_", with: " ")
    }
}

struct MetricCard: View {
    var title: String
    var value: String
    var detail: String
    var tint: Color = .accentColor

    var body: some View {
        MissionCard(tint: tint) {
            VStack(alignment: .leading, spacing: 8) {
                Text(title).sectionLabel()
                Text(value)
                    .font(.system(size: 30, weight: .black, design: .rounded))
                Text(detail)
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .lineLimit(4)
                GeometryReader { proxy in
                    Capsule()
                        .fill(tint.opacity(0.16))
                        .overlay(alignment: .leading) {
                            Capsule()
                                .fill(tint.gradient)
                                .frame(width: max(18, proxy.size.width * 0.62))
                        }
                }
                .frame(height: 5)
            }
        }
    }
}

struct TrustShieldMark: View {
    var size: CGFloat = 92
    var allowMotion: Bool = true
    @State private var pulse = false

    var body: some View {
        ZStack {
            Circle()
                .stroke(MissionTheme.mint.opacity(0.22), lineWidth: 1)
                .frame(width: size * 1.65, height: size * 1.65)
                .scaleEffect(pulse && allowMotion ? 1.08 : 0.96)
            Circle()
                .stroke(MissionTheme.mint.opacity(0.13), lineWidth: 1)
                .frame(width: size * 2.18, height: size * 2.18)
                .scaleEffect(pulse && allowMotion ? 1.02 : 1.10)
            Image(systemName: "shield.checkered")
                .font(.system(size: size * 0.72, weight: .semibold))
                .foregroundStyle(
                    LinearGradient(
                        colors: [MissionTheme.mint, MissionTheme.signal],
                        startPoint: .top,
                        endPoint: .bottom
                    )
                )
                .shadow(color: MissionTheme.mint.opacity(0.55), radius: 24)
        }
        .frame(width: size * 2.35, height: size * 2.35)
        .onAppear {
            guard allowMotion else { return }
            withAnimation(.easeInOut(duration: 2.7).repeatForever(autoreverses: true)) {
                pulse = true
            }
        }
    }
}

struct SovereignPledgeCard: View {
    var body: some View {
        MissionCard(tint: MissionTheme.lamp) {
            HStack(alignment: .center, spacing: 18) {
                Image(systemName: "lock.laptopcomputer")
                    .font(.system(size: 34, weight: .semibold))
                    .foregroundStyle(MissionTheme.lamp)
                VStack(alignment: .leading, spacing: 4) {
                    Text("Your compute")
                    Text("Your rules")
                    Text("Your data")
                }
                .font(.system(size: 17, weight: .black, design: .monospaced))
                .tracking(1.8)
                .textCase(.uppercase)
                .foregroundStyle(MissionTheme.cream)
                Spacer()
                Text("Local-first. Operator-held. Trusted devices only.")
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: 260, alignment: .trailing)
            }
        }
    }
}

struct DeviceStageCard: View {
    var allowMotion: Bool
    @State private var glow = false

    var body: some View {
        MissionCard(tint: MissionTheme.mint) {
            VStack(alignment: .leading, spacing: 18) {
                HStack {
                    Text("Trusted Device Stage").sectionLabel()
                    Spacer()
                    StatusPill(text: "local-first", status: "strong")
                }
                ZStack {
                    RoundedRectangle(cornerRadius: 22, style: .continuous)
                        .fill(.black.opacity(0.20))
                        .frame(height: 210)
                    RouteArc(from: CGPoint(x: 0.16, y: 0.62), to: CGPoint(x: 0.50, y: 0.38), glow: glow && allowMotion)
                    RouteArc(from: CGPoint(x: 0.50, y: 0.38), to: CGPoint(x: 0.84, y: 0.62), glow: glow && allowMotion)
                    HStack(alignment: .bottom, spacing: 30) {
                        DeviceGlyph(title: "Phone", subtitle: "Govern", icon: "iphone", tint: MissionTheme.mint)
                        DeviceGlyph(title: "Alpha Mac", subtitle: "Command", icon: "laptopcomputer", tint: MissionTheme.signal, scale: 1.18)
                        DeviceGlyph(title: "Beta Laptop", subtitle: "Compute", icon: "macbook", tint: MissionTheme.lamp)
                    }
                    .padding(.horizontal, 22)
                }
            }
        }
        .onAppear {
            guard allowMotion else { return }
            withAnimation(.easeInOut(duration: 2.4).repeatForever(autoreverses: true)) {
                glow = true
            }
        }
    }
}

struct RouteArc: View {
    var from: CGPoint
    var to: CGPoint
    var glow: Bool

    var body: some View {
        GeometryReader { proxy in
            Canvas { context, size in
                let start = CGPoint(x: proxy.size.width * from.x, y: proxy.size.height * from.y)
                let end = CGPoint(x: proxy.size.width * to.x, y: proxy.size.height * to.y)
                var path = Path()
                path.move(to: start)
                path.addQuadCurve(
                    to: end,
                    control: CGPoint(x: (start.x + end.x) / 2, y: min(start.y, end.y) - proxy.size.height * 0.25)
                )
                context.stroke(path, with: .color(MissionTheme.mint.opacity(glow ? 0.78 : 0.42)), lineWidth: glow ? 2.3 : 1.5)
            }
        }
    }
}

struct DeviceGlyph: View {
    var title: String
    var subtitle: String
    var icon: String
    var tint: Color
    var scale: CGFloat = 1

    var body: some View {
        VStack(spacing: 8) {
            Image(systemName: icon)
                .font(.system(size: 46 * scale, weight: .semibold))
                .foregroundStyle(tint)
                .shadow(color: tint.opacity(0.38), radius: 14)
            Text(title)
                .font(.headline)
            Text(subtitle)
                .font(.caption.bold())
                .foregroundStyle(.secondary)
                .textCase(.uppercase)
        }
        .frame(maxWidth: .infinity)
    }
}

struct MeshGauge: View {
    var score: Int
    var allowMotion: Bool = true
    @State private var animatedScore: Double = 0
    @State private var pulse = false

    var body: some View {
        ZStack {
            Circle()
                .fill(scoreColor.opacity(pulse && allowMotion ? 0.13 : 0.07))
                .blur(radius: pulse && allowMotion ? 20 : 12)
            Circle()
                .stroke(.secondary.opacity(0.15), lineWidth: 18)
            Circle()
                .trim(from: 0, to: CGFloat(min(100, max(0, animatedScore))) / 100)
                .stroke(scoreColor.gradient, style: StrokeStyle(lineWidth: 18, lineCap: .round))
                .rotationEffect(.degrees(-90))
                .shadow(color: scoreColor.opacity(0.50), radius: 16)
            ForEach(0..<28, id: \.self) { index in
                Capsule()
                    .fill(index % 4 == 0 ? scoreColor.opacity(0.75) : .secondary.opacity(0.20))
                    .frame(width: 2, height: index % 4 == 0 ? 10 : 6)
                    .offset(y: -112)
                    .rotationEffect(.degrees(Double(index) * (360 / 28)))
            }
            VStack(spacing: 4) {
                Text("\(Int(round(animatedScore)))")
                    .font(.system(size: 52, weight: .black, design: .rounded))
                Text("mesh score")
                    .font(.caption.bold())
                    .foregroundStyle(.secondary)
            }
        }
        .frame(width: 220, height: 220)
        .accessibilityLabel("Mesh score \(score)")
        .onAppear {
            animatedScore = allowMotion ? 0 : Double(score)
            updateAnimation()
        }
        .onChange(of: score) { _ in
            updateAnimation()
        }
    }

    private func updateAnimation() {
        if allowMotion {
            withAnimation(.spring(response: 0.9, dampingFraction: 0.82)) {
                animatedScore = Double(score)
            }
            withAnimation(.easeInOut(duration: 2.8).repeatForever(autoreverses: true)) {
                pulse = true
            }
        } else {
            animatedScore = Double(score)
            pulse = false
        }
    }

    private var scoreColor: Color {
        if score >= 80 { return MissionTheme.mint }
        if score >= 50 { return MissionTheme.copper }
        return MissionTheme.ember
    }
}

struct StatusPill: View {
    var text: String
    var status: String

    var body: some View {
        Text(text)
            .font(.caption.bold())
            .padding(.horizontal, 10)
            .padding(.vertical, 5)
            .background(color.opacity(0.16), in: Capsule())
            .foregroundStyle(color)
            .overlay(Capsule().stroke(color.opacity(0.25), lineWidth: 1))
    }

    private var color: Color {
        switch status.lowercased().replacingOccurrences(of: " ", with: "_") {
        case "ok", "ready", "strong", "completed", "reachable", "fresh", "verified", "active", "healthy", "repaired":
            return MissionTheme.mint
        case "running", "proving", "queued", "planned", "aging", "repairing":
            return MissionTheme.signal
        case "warning", "needs_attention", "stale", "attention":
            return MissionTheme.copper
        case "failed", "unreachable", "cancelled", "blocked":
            return MissionTheme.ember
        default:
            return .secondary
        }
    }
}

struct TimelineList: View {
    var events: [AppStatusSnapshot.TimelineEvent]

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            if events.isEmpty {
                EmptyState(text: "No setup events yet. Press Activate Mesh to start the proof timeline.")
            } else {
                ForEach(events) { event in
                    HStack(alignment: .top, spacing: 12) {
                        Image(systemName: icon(for: event.kind))
                            .font(.title3)
                            .foregroundStyle(color(for: event.status ?? "info"))
                            .frame(width: 28)
                        VStack(alignment: .leading, spacing: 3) {
                            Text(event.kind.replacingOccurrences(of: "_", with: " ").capitalized)
                                .font(.headline)
                            Text(event.summary ?? "OCP recorded a setup event.")
                                .foregroundStyle(.secondary)
                            if let peer = event.peerID, !peer.isEmpty {
                                Text(peer)
                                    .font(.caption)
                                    .foregroundStyle(.tertiary)
                            }
                        }
                    }
                }
            }
        }
    }

    private func icon(for kind: String) -> String {
        if kind.contains("route") { return "point.3.connected.trianglepath.dotted" }
        if kind.contains("worker") || kind.contains("helper") { return "cpu" }
        if kind.contains("artifact") { return "shippingbox" }
        if kind.contains("proof") { return "checkmark.seal" }
        if kind.contains("fix") { return "wrench.and.screwdriver" }
        return "circle.hexagongrid"
    }

    private func color(for status: String) -> Color {
        switch status.lowercased() {
        case "ok", "ready", "completed": MissionTheme.mint
        case "failed": MissionTheme.ember
        case "warning", "needs_attention": MissionTheme.copper
        default: MissionTheme.signal
        }
    }
}

struct EmptyState: View {
    var text: String

    var body: some View {
        Text(text)
            .foregroundStyle(.secondary)
            .frame(maxWidth: .infinity, minHeight: 80, alignment: .center)
    }
}

struct SetupGuideCard: View {
    var steps: [SetupGuideStep]
    var allowMotion: Bool
    var startMesh: () -> Void
    var copyPhoneLink: () -> Void
    var activateMesh: () -> Void
    var openSetup: () -> Void
    @State private var reveal = false

    var body: some View {
        MissionCard(tint: MissionTheme.signal) {
            VStack(alignment: .leading, spacing: 16) {
                HStack(alignment: .firstTextBaseline) {
                    VStack(alignment: .leading, spacing: 5) {
                        Text("Guided Path").sectionLabel()
                        Text("From this Mac to a proven personal mesh.")
                            .font(.title3.weight(.bold))
                    }
                    Spacer()
                    StatusPill(text: "\(steps.filter { $0.status == "complete" }.count)/\(steps.count)", status: steps.allSatisfy { $0.status == "complete" } ? "strong" : "running")
                }

                VStack(alignment: .leading, spacing: 12) {
                    ForEach(Array(steps.enumerated()), id: \.element.id) { index, step in
                        HStack(alignment: .top, spacing: 12) {
                            StatusRing(status: step.status, index: index + 1, allowMotion: allowMotion)
                            VStack(alignment: .leading, spacing: 4) {
                                HStack {
                                    Text(step.title)
                                        .font(.headline)
                                    Spacer()
                                    Button(step.action) {
                                        perform(step)
                                    }
                                    .buttonStyle(.bordered)
                                    .disabled(step.status == "blocked" || step.status == "complete")
                                }
                                Text(step.summary)
                                    .foregroundStyle(.secondary)
                            }
                        }
                        .opacity(reveal || !allowMotion ? 1 : 0)
                        .offset(y: reveal || !allowMotion ? 0 : 10)
                        .animation(.easeOut(duration: 0.35).delay(Double(index) * 0.06), value: reveal)
                    }
                }
            }
        }
        .onAppear { reveal = true }
    }

    private func perform(_ step: SetupGuideStep) {
        switch step.id {
        case "start_mesh":
            startMesh()
        case "copy_phone_link":
            copyPhoneLink()
        case "activate_mesh":
            activateMesh()
        default:
            openSetup()
        }
    }
}

struct StatusRing: View {
    var status: String
    var index: Int
    var allowMotion: Bool
    @State private var pulse = false

    var body: some View {
        ZStack {
            Circle()
                .stroke(color.opacity(0.18), lineWidth: 3)
            Circle()
                .trim(from: 0, to: progress)
                .stroke(color.gradient, style: StrokeStyle(lineWidth: 3, lineCap: .round))
                .rotationEffect(.degrees(-90))
            Text(status == "complete" ? "✓" : "\(index)")
                .font(.caption.bold())
                .foregroundStyle(color)
        }
        .frame(width: 30, height: 30)
        .scaleEffect(pulse && allowMotion && status == "active" ? 1.08 : 1.0)
        .onAppear {
            guard allowMotion, status == "active" else { return }
            withAnimation(.easeInOut(duration: 1.2).repeatForever(autoreverses: true)) {
                pulse = true
            }
        }
    }

    private var progress: CGFloat {
        switch status {
        case "complete": return 1
        case "active": return 0.66
        case "attention": return 0.50
        default: return 0.18
        }
    }

    private var color: Color {
        switch status {
        case "complete": return MissionTheme.mint
        case "active": return MissionTheme.signal
        case "attention": return MissionTheme.copper
        default: return .secondary
        }
    }
}

struct TopologyGraphView: View {
    var graph: TopologyGraph
    var compact: Bool = false
    var allowMotion: Bool = true
    @State private var phase: CGFloat = 0

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Route Topology").sectionLabel()
                Spacer()
                RouteEdgeLegend()
            }
            if graph.nodes.count <= 1 {
                EmptyState(text: "Topology appears after OCP sees this Mac and at least one peer route or worker.")
            } else {
                Canvas { context, size in
                    let layout = layout(in: size)
                    drawEdges(context: &context, layout: layout)
                    drawNodes(context: &context, layout: layout)
                }
                .frame(height: compact ? 220 : 360)
                .background(.black.opacity(0.10), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
                .onAppear {
                    guard allowMotion else { return }
                    withAnimation(.linear(duration: 2.4).repeatForever(autoreverses: false)) {
                        phase = 1
                    }
                }
            }
        }
    }

    private func layout(in size: CGSize) -> [String: CGPoint] {
        guard let local = graph.nodes.first else { return [:] }
        let center = CGPoint(x: size.width * 0.5, y: size.height * 0.5)
        var result = [local.id: center]
        let peers = graph.nodes.dropFirst()
        let radius = min(size.width, size.height) * (compact ? 0.32 : 0.36)
        for (index, node) in peers.enumerated() {
            let angle = (Double(index) / Double(max(1, peers.count))) * Double.pi * 2 - Double.pi / 2
            result[node.id] = CGPoint(
                x: center.x + CGFloat(cos(angle)) * radius,
                y: center.y + CGFloat(sin(angle)) * radius
            )
        }
        return result
    }

    private func drawEdges(context: inout GraphicsContext, layout: [String: CGPoint]) {
        for edge in graph.edges {
            guard let start = layout[edge.source], let end = layout[edge.target] else { continue }
            var path = Path()
            path.move(to: start)
            path.addLine(to: end)
            let color = edgeColor(edge.status, edge.freshness)
            context.stroke(path, with: .color(color.opacity(0.58)), style: StrokeStyle(lineWidth: 2, lineCap: .round, dash: edge.freshness == "stale" ? [5, 6] : []))

            if allowMotion && ["reachable", "ready", "verified", "active"].contains(edge.status.lowercased()) {
                let progress = (phase + CGFloat(abs(edge.id.hashValue % 25)) / 25).truncatingRemainder(dividingBy: 1)
                let pulsePoint = CGPoint(x: start.x + (end.x - start.x) * progress, y: start.y + (end.y - start.y) * progress)
                context.fill(Path(ellipseIn: CGRect(x: pulsePoint.x - 4, y: pulsePoint.y - 4, width: 8, height: 8)), with: .color(color.opacity(0.85)))
            }
        }
    }

    private func drawNodes(context: inout GraphicsContext, layout: [String: CGPoint]) {
        for node in graph.nodes {
            guard let point = layout[node.id] else { continue }
            let isLocal = node.role == "local"
            let radius: CGFloat = isLocal ? 30 : 23
            let color = nodeColor(node.status, node.role)
            context.fill(Path(ellipseIn: CGRect(x: point.x - radius, y: point.y - radius, width: radius * 2, height: radius * 2)), with: .color(color.opacity(isLocal ? 0.34 : 0.24)))
            context.stroke(Path(ellipseIn: CGRect(x: point.x - radius, y: point.y - radius, width: radius * 2, height: radius * 2)), with: .color(color.opacity(0.88)), lineWidth: isLocal ? 3 : 2)
            context.draw(Text(node.label).font(.caption.bold()).foregroundColor(.primary), at: CGPoint(x: point.x, y: point.y + radius + 14), anchor: .top)
            if !compact {
                context.draw(Text(node.subtitle).font(.caption2).foregroundColor(.secondary), at: CGPoint(x: point.x, y: point.y + radius + 30), anchor: .top)
            }
        }
    }

    private func edgeColor(_ status: String, _ freshness: String) -> Color {
        if freshness == "stale" { return MissionTheme.copper }
        switch status.lowercased() {
        case "reachable", "ready", "verified", "active": return MissionTheme.mint
        case "failed", "unreachable", "cancelled": return MissionTheme.ember
        case "needs_attention", "stale", "attention": return MissionTheme.copper
        default: return MissionTheme.signal
        }
    }

    private func nodeColor(_ status: String, _ role: String) -> Color {
        if role == "local" { return MissionTheme.signal }
        switch status.lowercased() {
        case "reachable", "ready", "verified", "strong", "active": return MissionTheme.mint
        case "failed", "unreachable", "cancelled": return MissionTheme.ember
        case "needs_attention", "stale", "attention": return MissionTheme.copper
        default: return MissionTheme.signal
        }
    }
}

struct RouteEdgeLegend: View {
    var body: some View {
        HStack(spacing: 8) {
            legend("fresh", MissionTheme.mint)
            legend("stale", MissionTheme.copper)
            legend("down", MissionTheme.ember)
        }
        .font(.caption2.bold())
        .foregroundStyle(.secondary)
    }

    private func legend(_ label: String, _ color: Color) -> some View {
        HStack(spacing: 4) {
            Circle().fill(color).frame(width: 7, height: 7)
            Text(label)
        }
    }
}

struct MeshScoreChart: View {
    var points: [MissionControlChartPoint]

    var body: some View {
        if points.isEmpty {
            EmptyState(text: "History will appear after the app records a few status samples.")
        } else {
            Chart(points) { point in
                AreaMark(
                    x: .value("Sample", point.sampledAt),
                    y: .value("Mesh score", point.meshScore)
                )
                .foregroundStyle(MissionTheme.signal.opacity(0.18))
                LineMark(
                    x: .value("Sample", point.sampledAt),
                    y: .value("Mesh score", point.meshScore)
                )
                .foregroundStyle(MissionTheme.signal)
                .lineStyle(.init(lineWidth: 3))
                PointMark(
                    x: .value("Sample", point.sampledAt),
                    y: .value("Mesh score", point.meshScore)
                )
                .foregroundStyle(MissionTheme.signal.opacity(0.72))
            }
            .chartYScale(domain: 0...100)
            .chartXAxis(.hidden)
            .chartPlotStyle { plot in
                plot
                    .background(.white.opacity(0.04), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
            }
            .frame(height: 180)
        }
    }
}

struct RouteHealthChart: View {
    var healthy: Int
    var total: Int

    var body: some View {
        Chart {
            BarMark(x: .value("State", "Healthy"), y: .value("Routes", healthy))
                .foregroundStyle(MissionTheme.mint)
            BarMark(x: .value("State", "Needs work"), y: .value("Routes", max(0, total - healthy)))
                .foregroundStyle(MissionTheme.copper)
        }
        .chartPlotStyle { plot in
            plot
                .background(.white.opacity(0.04), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
        }
        .frame(height: 150)
    }
}

extension Text {
    func sectionLabel() -> some View {
        self
            .font(.system(size: 11, weight: .bold, design: .monospaced))
            .foregroundStyle(MissionTheme.signal.opacity(0.85))
            .tracking(1.6)
            .textCase(.uppercase)
    }
}
