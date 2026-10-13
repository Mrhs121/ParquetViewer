import Cocoa
import WebKit

// Each pipe must be drained while Python is running, including stderr.
private final class PipeReader: @unchecked Sendable {
    private let handle: FileHandle
    private let finished = DispatchSemaphore(value: 0)
    private var data = Data()

    init(_ handle: FileHandle) {
        self.handle = handle
    }

    func start() {
        DispatchQueue.global(qos: .userInitiated).async {
            self.data = self.handle.readDataToEndOfFile()
            self.finished.signal()
        }
    }

    func result() -> Data {
        finished.wait()
        return data
    }
}

func jsonString(_ str: String) -> String {
    if let data = try? JSONEncoder().encode(str),
       let res = String(data: data, encoding: .utf8) {
        return res
    }
    let escaped = str.replacingOccurrences(of: "\\", with: "\\\\").replacingOccurrences(of: "\"", with: "\\\"")
    return "\"\(escaped)\""
}

class ParquetBridgeHandler: NSObject, WKScriptMessageHandler {
    weak var appDelegate: AppDelegate?

    init(appDelegate: AppDelegate) {
        self.appDelegate = appDelegate
        super.init()
    }

    func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        guard let dict = message.body as? [String: Any],
              let action = dict["action"] as? String,
              let reqId = dict["reqId"] as? String else {
            return
        }

        switch action {
        case "openFileDialog":
            handleOpenFileDialog(reqId: reqId)
        case "exportDialog":
            handleExportDialog(reqId: reqId, dict: dict)
        case "getSamplePath":
            handleGetSamplePath(reqId: reqId, dict: dict)
        case "info", "page", "query":
            handleEngineCommand(action: action, reqId: reqId, dict: dict)
        default:
            sendResponse(reqId: reqId, success: false, dataJson: "{}", error: "未知操作: \(action)")
        }
    }

    private func handleGetSamplePath(reqId: String, dict: [String: Any]) {
        guard let name = dict["name"] as? String else {
            sendResponse(reqId: reqId, success: false, dataJson: "null", error: "缺少示例名称")
            return
        }
        var resolvedPath: String?
        if let resPath = Bundle.main.resourcePath {
            let candidate = "\(resPath)/samples/\(name)"
            if FileManager.default.fileExists(atPath: candidate) {
                resolvedPath = candidate
            }
        }
        if resolvedPath == nil {
            // Dynamic development path candidates (relative to executable or current working directory)
            let execDir = Bundle.main.bundleURL.deletingLastPathComponent().path
            let devCandidates = [
                "\(execDir)/assets/samples/\(name)",
                "\(execDir)/../assets/samples/\(name)",
                "\(FileManager.default.currentDirectoryPath)/assets/samples/\(name)"
            ]
            for c in devCandidates {
                if FileManager.default.fileExists(atPath: c) {
                    resolvedPath = c
                    break
                }
            }
        }

        if let validPath = resolvedPath {
            sendResponse(reqId: reqId, success: true, dataJson: jsonString(validPath))
        } else {
            sendResponse(reqId: reqId, success: false, dataJson: "null", error: "未找到内置示例: \(name)")
        }
    }

    private func handleOpenFileDialog(reqId: String) {
        DispatchQueue.main.async {
            let panel = NSOpenPanel()
            panel.canChooseFiles = true
            panel.canChooseDirectories = true
            panel.allowsMultipleSelection = false
            panel.allowedContentTypes = [] // allow all or parquet
            panel.message = "选择 Parquet / Iceberg Avro 文件或分区目录"
            panel.prompt = "打开"

            if panel.runModal() == .OK, let url = panel.url {
                self.sendResponse(reqId: reqId, success: true, dataJson: jsonString(url.path))
            } else {
                self.sendResponse(reqId: reqId, success: false, dataJson: "null", error: "用户取消")
            }
        }
    }

    private func handleExportDialog(reqId: String, dict: [String: Any]) {
        guard let path = dict["path"] as? String else {
            sendResponse(reqId: reqId, success: false, dataJson: "{}", error: "缺少路径参数")
            return
        }
        let sql = dict["sql"] as? String ?? "SELECT * FROM data"

        DispatchQueue.main.async {
            let panel = NSSavePanel()
            panel.title = "导出 Parquet 数据"
            panel.prompt = "导出"
            panel.nameFieldStringValue = "export.csv"
            panel.allowedContentTypes = []

            if panel.runModal() == .OK, let destUrl = panel.url {
                let destPath = destUrl.path
                let ext = destUrl.pathExtension.lowercased()
                let format = (ext == "json") ? "json" : ((ext == "parquet" || ext == "pq") ? "parquet" : "csv")

                DispatchQueue.global(qos: .userInitiated).async {
                    self.executePython(args: ["export", path, format, destPath, sql]) { output, error in
                        if let output = output, error == nil {
                            self.sendResponse(reqId: reqId, success: true, dataJson: output)
                        } else {
                            self.sendResponse(reqId: reqId, success: false, dataJson: "{}", error: error ?? "导出失败")
                        }
                    }
                }
            } else {
                self.sendResponse(reqId: reqId, success: false, dataJson: "{}", error: "用户取消导出")
            }
        }
    }

    private func handleEngineCommand(action: String, reqId: String, dict: [String: Any]) {
        guard let path = dict["path"] as? String else {
            sendResponse(reqId: reqId, success: false, dataJson: "{}", error: "缺少路径参数")
            return
        }

        var args: [String] = []
        if action == "info" {
            args = ["info", path]
        } else if action == "page" {
            let page = dict["page"] as? Int ?? 1
            let pageSize = dict["pageSize"] as? Int ?? 100
            let search = dict["search"] as? String ?? "__NONE__"
            let sortCol = dict["sortCol"] as? String ?? "__NONE__"
            let sortOrder = dict["sortOrder"] as? String ?? "ASC"
            args = ["page", path, String(page), String(pageSize), search, sortCol, sortOrder]
        } else if action == "query" {
            let sql = dict["sql"] as? String ?? "SELECT * FROM data LIMIT 100"
            let limit = dict["limit"] as? Int ?? 1000
            args = ["query", path, sql, String(limit)]
        }

        DispatchQueue.global(qos: .userInitiated).async {
            self.executePython(args: args) { output, error in
                if let output = output, error == nil {
                    self.sendResponse(reqId: reqId, success: true, dataJson: output)
                } else {
                    self.sendResponse(reqId: reqId, success: false, dataJson: "{}", error: error ?? "执行错误")
                }
            }
        }
    }

    private func executePython(args: [String], completion: @escaping (String?, String?) -> Void) {
        let process = Process()
        let pipe = Pipe()
        let errPipe = Pipe()

        // Locate python executable
        let possiblePythons = [
            "/usr/bin/python3",
            "/opt/homebrew/bin/python3",
            "/usr/local/bin/python3"
        ]
        var pythonPath = "/usr/bin/python3"
        for p in possiblePythons {
            if FileManager.default.fileExists(atPath: p) {
                pythonPath = p
                break
            }
        }

        // Locate parquet_engine.py
        var scriptPath = Bundle.main.path(forResource: "parquet_engine", ofType: "py")
        if scriptPath == nil {
            // Check relative to executable or current working directory
            let execDir = Bundle.main.bundleURL.deletingLastPathComponent().path
            let candidates = [
                "\(execDir)/parquet_engine.py",
                "\(execDir)/src/parquet_engine.py",
                "\(FileManager.default.currentDirectoryPath)/src/parquet_engine.py",
                "\(FileManager.default.currentDirectoryPath)/parquet_engine.py"
            ]
            for c in candidates {
                if FileManager.default.fileExists(atPath: c) {
                    scriptPath = c
                    break
                }
            }
        }

        guard let validScript = scriptPath else {
            completion(nil, "找不到 parquet_engine.py 后端引擎")
            return
        }

        process.executableURL = URL(fileURLWithPath: pythonPath)
        process.arguments = [validScript] + args
        process.standardOutput = pipe
        process.standardError = errPipe
        let exited = DispatchSemaphore(value: 0)
        process.terminationHandler = { _ in exited.signal() }

        do {
            try process.run()
            let stdout = PipeReader(pipe.fileHandleForReading)
            let stderr = PipeReader(errPipe.fileHandleForReading)
            stdout.start()
            stderr.start()

            let timeout: TimeInterval = args.first == "export" ? 300 : 60
            let timedOut = exited.wait(timeout: .now() + timeout) == .timedOut
            if timedOut {
                if process.isRunning { process.terminate() }
                if exited.wait(timeout: .now() + 2) == .timedOut {
                    if process.isRunning { kill(process.processIdentifier, SIGKILL) }
                    exited.wait()
                }
            }

            let data = stdout.result()
            let errData = stderr.result()

            if timedOut {
                completion(nil, "请求超时（\(Int(timeout)) 秒），已停止执行。请缩小查询范围后重试。")
                return
            }

            let output = String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)
            let errOutput = String(data: errData, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)

            if process.terminationStatus == 0, let out = output, !out.isEmpty {
                completion(out, nil)
            } else {
                let errorObject = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
                let message = errorObject?["error"] as? String
                completion(nil, message ?? (errOutput.flatMap { $0.isEmpty ? nil : $0 })
                    ?? "进程退出，代码: \(process.terminationStatus)")
            }
        } catch {
            completion(nil, "启动后端引擎失败: \(error.localizedDescription)")
        }
    }

    private func sendResponse(reqId: String, success: Bool, dataJson: String, error: String? = nil) {
        DispatchQueue.main.async { [weak self] in
            guard let webView = self?.appDelegate?.webView else { return }
            let js: String
            if success {
                js = "window.onBridgeResponse(\(jsonString(reqId)), true, \(dataJson), null);"
            } else {
                js = "window.onBridgeResponse(\(jsonString(reqId)), false, null, \(jsonString(error ?? "执行失败")));"
            }
            webView.evaluateJavaScript(js) { _, responseError in
                if success, responseError != nil {
                    let fallback = "window.onBridgeResponse(\(jsonString(reqId)), false, null, '无法显示后台返回的数据，请减少每页条数或查询结果数量。');"
                    webView.evaluateJavaScript(fallback, completionHandler: nil)
                }
            }
        }
    }
}

// Receive file drops on the web view itself; WebKit consumes drops before a parent view can.
class ParquetWebView: WKWebView {
    weak var appDelegate: AppDelegate?
    private var isHandlingFileDrag = false

    private func droppedFileURL(_ sender: NSDraggingInfo) -> URL? {
        let urls = sender.draggingPasteboard.readObjects(
            forClasses: [NSURL.self],
            options: [.urlReadingFileURLsOnly: true]
        ) as? [URL]
        return urls?.first
    }

    private func setFileDragActive(_ active: Bool) {
        guard isHandlingFileDrag != active else { return }
        isHandlingFileDrag = active
        evaluateJavaScript("window.setNativeDragActive?.(\(active));", completionHandler: nil)
    }

    override func draggingEntered(_ sender: NSDraggingInfo) -> NSDragOperation {
        guard droppedFileURL(sender) != nil else {
            return super.draggingEntered(sender)
        }
        setFileDragActive(true)
        return .copy
    }

    override func draggingUpdated(_ sender: NSDraggingInfo) -> NSDragOperation {
        return isHandlingFileDrag ? .copy : super.draggingUpdated(sender)
    }

    override func draggingExited(_ sender: NSDraggingInfo?) {
        if isHandlingFileDrag {
            setFileDragActive(false)
        } else {
            super.draggingExited(sender)
        }
    }

    override func prepareForDragOperation(_ sender: NSDraggingInfo) -> Bool {
        return isHandlingFileDrag ? droppedFileURL(sender) != nil : super.prepareForDragOperation(sender)
    }

    override func performDragOperation(_ sender: NSDraggingInfo) -> Bool {
        guard isHandlingFileDrag else {
            return super.performDragOperation(sender)
        }
        setFileDragActive(false)
        guard let url = droppedFileURL(sender), let appDelegate = appDelegate else {
            return false
        }
        appDelegate.openFileFromPath(url.path)
        return true
    }

    override func draggingEnded(_ sender: NSDraggingInfo) {
        setFileDragActive(false)
    }
}

class AppDelegate: NSObject, NSApplicationDelegate, WKNavigationDelegate {
    var window: NSWindow!
    var webView: WKWebView!
    var initialFileToOpen: String?
    var pendingPathToLoad: String?
    var isWebReady: Bool = false

    func applicationDidFinishLaunching(_ notification: Notification) {
        setupMenuBar()
        setupWindow()

        if let pending = initialFileToOpen {
            openFileFromPath(pending)
            initialFileToOpen = nil
        }
    }

    func application(_ sender: NSApplication, openFiles filenames: [String]) {
        if let first = filenames.first {
            openFileFromPath(first)
        }
    }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        isWebReady = true
        if let path = pendingPathToLoad {
            let p = path
            pendingPathToLoad = nil
            openFileFromPath(p)
        }
    }

    func openFileFromPath(_ path: String) {
        let absPath = (path as NSString).expandingTildeInPath
        if !isWebReady || webView == nil {
            pendingPathToLoad = absPath
            return
        }

        let str = jsonString(absPath)
        let js = "window.loadFile(\(str));"
        DispatchQueue.main.async { [weak self] in
            self?.webView?.evaluateJavaScript(js, completionHandler: nil)
        }
    }

    private func setupWindow() {
        let windowRect = NSRect(x: 100, y: 100, width: 1180, height: 780)
        window = NSWindow(
            contentRect: windowRect,
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.title = "Parquet QuickView"
        window.titlebarAppearsTransparent = false
        window.center()
        window.setFrameAutosaveName("ParquetViewerMainWindow")
        window.minSize = NSSize(width: 850, height: 550)

        // WebKit Configuration
        let config = WKWebViewConfiguration()
        let handler = ParquetBridgeHandler(appDelegate: self)
        config.userContentController.add(handler, name: "parquetBridge")
        config.preferences.setValue(true, forKey: "developerExtrasEnabled")

        let containerView = NSView(frame: NSRect(origin: .zero, size: windowRect.size))
        window.contentView = containerView

        let parquetWebView = ParquetWebView(frame: .zero, configuration: config)
        parquetWebView.appDelegate = self
        parquetWebView.registerForDraggedTypes([.fileURL])
        webView = parquetWebView
        webView.navigationDelegate = self
        webView.translatesAutoresizingMaskIntoConstraints = false
        containerView.addSubview(webView)

        // Use content coordinates, not the window's position on the screen.
        NSLayoutConstraint.activate([
            webView.leadingAnchor.constraint(equalTo: containerView.leadingAnchor),
            webView.trailingAnchor.constraint(equalTo: containerView.trailingAnchor),
            webView.topAnchor.constraint(equalTo: containerView.topAnchor),
            webView.bottomAnchor.constraint(equalTo: containerView.bottomAnchor)
        ])
        window.makeKeyAndOrderFront(nil)

        // Load index.html
        loadAppHTML()
    }

    private func loadAppHTML() {
        if let htmlPath = Bundle.main.path(forResource: "index", ofType: "html") {
            let url = URL(fileURLWithPath: htmlPath)
            webView.loadFileURL(url, allowingReadAccessTo: url.deletingLastPathComponent())
            return
        }

        // Check relative to executable or current working directory for development
        let execDir = Bundle.main.bundleURL.deletingLastPathComponent().path
        let candidates = [
            "\(execDir)/index.html",
            "\(execDir)/src/index.html",
            "\(FileManager.default.currentDirectoryPath)/src/index.html",
            "\(FileManager.default.currentDirectoryPath)/index.html"
        ]
        for c in candidates {
            if FileManager.default.fileExists(atPath: c) {
                let url = URL(fileURLWithPath: c)
                webView.loadFileURL(url, allowingReadAccessTo: url.deletingLastPathComponent())
                return
            }
        }
    }

    private func setupMenuBar() {
        let mainMenu = NSMenu()

        // App Menu
        let appMenuItem = NSMenuItem()
        let appMenu = NSMenu()
        appMenu.addItem(withTitle: "关于 Parquet QuickView", action: #selector(NSApplication.orderFrontStandardAboutPanel(_:)), keyEquivalent: "")
        appMenu.addItem(NSMenuItem.separator())
        appMenu.addItem(withTitle: "隐藏 Parquet QuickView", action: #selector(NSApplication.hide(_:)), keyEquivalent: "h")
        let hideOthers = NSMenuItem(title: "隐藏其他", action: #selector(NSApplication.hideOtherApplications(_:)), keyEquivalent: "h")
        hideOthers.keyEquivalentModifierMask = [.command, .option]
        appMenu.addItem(hideOthers)
        appMenu.addItem(withTitle: "显示全部", action: #selector(NSApplication.unhideAllApplications(_:)), keyEquivalent: "")
        appMenu.addItem(NSMenuItem.separator())
        appMenu.addItem(withTitle: "退出 Parquet QuickView", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")
        appMenuItem.submenu = appMenu
        mainMenu.addItem(appMenuItem)

        // File Menu
        let fileMenuItem = NSMenuItem()
        let fileMenu = NSMenu(title: "文件")
        let openItem = NSMenuItem(title: "打开 Parquet / Iceberg Avro 文件...", action: #selector(menuOpenFile), keyEquivalent: "o")
        openItem.target = self
        fileMenu.addItem(openItem)
        fileMenu.addItem(NSMenuItem.separator())
        fileMenu.addItem(withTitle: "关闭窗口", action: #selector(NSWindow.performClose(_:)), keyEquivalent: "w")
        fileMenuItem.submenu = fileMenu
        mainMenu.addItem(fileMenuItem)

        // Edit Menu (Required for Cmd+C, Cmd+V, Cmd+A in WebKit)
        let editMenuItem = NSMenuItem()
        let editMenu = NSMenu(title: "编辑")
        editMenu.addItem(withTitle: "撤销", action: Selector(("undo:")), keyEquivalent: "z")
        editMenu.addItem(withTitle: "重做", action: Selector(("redo:")), keyEquivalent: "Z")
        editMenu.addItem(NSMenuItem.separator())
        editMenu.addItem(withTitle: "剪切", action: #selector(NSText.cut(_:)), keyEquivalent: "x")
        editMenu.addItem(withTitle: "拷贝", action: #selector(NSText.copy(_:)), keyEquivalent: "c")
        editMenu.addItem(withTitle: "粘贴", action: #selector(NSText.paste(_:)), keyEquivalent: "v")
        editMenu.addItem(withTitle: "全选", action: #selector(NSText.selectAll(_:)), keyEquivalent: "a")
        editMenuItem.submenu = editMenu
        mainMenu.addItem(editMenuItem)

        // Window Menu
        let windowMenuItem = NSMenuItem()
        let windowMenu = NSMenu(title: "窗口")
        windowMenu.addItem(withTitle: "最小化", action: #selector(NSWindow.performMiniaturize(_:)), keyEquivalent: "m")
        windowMenu.addItem(withTitle: "缩放", action: #selector(NSWindow.performZoom(_:)), keyEquivalent: "")
        windowMenuItem.submenu = windowMenu
        mainMenu.addItem(windowMenuItem)

        NSApplication.shared.mainMenu = mainMenu
    }

    @objc func menuOpenFile() {
        let panel = NSOpenPanel()
        panel.canChooseFiles = true
        panel.canChooseDirectories = true
        panel.allowsMultipleSelection = false
        panel.message = "选择 Parquet / Iceberg Avro 文件或分区目录"
        panel.prompt = "打开"

        if panel.runModal() == .OK, let url = panel.url {
            openFileFromPath(url.path)
        }
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        return true
    }
}

// App Entry Point
let app = NSApplication.shared
app.setActivationPolicy(.regular)
let delegate = AppDelegate()

// Check command line arguments for a file to open
let args = CommandLine.arguments
if args.count > 1 {
    let candidatePath = args[1]
    if !candidatePath.starts(with: "-") {
        delegate.initialFileToOpen = (candidatePath as NSString).expandingTildeInPath
    }
}

app.delegate = delegate
app.run()
