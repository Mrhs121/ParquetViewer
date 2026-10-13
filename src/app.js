/**
 * app.js - Parquet QuickView Client Logic
 */

let currentFilePath = null;
let currentInfo = null;
let currentPage = 1;
let pageSize = 100;
let searchQuery = "";
let sortCol = null;
let sortOrder = "ASC";
let totalPages = 1;
let totalRows = 0;
let reqCounter = 0;
let isSidebarOpen = true;
let fileLoadId = 0;
let pageLoadId = 0;
let queryLoadId = 0;
let isPageLoading = false;
let isQueryRunning = false;
const pendingRequests = new Map();

// Helper: Native Bridge Communication
function sendToNative(action, payload = {}) {
  return new Promise((resolve, reject) => {
    const reqId = "req_" + (++reqCounter);
    // Dialogs are user-paced; only data requests have a delivery deadline.
    const timeout = ["info", "page", "query"].includes(action) ? setTimeout(() => {
      if (pendingRequests.delete(reqId)) {
        reject(new Error("后台响应超时，请重试或缩小查询范围"));
      }
    }, 75000) : null;
    pendingRequests.set(reqId, { resolve, reject, timeout });

    if (window.webkit && window.webkit.messageHandlers && window.webkit.messageHandlers.parquetBridge) {
      try {
        window.webkit.messageHandlers.parquetBridge.postMessage({
          action: action,
          reqId: reqId,
          ...payload
        });
      } catch (err) {
        clearTimeout(timeout);
        pendingRequests.delete(reqId);
        reject(err);
      }
    } else {
      console.warn("Native bridge not found. Running in standalone web preview mode.");
      setTimeout(() => {
        if (pendingRequests.has(reqId)) {
          clearTimeout(timeout);
          pendingRequests.delete(reqId);
          reject(new Error("原生 macOS 桥接接口未连接"));
        }
      }, 500);
    }
  });
}

// Swift callback entry point
window.onBridgeResponse = function(reqId, success, data, errorMsg) {
  if (pendingRequests.has(reqId)) {
    const { resolve, reject, timeout } = pendingRequests.get(reqId);
    clearTimeout(timeout);
    pendingRequests.delete(reqId);
    if (success) {
      resolve(data);
    } else {
      reject(new Error(errorMsg || "未知错误"));
    }
  }
};

// Swift can trigger file opening directly
window.loadFile = function(path) {
  if (!path) return;
  loadParquetFile(path);
};

// macOS owns file drops because WKWebView's JavaScript File objects have no native path.
window.setNativeDragActive = function(active) {
  document.getElementById("dropZone")?.classList.toggle("dragover", active);
};

// Initialize App
document.addEventListener("DOMContentLoaded", () => {
  initRecentFiles();
  initShortcuts();
});

// Shortcut bindings
function initShortcuts() {
  window.addEventListener("keydown", (e) => {
    // Cmd+O: Open File
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "o") {
      e.preventDefault();
      openFileDialog();
    }
    // Cmd+R: Reload
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "r" && currentFilePath) {
      e.preventDefault();
      reloadCurrentFile();
    }
    // Cmd+B or Cmd+\: Toggle Sidebar
    if ((e.metaKey || e.ctrlKey) && (e.key.toLowerCase() === "b" || e.key === "\\")) {
      e.preventDefault();
      toggleSidebar();
    }
    // Cmd+Enter inside SQL editor
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter" && document.activeElement === document.getElementById("sqlEditor")) {
      e.preventDefault();
      executeSqlQuery();
    }
  });
}

// Sidebar toggle
function toggleSidebar() {
  const sb = document.getElementById("appSidebar");
  const btn = document.getElementById("btnToggleSidebar");
  isSidebarOpen = !isSidebarOpen;
  if (isSidebarOpen) {
    sb.classList.remove("collapsed");
    btn.classList.add("active");
  } else {
    sb.classList.add("collapsed");
    btn.classList.remove("active");
  }
}

// Tab Switching
function switchTab(tabName) {
  document.querySelectorAll(".tab-btn").forEach(btn => btn.classList.remove("active"));
  document.querySelectorAll(".tab-panel").forEach(panel => panel.classList.remove("active"));

  const targetTab = document.getElementById("tab-" + tabName);
  if (targetTab) targetTab.classList.add("active");

  const btnIndex = { data: 0, schema: 1, sql: 2, meta: 3 }[tabName] || 0;
  const navBtns = document.querySelectorAll(".tabs-nav .tab-btn");
  if (navBtns[btnIndex]) navBtns[btnIndex].classList.add("active");
}

// Open File Dialog via Swift
function openFileDialog() {
  sendToNative("openFileDialog")
    .then(path => {
      if (path) loadParquetFile(path);
    })
    .catch(err => {
      console.log("Open file dialog cancelled:", err);
    });
}

// Export Dialog via Swift
function exportDataDialog() {
  if (!currentFilePath) {
    showToast("请先打开一个 Parquet 文件", "warning");
    return;
  }
  const currentSql = document.getElementById("sqlEditor").value.trim() || "SELECT * FROM data";
  sendToNative("exportDialog", {
    path: currentFilePath,
    sql: currentSql
  }).then(res => {
    showToast(`成功导出 ${res.format.toUpperCase()} (${formatBytes(res.file_size)})`);
  }).catch(err => {
    showToast("导出失败: " + err.message, "danger");
  });
}

// Reload current file
function reloadCurrentFile() {
  if (!currentFilePath) return;
  loadParquetFile(currentFilePath);
}

// Core Load Function
async function loadParquetFile(path) {
  const loadId = ++fileLoadId;
  try {
    showToast("正在读取元数据...", "info");
    const info = await sendToNative("info", { path: path });
    if (loadId !== fileLoadId) return;
    ++pageLoadId;
    ++queryLoadId;
    isPageLoading = false;
    setQueryRunning(false);
    currentFilePath = path;
    currentInfo = info;
    saveRecentFile(path, info.file_name);

    // Update Header
    document.getElementById("welcomeScreen").style.display = "none";
    document.getElementById("mainHeader").style.display = "flex";
    document.getElementById("appSidebar").style.display = "flex";
    
    document.getElementById("hdrFileName").textContent = info.file_name + (info.is_dir ? " (多分区目录)" : "");
    document.getElementById("hdrFileName").title = info.path;
    document.getElementById("hdrFilePath").textContent = info.path;
    document.getElementById("hdrRowsBadge").textContent = formatNumber(info.total_rows) + " 行";
    document.getElementById("hdrColsBadge").textContent = info.num_columns + " 列";
    document.getElementById("hdrSizeBadge").textContent = info.file_size_formatted;

    // Header Format and Iceberg Badges
    const badgeIcon = document.getElementById("hdrBadgeIcon");
    const formatBadge = document.getElementById("hdrFormatBadge");
    if (badgeIcon && formatBadge) {
      badgeIcon.className = "file-badge-icon";
      formatBadge.className = "pill-badge";

      if (info.iceberg_info) {
        badgeIcon.textContent = "🧊";
        badgeIcon.classList.add("iceberg");
        formatBadge.textContent = info.iceberg_info.badge || "ICEBERG";
        formatBadge.classList.add("iceberg");
      } else if (info.format_type === "avro") {
        badgeIcon.textContent = "AV";
        badgeIcon.classList.add("avro");
        formatBadge.textContent = "AVRO";
        formatBadge.classList.add("avro");
      } else {
        badgeIcon.textContent = "PQ";
        formatBadge.textContent = "PARQUET";
        formatBadge.classList.add("accent");
      }
    }

    // Reset pagination and search
    currentPage = 1;
    searchQuery = "";
    sortCol = null;
    sortOrder = "ASC";
    document.getElementById("searchInput").value = "";

    // Populate Sidebar
    renderSidebar(info);

    // Populate Schema View
    renderSchemaView(info);

    // Populate Metadata View
    renderMetadataView(info);

    // Prepopulate SQL Editor
    document.getElementById("sqlEditor").value = `SELECT * FROM data LIMIT 100;`;
    document.getElementById("sqlExecutionStatus").textContent = "就绪 (表名直接写 `data`，按 Cmd+Enter 执行)";
    document.getElementById("sqlExecutionTime").textContent = "";
    document.getElementById("sqlResultThead").innerHTML = "";
    document.getElementById("sqlResultTbody").innerHTML = "";

    // Switch to data tab and load page 1
    switchTab("data");
    const loaded = await loadDataPage(1);

    if (loaded && loadId === fileLoadId) {
      const typeLabel = info.iceberg_info ? "Iceberg 元数据" : (info.format_type === "avro" ? "Avro 文件" : "Parquet 文件");
      showToast(`已加载 ${info.file_name} (${formatNumber(info.total_rows)} 行 ${typeLabel})`, "success");
    }
  } catch (err) {
    if (loadId !== fileLoadId) return;
    console.error("加载文件失败:", err);
    showToast("打开文件失败: " + err.message, "danger");
  }
}

// Render Left Sidebar
function renderSidebar(info) {
  document.getElementById("sbFormatBadge").textContent = `v${info.format_version || '2'}`;
  document.getElementById("sbRowsVal").textContent = formatNumber(info.total_rows);
  document.getElementById("sbColsVal").textContent = info.num_columns;
  document.getElementById("sbSizeVal").textContent = info.file_size_formatted;

  // Iceberg Section
  const sbIcebergSection = document.getElementById("sbIcebergSection");
  if (sbIcebergSection) {
    if (info.iceberg_info) {
      sbIcebergSection.style.display = "flex";
      const ice = info.iceberg_info;
      document.getElementById("sbIcebergBadge").textContent = ice.type === "manifest_list" ? "SNAPSHOT" : "MANIFEST";
      document.getElementById("sbIcebergTypeVal").textContent = ice.title || (ice.type === "manifest_list" ? "快照清单 (Manifest List)" : "数据清单 (Manifest)");

      const snapRow = document.getElementById("sbIcebergSnapRow");
      if (snapRow) {
        if (ice.snapshot_id) {
          snapRow.style.display = "block";
          document.getElementById("sbIcebergSnapVal").textContent = ice.snapshot_id;
        } else {
          snapRow.style.display = "none";
        }
      }

      const seqRow = document.getElementById("sbIcebergSeqRow");
      if (seqRow) {
        if (ice.sequence_number) {
          seqRow.style.display = "block";
          document.getElementById("sbIcebergSeqVal").textContent = ice.sequence_number;
        } else {
          seqRow.style.display = "none";
        }
      }

      const specRow = document.getElementById("sbIcebergSpecRow");
      if (specRow) {
        if (ice.partition_spec && Array.isArray(ice.partition_spec) && ice.partition_spec.length > 0) {
          specRow.style.display = "block";
          document.getElementById("sbIcebergSpecVal").textContent = ice.partition_spec.map(s => `${s.name} (${s.transform})`).join(", ");
        } else {
          specRow.style.display = "none";
        }
      }
    } else {
      sbIcebergSection.style.display = "none";
    }
  }

  // Partition files
  const partsSection = document.getElementById("sbPartsSection");
  const partsList = document.getElementById("sbPartsList");
  if (info.part_files && info.part_files.length > 0) {
    partsSection.style.display = "flex";
    document.getElementById("sbPartsCount").textContent = info.part_files.length;
    let partsHtml = "";
    for (const pf of info.part_files) {
      partsHtml += `
        <div class="sidebar-part-item">
          <div class="sidebar-part-name" title="${escapeAttr(pf.rel_path)}">📄 ${escapeHtml(pf.name)}</div>
          <div class="sidebar-part-size">${escapeHtml(pf.size_formatted)}</div>
        </div>
      `;
    }
    partsList.innerHTML = partsHtml;
  } else {
    partsSection.style.display = "none";
  }

  // Columns list
  document.getElementById("sbFieldsCount").textContent = info.columns.length;
  const colList = document.getElementById("sbColumnsList");
  let colHtml = "";
  for (const col of info.columns) {
    const typeBadgeClass = getTypeBadgeClass(col.type);
    colHtml += `
      <div class="sidebar-item" data-col="${escapeAttr(col.name.toLowerCase())}" onclick="highlightColumn('${escapeAttr(col.name)}')">
        <span class="sidebar-item-name" title="${escapeAttr(col.name)}">${escapeHtml(col.name)}</span>
        <span class="type-badge ${typeBadgeClass}">${escapeHtml(col.spark_ddl_type)}</span>
      </div>
    `;
  }
  colList.innerHTML = colHtml;
}

function filterSidebarColumns(kw) {
  const clean = kw.trim().toLowerCase();
  document.querySelectorAll("#sbColumnsList .sidebar-item").forEach(item => {
    const name = item.getAttribute("data-col") || "";
    item.style.display = name.includes(clean) ? "flex" : "none";
  });
}

function highlightColumn(colName) {
  switchTab("data");
  // Find column in table and scroll horizontally to it
  const ths = document.querySelectorAll("#dataThead th");
  for (const th of ths) {
    if (th.getAttribute("data-col-raw") === colName) {
      th.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
      th.style.background = "var(--accent-subtle)";
      setTimeout(() => { th.style.background = ""; }, 1500);
      break;
    }
  }
}

// Load Paginated Data
async function loadDataPage(page, requestedPageSize = pageSize) {
  if (!currentFilePath) return false;
  const loadId = ++pageLoadId;
  const path = currentFilePath;
  isPageLoading = true;
  updatePaginationUI();
  document.getElementById("tableStatusText").textContent = `正在读取第 ${page} 页...`;
  document.getElementById("tableStatusTime").textContent = "";
  try {
    const data = await sendToNative("page", {
      path: path,
      page: page,
      pageSize: requestedPageSize,
      search: searchQuery || "__NONE__",
      sortCol: sortCol || "__NONE__",
      sortOrder: sortOrder
    });

    if (loadId !== pageLoadId || path !== currentFilePath) return false;
    currentPage = data.page;
    pageSize = data.page_size;
    document.getElementById("pageSizeSelect").value = String(pageSize);
    totalPages = data.total_pages;
    totalRows = data.total_rows;

    renderDataTable(data.columns, data.rows, data.elapsed_ms);
    document.querySelector("#tableContainer .table-scroll-area").scrollTop = 0;
    return true;
  } catch (err) {
    if (loadId !== pageLoadId || path !== currentFilePath) return false;
    document.getElementById("pageSizeSelect").value = String(pageSize);
    document.getElementById("tableStatusText").textContent = "读取失败，请重试";
    showToast("读取数据页失败: " + err.message, "danger");
    return false;
  } finally {
    if (loadId === pageLoadId && path === currentFilePath) {
      isPageLoading = false;
      updatePaginationUI();
    }
  }
}

// Render Data Grid Table (Compact auto-width, no unnatural stretch)
function renderDataTable(columns, rows, elapsedMs) {
  const thead = document.getElementById("dataThead");
  const tbody = document.getElementById("dataTbody");
  
  // Header row with column name + type subtitle
  let thHtml = `<tr><th class="row-idx-th"><div class="th-inner"><span class="th-col-name">#</span><span class="th-col-type">序号</span></div></th>`;
  for (const col of columns) {
    let sortIndicator = "";
    if (sortCol === col) {
      sortIndicator = sortOrder === "ASC" ? " ▲" : " ▼";
    }
    // lookup type for column
    let colType = "";
    if (currentInfo && currentInfo.columns) {
      const found = currentInfo.columns.find(c => c.name === col);
      if (found) colType = found.spark_ddl_type || found.type;
    }
    thHtml += `
      <th data-col-raw="${escapeAttr(col)}" onclick="toggleSort('${escapeAttr(col)}')">
        <div class="th-inner">
          <div class="th-col-name"><span>${escapeHtml(col)}</span><span>${sortIndicator}</span></div>
          <div class="th-col-type">${escapeHtml(colType)}</div>
        </div>
      </th>
    `;
  }
  thHtml += `</tr>`;
  thead.innerHTML = thHtml;

  document.getElementById("tableStatusText").textContent =
    `共 ${formatNumber(totalRows)} 行数据 • ${columns.length} 个字段 • 第 ${currentPage}/${totalPages} 页`;
  document.getElementById("tableStatusTime").textContent = `读取耗时: ${elapsedMs} ms`;

  // Body rows
  if (rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="${columns.length + 1}" style="text-align: center; padding: 40px; color: var(--text-muted);">暂无匹配数据</td></tr>`;
    return;
  }

  const startIdx = (currentPage - 1) * pageSize;
  let trHtml = "";
  for (let r = 0; r < rows.length; r++) {
    const row = rows[r];
    trHtml += `<tr><td class="row-idx-td">${startIdx + r + 1}</td>`;
    for (const col of columns) {
      const val = row[col];
      trHtml += renderTableCell(val, col);
    }
    trHtml += `</tr>`;
  }

  // Add subtle ghost rows (like Excel/TablePlus) so small datasets don't leave an empty canvas
  const minRows = 16;
  if (rows.length < minRows) {
    for (let g = rows.length; g < minRows; g++) {
      trHtml += `<tr class="ghost-row"><td class="row-idx-td">${g + 1}</td>`;
      for (let c = 0; c < columns.length; c++) {
        trHtml += `<td>&nbsp;</td>`;
      }
      trHtml += `</tr>`;
    }
  }

  tbody.innerHTML = trHtml;

}

// File path helper to detect nested/linked data files
function extractFilePath(raw) {
  if (!raw) return null;
  const text = typeof raw === "string" ? raw : JSON.stringify(raw);
  const match = text.match(/(?:file:\/\/)?(\/[^\s"'<>]+\.(?:parquet|pq|avro|avsc))/i);
  if (match) {
    return match[1];
  }
  return null;
}

// Cell formatting
function renderTableCell(val, colName) {
  if (val === null || val === undefined) {
    return `<td><span class="cell-null">null</span></td>`;
  }
  if (typeof val === "number") {
    return `<td>${val.toLocaleString()}</td>`;
  }
  if (typeof val === "boolean") {
    return `<td><span class="${val ? 'cell-bool-true' : 'cell-bool-false'}">${val}</span></td>`;
  }
  if (typeof val === "object") {
    const jsonStr = JSON.stringify(val);
    const targetPath = extractFilePath(val);
    const preview = jsonStr.length > 30 ? jsonStr.substring(0, 27) + "..." : jsonStr;
    const fileHint = targetPath ? ` [🔗 ${targetPath.split("/").pop()}]` : "";
    return `<td class="cell-clickable" onclick="inspectCell('${escapeAttr(colName)}', ${escapeAttr(JSON.stringify(jsonStr))})" title="${targetPath ? '包含关联文件，点击可直接打开' : '点击查看完整结构'}">${escapeHtml(preview)}${fileHint ? `<span style="color: var(--accent); font-size: 11px;">${escapeHtml(fileHint)}</span>` : ""}</td>`;
  }
  
  const str = String(val);
  const targetPath = extractFilePath(str);
  if (targetPath) {
    const fileName = targetPath.split("/").pop();
    return `<td class="cell-clickable" onclick="inspectCell('${escapeAttr(colName)}', ${escapeAttr(JSON.stringify(str))})" title="点击查看详情或直接打开文件">
      <span class="cell-file-link" onclick="event.stopPropagation(); loadParquetFile('${escapeAttr(targetPath)}')">📄 ${escapeHtml(fileName)}</span>
    </td>`;
  }

  if (str.length > 40) {
    const preview = str.substring(0, 37) + "...";
    return `<td class="cell-clickable" onclick="inspectCell('${escapeAttr(colName)}', ${escapeAttr(JSON.stringify(str))})" title="点击查看完整文本">${escapeHtml(preview)}</td>`;
  }
  return `<td>${escapeHtml(str)}</td>`;
}

let inspectedFilePath = null;

// Cell Inspector Modal
function inspectCell(colName, rawVal) {
  document.getElementById("cellModalTitle").textContent = `字段: ${colName}`;
  const modalBody = document.getElementById("cellModalBody");
  
  let formatted = rawVal;
  try {
    const parsed = JSON.parse(rawVal);
    formatted = JSON.stringify(parsed, null, 2);
  } catch (e) {
    formatted = rawVal;
  }
  modalBody.textContent = formatted;

  const targetPath = extractFilePath(rawVal);
  const openBtn = document.getElementById("btnOpenCellFile");
  if (openBtn) {
    if (targetPath) {
      inspectedFilePath = targetPath;
      const fileName = targetPath.split("/").pop();
      document.getElementById("btnOpenCellFileText").textContent = `打开 ${fileName}`;
      openBtn.style.display = "inline-flex";
    } else {
      inspectedFilePath = null;
      openBtn.style.display = "none";
    }
  }

  document.getElementById("cellModal").classList.add("active");
}

function openInspectedFile() {
  if (inspectedFilePath) {
    const p = inspectedFilePath;
    closeCellModal();
    loadParquetFile(p);
  }
}

function closeCellModal() {
  document.getElementById("cellModal").classList.remove("active");
}

function copyModalContent() {
  const content = document.getElementById("cellModalBody").textContent;
  navigator.clipboard.writeText(content).then(() => {
    showToast("内容已复制到剪贴板");
  });
}

// Sort column toggle
function toggleSort(col) {
  if (sortCol === col) {
    sortOrder = sortOrder === "ASC" ? "DESC" : "ASC";
  } else {
    sortCol = col;
    sortOrder = "ASC";
  }
  loadDataPage(1);
}

// Search handling
function handleSearchKey(event) {
  if (event.key === "Enter") {
    searchQuery = document.getElementById("searchInput").value.trim();
    loadDataPage(1);
  }
}

// Pagination controls
function goToPage(p) {
  if (isPageLoading) return;
  if (p < 1) p = 1;
  if (p > totalPages) p = totalPages;
  if (p !== currentPage) {
    return loadDataPage(p);
  }
}

function changePageSize(newSize) {
  const requestedPageSize = Number(newSize);
  if (isPageLoading || ![50, 100, 200, 500].includes(requestedPageSize)) {
    document.getElementById("pageSizeSelect").value = String(pageSize);
    return;
  }
  return loadDataPage(1, requestedPageSize);
}

function updatePaginationUI() {
  document.getElementById("curPageNum").textContent = currentPage;
  document.getElementById("pageInfoText").textContent = 
    isPageLoading ? "正在读取..." : `第 ${currentPage} / ${totalPages} 页 (共 ${formatNumber(totalRows)} 行)`;
  
  document.getElementById("btnFirstPage").disabled = isPageLoading || currentPage <= 1;
  document.getElementById("btnPrevPage").disabled = isPageLoading || currentPage <= 1;
  document.getElementById("btnNextPage").disabled = isPageLoading || currentPage >= totalPages;
  document.getElementById("btnLastPage").disabled = isPageLoading || currentPage >= totalPages;
  document.getElementById("pageSizeSelect").disabled = isPageLoading;
}

// Render Schema View
function renderSchemaView(info) {
  // 1. Iceberg underlying table schema
  const iceCard = document.getElementById("icebergTableCard");
  const iceTbody = document.getElementById("icebergTableTbody");
  const iceDDL = document.getElementById("icebergDDLCode");
  if (info.iceberg_info && info.iceberg_info.table_schema && info.iceberg_info.table_schema.fields) {
    iceCard.style.display = "block";
    let iceHtml = "";
    for (const f of info.iceberg_info.table_schema.fields) {
      const typeStr = typeof f.type === "object" ? JSON.stringify(f.type) : String(f.type);
      const typeBadgeClass = getTypeBadgeClass(typeStr);
      iceHtml += `
        <tr>
          <td style="text-align: center; color: var(--text-dim); font-family: var(--font-mono);">${f.id}</td>
          <td style="font-weight: 600; font-family: var(--font-mono);">${escapeHtml(f.name)}</td>
          <td><span class="type-badge ${typeBadgeClass}">${escapeHtml(typeStr)}</span></td>
          <td>${f.required ? '<span style="color: var(--warning); font-weight: 600;">必填 (Required)</span>' : '<span style="color: var(--text-muted);">可空 (Nullable)</span>'}</td>
        </tr>
      `;
    }
    iceTbody.innerHTML = iceHtml;
    iceDDL.textContent = info.iceberg_table_ddl || "";
  } else {
    iceCard.style.display = "none";
  }

  // 2. Physical record column list
  document.getElementById("schemaColCount").textContent = info.num_columns;
  const colCardTitle = document.getElementById("schemaColCardTitle");
  if (colCardTitle) {
    colCardTitle.textContent = info.format_type === "avro"
      ? (info.iceberg_info ? "Iceberg Avro 文件物理字段结构" : "Avro 文件物理字段结构")
      : "列定义列表";
  }
  const sparkCardTitle = document.getElementById("sparkDDLCardTitle");
  if (sparkCardTitle) {
    sparkCardTitle.textContent = info.format_type === "avro"
      ? "Spark SQL 建表 DDL (USING AVRO)"
      : "Spark SQL 建表 DDL (直接在 Spark / Hive 中使用)";
  }

  const tbody = document.getElementById("schemaTbody");
  let html = "";
  for (const col of info.columns) {
    const typeBadgeClass = getTypeBadgeClass(col.type);
    html += `
      <tr data-col-name="${escapeAttr(col.name.toLowerCase())}">
        <td style="text-align: center; color: var(--text-dim);">${col.index + 1}</td>
        <td style="font-weight: 600; font-family: var(--font-mono);">${escapeHtml(col.name)}</td>
        <td><span class="type-badge ${typeBadgeClass}">${escapeHtml(col.type)}</span></td>
        <td><span class="type-badge ${typeBadgeClass}">${escapeHtml(col.spark_ddl_type)}</span></td>
        <td>${col.nullable ? '<span style="color: var(--text-muted);">是 (Nullable)</span>' : '<span style="color: var(--warning); font-weight: 600;">NOT NULL</span>'}</td>
        <td style="font-family: var(--font-mono); font-size: 11px; color: var(--text-muted);">${escapeHtml(col.compression)}</td>
      </tr>
    `;
  }
  tbody.innerHTML = html;

  document.getElementById("sparkDDLCode").textContent = info.spark_ddl;
  document.getElementById("pysparkCode").textContent = info.pyspark_code;
}

function copyIcebergDDL() {
  if (currentInfo && currentInfo.iceberg_table_ddl) {
    navigator.clipboard.writeText(currentInfo.iceberg_table_ddl).then(() => {
      showToast("Iceberg 建表 DDL 已复制到剪贴板");
    });
  }
}

function getTypeBadgeClass(typeStr) {
  const t = typeStr.toLowerCase();
  if (t.includes("string") || t.includes("utf8")) return "type-string";
  if (t.includes("int")) return "type-int";
  if (t.includes("double") || t.includes("float") || t.includes("decimal")) return "type-double";
  if (t.includes("bool")) return "type-bool";
  if (t.includes("list") || t.includes("struct") || t.includes("map") || t.includes("array")) return "type-complex";
  if (t.includes("timestamp") || t.includes("date")) return "type-timestamp";
  return "type-string";
}

function filterSchemaColumns(kw) {
  const clean = kw.trim().toLowerCase();
  document.querySelectorAll("#schemaTbody tr").forEach(tr => {
    const colName = tr.getAttribute("data-col-name") || "";
    tr.style.display = colName.includes(clean) ? "" : "none";
  });
}

function copySparkDDL() {
  if (currentInfo && currentInfo.spark_ddl) {
    navigator.clipboard.writeText(currentInfo.spark_ddl).then(() => {
      showToast("Spark DDL 已复制到剪贴板");
    });
  }
}

function copyPySparkSchema() {
  if (currentInfo && currentInfo.pyspark_code) {
    navigator.clipboard.writeText(currentInfo.pyspark_code).then(() => {
      showToast("PySpark 代码已复制到剪贴板");
    });
  }
}

// Render Metadata View
function renderMetadataView(info) {
  document.getElementById("metaTotalRows").textContent = formatNumber(info.total_rows);
  document.getElementById("metaNumCols").textContent = info.num_columns;
  document.getElementById("metaFileSize").textContent = info.file_size_formatted;
  document.getElementById("metaFileCountDesc").textContent = info.is_dir ? `目录包含 ${info.file_count} 个分片文件` : "单文件";
  document.getElementById("metaNumRowGroups").textContent = info.num_row_groups;
  document.getElementById("metaCreatedBy").textContent = info.created_by;
  document.getElementById("metaFormatVersion").textContent = `v${info.format_version || '2'}`;

  // Iceberg Metadata Cards
  const snapCard = document.getElementById("metaIcebergSnapCard");
  const seqCard = document.getElementById("metaIcebergSeqCard");
  const specCard = document.getElementById("metaIcebergSpecCard");
  const iceMetaCard = document.getElementById("icebergMetaCard");

  if (info.iceberg_info) {
    const ice = info.iceberg_info;
    if (ice.snapshot_id) {
      snapCard.style.display = "flex";
      document.getElementById("metaIcebergSnapId").textContent = ice.snapshot_id;
      document.getElementById("metaIcebergParentSnapDesc").textContent = ice.parent_snapshot_id ? `父快照 ID: ${ice.parent_snapshot_id}` : "根快照 (Root Snapshot)";
    } else {
      snapCard.style.display = "none";
    }

    if (ice.sequence_number) {
      seqCard.style.display = "flex";
      document.getElementById("metaIcebergSeqNum").textContent = ice.sequence_number;
    } else {
      seqCard.style.display = "none";
    }

    if (ice.partition_spec && Array.isArray(ice.partition_spec) && ice.partition_spec.length > 0) {
      specCard.style.display = "flex";
      document.getElementById("metaIcebergSpecVal").textContent = ice.partition_spec.map(s => `${s.name} (${s.transform})`).join(", ");
      document.getElementById("metaIcebergSpecIdDesc").textContent = `Partition Spec ID: ${ice.partition_spec_id || 0}`;
    } else {
      specCard.style.display = "none";
    }

    iceMetaCard.style.display = "block";
    document.getElementById("metaIcebergJson").textContent = JSON.stringify(ice, null, 2);
  } else {
    snapCard.style.display = "none";
    seqCard.style.display = "none";
    specCard.style.display = "none";
    iceMetaCard.style.display = "none";
  }

  const sparkJsonBox = document.getElementById("metaSparkJson");
  if (info.spark_metadata) {
    sparkJsonBox.textContent = typeof info.spark_metadata === "object" ? JSON.stringify(info.spark_metadata, null, 2) : info.spark_metadata;
  } else {
    sparkJsonBox.textContent = "未检测到 Spark row metadata (标准存储)";
  }
}

// SQL Studio Execution
function setQueryRunning(running) {
  isQueryRunning = running;
  document.getElementById("btnRunQuery").disabled = running;
  document.getElementById("runQueryIcon").innerHTML = running ? '<span class="spinner"></span>' : "▶";
}

async function executeSqlQuery() {
  if (isQueryRunning) return;
  if (!currentFilePath) {
    showToast("请先打开一个 Parquet 文件", "warning");
    return;
  }
  const sql = document.getElementById("sqlEditor").value.trim();
  if (!sql) return;

  const loadId = ++queryLoadId;
  const path = currentFilePath;
  setQueryRunning(true);
  document.getElementById("sqlExecutionStatus").textContent = "正在通过 DuckDB 执行查询...";

  try {
    const res = await sendToNative("query", {
      path: path,
      sql: sql,
      limit: 1000
    });
    if (loadId !== queryLoadId || path !== currentFilePath) return;

    if (!res.success) {
      document.getElementById("sqlExecutionStatus").innerHTML = `<span style="color: var(--danger);">错误: ${escapeHtml(res.error)}</span>`;
      document.getElementById("sqlExecutionTime").textContent = `${res.elapsed_ms} ms`;
      document.getElementById("sqlResultThead").innerHTML = "";
      document.getElementById("sqlResultTbody").innerHTML = `<tr><td style="color: var(--danger); padding: 20px;">SQL 执行异常: ${escapeHtml(res.error)}</td></tr>`;
      showToast("SQL 查询出错", "danger");
      return;
    }

    const resultMessage = res.truncated
      ? `✓ 已显示前 ${res.row_count} 行（结果较多，预览已截断，可导出完整结果）`
      : `✓ 成功执行，返回 ${res.row_count} 行`;
    document.getElementById("sqlExecutionStatus").innerHTML = `<span style="color: var(--success);">${resultMessage}</span>`;
    document.getElementById("sqlExecutionTime").textContent = `耗时: ${res.elapsed_ms} ms`;

    // Render results
    const thead = document.getElementById("sqlResultThead");
    const tbody = document.getElementById("sqlResultTbody");

    let thHtml = `<tr><th class="row-idx-th">#</th>`;
    for (let c = 0; c < res.columns.length; c++) {
      const col = res.columns[c];
      const type = res.column_types ? res.column_types[c] : "";
      thHtml += `<th>${escapeHtml(col)} <span style="font-size: 10px; font-weight: normal; color: var(--text-dim);">(${type})</span></th>`;
    }
    thHtml += `</tr>`;
    thead.innerHTML = thHtml;

    if (res.rows.length === 0) {
      tbody.innerHTML = `<tr><td colspan="${res.columns.length + 1}" style="text-align: center; padding: 30px; color: var(--text-muted);">查询结果为空 (0 行)</td></tr>`;
      return;
    }

    let trHtml = "";
    for (let r = 0; r < res.rows.length; r++) {
      const row = res.rows[r];
      trHtml += `<tr><td class="row-idx-td">${r + 1}</td>`;
      for (const col of res.columns) {
        trHtml += renderTableCell(row[col], col);
      }
      trHtml += `</tr>`;
    }
    tbody.innerHTML = trHtml;

  } catch (err) {
    if (loadId !== queryLoadId || path !== currentFilePath) return;
    document.getElementById("sqlExecutionStatus").innerHTML = `<span style="color: var(--danger);">执行失败: ${escapeHtml(err.message)}</span>`;
  } finally {
    if (loadId === queryLoadId && path === currentFilePath) setQueryRunning(false);
  }
}

function applySqlTemplate(tmpl) {
  if (tmpl) {
    document.getElementById("sqlEditor").value = tmpl;
  }
}

// Recent Files Management (LocalStorage)
function initRecentFiles() {
  const recents = getRecentFiles();
  const list = document.getElementById("recentItems");
  const badge = document.getElementById("recentCountBadge");
  
  if (!list) return;

  if (recents.length === 0) {
    list.innerHTML = `<div style="font-size: 11px; color: var(--text-dim); padding: 12px 0;">暂无最近记录</div>`;
    if (badge) badge.textContent = "";
    return;
  }

  if (badge) badge.textContent = `${recents.length} 个文件`;
  list.innerHTML = "";
  recents.forEach(item => {
    const el = document.createElement("div");
    el.className = "welcome-recent-item";
    el.innerHTML = `
      <div style="display: flex; align-items: center; gap: 8px; min-width: 0;">
        <span style="font-size: 14px;">📄</span>
        <div style="min-width: 0;">
          <div class="welcome-recent-item-name">${escapeHtml(item.name)}</div>
          <div class="welcome-recent-item-path" title="${escapeAttr(item.path)}">${escapeHtml(item.path)}</div>
        </div>
      </div>
      <span style="font-size: 11px; color: var(--accent); flex-shrink: 0;">打开 ›</span>
    `;
    el.onclick = () => loadParquetFile(item.path);
    list.appendChild(el);
  });
}

function getRecentFiles() {
  try {
    return JSON.parse(localStorage.getItem("pv_recent_files") || "[]");
  } catch (e) {
    return [];
  }
}

function saveRecentFile(path, name) {
  let recents = getRecentFiles().filter(item => item.path !== path);
  recents.unshift({ path, name, timestamp: Date.now() });
  recents = recents.slice(0, 5);
  localStorage.setItem("pv_recent_files", JSON.stringify(recents));
  initRecentFiles();
}

// Toast System
let toastTimer = null;
function showToast(msg, type = "success") {
  const toast = document.getElementById("toast");
  const icon = document.getElementById("toastIcon");
  const msgEl = document.getElementById("toastMsg");

  icon.textContent = type === "danger" ? "✕" : (type === "warning" ? "!" : "✓");
  msgEl.textContent = msg;
  toast.classList.add("show");

  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    toast.classList.remove("show");
  }, 2800);
}

// Utilities
function formatNumber(num) {
  if (num === null || num === undefined) return "0";
  return Number(num).toLocaleString();
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function escapeHtml(str) {
  if (typeof str !== "string") str = String(str);
  return str.replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
}

function escapeAttr(str) {
  return escapeHtml(str).replace(/'/g, "\\'");
}
