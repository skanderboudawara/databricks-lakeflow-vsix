// @ts-nocheck — untyped webview script; type annotations to be added incrementally
(function () {
    'use strict';
    let job = window.__DAG_JOB__;
    const allJobNames = window.__DAG_ALL_JOB_NAMES__;
    const vscodeApi = acquireVsCodeApi();
    // ── Layout constants ────────────────────────────────────────────────────────
    const NW = 190, NH = 62, CS = 44, HG = 130, VG = 28;
    // ── DOM refs ────────────────────────────────────────────────────────────────
    const dagSvg = document.getElementById('dag');
    const zg = document.getElementById('zg');
    const sidebar = document.getElementById('sidebar');
    const panelContent = document.getElementById('panel-content');
    const bottomBar = document.getElementById('bottom-bar');
    const taskBreadcrumb = document.getElementById('task-breadcrumb');
    const taskPanelContent = document.getElementById('task-panel-content');
    // ── State ───────────────────────────────────────────────────────────────────
    let tx = 40, ty = 0, sc = 1;
    let selectedKey = null;
    let positions = {};
    let sidebarOpen = true;
    let bottomBarOpen = false;
    let bottomBarHeight = 260;
    let _bbox = null;
    // ── Top bar ─────────────────────────────────────────────────────────────────
    const hdrNameInput = document.getElementById('hdr-name');
    hdrNameInput.value = job.name;
    hdrNameInput.addEventListener('input', function () {
        const newName = hdrNameInput.value.trim();
        if (newName && newName !== job.name) {
            pendingUpdates['name'] = newName;
        }
        else {
            delete pendingUpdates['name'];
        }
        markDirtyDirect();
    });
    document.getElementById('hdr-tasks').textContent =
        job.tasks.length + ' task' + (job.tasks.length !== 1 ? 's' : '');
    if (job.trigger) {
        const lbl = { table_update: 'Table trigger', periodic: 'Scheduled', file_arrival: 'File arrival', manual: 'Manual' };
        const el = document.getElementById('hdr-trigger');
        el.textContent = lbl[job.trigger.type] || job.trigger.type;
        el.style.display = '';
    }
    if (job.hasConditions)
        document.getElementById('hdr-cond').style.display = '';
    document.getElementById('btn-open-file').addEventListener('click', () => vscodeApi.postMessage({ command: 'openFile', filePath: job.filePath }));
    // ── Sidebar (left, job info) toggle ─────────────────────────────────────────
    function toggleSidebar(open) {
        sidebarOpen = open;
        sidebar.classList.toggle('collapsed', !open);
        if (open && _bbox)
            setTimeout(() => fitView(_bbox), 200);
        fab.style.display = open ? 'none' : '';
    }
    // FAB to re-open left sidebar (sits at left edge of canvas)
    const fab = document.createElement('button');
    fab.className = 'sidebar-toggle-fab';
    fab.title = 'Show panel';
    fab.textContent = '‹';
    fab.style.display = 'none';
    document.querySelector('.canvas-wrap').appendChild(fab);
    fab.addEventListener('click', () => toggleSidebar(true));
    document.getElementById('btn-collapse').addEventListener('click', () => toggleSidebar(false));
    // ── Bottom bar (task details) toggle ─────────────────────────────────────────
    function openBottomBar() {
        bottomBarOpen = true;
        bottomBar.style.height = bottomBarHeight + 'px';
    }
    function closeBottomBar() {
        bottomBarOpen = false;
        bottomBar.style.height = '0';
    }
    document.getElementById('btn-close-bottom').addEventListener('click', () => deselect());
    // ── Layout ──────────────────────────────────────────────────────────────────
    function computeLayout(tasks) {
        const tmap = {}, ch = {}, ind = {};
        for (const t of tasks) {
            tmap[t.task_key] = t;
            ch[t.task_key] = [];
            ind[t.task_key] = 0;
        }
        for (const t of tasks)
            for (const d of t.depends_on) {
                ind[t.task_key]++;
                if (ch[d.task_key])
                    ch[d.task_key].push(t.task_key);
            }
        const q = [], lv = {};
        for (const [k, d] of Object.entries(ind))
            if (d === 0) {
                q.push(k);
                lv[k] = 0;
            }
        let qi = 0;
        while (qi < q.length) {
            const k = q[qi++];
            for (const c of ch[k]) {
                lv[c] = Math.max(lv[c] || 0, lv[k] + 1);
                ind[c]--;
                if (ind[c] === 0)
                    q.push(c);
            }
        }
        for (const t of tasks)
            if (lv[t.task_key] === undefined)
                lv[t.task_key] = 0;
        const byLv = {};
        let maxLv = 0;
        for (const [k, l] of Object.entries(lv)) {
            maxLv = Math.max(maxLv, l);
            if (!byLv[l])
                byLv[l] = [];
            byLv[l].push(k);
        }
        for (const l in byLv)
            byLv[l].sort((a, b) => a.localeCompare(b));
        const pos = {};
        for (let l = 0; l <= maxLv; l++) {
            const keys = byLv[l] || [];
            const colH = keys.reduce((s, k) => s + (tmap[k].type === 'condition' ? CS * 2 : NH), 0)
                + Math.max(0, keys.length - 1) * VG;
            let y = -colH / 2;
            for (const k of keys) {
                const t = tmap[k], h = t.type === 'condition' ? CS * 2 : NH;
                pos[k] = { x: l * (NW + HG), y, w: t.type === 'condition' ? CS * 2 : NW, h, level: l };
                y += h + VG;
            }
        }
        return pos;
    }
    // ── Node colours ─────────────────────────────────────────────────────────────
    function nodeColors(type) {
        return ({
            notebook: { fill: 'transparent', stroke: '#d4d4d4', accent: '#4a9eff' },
            condition: { fill: 'transparent', stroke: '#d4d4d4', accent: '#ffb74d' },
            spark_python: { fill: 'transparent', stroke: '#d4d4d4', accent: '#66bb6a' },
            python_wheel: { fill: 'transparent', stroke: '#d4d4d4', accent: '#66bb6a' },
            sql: { fill: 'transparent', stroke: '#d4d4d4', accent: '#ce93d8' },
            run_job: { fill: 'transparent', stroke: '#d4d4d4', accent: '#4dd0e1' },
            unknown: { fill: 'transparent', stroke: '#d4d4d4', accent: '#78909c' },
        })[type] || { fill: 'transparent', stroke: '#d4d4d4', accent: '#78909c' };
    }
    // ── SVG helpers ──────────────────────────────────────────────────────────────
    const NS = 'http://www.w3.org/2000/svg';
    function el(tag, a = {}) {
        const e = document.createElementNS(NS, tag);
        for (const [k, v] of Object.entries(a))
            e.setAttribute(k, v);
        return e;
    }
    function txt(s, a = {}) { const e = el('text', a); e.textContent = s; return e; }
    function trunc(s, n) { return s && s.length > n ? s.slice(0, n - 1) + '…' : (s || ''); }
    function edgePath(sp, st, dp) {
        const x1 = st.type === 'condition' ? sp.x + CS * 2 : sp.x + NW;
        const y1 = sp.y + sp.h / 2;
        const x2 = dp.x, y2 = dp.y + dp.h / 2;
        const cx = (x1 + x2) / 2;
        return `M${x1},${y1} C${cx},${y1} ${cx},${y2} ${x2},${y2}`;
    }
    // ── Edge & port helpers ───────────────────────────────────────────────────────
    function getEffectiveDepsFor(taskKey) {
        if (taskKey in taskDepsOverride)
            return taskDepsOverride[taskKey];
        const found = getVirtualTasks().find(function (t) { return t.task_key === taskKey; });
        return found ? found.depends_on : [];
    }
    function getPortPos(taskKey, portType) {
        const p = positions[taskKey];
        const found = getVirtualTasks().find(function (t) { return t.task_key === taskKey; });
        if (!p || !found)
            return null;
        if (found.type === 'condition') {
            const cx = p.x + CS, cy = p.y + CS;
            if (portType === 'in')
                return { x: p.x, y: cy };
            if (portType === 'out-true')
                return { x: p.x + CS * 2, y: cy };
            if (portType === 'out-false')
                return { x: cx, y: p.y + CS * 2 };
        }
        else {
            if (portType === 'in')
                return { x: p.x, y: p.y + NH / 2 };
            if (portType === 'out')
                return { x: p.x + NW, y: p.y + NH / 2 };
        }
        return null;
    }
    function svgCoords(clientX, clientY) {
        const r = dagSvg.getBoundingClientRect();
        return { x: (clientX - r.left - tx) / sc, y: (clientY - r.top - ty) / sc };
    }
    function redrawEdges() {
        if (!edgesGroup)
            return;
        edgesGroup.innerHTML = '';
        const vtasks = getVirtualTasks();
        const tmap2 = {};
        for (const t of vtasks)
            tmap2[t.task_key] = t;
        for (const t of vtasks) {
            const effectiveDeps = getEffectiveDepsFor(t.task_key);
            for (const dep of effectiveDeps) {
                const sp = positions[dep.task_key], dp = positions[t.task_key];
                if (!sp || !dp)
                    continue;
                const srcTask = tmap2[dep.task_key];
                if (!srcTask)
                    continue;
                const iT = dep.outcome === 'true', iF = dep.outcome === 'false';
                const color = iT ? '#4caf50' : iF ? '#f44336' : '#546e7a';
                const marker = iT ? 'url(#arr-t)' : iF ? 'url(#arr-f)' : 'url(#arr-d)';
                const pathStr = edgePath(sp, srcTask, dp);
                const grp = el('g', { class: 'edge-grp' });
                grp.dataset.edgeFrom = dep.task_key;
                grp.dataset.edgeTo = t.task_key;
                grp.dataset.edgeOutcome = dep.outcome || '';
                // Visible edge line
                grp.appendChild(el('path', {
                    d: pathStr, stroke: color, 'stroke-width': '1.5', fill: 'none', 'marker-end': marker,
                }));
                // Wide invisible hit area
                grp.appendChild(el('path', {
                    d: pathStr, stroke: 'transparent', 'stroke-width': '14', fill: 'none', cursor: 'pointer',
                }));
                // Outcome label
                if (dep.outcome) {
                    const x1e = srcTask.type === 'condition' ? sp.x + CS * 2 : sp.x + NW;
                    const y1e = sp.y + sp.h / 2;
                    const lmx = (x1e + dp.x) / 2, lmy = (y1e + dp.y + dp.h / 2) / 2 - 8;
                    grp.appendChild(txt(dep.outcome, {
                        x: lmx, y: lmy, fill: color,
                        'font-size': '10', 'text-anchor': 'middle', 'font-family': 'monospace', 'pointer-events': 'none',
                    }));
                }
                // Delete × button (shown on hover via CSS / event)
                const x1d = srcTask.type === 'condition' ? sp.x + CS * 2 : sp.x + NW;
                const y1d = sp.y + sp.h / 2;
                const dmx = (x1d + dp.x) / 2, dmy = (y1d + dp.y + dp.h / 2) / 2;
                const delGrp = el('g', { class: 'edge-del-btn', cursor: 'pointer' });
                delGrp.appendChild(el('circle', { cx: dmx, cy: dmy, r: '9', fill: '#2a1010', stroke: '#f44336', 'stroke-width': '1.5' }));
                delGrp.appendChild(txt('\u00d7', { x: dmx, y: dmy + 4, fill: '#f44336', 'font-size': '13', 'text-anchor': 'middle', 'pointer-events': 'none' }));
                grp.appendChild(delGrp);
                (function (fromKey, toKey, outcome) {
                    delGrp.addEventListener('click', function (e) {
                        e.stopPropagation();
                        const cur = getEffectiveDepsFor(toKey);
                        taskDepsOverride[toKey] = cur.filter(function (d) {
                            return !(d.task_key === fromKey && (d.outcome || '') === outcome);
                        });
                        pendingUpdates['tasks.' + toKey + '.depends_on'] = taskDepsOverride[toKey];
                        markDirtyDirect();
                        redrawEdges();
                    });
                })(dep.task_key, t.task_key, dep.outcome || '');
                edgesGroup.appendChild(grp);
            }
        }
    }
    function drawPorts() {
        if (!portsGroup)
            return;
        portsGroup.innerHTML = '';
        for (const t of getVirtualTasks()) {
            const p = positions[t.task_key];
            if (!p)
                continue;
            var portDefs;
            if (t.type === 'condition') {
                const cx = p.x + CS, cy = p.y + CS;
                portDefs = [
                    { type: 'in', x: p.x, y: cy, color: '#4a9eff', label: '' },
                    { type: 'out-true', x: p.x + CS * 2, y: cy, color: '#4caf50', label: 'T' },
                    { type: 'out-false', x: cx, y: p.y + CS * 2, color: '#f44336', label: 'F' },
                ];
            }
            else {
                portDefs = [
                    { type: 'in', x: p.x, y: p.y + NH / 2, color: '#4a9eff', label: '' },
                    { type: 'out', x: p.x + NW, y: p.y + NH / 2, color: '#4caf50', label: '' },
                ];
            }
            for (var pi = 0; pi < portDefs.length; pi++) {
                var pd = portDefs[pi];
                var isOut = pd.type !== 'in';
                var portGrp = el('g', {
                    class: 'port-grp ' + (isOut ? 'port-out' : 'port-in'),
                    'data-port-task': t.task_key,
                    'data-port-type': pd.type,
                });
                portGrp.appendChild(el('circle', {
                    cx: pd.x, cy: pd.y, r: '7',
                    fill: isOut ? '#1a2e1a' : '#1a2036',
                    stroke: pd.color, 'stroke-width': '1.8',
                }));
                // Wide invisible hit area for easier clicking
                portGrp.appendChild(el('circle', {
                    cx: pd.x, cy: pd.y, r: '12',
                    fill: 'transparent', stroke: 'none',
                }));
                if (pd.label) {
                    portGrp.appendChild(txt(pd.label, {
                        x: pd.x, y: pd.y + 4,
                        fill: pd.color, 'font-size': '8', 'font-weight': 'bold',
                        'text-anchor': 'middle', 'pointer-events': 'none',
                    }));
                }
                portsGroup.appendChild(portGrp);
            }
        }
    }
    // ── Draw ─────────────────────────────────────────────────────────────────────
    function draw(tasks, pos) {
        zg.innerHTML = '';
        const tmap = {};
        for (const t of tasks)
            tmap[t.task_key] = t;
        // Create layer groups (order matters: edges below nodes below ports below overlay)
        edgesGroup = el('g');
        const ng = el('g');
        portsGroup = el('g');
        overlayGroup = el('g');
        // nodes
        for (const t of tasks) {
            const p = pos[t.task_key];
            if (!p)
                continue;
            const c = nodeColors(t.type);
            // Group is positioned via translate so drag only needs to update the transform
            const g = el('g', { 'data-key': t.task_key, cursor: 'pointer', transform: `translate(${p.x},${p.y})` });
            if (t.type === 'condition') {
                const s = CS - 3;
                g.appendChild(el('polygon', {
                    points: `${CS},${CS - s} ${CS + s},${CS} ${CS},${CS + s} ${CS - s},${CS}`,
                    fill: c.fill, stroke: c.stroke, 'stroke-width': '1.5',
                }));
                const opMap = { EQUAL_TO: '==', NOT_EQUAL_TO: '!=', GREATER_THAN: '>', LESS_THAN: '<' };
                g.appendChild(txt(opMap[t.condition?.op] || '?', {
                    x: CS, y: CS + 4, fill: c.accent, 'font-size': '13', 'font-weight': 'bold',
                    'text-anchor': 'middle', 'pointer-events': 'none',
                }));
                g.appendChild(txt(trunc(t.task_key, 22), {
                    x: CS, y: CS * 2 + 14, fill: '#aaa', 'font-size': '10',
                    'text-anchor': 'middle', 'pointer-events': 'none',
                }));
            }
            else {
                g.appendChild(el('rect', {
                    x: 0, y: 0, width: NW, height: NH, rx: '5',
                    fill: c.fill, stroke: c.stroke, 'stroke-width': '1.5',
                    'stroke-dasharray': t._isPending ? '5,3' : null,
                    opacity: t._isPending ? '0.75' : null,
                }));
                const typeLbl = { notebook: 'NOTEBOOK', spark_python: 'SPARK PYTHON', python_wheel: 'PYTHON WHEEL', sql: 'SQL', run_job: 'RUN JOB' };
                g.appendChild(txt(typeLbl[t.type] || '', {
                    x: 8, y: 14, fill: c.accent, 'font-size': '9', 'font-weight': '600',
                    'letter-spacing': '0.05em', 'pointer-events': 'none',
                }));
                g.appendChild(txt(trunc(t.task_key, 26), {
                    x: 8, y: 32, fill: '#e0e0e0', 'font-size': '11.5', 'font-weight': '500',
                    'pointer-events': 'none',
                }));
                if (t.notebook_path) {
                    const parts = t.notebook_path.split('/');
                    g.appendChild(txt(trunc(parts.slice(-2).join('/'), 30), {
                        x: 8, y: 49, fill: '#666', 'font-size': '9.5', 'pointer-events': 'none',
                    }));
                }
                if (t.run_if) {
                    const s = t.run_if === 'AT_LEAST_ONE_SUCCESS' ? '≥1 ok' : t.run_if.replace(/_/g, ' ').toLowerCase();
                    g.appendChild(txt(s, {
                        x: NW - 6, y: 13, fill: '#666', 'font-size': '9',
                        'text-anchor': 'end', 'pointer-events': 'none',
                    }));
                }
            }
            // Mousedown starts potential drag; click confirms select if no move occurred
            g.addEventListener('mousedown', function (e) {
                if (e.target.closest('.port-grp'))
                    return;
                e.stopPropagation();
                const cp = positions[t.task_key];
                draggingTask = { clientX: e.clientX, clientY: e.clientY, origX: cp.x, origY: cp.y, taskKey: t.task_key, moved: false };
            });
            g.addEventListener('click', function (e) {
                e.stopPropagation();
                if (taskDragWasMoved) {
                    taskDragWasMoved = false;
                    return;
                }
                selectTask(t.task_key);
            });
            if (t.type === 'notebook' && t.notebook_path) {
                // Double-click opens the file
                g.addEventListener('dblclick', (e) => {
                    e.stopPropagation();
                    vscodeApi.postMessage({ command: 'openNotebook', notebookPath: t.notebook_path });
                });
                // Open-file indicator at top-right (relative coords since group is translated)
                g.appendChild(txt('↗', {
                    x: NW - 7, y: 13,
                    fill: '#4a9eff', 'font-size': '10', 'text-anchor': 'middle',
                    'pointer-events': 'none', opacity: '0.5',
                }));
                // Native SVG tooltip
                const title = document.createElementNS(NS, 'title');
                title.textContent = 'Double-click to open file';
                g.prepend(title);
            }
            ng.appendChild(g);
        }
        zg.appendChild(edgesGroup);
        zg.appendChild(ng);
        zg.appendChild(portsGroup);
        zg.appendChild(overlayGroup);
        // Populate edges and ports
        redrawEdges();
        drawPorts();
        // Port drag: mousedown on output ports — wired once per draw() so it never duplicates
        portsGroup.addEventListener('mousedown', function (e) {
            var portGrp = e.target.closest('.port-grp');
            if (!portGrp)
                return;
            var portType = portGrp.dataset.portType;
            if (portType === 'in')
                return;
            e.stopPropagation();
            e.preventDefault();
            var taskKey = portGrp.dataset.portTask;
            var pos2 = getPortPos(taskKey, portType);
            if (!pos2)
                return;
            var portColor = portType === 'out-true' ? '#4caf50' : portType === 'out-false' ? '#f44336' : '#546e7a';
            draggingPort = { taskKey: taskKey, portType: portType, startX: pos2.x, startY: pos2.y };
            ghostPath = el('path', {
                stroke: portColor, 'stroke-width': '2', fill: 'none',
                'stroke-dasharray': '7,4', opacity: '0.85', 'pointer-events': 'none',
            });
            overlayGroup.appendChild(ghostPath);
        });
        let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
        for (const p of Object.values(pos)) {
            minX = Math.min(minX, p.x);
            minY = Math.min(minY, p.y);
            maxX = Math.max(maxX, p.x + p.w);
            maxY = Math.max(maxY, p.y + p.h);
        }
        return { minX, minY, maxX, maxY };
    }
    function applyT() { zg.setAttribute('transform', `translate(${tx},${ty}) scale(${sc})`); }
    function fitView(bbox) {
        const w = document.getElementById('canvas-wrap');
        const W = w.clientWidth - 80, H = w.clientHeight - 80;
        const bw = bbox.maxX - bbox.minX || 1, bh = bbox.maxY - bbox.minY || 1;
        sc = Math.min(W / bw, H / bh, 1.4);
        tx = (W - bw * sc) / 2 + 40 - bbox.minX * sc;
        ty = (H - bh * sc) / 2 + 40 - bbox.minY * sc;
        applyT();
    }
    // ── SVG background click → job panel ────────────────────────────────────────
    dagSvg.addEventListener('click', (e) => {
        if (e.target === dagSvg || e.target === zg) {
            deselect();
        }
    });
    // ── Selection ────────────────────────────────────────────────────────────────
    function deselect() {
        if (selectedKey) {
            const n = zg.querySelector(`[data-key="${selectedKey}"]`);
            if (n) {
                const shape = n.querySelector('rect') || n.querySelector('polygon');
                if (shape) {
                    shape.setAttribute('stroke-width', '1.5');
                    shape.setAttribute('stroke', '#d4d4d4');
                }
            }
        }
        selectedKey = null;
        closeBottomBar();
        if (sidebarMode !== 'job') {
            // Save spark conf if cluster editor was open
            if (sidebarMode === 'clusterEditor' && clusterEditorKey) {
                _saveClusterEditorKVToPending();
            }
            sidebarMode = 'job';
            libEditorTaskKey = null;
            clusterEditorKey = null;
            const breadcrumb = document.getElementById('sidebar-breadcrumb');
            if (breadcrumb)
                breadcrumb.textContent = 'Job';
        }
        showJobPanel();
    }
    function selectTask(key) {
        if (selectedKey === key) {
            deselect();
            return;
        }
        if (selectedKey) {
            const n = zg.querySelector(`[data-key="${selectedKey}"]`);
            if (n) {
                const shape = n.querySelector('rect') || n.querySelector('polygon');
                if (shape) {
                    shape.setAttribute('stroke-width', '1.5');
                    shape.setAttribute('stroke', '#d4d4d4');
                }
            }
        }
        selectedKey = key;
        const n = zg.querySelector(`[data-key="${key}"]`);
        if (n) {
            const shape = n.querySelector('rect') || n.querySelector('polygon');
            if (shape) {
                shape.setAttribute('stroke-width', '3');
                shape.setAttribute('stroke', '#4a9eff');
            }
        }
        const task = job.tasks.find(t => t.task_key === key);
        if (task)
            showTaskPanel(task);
        openBottomBar();
    }
    // ── HTML helpers ─────────────────────────────────────────────────────────────
    function esc(s) {
        return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }
    function section(title, bodyHtml, open = false, count = null) {
        const countBadge = count !== null ? `<span class="sec-count">${count}</span>` : '';
        return `<details class="section"${open ? ' open' : ''}>
      <summary><span class="sec-title">${esc(title)}</span>${countBadge}</summary>
      <div class="section-body">${bodyHtml}</div>
    </details>`;
    }
    function prop(label, valueHtml) {
        return `<div class="prop"><div class="prop-label">${esc(label)}</div><div class="prop-value">${valueHtml}</div></div>`;
    }
    // ── Dirty tracking & save ─────────────────────────────────────────────────────
    const saveBtn = document.getElementById('btn-save');
    const revertBtn = document.getElementById('btn-revert');
    const pendingUpdates = {}; // flat map: "path.to.field" → value
    let isDirty = false;
    window.markDirty = function (input) {
        const path = input.dataset.field;
        let val;
        if (input.type === 'checkbox')
            val = input.checked;
        else if (input.type === 'number')
            val = input.value !== '' ? Number(input.value) : null;
        else
            val = input.value;
        pendingUpdates[path] = val;
        isDirty = true;
        saveBtn.textContent = 'Save \u25cf';
        saveBtn.classList.add('dirty');
        saveBtn.classList.remove('saved');
        revertBtn.style.display = '';
    };
    function markDirtyDirect() {
        isDirty = true;
        saveBtn.textContent = 'Save \u25cf';
        saveBtn.classList.add('dirty');
        saveBtn.classList.remove('saved');
        revertBtn.style.display = '';
    }
    function markClean(savedOk) {
        isDirty = false;
        saveBtn.classList.remove('dirty');
        revertBtn.style.display = 'none';
        if (savedOk) {
            saveBtn.textContent = 'Saved \u2713';
            saveBtn.classList.add('saved');
            setTimeout(() => { if (!isDirty) {
                saveBtn.textContent = 'Save';
                saveBtn.classList.remove('saved');
            } }, 2000);
        }
        else {
            saveBtn.textContent = 'Save';
        }
    }
    saveBtn.addEventListener('click', () => {
        if (!isDirty)
            return;
        vscodeApi.postMessage({ command: 'saveJob', filePath: job.filePath, jobName: job.name, updates: buildUpdates() });
        markClean(true);
    });
    revertBtn.addEventListener('click', function () {
        if (!isDirty)
            return;
        // Reset all pending state
        for (const k of Object.keys(pendingUpdates))
            delete pendingUpdates[k];
        for (const k of Object.keys(taskDepsOverride))
            delete taskDepsOverride[k];
        pendingNewClusters = [];
        pendingRemovedClusterKeys = [];
        pendingNewEnvs = [];
        pendingRemovedEnvKeys = [];
        clusterEditorKey = null;
        clusterEditorWorkerMode = 'fixed';
        pendingNewTasks.length = 0;
        libEditorTaskKey = null;
        libEditorLibs = [];
        sidebarMode = 'job';
        const bc = document.getElementById('sidebar-breadcrumb');
        if (bc)
            bc.textContent = 'Job';
        hdrNameInput.value = job.name;
        markClean(false);
        // Redraw from scratch with original layout
        positions = computeLayout(job.tasks);
        _bbox = draw(job.tasks, positions);
        document.getElementById('hdr-tasks').textContent =
            job.tasks.length + ' task' + (job.tasks.length !== 1 ? 's' : '');
        requestAnimationFrame(function () { if (_bbox)
            fitView(_bbox); });
        selectedKey = null;
        closeBottomBar();
        showJobPanel();
    });
    window.addEventListener('message', evt => {
        const msg = evt.data;
        if (msg.type === 'saveError') {
            saveBtn.textContent = 'Save';
            saveBtn.classList.remove('dirty', 'saved');
            isDirty = true;
        }
        if (msg.type === 'saveDone' && msg.updatedJob) {
            // Replace job data and reset all pending state
            job = msg.updatedJob;
            pendingNewTasks.length = 0;
            for (const k of Object.keys(pendingUpdates))
                delete pendingUpdates[k];
            for (const k of Object.keys(taskDepsOverride))
                delete taskDepsOverride[k];
            pendingNewClusters = [];
            pendingRemovedClusterKeys = [];
            pendingNewEnvs = [];
            pendingRemovedEnvKeys = [];
            clusterEditorKey = null;
            clusterEditorWorkerMode = 'fixed';
            libEditorTaskKey = null;
            libEditorLibs = [];
            sidebarMode = 'job';
            const bc = document.getElementById('sidebar-breadcrumb');
            if (bc)
                bc.textContent = 'Job';
            // Redraw DAG
            positions = computeLayout(job.tasks);
            _bbox = draw(job.tasks, positions);
            requestAnimationFrame(function () { if (_bbox)
                fitView(_bbox); });
            // Update header
            hdrNameInput.value = job.name;
            document.getElementById('hdr-tasks').textContent = job.tasks.length + ' task' + (job.tasks.length !== 1 ? 's' : '');
            const trigEl = document.getElementById('hdr-trigger');
            if (job.trigger && job.trigger.type !== 'manual') {
                const lbl = { table_update: 'Table trigger', periodic: 'Scheduled', file_arrival: 'File arrival', manual: 'Manual' };
                trigEl.textContent = lbl[job.trigger.type] || job.trigger.type;
                trigEl.style.display = '';
            }
            else {
                trigEl.style.display = 'none';
            }
            // Deselect and show job panel
            selectedKey = null;
            closeBottomBar();
            showJobPanel();
        }
        // Immediate task add/remove — partial refresh, keeps other pending edits intact
        if (msg.type === 'jobChanged' && msg.updatedJob) {
            job = msg.updatedJob;
            pendingNewTasks.length = 0;
            // Clean up state for tasks that no longer exist
            const validKeys = new Set(job.tasks.map(function (t) { return t.task_key; }));
            for (const k of Object.keys(positions)) {
                if (!validKeys.has(k)) {
                    delete positions[k];
                }
            }
            for (const k of Object.keys(taskDepsOverride)) {
                if (!validKeys.has(k)) {
                    delete taskDepsOverride[k];
                }
            }
            // Also clean up pendingUpdates for removed tasks
            for (const k of Object.keys(pendingUpdates)) {
                const taskKey = k.startsWith('tasks.') ? k.split('.')[1] : null;
                if (taskKey && !validKeys.has(taskKey)) {
                    delete pendingUpdates[k];
                }
            }
            _bbox = draw(getVirtualTasks(), positions);
            document.getElementById('hdr-tasks').textContent =
                job.tasks.length + ' task' + (job.tasks.length !== 1 ? 's' : '');
            if (selectedKey && !validKeys.has(selectedKey)) {
                selectedKey = null;
                closeBottomBar();
                showJobPanel();
            }
        }
    });
    function setPath(obj, keys, value) {
        let cur = obj;
        for (let i = 0; i < keys.length - 1; i++) {
            const k = keys[i];
            if (cur[k] === undefined || cur[k] === null)
                cur[k] = isNaN(Number(keys[i + 1])) ? {} : [];
            cur = cur[k];
        }
        cur[keys[keys.length - 1]] = value;
    }
    function buildUpdates() {
        const updates = {};
        const renamedTasks = {};
        for (const [path, val] of Object.entries(pendingUpdates)) {
            if (path.startsWith('_renamedTasks.')) {
                renamedTasks[path.slice('_renamedTasks.'.length)] = val;
            }
            else {
                setPath(updates, path.split('.'), val);
            }
        }
        if (Object.keys(renamedTasks).length > 0) {
            updates._renamedTasks = renamedTasks;
        }
        if (pendingNewTasks.length > 0) {
            updates._newTasks = pendingNewTasks.map(pendingTaskToRaw);
        }
        return updates;
    }
    function getVal(path, defaultVal) {
        return path in pendingUpdates ? pendingUpdates[path] : defaultVal;
    }
    // ── Sidebar mode ─────────────────────────────────────────────────────────────
    let sidebarMode = 'job'; // 'job' | 'libEditor' | 'clusterEditor'
    let libEditorTaskKey = null;
    let libEditorLibs = [];
    let clusterEditorKey = null;
    let clusterEditorWorkerMode = 'fixed'; // 'fixed' | 'autoscale' | 'single_node'
    let clusterEditorSparkConf = []; // [{k:'', v:''}]
    let clusterEditorSparkEnvVars = []; // [{k:'', v:''}]
    let pendingNewClusters = [];
    let pendingRemovedClusterKeys = [];
    let pendingNewEnvs = [];
    let pendingRemovedEnvKeys = [];
    // ── Pending new tasks ────────────────────────────────────────────────────────
    let pendingNewTasks = []; // array of task-like objects with _isPending:true
    function getVirtualTasks() {
        return job.tasks.concat(pendingNewTasks);
    }
    function pendingTaskToRaw(t) {
        var raw = { task_key: t.task_key };
        var deps = getEffectiveDepsFor(t.task_key);
        if (deps.length > 0) {
            raw.depends_on = deps.map(function (d) {
                return d.outcome ? { task_key: d.task_key, outcome: d.outcome } : { task_key: d.task_key };
            });
        }
        if (t.type === 'notebook') {
            raw.notebook_task = { notebook_path: t.notebook_path || '' };
        }
        else if (t.type === 'run_job') {
            raw.run_job_task = { job_name: t._run_job_name || '' };
        }
        else if (t.type === 'condition') {
            var c = t.condition || {};
            raw.condition_task = { op: c.op || 'EQUAL_TO', left: c.left || '', right: c.right || '' };
        }
        return raw;
    }
    // ── Task drag state ──────────────────────────────────────────────────────────
    let draggingTask = null; // { clientX, clientY, origX, origY, taskKey, moved }
    let taskDragWasMoved = false; // prevents click-select after a drag
    // ── Edge-editing state ───────────────────────────────────────────────────────
    let taskDepsOverride = {}; // { taskKey: [{task_key, outcome?}] }
    let draggingPort = null; // { taskKey, portType, startX, startY }
    let ghostPath = null; // SVG element
    let edgesGroup = null; // persistent SVG group for edges
    let portsGroup = null; // persistent SVG group for port circles
    let overlayGroup = null; // persistent SVG group for ghost arrow
    // ── Library modal ─────────────────────────────────────────────────────────────
    let modalTaskKey = null;
    let modalLibs = [];
    const LIB_COLORS = { whl: '#4a9eff', jar: '#ffb74d', pypi: '#4caf50', maven: '#ce93d8', requirements: '#4dd0e1' };
    function libDesc(lib) {
        if (lib.type === 'whl' || lib.type === 'jar' || lib.type === 'requirements')
            return lib.path || '';
        if (lib.type === 'pypi')
            return lib.package + (lib.repo ? `<div class="lib-sub">repo: ${esc(lib.repo)}</div>` : '');
        if (lib.type === 'maven') {
            let d = lib.coordinates || '';
            if (lib.repo)
                d += `<div class="lib-sub">repo: ${esc(lib.repo)}</div>`;
            if (lib.exclusions?.length)
                d += `<div class="lib-sub">excl: ${esc(lib.exclusions.join(', '))}</div>`;
            return d;
        }
        return JSON.stringify(lib);
    }
    function renderModalList() {
        const listEl = document.getElementById('lib-list');
        if (modalLibs.length === 0) {
            listEl.innerHTML = `<div style="padding:14px;color:var(--fg-dim);font-style:italic;font-size:11px">No libraries — use the form below to add one.</div>`;
            return;
        }
        listEl.innerHTML = modalLibs.map((lib, i) => {
            const color = LIB_COLORS[lib.type] || '#888';
            return `<div class="lib-row">
        <span class="lib-type-badge" style="color:${color};background:${color}22;border:1px solid ${color}44">${lib.type.toUpperCase()}</span>
        <span class="lib-desc">${libDesc(lib)}</span>
        <button class="lib-remove-btn" onclick="removeLib(${i})" title="Remove">&times;</button>
      </div>`;
        }).join('');
    }
    window.updateLibForm = function () {
        const type = document.getElementById('lib-add-type').value;
        const isPath = type === 'whl' || type === 'jar' || type === 'requirements';
        const isPypi = type === 'pypi';
        const isMaven = type === 'maven';
        document.getElementById('lib-form-path').style.display = isPath ? '' : 'none';
        document.getElementById('lib-form-pypi-pkg').style.display = isPypi ? '' : 'none';
        document.getElementById('lib-form-maven-coords').style.display = isMaven ? '' : 'none';
        const hasOpts = isPypi || isMaven;
        document.getElementById('lib-form-opts').style.display = hasOpts ? 'flex' : 'none';
        document.getElementById('lib-form-repo').style.display = hasOpts ? '' : 'none';
        document.getElementById('lib-form-excl').style.display = isMaven ? '' : 'none';
        // Update path placeholder
        if (isPath) {
            const ph = { whl: '/Volumes/…/lib.whl', jar: '/Volumes/…/lib.jar', requirements: './requirements.txt' };
            document.getElementById('lib-path').placeholder = ph[type] || '';
        }
    };
    window.removeLib = function (i) {
        modalLibs.splice(i, 1);
        renderModalList();
    };
    window.addLib = function () {
        const type = document.getElementById('lib-add-type').value;
        let lib = null;
        if (type === 'whl' || type === 'jar' || type === 'requirements') {
            const path = document.getElementById('lib-path').value.trim();
            if (!path) {
                document.getElementById('lib-path').focus();
                return;
            }
            lib = { type, path };
            document.getElementById('lib-path').value = '';
        }
        else if (type === 'pypi') {
            const pkg = document.getElementById('lib-pypi-pkg').value.trim();
            if (!pkg) {
                document.getElementById('lib-pypi-pkg').focus();
                return;
            }
            const repo = document.getElementById('lib-repo').value.trim();
            lib = { type: 'pypi', package: pkg, ...(repo ? { repo } : {}) };
            document.getElementById('lib-pypi-pkg').value = '';
            document.getElementById('lib-repo').value = '';
        }
        else if (type === 'maven') {
            const coords = document.getElementById('lib-maven-coords').value.trim();
            if (!coords) {
                document.getElementById('lib-maven-coords').focus();
                return;
            }
            const repo = document.getElementById('lib-repo').value.trim();
            const exclRaw = document.getElementById('lib-excl').value.trim();
            const excl = exclRaw ? exclRaw.split('\\n').map(s => s.trim()).filter(Boolean) : [];
            lib = { type: 'maven', coordinates: coords, ...(repo ? { repo } : {}), ...(excl.length ? { exclusions: excl } : {}) };
            document.getElementById('lib-maven-coords').value = '';
            document.getElementById('lib-repo').value = '';
            document.getElementById('lib-excl').value = '';
        }
        if (lib) {
            modalLibs.push(lib);
            renderModalList();
        }
    };
    window.openLibraryModal = function (taskKey) {
        const backdrop = document.getElementById('lib-backdrop');
        if (!backdrop) {
            console.error('lib-backdrop not found');
            return;
        }
        modalTaskKey = taskKey;
        const libPath = `tasks.${taskKey}.libraries`;
        const src = libPath in pendingUpdates
            ? pendingUpdates[libPath]
            : (job.tasks.find(t => t.task_key === taskKey)?.libraries || []);
        modalLibs = src.map(function (l) {
            return Object.assign({}, l, { exclusions: l.exclusions ? l.exclusions.slice() : undefined });
        });
        document.getElementById('lib-modal-title').textContent = `Libraries \u2014 ${taskKey}`;
        document.getElementById('lib-add-type').value = 'whl';
        updateLibForm();
        renderModalList();
        backdrop.classList.add('open');
    };
    window.closeAndSaveModal = function () {
        const libPath = `tasks.${modalTaskKey}.libraries`;
        pendingUpdates[libPath] = modalLibs.slice();
        isDirty = true;
        saveBtn.textContent = 'Save \u25cf';
        saveBtn.classList.add('dirty');
        saveBtn.classList.remove('saved');
        document.getElementById('lib-backdrop').classList.remove('open');
        // Re-render task panel to reflect updated libraries
        if (selectedKey === modalTaskKey) {
            const task = job.tasks.find(t => t.task_key === modalTaskKey);
            if (task)
                showTaskPanel(task);
        }
    };
    // Close modal on backdrop click
    document.getElementById('lib-backdrop').addEventListener('click', (e) => {
        if (e.target === document.getElementById('lib-backdrop'))
            closeAndSaveModal();
    });
    // ── Library editor in sidebar ─────────────────────────────────────────────────
    function openLibraryEditor(taskKey) {
        libEditorTaskKey = taskKey;
        const libPath = `tasks.${taskKey}.libraries`;
        const src = libPath in pendingUpdates
            ? pendingUpdates[libPath]
            : (job.tasks.find(t => t.task_key === taskKey)?.libraries || []);
        libEditorLibs = src.map(function (l) {
            return Object.assign({}, l, { exclusions: l.exclusions ? l.exclusions.slice() : undefined });
        });
        sidebarMode = 'libEditor';
        const breadcrumb = document.getElementById('sidebar-breadcrumb');
        if (breadcrumb) {
            breadcrumb.innerHTML =
                `<span class="breadcrumb-link" id="bc-lib-job">Job</span> \u203a <span>${esc(taskKey)}</span>`;
            document.getElementById('bc-lib-job')?.addEventListener('click', closeLibraryEditor);
        }
        if (!sidebarOpen)
            toggleSidebar(true);
        renderLibEditorContent();
    }
    function closeLibraryEditor() {
        const libPath = `tasks.${libEditorTaskKey}.libraries`;
        pendingUpdates[libPath] = libEditorLibs.slice();
        isDirty = true;
        saveBtn.textContent = 'Save \u25cf';
        saveBtn.classList.add('dirty');
        saveBtn.classList.remove('saved');
        sidebarMode = 'job';
        const breadcrumb = document.getElementById('sidebar-breadcrumb');
        breadcrumb.textContent = 'Job';
        showJobPanel();
        if (selectedKey) {
            const task = job.tasks.find(t => t.task_key === selectedKey);
            if (task)
                showTaskPanel(task);
        }
    }
    window.closeLibraryEditor = closeLibraryEditor;
    // ── Cluster editor in sidebar ───────────────────────────────────────────────────────
    function openClusterEditor(key) {
        clusterEditorKey = key;
        const c = job.jobClusters[key] || {};
        // Build spark conf array from current pending or original
        const scRaw = (`jobClusters.${key}.sparkConf` in pendingUpdates)
            ? pendingUpdates[`jobClusters.${key}.sparkConf`]
            : (c.sparkConf || {});
        clusterEditorSparkConf = Object.keys(scRaw).map(function (k2) { return { k: k2, v: String(scRaw[k2]) }; });
        const seRaw = (`jobClusters.${key}.sparkEnvVars` in pendingUpdates)
            ? pendingUpdates[`jobClusters.${key}.sparkEnvVars`]
            : (c.sparkEnvVars || {});
        clusterEditorSparkEnvVars = Object.keys(seRaw).map(function (k2) { return { k: k2, v: String(seRaw[k2]) }; });
        // Determine worker mode
        const _numW = (`jobClusters.${key}.numWorkers` in pendingUpdates) ? pendingUpdates[`jobClusters.${key}.numWorkers`] : c.numWorkers;
        const _minW = (`jobClusters.${key}.minWorkers` in pendingUpdates) ? pendingUpdates[`jobClusters.${key}.minWorkers`] : c.minWorkers;
        const _maxW = (`jobClusters.${key}.maxWorkers` in pendingUpdates) ? pendingUpdates[`jobClusters.${key}.maxWorkers`] : c.maxWorkers;
        if (Number(_numW) === 0)
            clusterEditorWorkerMode = 'single_node';
        else if (_minW != null || _maxW != null)
            clusterEditorWorkerMode = 'autoscale';
        else
            clusterEditorWorkerMode = 'fixed';
        sidebarMode = 'clusterEditor';
        const breadcrumb = document.getElementById('sidebar-breadcrumb');
        if (breadcrumb) {
            breadcrumb.innerHTML = `<span class="breadcrumb-link" id="bc-cluster-job">Job</span> \u203a <span>${esc(key)}</span>`;
            document.getElementById('bc-cluster-job')?.addEventListener('click', closeClusterEditor);
        }
        if (!sidebarOpen)
            toggleSidebar(true);
        renderClusterEditorContent();
    }
    function _saveClusterEditorKVToPending() {
        if (!clusterEditorKey)
            return;
        const scObj = {};
        clusterEditorSparkConf.forEach(function (row) { if (row.k.trim())
            scObj[row.k.trim()] = row.v; });
        pendingUpdates[`jobClusters.${clusterEditorKey}.sparkConf`] = scObj;
        const seObj = {};
        clusterEditorSparkEnvVars.forEach(function (row) { if (row.k.trim())
            seObj[row.k.trim()] = row.v; });
        pendingUpdates[`jobClusters.${clusterEditorKey}.sparkEnvVars`] = seObj;
    }
    function closeClusterEditor() {
        _saveClusterEditorKVToPending();
        markDirtyDirect();
        sidebarMode = 'job';
        clusterEditorKey = null;
        const breadcrumb = document.getElementById('sidebar-breadcrumb');
        if (breadcrumb)
            breadcrumb.textContent = 'Job';
        showJobPanel();
    }
    window.closeClusterEditor = closeClusterEditor;
    function renderClusterEditorContent() {
        const key = clusterEditorKey;
        const c = job.jobClusters[key] || {};
        // clusterEditorWorkerMode is tracked in module-level state
        const scRows = clusterEditorSparkConf.map(function (row, i) {
            return `<div class="kv-row">
        <input class="prop-input kv-key" type="text" data-kv-sc-key="${i}" value="${esc(row.k)}" placeholder="key">
        <input class="prop-input kv-val" type="text" data-kv-sc-val="${i}" value="${esc(row.v)}" placeholder="value">
        <button class="kv-remove-btn" data-kv-sc-remove="${i}">&times;</button>
      </div>`;
        }).join('');
        const seRows = clusterEditorSparkEnvVars.map(function (row, i) {
            return `<div class="kv-row">
        <input class="prop-input kv-key" type="text" data-kv-se-key="${i}" value="${esc(row.k)}" placeholder="key">
        <input class="prop-input kv-val" type="text" data-kv-se-val="${i}" value="${esc(row.v)}" placeholder="value">
        <button class="kv-remove-btn" data-kv-se-remove="${i}">&times;</button>
      </div>`;
        }).join('');
        const configBody =
            propEdit('Node type ID', getVal(`jobClusters.${key}.nodeTypeId`, c.nodeTypeId || ''), `jobClusters.${key}.nodeTypeId`) +
            propEdit('Driver node type ID', getVal(`jobClusters.${key}.driverNodeTypeId`, c.driverNodeTypeId || ''), `jobClusters.${key}.driverNodeTypeId`) +
            propEdit('Spark version', getVal(`jobClusters.${key}.sparkVersion`, c.sparkVersion || ''), `jobClusters.${key}.sparkVersion`) +
            propEdit('Policy ID', getVal(`jobClusters.${key}.policyId`, c.policyId || ''), `jobClusters.${key}.policyId`) +
            propEdit('Instance pool ID', getVal(`jobClusters.${key}.instancePoolId`, c.instancePoolId || ''), `jobClusters.${key}.instancePoolId`) +
            propEdit('Autotermination (min)', getVal(`jobClusters.${key}.autoterminationMinutes`, c.autoterminationMinutes ?? ''), `jobClusters.${key}.autoterminationMinutes`, 'number') +
            propEdit('Enable elastic disk', getVal(`jobClusters.${key}.enableElasticDisk`, c.enableElasticDisk || false), `jobClusters.${key}.enableElasticDisk`, 'checkbox');
        const securityBody =
            propSelect('Data security mode', getVal(`jobClusters.${key}.dataSecurityMode`, c.dataSecurityMode || ''), `jobClusters.${key}.dataSecurityMode`, [
                { v: '', l: '(default)' },
                { v: 'SINGLE_USER', l: 'Single user' },
                { v: 'USER_ISOLATION', l: 'User isolation' },
                { v: 'NONE', l: 'None' },
                { v: 'LEGACY_SINGLE_USER', l: 'Legacy single user' },
                { v: 'LEGACY_TABLE_ACL', l: 'Legacy table ACL' },
            ]) +
            propSelect('Runtime engine', getVal(`jobClusters.${key}.runtimeEngine`, c.runtimeEngine || ''), `jobClusters.${key}.runtimeEngine`, [{ v: '', l: '(default)' }, { v: 'STANDARD', l: 'Standard' }, { v: 'PHOTON', l: 'Photon' }]) +
            propEdit('Single user name', getVal(`jobClusters.${key}.singleUserName`, c.singleUserName || ''), `jobClusters.${key}.singleUserName`);
        const workersBody = `<div class="prop">
          <div class="prop-label">Worker Mode</div>
          <select class="prop-input" id="cluster-worker-mode-select">
            <option value="fixed"${clusterEditorWorkerMode === 'fixed' ? ' selected' : ''}>Fixed number</option>
            <option value="autoscale"${clusterEditorWorkerMode === 'autoscale' ? ' selected' : ''}>Autoscale (min / max)</option>
            <option value="single_node"${clusterEditorWorkerMode === 'single_node' ? ' selected' : ''}>Single node (driver only)</option>
          </select>
        </div>` + (clusterEditorWorkerMode === 'autoscale'
            ? propEdit('Min workers', getVal(`jobClusters.${key}.minWorkers`, c.minWorkers ?? ''), `jobClusters.${key}.minWorkers`, 'number') +
                propEdit('Max workers', getVal(`jobClusters.${key}.maxWorkers`, c.maxWorkers ?? ''), `jobClusters.${key}.maxWorkers`, 'number')
            : clusterEditorWorkerMode === 'fixed'
                ? propEdit('Num workers', getVal(`jobClusters.${key}.numWorkers`, c.numWorkers ?? 1), `jobClusters.${key}.numWorkers`, 'number')
                : '<div class="prop"><span class="prop-value" style="color:var(--fg-dim);font-size:11px;font-style:italic">Driver-only cluster — no worker nodes allocated.</span></div>');
        const scBody = `<div id="sc-rows">${scRows}</div>
          <button class="section-add-btn" id="btn-add-sc" style="margin-top:6px">+ Add Entry</button>`;
        const seBody = `<div id="se-rows">${seRows}</div>
          <button class="section-add-btn" id="btn-add-se" style="margin-top:6px">+ Add Entry</button>`;
        panelContent.innerHTML = `
      <div class="panel-header">
        <div class="panel-title">${esc(key)}</div>
        <div class="panel-subtitle">Cluster Configuration</div>
      </div>
      ${section('Configuration', configBody, true)}
      ${section('Security', securityBody, false)}
      ${section('Workers', workersBody, true)}
      ${section('Spark Config', scBody, clusterEditorSparkConf.length > 0, clusterEditorSparkConf.length || null)}
      ${section('Spark Env Vars', seBody, clusterEditorSparkEnvVars.length > 0, clusterEditorSparkEnvVars.length || null)}
      <div class="section-body" style="border-top:1px solid var(--border)">
        <button class="lib-modal-done" id="btn-cluster-done" style="width:100%">Done</button>
      </div>`;
        // Wire events
        document.getElementById('btn-cluster-done')?.addEventListener('click', closeClusterEditor);
        document.getElementById('cluster-worker-mode-select')?.addEventListener('change', function () {
            clusterEditorWorkerMode = this.value;
            if (clusterEditorWorkerMode === 'single_node') {
                pendingUpdates[`jobClusters.${key}.numWorkers`] = 0;
                delete pendingUpdates[`jobClusters.${key}.minWorkers`];
                delete pendingUpdates[`jobClusters.${key}.maxWorkers`];
            }
            else if (clusterEditorWorkerMode === 'autoscale') {
                delete pendingUpdates[`jobClusters.${key}.numWorkers`];
            }
            else {
                delete pendingUpdates[`jobClusters.${key}.minWorkers`];
                delete pendingUpdates[`jobClusters.${key}.maxWorkers`];
            }
            markDirtyDirect();
            renderClusterEditorContent();
        });
        document.getElementById('btn-add-sc')?.addEventListener('click', function () {
            clusterEditorSparkConf.push({ k: '', v: '' });
            renderClusterEditorContent();
        });
        document.getElementById('btn-add-se')?.addEventListener('click', function () {
            clusterEditorSparkEnvVars.push({ k: '', v: '' });
            renderClusterEditorContent();
        });
        // KV input wiring
        panelContent.querySelectorAll('[data-kv-sc-key]').forEach(function (inp) {
            inp.addEventListener('input', function () {
                const i = parseInt(inp.dataset.kvScKey, 10);
                clusterEditorSparkConf[i].k = inp.value;
            });
        });
        panelContent.querySelectorAll('[data-kv-sc-val]').forEach(function (inp) {
            inp.addEventListener('input', function () {
                const i = parseInt(inp.dataset.kvScVal, 10);
                clusterEditorSparkConf[i].v = inp.value;
            });
        });
        panelContent.querySelectorAll('[data-kv-sc-remove]').forEach(function (btn) {
            btn.addEventListener('click', function () {
                const i = parseInt(btn.dataset.kvScRemove, 10);
                clusterEditorSparkConf.splice(i, 1);
                renderClusterEditorContent();
            });
        });
        panelContent.querySelectorAll('[data-kv-se-key]').forEach(function (inp) {
            inp.addEventListener('input', function () {
                const i = parseInt(inp.dataset.kvSeKey, 10);
                clusterEditorSparkEnvVars[i].k = inp.value;
            });
        });
        panelContent.querySelectorAll('[data-kv-se-val]').forEach(function (inp) {
            inp.addEventListener('input', function () {
                const i = parseInt(inp.dataset.kvSeVal, 10);
                clusterEditorSparkEnvVars[i].v = inp.value;
            });
        });
        panelContent.querySelectorAll('[data-kv-se-remove]').forEach(function (btn) {
            btn.addEventListener('click', function () {
                const i = parseInt(btn.dataset.kvSeRemove, 10);
                clusterEditorSparkEnvVars.splice(i, 1);
                renderClusterEditorContent();
            });
        });
    }
    function renderLibEditorContent() {
        const listHtml = libEditorLibs.length > 0
            ? libEditorLibs.map(function (lib, i) {
                const color = LIB_COLORS[lib.type] || '#888';
                return `<div class="lib-row" data-lib-index="${i}">
            <span class="lib-type-badge" style="color:${color};background:${color}22;border:1px solid ${color}44">${lib.type.toUpperCase()}</span>
            <span class="lib-desc">${libDesc(lib)}</span>
            <button class="lib-remove-btn" data-lib-remove="${i}" title="Remove">&times;</button>
          </div>`;
            }).join('')
            : `<div style="padding:14px;color:var(--fg-dim);font-style:italic;font-size:11px">No libraries — use the form below to add one.</div>`;
        panelContent.innerHTML = `
      <div class="panel-header">
        <div class="panel-title">${esc(libEditorTaskKey)}</div>
        <div class="panel-subtitle">Libraries</div>
      </div>
      <div class="lib-list" id="lib-ed-list">${listHtml}</div>
      <div class="lib-add-form">
        <div class="lib-add-row" style="flex-wrap:wrap">
          <div style="flex:0 0 auto">
            <label>Type</label>
            <select class="prop-input" id="lib-ed-type" style="width:160px">
              <option value="whl">whl \u2014 Python wheel</option>
              <option value="jar">jar \u2014 JAR file</option>
              <option value="pypi">pypi \u2014 PyPI package</option>
              <option value="maven">maven \u2014 Maven package</option>
              <option value="requirements">requirements \u2014 requirements.txt</option>
            </select>
          </div>
          <div id="lib-ed-form-path" style="flex:1;min-width:140px">
            <label>Path</label>
            <input id="lib-ed-path" class="prop-input" type="text" placeholder="/Volumes/\u2026/lib.whl">
          </div>
          <div id="lib-ed-form-pypi-pkg" style="flex:1;min-width:140px;display:none">
            <label>Package</label>
            <input id="lib-ed-pypi-pkg" class="prop-input" type="text" placeholder="numpy==1.25.2">
          </div>
          <div id="lib-ed-form-maven-coords" style="flex:1;min-width:140px;display:none">
            <label>Coordinates</label>
            <input id="lib-ed-maven-coords" class="prop-input" type="text" placeholder="com.example:lib:1.0">
          </div>
        </div>
        <div id="lib-ed-form-opts" style="display:none;flex-direction:column;gap:6px">
          <div id="lib-ed-form-repo" style="display:none">
            <label>Repo (optional)</label>
            <input id="lib-ed-repo" class="prop-input" type="text" placeholder="https://pypi.org/simple/">
          </div>
          <div id="lib-ed-form-excl" style="display:none">
            <label>Exclusions (optional \u2014 one per line)</label>
            <textarea id="lib-ed-excl" class="prop-input" rows="2" style="resize:vertical;font-family:var(--mono);font-size:10px"></textarea>
          </div>
        </div>
        <div style="display:flex;gap:8px;margin-top:6px">
          <button class="lib-add-btn" id="btn-lib-ed-add" style="flex:1">+ Add Library</button>
          <button class="lib-modal-done" id="btn-lib-ed-done">Done</button>
        </div>
      </div>`;
        // Wire up events via addEventListener (no inline onclick)
        document.getElementById('lib-ed-type')?.addEventListener('change', libEdUpdateForm);
        document.getElementById('btn-lib-ed-add')?.addEventListener('click', libEdAdd);
        document.getElementById('btn-lib-ed-done')?.addEventListener('click', closeLibraryEditor);
        document.getElementById('lib-ed-list')?.addEventListener('click', function (e) {
            const btn = e.target.closest('[data-lib-remove]');
            if (btn) {
                const idx = parseInt(btn.dataset.libRemove || '0', 10);
                libEditorLibs.splice(idx, 1);
                renderLibEditorContent();
            }
        });
    }
    function libEdUpdateForm() {
        const typeEl = document.getElementById('lib-ed-type');
        if (!typeEl)
            return;
        const type = typeEl.value;
        const isPath = type === 'whl' || type === 'jar' || type === 'requirements';
        const isPypi = type === 'pypi';
        const isMaven = type === 'maven';
        document.getElementById('lib-ed-form-path').style.display = isPath ? '' : 'none';
        document.getElementById('lib-ed-form-pypi-pkg').style.display = isPypi ? '' : 'none';
        document.getElementById('lib-ed-form-maven-coords').style.display = isMaven ? '' : 'none';
        const hasOpts = isPypi || isMaven;
        document.getElementById('lib-ed-form-opts').style.display = hasOpts ? 'flex' : 'none';
        document.getElementById('lib-ed-form-repo').style.display = hasOpts ? '' : 'none';
        document.getElementById('lib-ed-form-excl').style.display = isMaven ? '' : 'none';
        if (isPath) {
            const ph = { whl: '/Volumes/\u2026/lib.whl', jar: '/Volumes/\u2026/lib.jar', requirements: './requirements.txt' };
            document.getElementById('lib-ed-path').placeholder = ph[type] || '';
        }
    }
    function libEdAdd() {
        const typeEl = document.getElementById('lib-ed-type');
        if (!typeEl)
            return;
        const type = typeEl.value;
        let lib = null;
        if (type === 'whl' || type === 'jar' || type === 'requirements') {
            const pathEl = document.getElementById('lib-ed-path');
            const path = pathEl.value.trim();
            if (!path) {
                pathEl.focus();
                return;
            }
            lib = { type, path };
            pathEl.value = '';
        }
        else if (type === 'pypi') {
            const pkgEl = document.getElementById('lib-ed-pypi-pkg');
            const repoEl = document.getElementById('lib-ed-repo');
            const pkg = pkgEl.value.trim();
            if (!pkg) {
                pkgEl.focus();
                return;
            }
            const repo = repoEl.value.trim();
            lib = Object.assign({ type: 'pypi', package: pkg }, repo ? { repo } : {});
            pkgEl.value = '';
            repoEl.value = '';
        }
        else if (type === 'maven') {
            const coordsEl = document.getElementById('lib-ed-maven-coords');
            const repoEl = document.getElementById('lib-ed-repo');
            const exclEl = document.getElementById('lib-ed-excl');
            const coords = coordsEl.value.trim();
            if (!coords) {
                coordsEl.focus();
                return;
            }
            const repo = repoEl.value.trim();
            const excl = exclEl.value.trim() ? exclEl.value.trim().split('\\n').map(function (s) { return s.trim(); }).filter(Boolean) : [];
            lib = Object.assign({ type: 'maven', coordinates: coords }, repo ? { repo } : {}, excl.length ? { exclusions: excl } : {});
            coordsEl.value = '';
            repoEl.value = '';
            exclEl.value = '';
        }
        if (lib) {
            libEditorLibs.push(lib);
            renderLibEditorContent();
        }
    }
    // ── Editable field helpers ────────────────────────────────────────────────────
    function propEdit(label, value, fieldPath, inputType) {
        inputType = inputType || 'text';
        if (inputType === 'checkbox') {
            return `<div class="prop"><label class="prop-check-label">
        <input type="checkbox" class="prop-input" data-field="${esc(fieldPath)}" ${value ? 'checked' : ''} onchange="markDirty(this)">
        <span class="prop-label">${esc(label)}</span>
      </label></div>`;
        }
        return `<div class="prop">
      <div class="prop-label">${esc(label)}</div>
      <input type="${inputType}" class="prop-input" data-field="${esc(fieldPath)}" value="${esc(String(value ?? ''))}" oninput="markDirty(this)">
    </div>`;
    }
    function propSelect(label, value, fieldPath, options) {
        const opts = options.map(o => `<option value="${esc(o.v)}"${(o.v === (value || '')) ? ' selected' : ''}>${esc(o.l)}</option>`).join('');
        return `<div class="prop">
      <div class="prop-label">${esc(label)}</div>
      <select class="prop-input" data-field="${esc(fieldPath)}" onchange="markDirty(this)">${opts}</select>
    </div>`;
    }
    // ── Job panel (always shown in right sidebar) ──────────────────────────────────
    function showJobPanel() {
        const parts = [];
        // Header (read-only)
        const taskLabel = job.tasks.length + ' task' + (job.tasks.length !== 1 ? 's' : '');
        parts.push(`<div class="panel-header">
      <div class="panel-title">${esc(job.name)}</div>
      <div class="panel-subtitle">${esc(taskLabel)}</div>
    </div>`);
        // --- Trigger ---
        const effectiveTriggerType = getVal('trigger.type', job.trigger ? job.trigger.type : 'manual');
        const t = job.trigger || {};
        let triggerBody = `<div class="prop">
      <div class="prop-label">Trigger Type</div>
      <select class="prop-input" id="trigger-type-select">
        <option value="manual"${effectiveTriggerType === 'manual' ? ' selected' : ''}>Manual (no trigger)</option>
        <option value="periodic"${effectiveTriggerType === 'periodic' ? ' selected' : ''}>Scheduled (cron / interval)</option>
        <option value="table_update"${effectiveTriggerType === 'table_update' ? ' selected' : ''}>Continuous (table update)</option>
        <option value="file_arrival"${effectiveTriggerType === 'file_arrival' ? ' selected' : ''}>File arrival</option>
      </select>
    </div>`;
        if (effectiveTriggerType !== 'manual') {
            triggerBody += propSelect('Status', getVal('trigger.pauseStatus', t.pauseStatus || 'UNPAUSED'), 'trigger.pauseStatus', [{ v: 'UNPAUSED', l: 'UNPAUSED' }, { v: 'PAUSED', l: 'PAUSED' }]);
            if (effectiveTriggerType === 'table_update') {
                triggerBody += propSelect('Trigger when', getVal('trigger.tableCondition', t.tableCondition || 'ANY_UPDATED'), 'trigger.tableCondition', [
                    { v: 'ANY_UPDATED', l: 'Any table is updated' },
                    { v: 'ALL_UPDATED', l: 'All tables are updated' },
                ]);
                // Editable table names list
                const currentTableNames = 'trigger.tableNames' in pendingUpdates ? pendingUpdates['trigger.tableNames'] : (t.tableNames || []);
                triggerBody += `<div class="prop">
          <div class="prop-label">Table Names <span id="btn-add-table-name" style="cursor:pointer;color:var(--link);font-size:10px;margin-left:6px">+ Add</span></div>
          <div id="table-names-list">${currentTableNames.map(function (n, i) {
                    return `<div class="list-item-row"><span class="list-item-val">${esc(n)}</span><button class="list-item-btn" data-table-remove="${i}" title="Remove">&times;</button></div>`;
                }).join('')}</div>
          <div class="list-add-row" id="table-name-add-row" style="display:none">
            <input class="prop-input" id="table-name-input" type="text" placeholder="catalog.schema.table">
            <button class="lib-add-btn" id="btn-table-name-confirm" style="padding:3px 10px;white-space:nowrap">Add</button>
          </div>
        </div>`;
            }
            else if (effectiveTriggerType === 'periodic') {
                if (t.interval !== undefined || 'trigger.interval' in pendingUpdates) {
                    triggerBody += propEdit('Interval', getVal('trigger.interval', t.interval ?? ''), 'trigger.interval', 'number');
                    triggerBody += propSelect('Unit', getVal('trigger.intervalUnit', t.intervalUnit || 'HOURS'), 'trigger.intervalUnit', [{ v: 'HOURS', l: 'Hours' }, { v: 'DAYS', l: 'Days' }, { v: 'WEEKS', l: 'Weeks' }]);
                }
                else {
                    triggerBody += propEdit('Cron expression', getVal('trigger.cronExpression', t.cronExpression || ''), 'trigger.cronExpression');
                    triggerBody += propEdit('Timezone', getVal('trigger.timezone', t.timezone || ''), 'trigger.timezone');
                }
            }
            else if (effectiveTriggerType === 'file_arrival') {
                triggerBody += propEdit('Path / URL', getVal('trigger.fileArrivalUrl', t.fileArrivalUrl || ''), 'trigger.fileArrivalUrl');
            }
        }
        parts.push(section('Trigger', triggerBody, true));
        // --- Health ---
        let healthBody = '<span class="empty-note">No health rules</span>';
        if (job.health.length > 0) {
            healthBody = job.health.map((r, i) => {
                const opLabel = { GREATER_THAN: '>', LESS_THAN: '<', EQUAL_TO: '==', NOT_EQUAL: '\u2260' }[r.op] || r.op;
                return `<div class="health-rule">
          <span class="health-metric">${esc(r.metric.replace(/_/g, ' ').toLowerCase())}</span>
          <span class="health-op">${esc(opLabel)}</span>
          <input type="number" class="prop-input health-val-input" style="width:64px"
                 data-field="health.${i}.value" value="${esc(String(getVal('health.' + i + '.value', r.value)))}"
                 oninput="markDirty(this)">
        </div>`;
            }).join('');
        }
        parts.push(section('Health', healthBody, true));
        // --- Notifications (read-only) ---
        const notifBody = job.emailNotifications
            ? prop('Email', `<span class="prop-value mono">${esc(job.emailNotifications)}</span>`)
            : '<span class="empty-note">No notifications configured</span>';
        parts.push(section('Notifications', notifBody));
        // --- Job parameters ---
        let paramBody = '';
        if (job.parameters.length === 0) {
            paramBody = '<span class="empty-note">No parameters</span>';
        }
        else {
            paramBody = job.parameters.map((p, i) => `<div class="param-item" style="display:flex;align-items:center;gap:6px">
          <div style="flex:1">
            <div class="param-name">${esc(p.name)}</div>
            <input type="text" class="prop-input" data-field="parameters.${i}.default"
                   value="${esc(getVal('parameters.' + i + '.default', p.default || ''))}"
                   placeholder="(empty)" oninput="markDirty(this)">
          </div>
          <button class="section-danger-btn" style="flex-shrink:0;margin-left:0" data-remove-param="${esc(p.name)}" title="Remove parameter">×</button>
        </div>`).join('');
        }
        paramBody += `<div class="add-cluster-form" id="add-param-form">
          <div class="prop-label" style="font-size:11px;color:var(--blue);margin-bottom:4px">New Parameter</div>
          ${propEdit('Name', '', 'new')}
          ${propEdit('Default value', '', 'new')}
          <div class="add-form-btns">
            <button class="add-form-confirm" id="btn-param-confirm">Add</button>
            <button class="add-form-cancel" id="btn-param-cancel">Cancel</button>
          </div>
        </div>
        <button class="section-add-btn" id="btn-add-param">+ Add Parameter</button>`;
        parts.push(section('Job Parameters', paramBody, false, job.parameters.length || null));
        // --- Queue & concurrency ---
        const queueBody = propEdit('Queue enabled', getVal('queueEnabled', job.queueEnabled), 'queueEnabled', 'checkbox') +
            propEdit('Max concurrent runs', getVal('maxConcurrentRuns', job.maxConcurrentRuns ?? 1), 'maxConcurrentRuns', 'number') +
            propEdit('Timeout (seconds)', getVal('timeoutSeconds', job.timeoutSeconds ?? ''), 'timeoutSeconds', 'number');
        parts.push(section('Queue & Limits', queueBody));
        // --- Permissions (read-only) ---
        let permBody = '<span class="empty-note">No permissions listed</span>';
        if (job.permissions.length > 0) {
            permBody = job.permissions.map(p => `<div class="perm-item">
          <div class="param-name">${esc(p.level.replace(/_/g, ' '))}</div>
          <div class="prop-value mono" style="font-size:10px">${esc(p.principal || p.principalType)}</div>
        </div>`).join('');
        }
        parts.push(section('Permissions', permBody, false, job.permissions.length || null));
        // --- Clusters ---
        const allClusterKeys = Object.keys(job.jobClusters).filter(function (k) {
            return !pendingRemovedClusterKeys.includes(k);
        });
        const pendingNewClusterKeys = pendingNewClusters.map(function (c) { return c.job_cluster_key; });
        {
            const clusterBodies = allClusterKeys.map(function (k) {
                const c = job.jobClusters[k];
                const isRemoved = pendingRemovedClusterKeys.includes(k);
                if (isRemoved)
                    return '';
                let rows = `<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
          <span class="prop-label" style="margin:0">${esc(k)}</span>
          <div style="display:flex;gap:4px;align-items:center">
            <button class="topbar-btn" data-edit-cluster="${esc(k)}" style="padding:2px 8px;font-size:10px">Edit…</button>
            <button class="section-danger-btn" style="margin-left:0" data-remove-cluster="${esc(k)}">Remove</button>
          </div>
        </div>`;
                rows += propEdit('Node type', getVal(`jobClusters.${k}.nodeTypeId`, c.nodeTypeId || ''), `jobClusters.${k}.nodeTypeId`);
                rows += propEdit('Runtime', getVal(`jobClusters.${k}.sparkVersion`, c.sparkVersion || ''), `jobClusters.${k}.sparkVersion`);
                if (c.minWorkers != null || c.maxWorkers != null) {
                    rows += propEdit('Min workers', getVal(`jobClusters.${k}.minWorkers`, c.minWorkers ?? ''), `jobClusters.${k}.minWorkers`, 'number');
                    rows += propEdit('Max workers', getVal(`jobClusters.${k}.maxWorkers`, c.maxWorkers ?? ''), `jobClusters.${k}.maxWorkers`, 'number');
                }
                else if (getVal(`jobClusters.${k}.numWorkers`, c.numWorkers) === 0) {
                    rows += prop('Workers', 'Single node (driver only)');
                }
                else {
                    rows += propEdit('Workers', getVal(`jobClusters.${k}.numWorkers`, c.numWorkers ?? ''), `jobClusters.${k}.numWorkers`, 'number');
                }
                return rows;
            });
            const pendingClusterBodies = pendingNewClusters.map(function (c) {
                return `<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px">
          <span class="prop-label" style="margin:0;color:var(--green)">${esc(c.job_cluster_key)} <span style="font-size:9px">(new)</span></span>
          <button class="section-danger-btn" data-remove-pending-cluster="${esc(c.job_cluster_key)}">Remove</button>
        </div>
        <div class="prop-value" style="font-size:10px;color:var(--fg-dim)">${esc(c.new_cluster.node_type_id || '')} · ${esc(c.new_cluster.spark_version || '')}</div>`;
            });
            const allBodies = clusterBodies.concat(pendingClusterBodies).filter(function (b) { return b; });
            const totalClusterCount = allClusterKeys.length + pendingNewClusters.length;
            const cBody = allBodies.join('<hr style="border:none;border-top:1px solid var(--border);margin:10px 0">') +
                `<div class="add-cluster-form" id="add-cluster-form">
          <div class="prop-label" style="font-size:11px;color:var(--blue);margin-bottom:4px">New Job Cluster</div>
          ${propEdit('Cluster key', '', 'new')}
          ${propEdit('Node type ID', '', 'new')}
          ${propEdit('Spark version', '', 'new')}
          ${propEdit('Num workers', '1', 'new')}
          <div class="add-form-btns">
            <button class="add-form-confirm" id="btn-add-cluster-confirm">Add Cluster</button>
            <button class="add-form-cancel" id="btn-add-cluster-cancel">Cancel</button>
          </div>
        </div>
        <button class="section-add-btn" id="btn-show-add-cluster">+ Add Cluster</button>`;
            parts.push(section('Clusters', cBody, totalClusterCount > 0, totalClusterCount || null));
        }
        // --- Serverless environments ---
        const allEnvKeys = Object.keys(job.environments).filter(function (k) {
            return !pendingRemovedEnvKeys.includes(k);
        });
        {
            const envBodies = allEnvKeys.map(function (k) {
                const e = job.environments[k];
                const currentDeps = (`environments.${k}.dependencies` in pendingUpdates)
                    ? pendingUpdates[`environments.${k}.dependencies`]
                    : (e.dependencies || []);
                let html = `<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
          <span class="prop-label" style="margin:0">${esc(k)}</span>
          <button class="section-danger-btn" data-remove-env="${esc(k)}">Remove</button>
        </div>`;
                html += propEdit('Version', getVal(`environments.${k}.version`, e.version || ''), `environments.${k}.version`);
                html += `<div class="prop">
          <div class="prop-label">Dependencies <span data-add-dep="${esc(k)}" style="cursor:pointer;color:var(--link);font-size:10px;margin-left:6px">+ Add</span></div>
          <div id="dep-list-${esc(k)}">${currentDeps.map(function (d, i) {
                    return `<div class="list-item-row"><span class="list-item-val">${esc(d)}</span><button class="list-item-btn" data-dep-remove="${esc(k)}" data-dep-idx="${i}" title="Remove">&times;</button></div>`;
                }).join('')}</div>
          <div class="list-add-row" id="dep-add-row-${esc(k)}" style="display:none">
            <input class="prop-input" id="dep-input-${esc(k)}" type="text" placeholder="numpy==1.25.2">
            <button class="lib-add-btn" data-dep-confirm="${esc(k)}" style="padding:3px 10px;white-space:nowrap">Add</button>
          </div>
        </div>`;
                return html;
            });
            const pendingEnvBodies = pendingNewEnvs.map(function (e) {
                return `<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px">
          <span class="prop-label" style="margin:0;color:var(--green)">${esc(e.environment_key)} <span style="font-size:9px">(new)</span></span>
          <button class="section-danger-btn" data-remove-pending-env="${esc(e.environment_key)}">Remove</button>
        </div>
        <div class="prop-value" style="font-size:10px;color:var(--fg-dim)">v${esc(e.spec.environment_version || '?')}</div>`;
            });
            const totalEnvCount = allEnvKeys.length + pendingNewEnvs.length;
            const eBody = envBodies.concat(pendingEnvBodies).join('<hr style="border:none;border-top:1px solid var(--border);margin:10px 0">') +
                `<div class="add-env-form" id="add-env-form">
          <div class="prop-label" style="font-size:11px;color:var(--blue);margin-bottom:4px">New Serverless Environment</div>
          ${propEdit('Environment key', '', 'new')}
          ${propEdit('Version (environment_version)', '', 'new')}
          <div class="add-form-btns">
            <button class="add-form-confirm" id="btn-add-env-confirm">Add Environment</button>
            <button class="add-form-cancel" id="btn-add-env-cancel">Cancel</button>
          </div>
        </div>
        <button class="section-add-btn" id="btn-show-add-env">+ Add Environment</button>`;
            if (totalEnvCount > 0 || true) { // always show environments section
                parts.push(section('Environments', eBody, totalEnvCount > 0, totalEnvCount || null));
            }
        }
        panelContent.innerHTML = parts.join('');
        // Wire trigger type select
        const triggerTypeSelect = document.getElementById('trigger-type-select');
        if (triggerTypeSelect) {
            triggerTypeSelect.addEventListener('change', function () {
                pendingUpdates['trigger.type'] = triggerTypeSelect.value;
                markDirtyDirect();
                showJobPanel();
            });
        }
        // Wire cluster edit/remove buttons
        panelContent.addEventListener('click', function (e) {
            const editBtn = e.target.closest('[data-edit-cluster]');
            if (editBtn) {
                openClusterEditor(editBtn.dataset.editCluster);
                return;
            }
            const removeBtn = e.target.closest('[data-remove-cluster]');
            if (removeBtn) {
                const key = removeBtn.dataset.removeCluster;
                if (!pendingRemovedClusterKeys.includes(key)) {
                    pendingRemovedClusterKeys.push(key);
                    pendingUpdates['_removedClusterKeys'] = pendingRemovedClusterKeys.slice();
                    markDirtyDirect();
                }
                showJobPanel();
                return;
            }
            const removePendingBtn = e.target.closest('[data-remove-pending-cluster]');
            if (removePendingBtn) {
                const key = removePendingBtn.dataset.removePendingCluster;
                pendingNewClusters = pendingNewClusters.filter(function (c) { return c.job_cluster_key !== key; });
                pendingUpdates['_newClusters'] = pendingNewClusters.slice();
                markDirtyDirect();
                showJobPanel();
                return;
            }
            const removeEnvBtn = e.target.closest('[data-remove-env]');
            if (removeEnvBtn) {
                const key = removeEnvBtn.dataset.removeEnv;
                if (!pendingRemovedEnvKeys.includes(key)) {
                    pendingRemovedEnvKeys.push(key);
                    pendingUpdates['_removedEnvironmentKeys'] = pendingRemovedEnvKeys.slice();
                    markDirtyDirect();
                }
                showJobPanel();
                return;
            }
            const removePendingEnvBtn = e.target.closest('[data-remove-pending-env]');
            if (removePendingEnvBtn) {
                const key = removePendingEnvBtn.dataset.removePendingEnv;
                pendingNewEnvs = pendingNewEnvs.filter(function (e2) { return e2.environment_key !== key; });
                pendingUpdates['_newEnvironments'] = pendingNewEnvs.slice();
                markDirtyDirect();
                showJobPanel();
                return;
            }
            // Add dep button (the "+Add" span)
            const addDepSpan = e.target.closest('[data-add-dep]');
            if (addDepSpan) {
                const key = addDepSpan.dataset.addDep;
                const row = document.getElementById('dep-add-row-' + key);
                if (row)
                    row.style.display = row.style.display === 'none' ? 'flex' : 'none';
                return;
            }
            const depConfirmBtn = e.target.closest('[data-dep-confirm]');
            if (depConfirmBtn) {
                const key = depConfirmBtn.dataset.depConfirm;
                const inp = document.getElementById('dep-input-' + key);
                if (!inp || !inp.value.trim()) {
                    if (inp)
                        inp.focus();
                    return;
                }
                const depPath = 'environments.' + key + '.dependencies';
                const env = job.environments[key];
                const currentDeps = (depPath in pendingUpdates) ? pendingUpdates[depPath] : (env ? (env.dependencies || []) : []);
                pendingUpdates[depPath] = currentDeps.concat([inp.value.trim()]);
                markDirtyDirect();
                showJobPanel();
                return;
            }
            const depRemoveBtn = e.target.closest('[data-dep-remove]');
            if (depRemoveBtn) {
                const key = depRemoveBtn.dataset.depRemove;
                const idx = parseInt(depRemoveBtn.dataset.depIdx || '0', 10);
                const depPath = 'environments.' + key + '.dependencies';
                const env = job.environments[key];
                const currentDeps = (depPath in pendingUpdates) ? pendingUpdates[depPath] : (env ? (env.dependencies || []) : []);
                pendingUpdates[depPath] = currentDeps.filter(function (_d, i) { return i !== idx; });
                markDirtyDirect();
                showJobPanel();
                return;
            }
            // Parameter remove
            const removeParamBtn = e.target.closest('[data-remove-param]');
            if (removeParamBtn) {
                const name = removeParamBtn.dataset.removeParam;
                removeParamBtn.disabled = true;
                removeParamBtn.textContent = '…';
                vscodeApi.postMessage({
                    command: 'applyJobChange',
                    filePath: job.filePath,
                    jobName: job.name,
                    updates: { _removedParamNames: [name] },
                });
                return;
            }
            // Table name buttons
            const tableRemoveBtn = e.target.closest('[data-table-remove]');
            if (tableRemoveBtn) {
                const idx = parseInt(tableRemoveBtn.dataset.tableRemove || '0', 10);
                const currentNames = ('trigger.tableNames' in pendingUpdates) ? pendingUpdates['trigger.tableNames'] : (job.trigger ? (job.trigger.tableNames || []) : []);
                pendingUpdates['trigger.tableNames'] = currentNames.filter(function (_n, i) { return i !== idx; });
                markDirtyDirect();
                showJobPanel();
                return;
            }
        });
        // Add table name toggle
        const addTableNameBtn = document.getElementById('btn-add-table-name');
        if (addTableNameBtn) {
            addTableNameBtn.addEventListener('click', function () {
                const row = document.getElementById('table-name-add-row');
                if (row)
                    row.style.display = row.style.display === 'none' ? 'flex' : 'none';
            });
        }
        const confirmTableBtn = document.getElementById('btn-table-name-confirm');
        if (confirmTableBtn) {
            confirmTableBtn.addEventListener('click', function () {
                const inp = document.getElementById('table-name-input');
                if (!inp || !inp.value.trim()) {
                    if (inp)
                        inp.focus();
                    return;
                }
                const currentNames = ('trigger.tableNames' in pendingUpdates) ? pendingUpdates['trigger.tableNames'] : (job.trigger ? (job.trigger.tableNames || []) : []);
                pendingUpdates['trigger.tableNames'] = currentNames.concat([inp.value.trim()]);
                inp.value = '';
                markDirtyDirect();
                showJobPanel();
            });
        }
        // Add cluster form
        const showAddClusterBtn = document.getElementById('btn-show-add-cluster');
        if (showAddClusterBtn) {
            showAddClusterBtn.addEventListener('click', function () {
                const form = document.getElementById('add-cluster-form');
                if (form) {
                    form.classList.toggle('visible');
                    showAddClusterBtn.style.display = form.classList.contains('visible') ? 'none' : '';
                }
            });
        }
        const cancelAddClusterBtn = document.getElementById('btn-add-cluster-cancel');
        if (cancelAddClusterBtn) {
            cancelAddClusterBtn.addEventListener('click', function () {
                const form = document.getElementById('add-cluster-form');
                if (form)
                    form.classList.remove('visible');
                const btn = document.getElementById('btn-show-add-cluster');
                if (btn)
                    btn.style.display = '';
            });
        }
        const confirmAddClusterBtn = document.getElementById('btn-add-cluster-confirm');
        if (confirmAddClusterBtn) {
            confirmAddClusterBtn.addEventListener('click', function () {
                const inputs = document.querySelectorAll('#add-cluster-form .prop-input');
                const keyVal = inputs[0] ? inputs[0].value.trim() : '';
                const nodeType = inputs[1] ? inputs[1].value.trim() : '';
                const sparkVer = inputs[2] ? inputs[2].value.trim() : '';
                const workers = inputs[3] ? parseInt(inputs[3].value || '1', 10) : 1;
                if (!keyVal) {
                    if (inputs[0])
                        inputs[0].focus();
                    return;
                }
                const newCluster = { job_cluster_key: keyVal, new_cluster: { node_type_id: nodeType || undefined, spark_version: sparkVer || undefined, num_workers: workers } };
                pendingNewClusters = pendingNewClusters.concat([newCluster]);
                pendingUpdates['_newClusters'] = pendingNewClusters.slice();
                markDirtyDirect();
                showJobPanel();
            });
        }
        // Add environment form
        const showAddEnvBtn = document.getElementById('btn-show-add-env');
        if (showAddEnvBtn) {
            showAddEnvBtn.addEventListener('click', function () {
                const form = document.getElementById('add-env-form');
                if (form) {
                    form.classList.toggle('visible');
                    showAddEnvBtn.style.display = form.classList.contains('visible') ? 'none' : '';
                }
            });
        }
        const cancelAddEnvBtn = document.getElementById('btn-add-env-cancel');
        if (cancelAddEnvBtn) {
            cancelAddEnvBtn.addEventListener('click', function () {
                const form = document.getElementById('add-env-form');
                if (form)
                    form.classList.remove('visible');
                const btn = document.getElementById('btn-show-add-env');
                if (btn)
                    btn.style.display = '';
            });
        }
        const confirmAddEnvBtn = document.getElementById('btn-add-env-confirm');
        if (confirmAddEnvBtn) {
            confirmAddEnvBtn.addEventListener('click', function () {
                const inputs = document.querySelectorAll('#add-env-form .prop-input');
                const keyVal = inputs[0] ? inputs[0].value.trim() : '';
                const version = inputs[1] ? inputs[1].value.trim() : '';
                if (!keyVal) {
                    if (inputs[0])
                        inputs[0].focus();
                    return;
                }
                const newEnv = { environment_key: keyVal, spec: { environment_version: version || undefined, dependencies: [] } };
                pendingNewEnvs = pendingNewEnvs.concat([newEnv]);
                pendingUpdates['_newEnvironments'] = pendingNewEnvs.slice();
                markDirtyDirect();
                showJobPanel();
            });
        }
        // Add parameter form
        const showAddParamBtn = document.getElementById('btn-add-param');
        if (showAddParamBtn) {
            showAddParamBtn.addEventListener('click', function () {
                const form = document.getElementById('add-param-form');
                if (form) {
                    form.classList.toggle('visible');
                    showAddParamBtn.style.display = form.classList.contains('visible') ? 'none' : '';
                }
            });
        }
        const cancelAddParamBtn = document.getElementById('btn-param-cancel');
        if (cancelAddParamBtn) {
            cancelAddParamBtn.addEventListener('click', function () {
                const form = document.getElementById('add-param-form');
                if (form)
                    form.classList.remove('visible');
                const btn = document.getElementById('btn-add-param');
                if (btn)
                    btn.style.display = '';
            });
        }
        const confirmAddParamBtn = document.getElementById('btn-param-confirm');
        if (confirmAddParamBtn) {
            confirmAddParamBtn.addEventListener('click', function () {
                const inputs = document.querySelectorAll('#add-param-form .prop-input');
                const nameVal = inputs[0] ? inputs[0].value.trim() : '';
                const defaultVal = inputs[1] ? inputs[1].value.trim() : '';
                if (!nameVal) {
                    if (inputs[0])
                        inputs[0].focus();
                    return;
                }
                vscodeApi.postMessage({
                    command: 'applyJobChange',
                    filePath: job.filePath,
                    jobName: job.name,
                    updates: { _newParams: [{ name: nameVal, default: defaultVal }] },
                });
            });
        }
    }
    // ── Task panel (shown in bottom bar) ─────────────────────────────────────────
    function showTaskPanel(task) {
        const c = nodeColors(task.type);
        const typeLbl = { notebook: 'Notebook', condition: 'Condition', spark_python: 'Spark Python',
            python_wheel: 'Python Wheel', sql: 'SQL', run_job: 'Run Job', unknown: 'Unknown' }[task.type] || 'Unknown';
        taskBreadcrumb.innerHTML =
            `<span class="breadcrumb-link" id="bc-job">Job</span> › <span>${esc(task.task_key)}</span>`;
        document.getElementById('bc-job')?.addEventListener('click', deselect);
        const typeBadgeStyle = `background:${c.fill};color:${c.stroke};border:1px solid ${c.stroke}`;
        const tk = task.task_key;
        const parts = [];
        // Header: task name (read-only), type badge, editable notebook path + run-if
        const nbInput = task.notebook_path !== undefined
            ? `<input type="text" class="prop-input prop-value"
               style="font-size:10px;margin-top:8px;font-family:var(--mono)"
               data-field="tasks.${esc(tk)}.notebook_path"
               value="${esc(getVal('tasks.' + tk + '.notebook_path', task.notebook_path || ''))}"
               placeholder="notebook path" oninput="markDirty(this)">`
            : '';
        parts.push(`<div class="panel-header">
      <div class="prop-label" style="margin-bottom:2px">Task name</div>
      <input type="text" class="prop-input" id="task-name-input"
             style="font-size:13px;font-weight:600;margin-bottom:6px"
             data-original-key="${esc(tk)}"
             value="${esc(getVal('_renamedTasks.' + tk, tk))}">
      <span class="type-badge" style="${typeBadgeStyle}">${esc(typeLbl.toUpperCase())}</span>
      ${nbInput}
    </div>
    <div class="section-body" style="border-top:1px solid var(--border);padding-top:8px">${propSelect('Run if', getVal(`tasks.${tk}.run_if`, task.run_if || ''), `tasks.${tk}.run_if`, [
            { v: '', l: '(default)' },
            { v: 'ALL_SUCCESS', l: 'All success' },
            { v: 'AT_LEAST_ONE_SUCCESS', l: 'At least one success' },
            { v: 'NONE_FAILED', l: 'None failed' },
            { v: 'ALL_DONE', l: 'All done' },
            { v: 'AT_LEAST_ONE_FAILED', l: 'At least one failed' },
            { v: 'ALL_FAILED', l: 'All failed' },
        ])}</div>`);
        // --- Upstream dependencies (interactive) ---
        const effectiveDeps = getEffectiveDepsFor(tk);
        let depsBody = '';
        if (effectiveDeps.length === 0) {
            depsBody = '<span class="empty-note">No upstream dependencies (root task)</span>';
        }
        else {
            depsBody = effectiveDeps.map(function (d, i) {
                const oc2 = d.outcome === 'true' ? 'true' : d.outcome === 'false' ? 'false' : null;
                const badge2 = oc2 ? `<span class="outcome-badge outcome-${oc2}">${oc2}</span>` : '';
                return `<div class="dep-item" style="display:flex;align-items:center;gap:6px">
          <span class="dep-arrow">→</span>
          <span class="dep-key" style="cursor:pointer;color:var(--link);flex:1" onclick="selectTaskByKey('${esc(d.task_key)}')">${esc(d.task_key)}</span>
          ${badge2}
          <button style="background:transparent;border:1px solid var(--border);color:var(--fg-dim);border-radius:3px;width:18px;height:18px;cursor:pointer;font-size:14px;line-height:1;display:flex;align-items:center;justify-content:center;flex-shrink:0;padding:0"
            onclick="removeDepFromTask('${esc(tk)}',${i})" title="Remove dependency">−</button>
        </div>`;
            }).join('');
        }
        const otherTasks = getVirtualTasks().filter(function (t) {
            return t.task_key !== tk && !effectiveDeps.some(function (d) { return d.task_key === t.task_key; });
        });
        if (otherTasks.length > 0) {
            const addSelectId = `dep-add-sel-${esc(tk)}`;
            const addOutcomeId = `dep-add-oc-${esc(tk)}`;
            const opts = otherTasks.map(function (t) {
                return `<option value="${esc(t.task_key)}">${esc(t.task_key)}</option>`;
            }).join('');
            depsBody += `<div style="display:flex;gap:4px;margin-top:6px;align-items:center">
        <select id="${addSelectId}" class="prop-input" style="flex:1;font-size:11px">
          <option value="">— Add upstream task —</option>${opts}
        </select>
        <select id="${addOutcomeId}" class="prop-input" style="width:64px;font-size:11px">
          <option value="">any</option>
          <option value="true">true</option>
          <option value="false">false</option>
        </select>
        <button class="topbar-btn" onclick="(function(){var s=document.getElementById('${addSelectId}');var o=document.getElementById('${addOutcomeId}');addDepToTask('${esc(tk)}',s.value,o.value);})()">Add</button>
      </div>`;
        }
        parts.push(section('Upstream Dependencies', depsBody, true, effectiveDeps.length || null));
        // --- Condition (read-only) ---
        if (task.condition) {
            const opLabel = { EQUAL_TO: '==', NOT_EQUAL_TO: '!=', GREATER_THAN: '>', LESS_THAN: '<' }[task.condition.op] || task.condition.op;
            const condBody = `<div class="cluster-grid">
        <span class="cluster-key">Left</span><span class="cluster-val mono prop-value">${esc(task.condition.left)}</span>
        <span class="cluster-key">Op</span><span class="cluster-val" style="color:var(--orange);font-family:monospace">${esc(opLabel)}</span>
        <span class="cluster-key">Right</span><span class="cluster-val mono prop-value">${esc(task.condition.right)}</span>
      </div>`;
            parts.push(section('Condition', condBody, true));
        }
        // --- Compute ---
        const clusterOpts = [{ v: '', l: '(none)' }].concat(Object.keys(job.jobClusters).map(k => ({ v: k, l: k })));
        const envOpts = [{ v: '', l: '(none)' }].concat(Object.keys(job.environments).map(k => ({ v: k, l: k })));
        let computeBody = propSelect('Job cluster', getVal(`tasks.${tk}.job_cluster_key`, task.job_cluster_key || ''), `tasks.${tk}.job_cluster_key`, clusterOpts) +
            propSelect('Environment', getVal(`tasks.${tk}.environment_key`, task.environment_key || ''), `tasks.${tk}.environment_key`, envOpts) +
            propEdit('Max retries', getVal(`tasks.${tk}.max_retries`, task.max_retries ?? ''), `tasks.${tk}.max_retries`, 'number') +
            propEdit('Retry interval (ms)', getVal(`tasks.${tk}.min_retry_interval_millis`, task.min_retry_interval_millis ?? ''), `tasks.${tk}.min_retry_interval_millis`, 'number');
        parts.push(section('Compute', computeBody, true));
        // --- Task parameters ---
        const paramKeys = Object.keys(task.base_parameters || {});
        let taskParamBody = '<span class="empty-note">No task-level parameters</span>';
        if (paramKeys.length > 0) {
            taskParamBody = paramKeys.map(k => `<div class="param-item">
          <div class="param-name">${esc(k)}</div>
          <input type="text" class="prop-input" data-field="tasks.${esc(tk)}.base_parameters.${esc(k)}"
                 value="${esc(getVal('tasks.' + tk + '.base_parameters.' + k, task.base_parameters[k]))}"
                 oninput="markDirty(this)">
        </div>`).join('');
        }
        parts.push(section('Task Parameters', taskParamBody, paramKeys.length > 0, paramKeys.length || null));
        // --- Libraries (context-aware) ---
        const effectiveEnvKey = getVal(`tasks.${tk}.environment_key`, task.environment_key || '');
        const libPath = `tasks.${tk}.libraries`;
        const currentLibs = libPath in pendingUpdates ? pendingUpdates[libPath] : task.libraries;
        let libBody;
        if (effectiveEnvKey) {
            libBody = `<div class="lib-serverless-note">
        Serverless tasks manage dependencies at the environment level.<br>
        Edit <strong>${esc(effectiveEnvKey)}</strong> dependencies in the Job sidebar → Environments section.
      </div>`;
        }
        else {
            const libItems = currentLibs.length > 0
                ? currentLibs.map(lib => {
                    const color = LIB_COLORS[lib.type] || '#888';
                    let desc = '';
                    if (lib.type === 'whl' || lib.type === 'jar' || lib.type === 'requirements')
                        desc = lib.path || '';
                    else if (lib.type === 'pypi')
                        desc = (lib.package || '') + (lib.repo ? ` (repo: ${lib.repo})` : '');
                    else if (lib.type === 'maven')
                        desc = (lib.coordinates || '') + (lib.repo ? ` (repo: ${lib.repo})` : '');
                    return `<div class="lib-item" style="display:flex;align-items:baseline;gap:5px">
              <span style="font-size:9px;font-weight:700;color:${color};background:${color}22;border:1px solid ${color}44;border-radius:2px;padding:0 4px;flex-shrink:0">${lib.type.toUpperCase()}</span>
              <span class="prop-value mono" style="font-size:10px;flex:1;word-break:break-all">${esc(desc)}</span>
            </div>`;
                }).join('')
                : '<span class="empty-note">No libraries</span>';
            libBody = `${libItems}
        <button class="lib-modify-btn" id="btn-lib-modify">Modify Libraries\u2026</button>`;
        }
        parts.push(section('Libraries', libBody, true, currentLibs.length || null));
        // --- Remove task button ---
        parts.push(section('Danger Zone',
            `<button class="section-danger-btn" id="btn-remove-task" style="margin-left:0;width:100%">Remove task "${esc(tk)}"</button>`,
            false));
        taskPanelContent.innerHTML = parts.join('');
        const taskNameInput = document.getElementById('task-name-input');
        if (taskNameInput) {
            taskNameInput.addEventListener('input', function () {
                const origKey = taskNameInput.dataset.originalKey;
                const newKey = taskNameInput.value;
                if (newKey && newKey !== origKey) {
                    pendingUpdates['_renamedTasks.' + origKey] = newKey;
                }
                else {
                    delete pendingUpdates['_renamedTasks.' + origKey];
                }
                markDirtyDirect();
            });
        }
        const libModifyBtn = document.getElementById('btn-lib-modify');
        if (libModifyBtn)
            libModifyBtn.addEventListener('click', () => openLibraryEditor(tk));
        const removeBtn = document.getElementById('btn-remove-task');
        if (removeBtn) {
            removeBtn.addEventListener('click', function () {
                removeBtn.disabled = true;
                removeBtn.textContent = 'Removing…';
                vscodeApi.postMessage({
                    command: 'applyJobChange',
                    filePath: job.filePath,
                    jobName: job.name,
                    updates: { _removedTaskKeys: [tk] },
                });
            });
        }
    }
    // Global helpers
    window.selectTaskByKey = function (key) { selectTask(key); };
    window.removeDepFromTask = function (taskKey, depIdx) {
        var deps = getEffectiveDepsFor(taskKey).slice();
        deps.splice(depIdx, 1);
        taskDepsOverride[taskKey] = deps;
        pendingUpdates['tasks.' + taskKey + '.depends_on'] = deps;
        markDirtyDirect();
        redrawEdges();
        if (selectedKey === taskKey) {
            var found = getVirtualTasks().find(function (t) { return t.task_key === taskKey; });
            if (found)
                showTaskPanel(found);
        }
    };
    window.addDepToTask = function (taskKey, fromKey, outcome) {
        if (!fromKey)
            return;
        var deps = getEffectiveDepsFor(taskKey).slice();
        if (deps.some(function (d) { return d.task_key === fromKey; }))
            return;
        var newDep = { task_key: fromKey };
        if (outcome)
            newDep.outcome = outcome;
        deps.push(newDep);
        taskDepsOverride[taskKey] = deps;
        pendingUpdates['tasks.' + taskKey + '.depends_on'] = deps;
        markDirtyDirect();
        redrawEdges();
        if (selectedKey === taskKey) {
            var found = getVirtualTasks().find(function (t) { return t.task_key === taskKey; });
            if (found)
                showTaskPanel(found);
        }
    };
    // ── Add-task FAB ─────────────────────────────────────────────────────────────
    document.getElementById('btn-add-task').addEventListener('click', function () {
        showNewTaskPanel();
    });
    function showNewTaskPanel() {
        selectedKey = null;
        taskBreadcrumb.innerHTML = 'New Task';
        openBottomBar();
        const otherJobNames = allJobNames.filter(function (n) { return n !== job.name; });
        const jobOpts = otherJobNames.map(function (n) {
            return `<option value="${esc(n)}">${esc(n)}</option>`;
        }).join('');
        taskPanelContent.innerHTML = `
      <div class="panel-header" style="padding:10px 14px 6px">
        <div class="prop-label" style="margin-bottom:4px">Task key *</div>
        <input type="text" id="nt-key" class="prop-input" placeholder="my_new_task" style="font-size:13px;font-weight:600">
      </div>
      <div class="nt-section">
        <div class="nt-title">Type</div>
        <select id="nt-type" class="prop-input" style="width:100%">
          <option value="notebook">Notebook</option>
          <option value="run_job">Run Job (reference another job)</option>
          <option value="condition">Condition</option>
        </select>
      </div>
      <div class="nt-section" id="nt-notebook-fields">
        <div class="nt-title">Notebook</div>
        ${propEdit('Notebook path', '', '', 'text')}
      </div>
      <div class="nt-section" id="nt-runjob-fields" style="display:none">
        <div class="nt-title">Referenced Job</div>
        <div class="prop"><div class="prop-label">Job name</div>
          <select id="nt-job-select" class="prop-input" style="width:100%">
            <option value="">— select job —</option>${jobOpts}
          </select>
        </div>
      </div>
      <div class="nt-section" id="nt-cond-fields" style="display:none">
        <div class="nt-title">Condition</div>
        <div class="prop"><div class="prop-label">Operator</div>
          <select id="nt-cond-op" class="prop-input" style="width:100%">
            <option value="EQUAL_TO">== Equal to</option>
            <option value="NOT_EQUAL_TO">!= Not equal to</option>
            <option value="GREATER_THAN">&gt; Greater than</option>
            <option value="LESS_THAN">&lt; Less than</option>
          </select>
        </div>
        ${propEdit('Left value', '', '', 'text')}
        ${propEdit('Right value', '', '', 'text')}
      </div>
      <div class="nt-section" style="display:flex;gap:8px">
        <button class="btn-add-confirm" id="btn-nt-confirm">Add to DAG</button>
        <button class="topbar-btn" id="btn-nt-cancel">Cancel</button>
      </div>`;
        // Swap visible section based on type
        const ntType = document.getElementById('nt-type');
        const ntNbFlds = document.getElementById('nt-notebook-fields');
        const ntRjFlds = document.getElementById('nt-runjob-fields');
        const ntCdFlds = document.getElementById('nt-cond-fields');
        ntType.addEventListener('change', function () {
            ntNbFlds.style.display = ntType.value === 'notebook' ? '' : 'none';
            ntRjFlds.style.display = ntType.value === 'run_job' ? '' : 'none';
            ntCdFlds.style.display = ntType.value === 'condition' ? '' : 'none';
        });
        document.getElementById('btn-nt-cancel').addEventListener('click', function () {
            closeBottomBar();
        });
        document.getElementById('btn-nt-confirm').addEventListener('click', function () {
            const key = document.getElementById('nt-key').value.trim();
            if (!key) {
                document.getElementById('nt-key').focus();
                return;
            }
            if (getVirtualTasks().some(function (t) { return t.task_key === key; })) {
                document.getElementById('nt-key').style.borderColor = 'var(--red)';
                document.getElementById('nt-key').title = 'Task key already exists';
                return;
            }
            const type = ntType.value;
            const newTask = {
                task_key: key, type: type,
                depends_on: [], run_if: undefined,
                job_cluster_key: undefined, environment_key: undefined,
                base_parameters: {}, libraries: [],
                _isPending: true,
            };
            if (type === 'notebook') {
                // extract value from the propEdit input — it's the 3rd .prop-input in notebook-fields
                const nbInput = ntNbFlds.querySelector('.prop-input[type="text"]');
                newTask.notebook_path = nbInput ? nbInput.value.trim() : '';
            }
            else if (type === 'run_job') {
                newTask._run_job_name = document.getElementById('nt-job-select').value;
            }
            else if (type === 'condition') {
                const condInputs = ntCdFlds.querySelectorAll('.prop-input[type="text"]');
                newTask.condition = {
                    op: document.getElementById('nt-cond-op').value,
                    left: condInputs[0] ? condInputs[0].value.trim() : '',
                    right: condInputs[1] ? condInputs[1].value.trim() : '',
                };
            }
            // Place new task below existing layout immediately for visual feedback
            taskDepsOverride[key] = [];
            const allPos = Object.values(positions);
            const maxY = allPos.length > 0 ? Math.max.apply(null, allPos.map(function (p) { return p.y + p.h; })) : 0;
            const avgX = allPos.length > 0 ? allPos.reduce(function (s, p) { return s + p.x; }, 0) / allPos.length : 0;
            const posW = type === 'condition' ? CS * 2 : NW;
            const posH = type === 'condition' ? CS * 2 : NH;
            positions[key] = { x: avgX, y: maxY + 60, w: posW, h: posH };
            pendingNewTasks.push(newTask);
            _bbox = draw(getVirtualTasks(), positions);
            document.getElementById('hdr-tasks').textContent =
                getVirtualTasks().length + ' task' + (getVirtualTasks().length !== 1 ? 's' : '');
            closeBottomBar();
            showJobPanel();
            // Immediately write to file — no Save button needed
            vscodeApi.postMessage({
                command: 'applyJobChange',
                filePath: job.filePath,
                jobName: job.name,
                updates: { _newTasks: [pendingTaskToRaw(newTask)] },
            });
        });
    }
    // ── Zoom & pan ────────────────────────────────────────────────────────────────
    dagSvg.addEventListener('wheel', (e) => {
        e.preventDefault();
        const r = dagSvg.getBoundingClientRect();
        const mx = e.clientX - r.left, my = e.clientY - r.top;
        const d = e.deltaY > 0 ? 0.85 : 1.18;
        tx = mx - (mx - tx) * d;
        ty = my - (my - ty) * d;
        sc *= d;
        applyT();
    }, { passive: false });
    let drag = false, dx = 0, dy = 0;
    dagSvg.addEventListener('mousedown', (e) => {
        const t = e.target;
        if (t === dagSvg || t === zg || t.tagName === 'path' || t.tagName === 'svg') {
            drag = true;
            dx = e.clientX;
            dy = e.clientY;
            dagSvg.classList.add('panning');
        }
    });
    window.addEventListener('mousemove', (e) => {
        // Task drag
        if (draggingTask && !draggingPort) {
            const dxSvg = (e.clientX - draggingTask.clientX) / sc;
            const dySvg = (e.clientY - draggingTask.clientY) / sc;
            if (!draggingTask.moved && Math.sqrt(dxSvg * dxSvg + dySvg * dySvg) > 4) {
                draggingTask.moved = true;
                taskDragWasMoved = true;
                dagSvg.style.cursor = 'grabbing';
            }
            if (draggingTask.moved) {
                const newX = draggingTask.origX + dxSvg;
                const newY = draggingTask.origY + dySvg;
                positions[draggingTask.taskKey].x = newX;
                positions[draggingTask.taskKey].y = newY;
                const svgGroup = zg.querySelector(`[data-key="${draggingTask.taskKey}"]`);
                if (svgGroup)
                    svgGroup.setAttribute('transform', `translate(${newX},${newY})`);
                // Move ports with the card in real-time
                if (portsGroup)
                    portsGroup.querySelectorAll(`[data-port-task="${draggingTask.taskKey}"]`).forEach(function (pg) {
                        pg.setAttribute('transform', `translate(${dxSvg},${dySvg})`);
                    });
                redrawEdges();
            }
            return;
        }
        if (draggingPort && ghostPath) {
            const sc2 = svgCoords(e.clientX, e.clientY);
            const sx = draggingPort.startX, sy = draggingPort.startY;
            const cx2 = (sx + sc2.x) / 2;
            ghostPath.setAttribute('d', `M${sx},${sy} C${cx2},${sy} ${cx2},${sc2.y} ${sc2.x},${sc2.y}`);
            // Highlight nearest input port
            portsGroup && portsGroup.querySelectorAll('.port-in').forEach(function (pg) {
                const taskKey2 = pg.dataset.portTask;
                const pp2 = getPortPos(taskKey2, 'in');
                if (!pp2)
                    return;
                const dist = Math.sqrt(Math.pow(sc2.x - pp2.x, 2) + Math.pow(sc2.y - pp2.y, 2));
                pg.style.opacity = dist < 18 ? '1' : '';
            });
            return;
        }
        if (!drag)
            return;
        tx += e.clientX - dx;
        ty += e.clientY - dy;
        dx = e.clientX;
        dy = e.clientY;
        applyT();
    });
    window.addEventListener('mouseup', (e) => {
        if (draggingPort) {
            const sc3 = svgCoords(e.clientX, e.clientY);
            // Find nearest input port within snap radius (20 SVG units)
            var bestTarget = null, bestDist = 20;
            for (var ti = 0; ti < job.tasks.length; ti++) {
                var tsk = job.tasks[ti];
                if (tsk.task_key === draggingPort.taskKey)
                    continue;
                var inPos = getPortPos(tsk.task_key, 'in');
                if (!inPos)
                    continue;
                var dist = Math.sqrt(Math.pow(sc3.x - inPos.x, 2) + Math.pow(sc3.y - inPos.y, 2));
                if (dist < bestDist) {
                    bestDist = dist;
                    bestTarget = tsk.task_key;
                }
            }
            if (bestTarget) {
                var outcome = draggingPort.portType === 'out-true' ? 'true'
                    : draggingPort.portType === 'out-false' ? 'false'
                        : undefined;
                var curDeps = getEffectiveDepsFor(bestTarget);
                var alreadyExists = curDeps.some(function (d) {
                    return d.task_key === draggingPort.taskKey && (d.outcome || undefined) === outcome;
                });
                // Also prevent cycles: don't connect if bestTarget is already an ancestor of draggingPort.taskKey
                if (!alreadyExists) {
                    var newDep = { task_key: draggingPort.taskKey };
                    if (outcome)
                        newDep.outcome = outcome;
                    taskDepsOverride[bestTarget] = curDeps.concat([newDep]);
                    pendingUpdates['tasks.' + bestTarget + '.depends_on'] = taskDepsOverride[bestTarget];
                    markDirtyDirect();
                    redrawEdges();
                }
            }
            if (ghostPath && ghostPath.parentNode)
                ghostPath.parentNode.removeChild(ghostPath);
            ghostPath = null;
            draggingPort = null;
            // Reset port highlights
            portsGroup && portsGroup.querySelectorAll('.port-in').forEach(function (pg) { pg.style.opacity = ''; });
            return;
        }
        if (draggingTask) {
            if (draggingTask.moved) {
                drawPorts();
                redrawEdges();
                dagSvg.style.cursor = '';
            }
            draggingTask = null;
            return;
        }
        drag = false;
        dagSvg.classList.remove('panning');
    });
    function zoomCenter(delta) {
        const w = document.getElementById('canvas-wrap');
        const cx = w.clientWidth / 2, cy = w.clientHeight / 2;
        tx = cx - (cx - tx) * delta;
        ty = cy - (cy - ty) * delta;
        sc *= delta;
        applyT();
    }
    document.getElementById('z-in').addEventListener('click', () => zoomCenter(1.25));
    document.getElementById('z-out').addEventListener('click', () => zoomCenter(0.8));
    document.getElementById('z-fit').addEventListener('click', () => { if (_bbox)
        fitView(_bbox); });
    // ── Right sidebar resize (drag left to expand) ───────────────────────────────
    const resizeHandle = document.getElementById('resize-handle');
    const sidebarInner = document.getElementById('sidebar-inner');
    let resizing = false, resizeStartX = 0, resizeStartW = 0;
    const MIN_W = 180, MAX_W = 600;
    resizeHandle.addEventListener('mousedown', (e) => {
        if (!sidebarOpen)
            return;
        resizing = true;
        resizeStartX = e.clientX;
        resizeStartW = sidebar.offsetWidth;
        resizeHandle.classList.add('dragging');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
        e.preventDefault();
    });
    window.addEventListener('mousemove', (e) => {
        if (!resizing)
            return;
        // Right sidebar: drag left (negative delta) increases width
        const newW = Math.max(MIN_W, Math.min(MAX_W, resizeStartW - (e.clientX - resizeStartX)));
        sidebar.style.width = newW + 'px';
        sidebarInner.style.width = newW + 'px';
    });
    window.addEventListener('mouseup', () => {
        if (!resizing)
            return;
        resizing = false;
        resizeHandle.classList.remove('dragging');
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        if (_bbox)
            fitView(_bbox);
    });
    // ── Bottom bar resize (drag top edge up to expand) ───────────────────────────
    const bottomBarResizeHandle = document.getElementById('bottom-bar-resize');
    let bResizing = false, bResizeStartY = 0, bResizeStartH = 0;
    const MIN_BH = 120, MAX_BH = 520;
    bottomBarResizeHandle.addEventListener('mousedown', (e) => {
        if (!bottomBarOpen)
            return;
        bResizing = true;
        bResizeStartY = e.clientY;
        bResizeStartH = bottomBar.offsetHeight;
        bottomBarResizeHandle.classList.add('dragging');
        document.body.style.cursor = 'row-resize';
        document.body.style.userSelect = 'none';
        e.preventDefault();
    });
    window.addEventListener('mousemove', (e) => {
        if (!bResizing)
            return;
        // Drag up (negative delta) increases height
        const newH = Math.max(MIN_BH, Math.min(MAX_BH, bResizeStartH - (e.clientY - bResizeStartY)));
        bottomBar.style.height = newH + 'px';
        bottomBarHeight = newH;
    });
    window.addEventListener('mouseup', () => {
        if (!bResizing)
            return;
        bResizing = false;
        bottomBarResizeHandle.classList.remove('dragging');
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
    });
    // ── Init ──────────────────────────────────────────────────────────────────────
    positions = computeLayout(job.tasks);
    _bbox = draw(job.tasks, positions);
    showJobPanel();
    requestAnimationFrame(() => fitView(_bbox));
    window.addEventListener('resize', () => { if (_bbox)
        fitView(_bbox); });
})();
