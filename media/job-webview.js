// @ts-nocheck — untyped webview script; type annotations to be added incrementally
(function () {
    const vscode = acquireVsCodeApi();
    const jobs = window.__JOB_LIST__;
    const searchInput = document.getElementById('search');
    const searchClear = document.getElementById('search-clear');
    const jobList = document.getElementById('job-list');
    if (!jobs) {
        return;
    } // still loading
    // Persist collapse state across view reloads
    let colState = {};
    try {
        colState = vscode.getState()?.col || {};
    }
    catch (e) { }
    function saveState() { try {
        vscode.setState({ col: colState });
    }
    catch (e) { } }
    function esc(s) {
        return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }
    function fuzzyMatch(q, target) {
        const ql = q.toLowerCase(), tl = target.toLowerCase();
        let qi = 0;
        for (let i = 0; i < tl.length && qi < ql.length; i++) {
            if (tl[i] === ql[qi]) {
                qi++;
            }
        }
        return qi === ql.length;
    }
    function fuzzyHighlight(q, text) {
        if (!q) {
            return esc(text);
        }
        const ql = q.toLowerCase();
        let out = '', qi = 0;
        for (let i = 0; i < text.length; i++) {
            const ch = text[i];
            if (qi < ql.length && ch.toLowerCase() === ql[qi]) {
                out += '<mark>' + esc(ch) + '</mark>';
                qi++;
            }
            else {
                out += esc(ch);
            }
        }
        return out;
    }
    // Group jobs by category → subcategory
    const grouped = new Map();
    for (const job of jobs) {
        if (!grouped.has(job.category)) {
            grouped.set(job.category, new Map());
        }
        const byCat = grouped.get(job.category);
        if (!byCat.has(job.subCategory)) {
            byCat.set(job.subCategory, []);
        }
        byCat.get(job.subCategory).push(job);
    }
    const CAT_ORDER = ['app_jobs', 'etl_jobs'];
    function jobItemHtml(job, idx, flat, query) {
        const name = flat ? fuzzyHighlight(query, job.name) : esc(job.name);
        const subLine = flat
            ? '<div class="job-item-sub">' + esc(job.subCategory) + '</div>'
            : '';
        return '<div class="job-item' + (flat ? ' flat' : '') + '" data-job="' + idx + '">'
            + '<span class="job-item-icon">◈</span>'
            + '<div style="flex:1;min-width:0"><div class="job-item-name">' + name + '</div>' + subLine + '</div>'
            + '<span class="job-item-count">' + esc(String(job.tasks.length)) + '</span>'
            + '<button class="job-open-btn" data-open="' + esc(job.filePath) + '" title="Open YAML">↗</button>'
            + '</div>';
    }
    function renderNormal() {
        if (jobs.length === 0) {
            jobList.innerHTML = '<div class="info-msg">No jobs found in workspace.</div>';
            return;
        }
        let html = '';
        const cats = [...grouped.keys()].sort(function (a, b) {
            const ia = CAT_ORDER.indexOf(a), ib = CAT_ORDER.indexOf(b);
            if (ia >= 0 && ib >= 0) {
                return ia - ib;
            }
            if (ia >= 0) {
                return -1;
            }
            if (ib >= 0) {
                return 1;
            }
            return a.localeCompare(b);
        });
        for (const cat of cats) {
            const byCat = grouped.get(cat);
            const label = cat === 'app_jobs' ? 'App Jobs' : cat === 'etl_jobs' ? 'ETL Jobs' : cat;
            const catOpen = colState['c:' + cat] !== false;
            const total = [...byCat.values()].reduce(function (n, arr) { return n + arr.length; }, 0);
            html += '<div data-cat="' + esc(cat) + '">'
                + '<div class="cat-header" data-tcl="c:' + esc(cat) + '" data-body="cb:' + esc(cat) + '">'
                + '<span class="arrow' + (catOpen ? ' open' : '') + '">▶</span>'
                + '<span>' + esc(label) + '</span>'
                + '<span class="hdr-count">' + total + '</span>'
                + '</div>'
                + '<div id="cb:' + esc(cat) + '" style="' + (catOpen ? '' : 'display:none') + '">';
            const subs = [...byCat.keys()].sort();
            for (const sub of subs) {
                const subjobs = byCat.get(sub);
                const subKey = 's:' + cat + ':' + sub;
                const subOpen = colState[subKey] !== false;
                html += '<div>'
                    + '<div class="subcat-header" data-tcl="' + esc(subKey) + '" data-body="' + esc(subKey) + 'b">'
                    + '<span class="arrow' + (subOpen ? ' open' : '') + '">▶</span>'
                    + '<span>' + esc(sub) + '</span>'
                    + '<span class="hdr-count">' + subjobs.length + '</span>'
                    + '</div>'
                    + '<div id="' + esc(subKey) + 'b" style="' + (subOpen ? '' : 'display:none') + '">';
                for (const job of subjobs) {
                    html += jobItemHtml(job, jobs.indexOf(job), false, '');
                }
                html += '</div></div>';
            }
            html += '</div></div>';
        }
        jobList.innerHTML = html;
    }
    function renderSearch(query) {
        const matches = jobs.filter(function (j) { return fuzzyMatch(query, j.name); });
        if (matches.length === 0) {
            jobList.innerHTML = '<div class="info-msg">No jobs match <strong>' + esc(query) + '</strong></div>';
            return;
        }
        jobList.innerHTML = matches.map(function (job) {
            return jobItemHtml(job, jobs.indexOf(job), true, query);
        }).join('');
    }
    function render() {
        const q = searchInput.value.trim();
        searchClear.style.display = q ? 'block' : 'none';
        if (q) {
            renderSearch(q);
        }
        else {
            renderNormal();
        }
    }
    // Event delegation on the list
    jobList.addEventListener('click', function (e) {
        // Open file button
        const openBtn = e.target.closest('[data-open]');
        if (openBtn) {
            e.stopPropagation();
            vscode.postMessage({ command: 'openFile', filePath: openBtn.dataset.open });
            return;
        }
        // Toggle collapse
        const hdr = e.target.closest('[data-tcl]');
        if (hdr) {
            const key = hdr.dataset.tcl;
            const bodyId = hdr.dataset.body;
            const body = document.getElementById(bodyId);
            const arrow = hdr.querySelector('.arrow');
            if (!body) {
                return;
            }
            const open = body.style.display !== 'none';
            body.style.display = open ? 'none' : '';
            if (arrow) {
                arrow.classList.toggle('open', !open);
            }
            colState[key] = !open;
            saveState();
            return;
        }
        // Open job DAG
        const item = e.target.closest('[data-job]');
        if (item) {
            const idx = parseInt(item.dataset.job, 10);
            vscode.postMessage({ command: 'openDag', job: jobs[idx], allJobNames: jobs.map(function (j) { return j.name; }) });
        }
    });
    searchInput.addEventListener('input', render);
    searchClear.addEventListener('click', function () {
        searchInput.value = '';
        render();
        searchInput.focus();
    });
    render();
})();
