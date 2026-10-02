/* ==========================================================================
   mkvbase control room — Alpine application layer
   One SSE stream from /api/stream drives every panel. State lives in a
   single `d` payload; each card reads it through a getter so the template
   stays declarative and the wire format stays untouched.
   ========================================================================== */

const BLOCK = 25000;          /* ids per heatmap column                       */
const RANGES = [              /* chart window switcher                         */
  { k: '1h',  lab: '1H',  secs: 3600 },
  { k: '6h',  lab: '6H',  secs: 21600 },
  { k: '24h', lab: '24H', secs: 86400 },
  { k: '7d',  lab: '7D',  secs: 604800 },
];

const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g,
  (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

const fmt = (n) => (n == null || isNaN(n) ? '—' : Math.round(n).toLocaleString('en-US'));
const compact = (n) => {
  if (n == null || isNaN(n)) return '—';
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + 'k';
  return String(Math.round(n));
};

/* log line severity — same vocabulary as the previous renderer */
function logClass(l) {
  if (/\bFAIL\b|\berror\b|Traceback/i.test(l)) return 'e';
  if (/\[alive\]|ok |READY|online|seeded/.test(l)) return 'i';
  if (/^\[discovery/.test(l)) return 'm';
  return 'd';
}

function dash() {
  return {
    /* ------------------------------------------------------------ state */
    d: {},                    /* latest SSE payload                            */
    ranges: RANGES,
    range: '24h',
    selBlock: null,
    term: '',
    busy: false,
    pop: null, popTitle: '', popLines: [],
    tip: null, tipStyle: '',
    lastFeedId: null,
    _es: null,
    _ready: false,
    _timer: null,
    _prevRows: null,
    _tween: {},
    _chart: null,

    /* --------------------------------------------------------- lifecycle */
    boot() {
      this._es = new EventSource('/api/stream');
      this._es.onmessage = (e) => { try { this.ingest(JSON.parse(e.data)); } catch (_) {} };
      this._es.onopen = () => { this._ready = true; };
      this._es.onerror = () => { this._ready = false; };
      /* re-render on a wall clock so poll-age / uptime readouts stay honest
         even when the crawler is quiet and no SSE frame arrives */
      this._timer = setInterval(() => { this._ready = this._es.readyState === 1; }, 5000);
    },

    ingest(d) {
      this.d = d;

      const v = d.vault || {};
      if (v.rows != null) {
        if (this._prevRows != null && v.rows > this._prevRows) this.flashRows(v.rows - this._prevRows);
        this._prevRows = v.rows;
        this.tween('rows', v.rows, (n) => fmt(n));
      }
      const h = d.health || {};
      this.tween('health', h.ok_min || 0, (n) => String(Math.round(n)));
      this.tween('rate', (d.velocity || {}).per_min, (n) =>
        n == null ? '—' : ((n >= 0 ? '+' : '') + (+n).toFixed(1) + '<small> rows/min</small>'));
      if (d.db && d.db.used_mb != null) {
        this.tween('dbused', d.db.used_mb, (n) => fmt(n) + '<small> MB used</small>');
      }
      if (d.newest && d.newest.length) this.lastFeedId = d.newest[0].id;
      /* the log pane is reverse-chronological on screen; keep it pinned to the tail */
      requestAnimationFrame(() => this.scrollLog());
    },

    /* -------------------------------------------------------- count-up tween
       Written straight to the DOM (not through Alpine) so the 500ms ramp
       costs 30 cheap text writes instead of 30 reactive re-renders. */
    tween(id, target, fmtFn) {
      const el = document.getElementById(id);
      if (!el || target == null) return;
      const key = '_t_' + id;
      const from = this._tween[key] === undefined ? target : this._tween[key];
      this._tween[key] = target;
      if (Math.abs(from - target) < 1e-9) { el.innerHTML = fmtFn(target); return; }
      const t0 = performance.now(), D = 500;
      const step = (t) => {
        const p = Math.min(1, (t - t0) / D);
        const e = 1 - Math.pow(1 - p, 3);
        const cur = from + (target - from) * e;
        el.innerHTML = fmtFn(cur);
        this._tween[key] = cur;
        if (p < 1) requestAnimationFrame(step);
        else { this._tween[key] = target; el.innerHTML = fmtFn(target); }
      };
      requestAnimationFrame(step);
    },

    flashRows(gain) {
      const el = document.getElementById('rowsdelta');
      if (!el) return;
      el.textContent = '+' + fmt(gain);
      el.classList.remove('pop'); void el.offsetWidth; el.classList.add('pop');
    },

    /* ------------------------------------------------------------ format */
    fmt, compact, esc,

    /* ------------------------------------------------------- connection */
    get dotClass() {
      const age = this.d.log_age_s;
      if (this._ready === false) return 'warn';
      if (age == null || age < 0) return 'dead';
      return age >= 90 ? 'warn' : '';
    },
    get pillText() {
      if (this._ready === false) return 'reconnecting…';
      const h = this.d.health || {};
      if ((h.ok_min || 0) === 0 && (h.fail_min || 0) > 0) return 'crawls failing';
      const age = this.d.log_age_s;
      if (age == null || age < 0) return 'log missing';
      return age >= 90 ? 'stale' : 'live';
    },
    get pillClass() {
      if (this._ready === false) return 'pill warn';
      const h = this.d.health || {};
      if ((h.ok_min || 0) === 0 && (h.fail_min || 0) > 0) return 'pill dead';
      return this.pillText === 'live' ? 'pill' : 'pill warn';
    },
    get pageMeta() {
      const age = this.d.log_age_s;
      return `page ${this.d.uptime_min || 0}m · log ${age == null ? 'missing' : (age < 0 ? 'missing' : age + 's')}`;
    },

    /* ------------------------------------------------------------- clock */
    get _skew() { return (this.d.clock || {}).skew_s; },
    get clockText() {
      const s = this._skew;
      return s == null ? '—' : (s > 0 ? '+' : '') + s + 's';
    },
    get clockStyle() {
      const a = Math.abs(this._skew || 0);
      return 'color:' + (a > 120 ? 'var(--red)' : a > 30 ? 'var(--amb)' : 'var(--grn)');
    },
    get clockTitle() {
      const s = this._skew;
      if (s == null) return 'clock drift unknown';
      return `PC clock vs mkvbase server: ${s}s. Signed search URLs break past ~30s.`;
    },
    get clockChecked() {
      const t = (this.d.clock || {}).checked_at;
      return t ? Math.round(Date.now() / 1000 - t) + 's ago' : '—';
    },

    /* ------------------------------------------------------------ vault */
    get vault() { return this.d.vault || {}; },
    get rowChips() {
      const v = this.vault;
      if (v.rows == null) return `<span class="chip r">${esc(v.error || 'no data')}</span>`;
      const st = this.d.site || {};
      const smax = st.site_max_id || v.max_id || 0;
      const lag = Math.max(0, smax - (v.max_id || 0));
      const lagCls = lag > 60 ? 'r' : lag > 15 ? 'a' : 'g';
      const polled = st.seen_at ? Math.round(Date.now() / 1000 - st.seen_at) : null;
      return [
        `<span class="chip" title="newest id in the vault">vault id <b>${fmt(v.max_id)}</b></span>`,
        `<span class="chip" title="newest id seen on mkvbase.site${polled != null ? ` (polled ${polled}s ago)` : ''}">site id <b>${fmt(st.site_max_id)}</b></span>`,
        `<span class="chip" title="uploads the vault has not caught yet">lag <b class="${lagCls}">${fmt(lag)}</b></span>`,
        `<span class="chip">not yet mined <b class="a">${fmt(smax - (v.rows || 0))}</b></span>`,
        (v.shards > 1 ? '<span class="chip" title="overflow cluster B active for new ids"><b class="p">2 shards</b></span>' : ''),
      ].join('');
    },

    get ringStyle() {
      const p = this.vault.coverage_pct;
      return p == null ? '' : `stroke-dasharray:${(p / 100 * 301.59).toFixed(1)} 302`;
    },
    get covPct() {
      const p = this.vault.coverage_pct;
      return p == null ? '—' : p.toFixed(1) + '%';
    },
    get covSub() {
      const v = this.vault;
      if (v.max_id == null) return 'waiting for vault head';
      return `${fmt(v.rows)} of ${fmt(v.max_id)} site ids · <b>${fmt((v.max_id || 0) - (v.rows || 0))}</b> still missing`;
    },
    get covChips() {
      return (this.vault.thin || []).slice(0, 4).map((b) =>
        `<span class="chip clickable" @click="jumpBlock(${b.block})">b${b.block * 25}k <b>${fmt(b.have)}</b></span>`).join('');
    },

    /* ---------------------------------------------------------- velocity */
    get vel() { return this.d.velocity || {}; },
    get barH1() { return 'width:' + this._velPct(this.vel.h1) + '%'; },
    get barH24() { return 'width:' + this._velPct(this.vel.h24) + '%'; },
    get vh1() { return this.vel.h1 == null ? '—' : '+' + fmt(this.vel.h1); },
    get vh24() { return this.vel.h24 == null ? '—' : '+' + fmt(this.vel.h24); },
    _velPct(n) {
      const mx = Math.max(this.vel.h1 || 0, this.vel.h24 || 0, 1);
      return Math.min(100, (n || 0) / mx * 100).toFixed(1);
    },
    get rateSub() {
      return this.vel.per_min == null ? 'building history (5-min samples)…' : 'rolling 30-min average';
    },

    /* ------------------------------------------------------------ health */
    get barOk() {
      const h = this.d.health || {}, den = Math.max((h.ok_min || 0) + (h.fail_min || 0), 1);
      return 'width:' + ((h.ok_min || 0) / den * 100).toFixed(1) + '%';
    },
    get barFail() {
      const h = this.d.health || {}, den = Math.max((h.ok_min || 0) + (h.fail_min || 0), 1);
      return 'width:' + ((h.fail_min || 0) / den * 100).toFixed(1) + '%';
    },
    get vok() { return String((this.d.health || {}).ok_min || 0); },
    get vfail() { return String((this.d.health || {}).fail_min || 0); },
    get healthSub() {
      const h = this.d.health || {}, ok = h.ok_min || 0, bad = h.fail_min || 0;
      if (ok === 0 && bad > 0) return '<b class="r">all crawls failing — check clock badge / session</b>';
      if (bad > 0) return `<span class="a">${bad} failing — usually transient challenges</span>`;
      return 'all lanes healthy';
    },

    /* -------------------------------------------------------------- mongo */
    get db() { return this.d.db || {}; },
    get _dbPct() {
      const db = this.db;
      if (!db.used_mb || !db.total_mb) return null;
      return Math.min(100, db.used_mb / db.total_mb * 100);
    },
    get dbTag() {
      const p = this._dbPct;
      if (p == null) return 'waiting for dbStats';
      if (p >= 90) return '⚠ quota nearly full — raise the Atlas tier';
      if (p >= 75) return 'quota getting tight';
      return 'live from dbStats';
    },
    get dbPct() {
      const p = this._dbPct;
      if (p == null) return '';
      const cls = p >= 90 ? 'r' : p >= 75 ? 'a' : 'g';
      return `<b class="${cls}">${p.toFixed(1)}%</b> of ${(this.db.total_mb / 1024).toFixed(2)} GB quota`;
    },
    get dbDataW() {
      const p = this._dbPct; if (p == null) return 'width:0%';
      return 'width:' + Math.min(p, (this.db.data_mb || 0) / this.db.total_mb * 100).toFixed(2) + '%';
    },
    get dbIdxW() {
      const p = this._dbPct; if (p == null) return 'width:0%';
      const dp = Math.min(p, (this.db.data_mb || 0) / this.db.total_mb * 100);
      return 'width:' + Math.max(0, Math.min(p - dp, (this.db.idx_mb || 0) / this.db.total_mb * 100)).toFixed(2) + '%';
    },
    get dbFreeW() {
      const p = this._dbPct; if (p == null) return 'width:0%';
      return 'width:' + Math.max(0, 100 - p).toFixed(2) + '%';
    },
    get dbChips() {
      const db = this.db;
      if (!db.total_mb) return '';
      return [
        `<span class="chip">rows <b>${fmt(db.rows)}</b></span>`,
        `<span class="chip">data <b>${fmt(db.data_mb)} MB</b></span>`,
        `<span class="chip">indexes <b>${fmt(db.idx_mb)} MB</b></span>`,
        `<span class="chip">avg doc <b>${db.avg_b ? Math.round(db.avg_b) + ' B' : '—'}</b></span>`,
        `<span class="chip">free <b class="g">${fmt(db.total_mb - db.used_mb)} MB</b></span>`,
      ].join('');
    },
    /* NEW: headroom projection */
    get dbFree() {
      const db = this.db;
      return db.total_mb ? fmt(db.total_mb - db.used_mb) + ' MB' : '—';
    },
    get dbRunway() {
      const db = this.db, free = (db.total_mb || 0) - (db.used_mb || 0);
      const perDay = this._perDayMb();
      if (!perDay || perDay <= 0) return 'no growth signal yet';
      const days = free / perDay;
      if (!isFinite(days)) return '—';
      const basis = ` at ${perDay.toFixed(1)} MB/day over ${this._coverH.toFixed(1)}h`;
      if (days < 1) return `<b class="r">under a day</b>${basis}`;
      return `<b class="${days < 30 ? 'a' : 'g'}">${days < 60 ? days.toFixed(0) + ' days' : (days / 30).toFixed(1) + ' months'}</b>${basis}`;
    },
    _perDayMb() {
      /* extrapolate from the span we actually have, not from the requested
         range: the buffer is young, and using the range would overstate growth */
      const g = this._chartGain();
      const hrs = this._coverH;
      if (g == null || g <= 0 || !hrs || hrs < 0.5) return null;
      const rowsPerDay = (g / hrs) * 24;
      const avg = this.db.avg_b || 0;
      if (!avg) return null;
      return rowsPerDay * avg / (1024 * 1024);
    },
    /* NEW: index overhead */
    get dbIdxPct() {
      const db = this.db;
      if (!db.data_mb || !db.idx_mb) return '—';
      return (db.idx_mb / db.data_mb * 100).toFixed(1) + '%';
    },
    get dbIdxSub() {
      const db = this.db;
      if (!db.idx_mb) return '';
      return `<b>${fmt(db.idx_mb)} MB</b> of indexes on top of <b>${fmt(db.data_mb)} MB</b> data`;
    },
    /* NEW: doc economics */
    get dbAvg() {
      const db = this.db;
      return db.avg_b ? Math.round(db.avg_b) + ' B' : '—';
    },
    get dbAvgSub() {
      const db = this.db;
      if (!db.rows || !db.used_mb) return '';
      return `<b>${fmt(db.rows)}</b> docs · <b>${fmt(db.used_mb / db.rows * 1024)} KB</b> per doc all-in`;
    },

    /* ------------------------------------------------------------ heatmap */
    get blocks() {
      const bs = this.vault.blocks || [];
      return bs.map((b) => {
        const pct = Math.min(100, Math.round(100 * b.n / BLOCK));
        const color = pct >= 60 ? 'var(--grn)' : pct >= 40 ? 'var(--cyn)'
          : pct >= 15 ? 'var(--amb)' : 'var(--red)';
        return {
          block: b.block, n: b.n, pct,
          lab: b.block * 25 + 'k',
          style: `height:${pct}%;background:${color}`,
          title: `ids ${fmt(b.block * BLOCK)}–${fmt((b.block + 1) * BLOCK - 1)} · ${fmt(b.n)}/${fmt(BLOCK)} (${pct}%)`,
        };
      });
    },
    get thinChips() {
      const t = this.blocks.filter((b) => b.pct < 60).sort((a, b) => a.pct - b.pct).slice(0, 3);
      if (!t.length) return '';
      return 'thinnest: ' + t.map((x) =>
        `<b class="r clickable" @click="jumpBlock(${x.block})">${x.lab} · ${x.pct}%</b>`).join(' · ');
    },
    jumpBlock(n) { this.selBlock = this.selBlock === n ? null : n; },
    get blockInfo() {
      if (this.selBlock == null) return null;
      const b = this.blocks.find((x) => x.block === this.selBlock);
      if (!b) return null;
      return {
        no: b.block,
        range: fmt(b.block * BLOCK) + '–' + fmt((b.block + 1) * BLOCK - 1),
        have: fmt(b.n),
        pct: b.pct + '%',
        style: `color:${b.pct >= 60 ? 'var(--grn)' : b.pct >= 40 ? 'var(--cyn)' : b.pct >= 15 ? 'var(--amb)' : 'var(--red)'}`,
        missing: fmt(Math.max(0, BLOCK - b.n)),
        verdict: b.pct >= 60 ? 'good — back off' : b.pct >= 40 ? 'nearly there' : b.pct >= 15 ? 'filling' : 'needs work',
      };
    },

    /* -------------------------------------------------------------- chart */
    get _rangeSecs() { return (RANGES.find((r) => r.k === this.range) || RANGES[2]).secs; },
    get _pts() {
      const h = this.d.history || [];
      if (h.length < 2) return [];
      const cut = h[h.length - 1][0] - this._rangeSecs * 1000;
      return h.filter((p) => p[0] >= cut);
    },
    _chartGain() {
      const p = this._pts;
      return p.length < 2 ? null : p[p.length - 1][1] - p[0][1];
    },
    get chartSvg() {
      const pts = this._pts;
      const L = 2, R = 612, T = 10, B = 140;
      if (pts.length < 2) { this._chart = null; return ''; }

      const vs = pts.map((p) => p[1]);
      let mn = Math.min(...vs), mx = Math.max(...vs);
      const span = (mx - mn) || 1; mn -= span * .06; mx += span * .06;
      const n = pts.length;
      const X = (i) => L + (i / (n - 1)) * (R - L);
      const Y = (v) => T + (1 - (v - mn) / (mx - mn)) * (B - T);

      let dpath = 'M' + X(0).toFixed(1) + ' ' + Y(vs[0]).toFixed(1);
      for (let i = 1; i < n; i++) dpath += ' L' + X(i).toFixed(1) + ' ' + Y(vs[i]).toFixed(1);
      const grid = (y) => `<line x1="${L}" y1="${y}" x2="${R}" y2="${y}" stroke="rgba(154,172,207,.09)" stroke-dasharray="3 4"/>`;

      this._chart = { pts, vs, X, Y, L, R, n };
      this._mn = mn; this._mx = mx;

      return grid(T) + grid((T + B) / 2) + grid(B) +
        `<path d="${dpath} L${R} ${B} L${L} ${B} Z" fill="url(#gr)"/>` +
        `<path d="${dpath}" fill="none" stroke="#60a5fa" stroke-width="1.6"/>`;
    },
    get ylMax() { return this._mx == null ? '—' : fmt(this._mx); },
    get ylMid() { return this._mx == null ? '—' : fmt((this._mx + this._mn) / 2); },
    get ylMin() { return this._mn == null ? '—' : fmt(this._mn); },
    get chartSum() {
      const pts = this._pts;
      if (pts.length < 2) return 'collecting history…';
      const gain = pts[pts.length - 1][1] - pts[0][1];
      const now = fmt(pts[pts.length - 1][1]);
      /* the sample buffer is 5-min granularity and only grows with uptime,
         so report what is actually on screen rather than the requested range */
      const hrs = this._coverH;
      const cover = hrs == null ? this.range
        : hrs < 1 ? Math.max(1, Math.round(hrs * 60)) + 'm'
        : hrs < 48 ? hrs.toFixed(1) + 'h'
        : (hrs / 24).toFixed(1) + 'd';
      return gain > 0 ? `+${fmt(gain)} rows / ${cover} · now ${now}` : `now ${now}`;
    },
    /* hours of history actually present in the current window (null if unknown) */
    get _coverH() {
      const p = this._pts;
      return p.length < 2 ? null : (p[p.length - 1][0] - p[0][0]) / 3600;
    },
    get xLeft() {
      const p = this._pts;
      if (!p.length) return '—';
      return new Date(p[0][0] * 1000).toLocaleString('en-GB', { hour: '2-digit', minute: '2-digit', day: '2-digit', month: 'short' });
    },
    get xRight() {
      const p = this._pts;
      if (!p.length) return '—';
      return new Date(p[p.length - 1][0] * 1000).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
    },
    /* hover crosshair */
    chartMove(ev) {
      const c = this._chart;
      if (!c || !c.n) { this.tip = null; return; }
      const svg = ev.currentTarget.querySelector('svg');
      const box = svg.getBoundingClientRect();
      const vx = (ev.clientX - box.left) / box.width * 620;
      let i = Math.round((vx - c.L) / (c.R - c.L) * (c.n - 1));
      i = Math.max(0, Math.min(c.n - 1, i));
      const when = new Date(c.pts[i][0] * 1000)
        .toLocaleString('en-GB', { hour: '2-digit', minute: '2-digit', day: '2-digit', month: 'short' });
      this.tip = `${when} · ${fmt(c.vs[i])} rows`;
      this.tipStyle = `left:${Math.round((ev.clientX - box.left) - 4)}px;top:6px`;
    },

    /* ---------------------------------------------------------- scoreboard */
    get sb() { return this.d.scoreboard || {}; },
    get sbTag() {
      const s = this.sb;
      return 'cov ' + (s.cov != null ? s.cov + '%' : '—') + ' · blk ' +
        (s.block_start != null ? Math.round(s.block_start / 1000) + 'k' : '—');
    },
    get sbH1() { return this.sb.h1 != null ? fmt(this.sb.h1) : '—'; },
    get sbH2() { return this.sb.h2 != null ? fmt(this.sb.h2) : '—'; },
    get sbSearches() { return fmt(this.sb.searches); },
    get sbYield() { return this.sb.yield != null ? this.sb.yield : '—'; },
    get sbCapped() { return this.sb.capped_pct != null ? this.sb.capped_pct + '%' : '—'; },
    get sbRetries() { return fmt(this.sb.retries); },
    get sbPerMin() { return this.sb.per_min != null ? (+this.sb.per_min).toFixed(1) : '—'; },
    get sbKind() { return this.sb.top_kind || '—'; },
    get jackpots() { return this.sb.jackpots || []; },
    /* NEW: era/block context strip */
    get sbContext() {
      const s = this.sb, t = this.d.idgap || {};
      return `era <b>${esc(s.era || '—')}</b> · block <b>${s.block != null ? Math.round(s.block / 1000) + 'k' : '—'}</b>` +
        ` · terms searched <b>${fmt(t.terms)}</b> · new rows <b class="p">+${fmt(t.new_rows)}</b>`;
    },

    /* NEW: lane dispatch table */
    get lanes() {
      const lanes = (this.d.fleet || {}).lanes || {};
      const entries = Object.entries(lanes);
      const total = entries.reduce((a, [, v]) => a + v, 0) || 1;
      const hue = { priority: 'var(--red)', day: 'var(--amb)', year: 'var(--blu)', alpha: 'var(--pur)', words: 'var(--cyn)', series: 'var(--grn)', facet: 'var(--txt)' };
      return entries
        .map(([name, n]) => ({ name, n, pct: (n / total * 100).toFixed(1),
          style: `width:${(n / total * 100).toFixed(1)}%;background:${hue[name] || 'var(--dim)'}` }))
        .sort((a, b) => b.n - a.n);
    },
    get laneTag() {
      const l = this.lanes;
      return l.length ? `${l.length} lanes · ${fmt(l.reduce((a, x) => a + x.n, 0))} dispatched` : 'no session';
    },
    get laneSub() {
      const l = this.lanes;
      if (!l.length) return 'no dispatch recorded this session';
      const top = l[0];
      return `heaviest lane <b class="a">${esc(top.name)}</b> at <b>${top.pct}%</b>` +
        (l.length > 1 ? ` · lightest <b>${esc(l[l.length - 1].name)}</b> ${l[l.length - 1].pct}%` : '');
    },

    /* NEW: idgap term leaderboard */
    get terms() {
      const ts = (this.d.idgap || {}).term_stats || {};
      return Object.entries(ts)
        .map(([name, v]) => {
          const tries = v.tries || 0, nu = v.new || 0;
          return { name, tries, new: nu,
            hit: tries ? (nu / tries * 100).toFixed(1) + '%' : '—' };
        })
        .sort((a, b) => b.new - a.new);
    },
    get termTag() {
      const t = this.d.idgap || {};
      return `${fmt(t.terms)} terms searched`;
    },
    get termSub() {
      const t = this.terms;
      if (!t.length) return 'no term statistics yet';
      const tot = t.reduce((a, x) => a + x.new, 0);
      return `<b>${fmt(tot)}</b> new rows from these terms · <b>${fmt(t.reduce((a, x) => a + x.tries, 0))}</b> tries`;
    },

    /* -------------------------------------------------------------- feeds */
    get newest() { return this.d.newest || []; },
    get logHtml() {
      return (this.d.log || []).map((l) => `<div class="${logClass(l)}">${esc(l)}</div>`).join('');
    },
    scrollLog() {
      const el = this.$refs && this.$refs.log;
      if (el) el.scrollTop = el.scrollHeight;
    },

    /* -------------------------------------------------------------- fleet */
    get fleet() { return this.d.fleet || {}; },
    get fleetTag() {
      const f = this.fleet;
      return `crawler up ${f.uptime_min || 0}m · session ${f.session || '?'}`;
    },
    get fleetV() {
      const f = this.fleet, t = f.tick || {};
      const tot = (t.agents || 0) + (f.idgap_agents || 0);
      return `${tot} <span class="g">agents</span> <small style="font-size:14px;color:var(--dim)">(${t.agents || 0} discovery + ${f.idgap_agents || 0} idgap)</small>`;
    },
    get fleetSub() {
      const f = this.fleet, t = f.tick || {}, ig = this.d.idgap || {};
      return `done <b>${fmt(t.done)}</b> · queued <b>${fmt(t.queued)}</b> · rows mined <b>${fmt(t.rows)}</b>` +
        (ig.terms ? ` · idgap <b class="p">+${fmt(ig.new_rows)}</b>` : '');
    },
    get lanesHtml() {
      const lanes = (this.fleet || {}).lanes || {};
      return Object.entries(lanes).map(([k, v]) =>
        `<span class="lane">${esc(k)} <b>${compact(v)}</b></span>`).join('');
    },
    get srcBar() {
      const cols = ['#34d399', '#60a5fa', '#a78bfa', '#fbbf24', '#f87171', '#22d3ee'];
      return (this.d.sources || []).map((s, i) =>
        `<div style="width:${s.pct}%;background:${cols[i % 6]}" title="${esc(s.name)}: ${fmt(s.n)} (${s.pct}%)"></div>`).join('');
    },
    get srcLeg() {
      return (this.d.sources || []).map((s) => `${esc(s.name)} ${s.pct}%`).join(' · ');
    },
    get renderSub() {
      const r = this.d.render || {};
      return r.ok ? `push target healthy · <b>${fmt(r.rows)}</b> rows served`
        : 'push target <b class="r">unreachable</b>';
    },

    /* NEW: render serve panel */
    get render() { return this.d.render || {}; },
    get renderDot() {
      const r = this.render;
      return r.ok ? 'ok' : (r.err ? 'bad' : 'unknown');
    },
    get renderState() {
      const r = this.render;
      return r.ok ? 'online' : (r.err ? 'error' : 'unknown');
    },
    get renderTag() {
      const r = this.render;
      return r.err ? esc(String(r.err).slice(0, 40)) : 'pro-movieapidrive.onrender.com';
    },
    get renderNote() {
      const r = this.render;
      if (r.err) return `<span class="r">${esc(String(r.err))}</span>`;
      if (!r.ok) return 'waiting on first probe';
      return `serve-only mode: the box pushes, Render never pulls.`;
    },

    /* NEW: site crawler panel */
    get site() { return this.d.site || {}; },
    get lag() {
      const st = this.site, v = this.vault;
      return Math.max(0, (st.site_max_id || v.max_id || 0) - (v.max_id || 0));
    },
    get lagCls() {
      const l = this.lag;
      return l > 60 ? 'r' : l > 15 ? 'a' : 'g';
    },
    get polledText() {
      const t = this.site.seen_at;
      return t ? Math.round(Date.now() / 1000 - t) + 's ago' : '—';
    },
    get siteTag() {
      return this.site.site_max_id ? 'mkvbase.site head' : 'no poll yet';
    },
    get siteSub() {
      const st = this.site, v = this.vault;
      if (!st.site_max_id) return 'waiting for the site watcher';
      const behind = Math.max(0, (st.site_max_id || 0) - (v.rows || 0));
      return `<b>${fmt(behind)}</b> site uploads not in the vault yet`;
    },
    get siteNote() {
      const l = this.lag;
      if (l > 60) return '<span class="r">pusher is falling behind the site</span>';
      if (l > 15) return '<span class="a">slight lag — usually a slow poll</span>';
      return '<span class="g">vault is level with the site</span>';
    },

    /* ------------------------------------------------------- vault it flow */
    async vaultIt() {
      const term = this.term.trim();
      if (!term || this.busy) return;
      this.busy = true;
      this.pop = true;
      this.popTitle = `vaulting '${term}'`;
      this.popLines = [{ cls: '', html: 'crawling mkvbase with the shared session…' }];
      try {
        const r = await fetch('/api/search?term=' + encodeURIComponent(term), { method: 'POST' });
        const d = await r.json();
        if (d.ok) {
          this.popTitle = `'${term}'`;
          this.popLines = [
            { cls: 'ok', html: `<b>${fmt(d.rows)}</b> rows · <b class="ok">+${fmt(d.new)} new</b>, ${fmt(d.updated)} updated · ${esc(d.push)}` },
            ...(d.top || []).map((x) => ({ cls: '', html: `<b>${esc(x.id)}</b> ${esc(x.title)}` })),
          ];
          this.term = '';
        } else {
          this.popTitle = 'failed';
          this.popLines = [{ cls: 'err', html: esc(d.err || d.err_ || 'error') }];
        }
      } catch (e) {
        this.popTitle = 'failed';
        this.popLines = [{ cls: 'err', html: esc(String(e)) }];
      } finally {
        this.busy = false;
      }
    },
  };
}

/* register before Alpine auto-starts */
document.addEventListener('alpine:init', () => { Alpine.data('dash', dash); });