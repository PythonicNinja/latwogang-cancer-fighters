const PLN = new Intl.NumberFormat("pl-PL", {
  style: "currency",
  currency: "PLN",
  maximumFractionDigits: 0,
});
const PLN_DETAILED = new Intl.NumberFormat("pl-PL", {
  style: "currency",
  currency: "PLN",
});
const INT = new Intl.NumberFormat("pl-PL");
const DT = new Intl.DateTimeFormat("pl-PL", {
  dateStyle: "medium",
  timeStyle: "short",
});
const D = new Intl.DateTimeFormat("pl-PL", { dateStyle: "medium" });

const PAGE_SIZE = 100;

const PALETTE = [
  "#fb2e60",
  "#f97316",
  "#f59e0b",
  "#10b981",
  "#06b6d4",
  "#3b82f6",
  "#8b5cf6",
  "#ec4899",
];

function fmtPLN(grosze) {
  if (grosze == null || Number.isNaN(grosze)) return "—";
  const pln = grosze / 100;
  return Math.abs(pln) >= 1000 ? PLN.format(pln) : PLN_DETAILED.format(pln);
}

function fmtInt(n) {
  if (n == null || Number.isNaN(n)) return "—";
  return INT.format(n);
}

function fmtDateTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : DT.format(d);
}

function fmtDate(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : D.format(d);
}

function initial(name) {
  if (!name) return "?";
  const t = name.trim();
  return (t[0] || "?").toUpperCase();
}

function colorFor(name) {
  if (!name) return PALETTE[0];
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) | 0;
  return PALETTE[Math.abs(h) % PALETTE.length];
}

function chartTextColor() {
  return document.documentElement.classList.contains("dark")
    ? "#a1a1aa"
    : "#52525b";
}

function chartGridColor() {
  return document.documentElement.classList.contains("dark")
    ? "rgba(255,255,255,0.06)"
    : "rgba(0,0,0,0.06)";
}

function applyChartTheme() {
  if (typeof Chart === "undefined") return;
  Chart.defaults.color = chartTextColor();
  Chart.defaults.borderColor = chartGridColor();
  Chart.defaults.font.family =
    "ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Inter, sans-serif";
}

const DUCKDB_ESM =
  "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

let duckdbConnPromise = null;

async function getDuckConn() {
  if (duckdbConnPromise) return duckdbConnPromise;
  duckdbConnPromise = (async () => {
    const duckdb = await import(DUCKDB_ESM);
    const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], {
        type: "text/javascript",
      }),
    );
    const worker = new Worker(workerUrl);
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    const url = new URL(
      "data/payments.parquet",
      window.location.href,
    ).toString();
    await db.registerFileURL(
      "payments.parquet",
      url,
      duckdb.DuckDBDataProtocol.HTTP,
      false,
    );
    const conn = await db.connect();
    await conn.query(
      `CREATE VIEW payments AS SELECT * FROM 'payments.parquet'`,
    );
    return conn;
  })().catch((err) => {
    duckdbConnPromise = null;
    throw err;
  });
  return duckdbConnPromise;
}

const SORT_COLUMNS = {
  amount_desc: '"amount" DESC',
  amount_asc: '"amount" ASC',
  date_desc: '"at" DESC',
  date_asc: '"at" ASC',
};

function csvCell(v) {
  const s = v == null ? "" : String(v);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function toCsv(rows, fields) {
  const out = [fields.join(",")];
  for (const r of rows) out.push(fields.map((f) => csvCell(r[f])).join(","));
  return out.join("\n");
}

function arrowToObjects(result) {
  return result.toArray().map((r) => r.toJSON());
}

function dashboard() {
  return {
    stats: null,
    theme: localStorage.getItem("theme") || "dark",
    donorTab: "all",
    donorTabs: [
      { id: "all", label: "Wszyscy" },
      { id: "company", label: "Firmy" },
      { id: "individual", label: "Osoby" },
    ],
    hourDay: "all",
    csv: {
      loading: false,
      loaded: false,
      querying: false,
      error: null,
      totalCount: 0,
      matchCount: 0,
      matchSumGrosze: 0,
      pageRows: [],
      _seq: 0,
      _debounce: null,
    },
    filter: {
      q: "",
      min: null,
      max: null,
      onlyComments: false,
      onlyCompanies: false,
      dateFrom: "",
      dateTo: "",
      sort: "amount_desc",
      page: 1,
    },
    charts: {},

    async init() {
      if (this._initted) return;
      this._initted = true;
      this.applyTheme();
      try {
        const res = await fetch("data/stats.json", { cache: "no-cache" });
        if (!res.ok) throw new Error(`stats: ${res.status}`);
        this.stats = await res.json();
      } catch (err) {
        console.error("failed to load stats.json", err);
        return;
      }
      this.$nextTick(() => this.renderCharts());
      const resetPage = () => {
        this.filter.page = 1;
        this.scheduleQuery();
      };
      this.$watch("filter.q", resetPage);
      this.$watch("filter.min", resetPage);
      this.$watch("filter.max", resetPage);
      this.$watch("filter.onlyComments", resetPage);
      this.$watch("filter.onlyCompanies", resetPage);
      this.$watch("filter.dateFrom", resetPage);
      this.$watch("filter.dateTo", resetPage);
      this.$watch("filter.sort", resetPage);
      this.$watch("filter.page", () => this.scheduleQuery(0));
    },

    fmtPLN,
    fmtInt,
    fmtDateTime,
    fmtDate,
    initial,
    colorFor,

    fmtDateOnly(iso) {
      if (!iso) return "—";
      const d = new Date(iso + "T00:00:00");
      return Number.isNaN(d.getTime()) ? iso : D.format(d);
    },

    genderColor(g) {
      return g === "F" ? "#ec4899" : g === "M" ? "#3b82f6" : g === "company" ? "#f59e0b" : "#71717a";
    },
    genderEmoji(g) {
      return g === "F" ? "♀" : g === "M" ? "♂" : g === "company" ? "★" : "?";
    },
    genderLabel(g) {
      return g === "F" ? "Kobieta" : g === "M" ? "Mężczyzna" : g === "company" ? "Firma" : "Nieokreślone";
    },

    applyTheme() {
      const root = document.documentElement;
      if (this.theme === "dark") root.classList.add("dark");
      else root.classList.remove("dark");
      localStorage.setItem("theme", this.theme);
    },

    toggleTheme() {
      this.theme = this.theme === "dark" ? "light" : "dark";
      this.applyTheme();
      this.renderCharts();
    },

    donorList() {
      if (!this.stats) return [];
      if (this.donorTab === "company") return this.stats.top_companies || [];
      if (this.donorTab === "individual") return this.stats.top_individuals || [];
      return this.stats.top_donors_by_total || [];
    },

    renderCharts() {
      if (!this.stats) return;
      applyChartTheme();
      this.renderDayChart();
      this.renderHourChart();
      this.renderBucketChart();
      this.renderAmountSharePie();
      this.renderGenderPie();
    },

    destroyChart(key) {
      if (this.charts[key]) {
        this.charts[key].destroy();
        delete this.charts[key];
      }
    },

    renderDayChart() {
      const el = document.getElementById("chart-day");
      if (!el) return;
      this.destroyChart("day");
      const data = this.stats.by_day || [];
      this.charts.day = new Chart(el, {
        type: "bar",
        data: {
          labels: data.map((d) => d.date),
          datasets: [
            {
              label: "Wpłat dziennie",
              data: data.map((d) => d.count),
              backgroundColor: "#fb2e60",
              borderRadius: 6,
              yAxisID: "y",
            },
            {
              type: "line",
              label: "Suma PLN",
              data: data.map((d) => d.total_grosze / 100),
              borderColor: "#10b981",
              backgroundColor: "rgba(16,185,129,0.1)",
              tension: 0.3,
              yAxisID: "y1",
              pointRadius: 0,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          scales: {
            x: { grid: { display: false } },
            y: {
              position: "left",
              beginAtZero: true,
              ticks: { callback: (v) => INT.format(v) },
            },
            y1: {
              position: "right",
              beginAtZero: true,
              grid: { drawOnChartArea: false },
              ticks: { callback: (v) => INT.format(v) + " zł" },
            },
          },
          plugins: {
            legend: { position: "bottom" },
            tooltip: {
              callbacks: {
                label: (ctx) =>
                  ctx.dataset.yAxisID === "y1"
                    ? `${ctx.dataset.label}: ${PLN.format(ctx.parsed.y)}`
                    : `${ctx.dataset.label}: ${INT.format(ctx.parsed.y)}`,
              },
            },
          },
        },
      });
    },

    hourSeries() {
      if (this.hourDay === "all") {
        const data = this.stats?.by_hour || [];
        return {
          counts: data.map((d) => d.count),
          totalsGrosze: data.map((d) => d.total_grosze || 0),
        };
      }
      const day = (this.stats?.by_hour_per_day || []).find(
        (d) => d.date === this.hourDay,
      );
      return {
        counts: day ? day.counts : new Array(24).fill(0),
        totalsGrosze: day ? day.totals_grosze : new Array(24).fill(0),
      };
    },

    renderHourChart() {
      const el = document.getElementById("chart-hour");
      if (!el || !this.stats) return;
      applyChartTheme();
      this.destroyChart("hour");
      const { counts, totalsGrosze } = this.hourSeries();
      this.charts.hour = new Chart(el, {
        type: "bar",
        data: {
          labels: Array.from({ length: 24 }, (_, h) => `${h}:00`),
          datasets: [
            {
              label: "Wpłaty",
              data: counts,
              backgroundColor: "#3b82f6",
              borderRadius: 4,
              yAxisID: "y",
            },
            {
              type: "line",
              label: "Suma PLN",
              data: totalsGrosze.map((g) => g / 100),
              borderColor: "#10b981",
              backgroundColor: "rgba(16,185,129,0.1)",
              tension: 0.3,
              pointRadius: 0,
              yAxisID: "y1",
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          plugins: {
            legend: { position: "bottom" },
            tooltip: {
              callbacks: {
                label: (ctx) =>
                  ctx.dataset.yAxisID === "y1"
                    ? `${ctx.dataset.label}: ${PLN.format(ctx.parsed.y)}`
                    : `${ctx.dataset.label}: ${INT.format(ctx.parsed.y)}`,
              },
            },
          },
          scales: {
            x: { grid: { display: false } },
            y: {
              position: "left",
              beginAtZero: true,
              ticks: { callback: (v) => INT.format(v) },
            },
            y1: {
              position: "right",
              beginAtZero: true,
              grid: { drawOnChartArea: false },
              ticks: { callback: (v) => INT.format(v) + " zł" },
            },
          },
        },
      });
    },

    renderBucketChart() {
      const el = document.getElementById("chart-bucket");
      if (!el) return;
      this.destroyChart("bucket");
      const data = this.stats.by_amount_bucket || [];
      this.charts.bucket = new Chart(el, {
        type: "bar",
        data: {
          labels: data.map((d) => d.range),
          datasets: [
            {
              label: "Liczba wpłat",
              data: data.map((d) => d.count),
              backgroundColor: "#8b5cf6",
              borderRadius: 4,
              xAxisID: "x",
            },
            {
              label: "Suma w PLN",
              data: data.map((d) => d.total_grosze / 100),
              backgroundColor: "#10b981",
              borderRadius: 4,
              xAxisID: "x2",
            },
          ],
        },
        options: {
          indexAxis: "y",
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          plugins: {
            legend: { position: "bottom" },
            tooltip: {
              callbacks: {
                label: (ctx) =>
                  ctx.dataset.xAxisID === "x2"
                    ? `${ctx.dataset.label}: ${PLN.format(ctx.parsed.x)}`
                    : `${ctx.dataset.label}: ${INT.format(ctx.parsed.x)}`,
              },
            },
          },
          scales: {
            x: {
              position: "bottom",
              beginAtZero: true,
              title: { display: true, text: "Liczba wpłat" },
              ticks: { callback: (v) => INT.format(v) },
            },
            x2: {
              position: "top",
              beginAtZero: true,
              grid: { drawOnChartArea: false },
              title: { display: true, text: "Suma PLN" },
              ticks: { callback: (v) => INT.format(v) + " zł" },
            },
            y: { grid: { display: false } },
          },
        },
      });
    },

    renderAmountSharePie() {
      const el = document.getElementById("chart-amount-share");
      if (!el || !this.stats) return;
      this.destroyChart("amountShare");
      const data = (this.stats.by_amount_bucket || []).filter(
        (d) => (d.total_grosze || 0) > 0,
      );
      const palette = [
        "#fb2e60", "#f97316", "#f59e0b", "#10b981", "#06b6d4",
        "#3b82f6", "#8b5cf6", "#ec4899", "#a3a3a3", "#525252",
      ];
      const total = data.reduce((s, d) => s + (d.total_grosze || 0), 0);
      this.charts.amountShare = new Chart(el, {
        type: "doughnut",
        data: {
          labels: data.map((d) => d.range),
          datasets: [
            {
              data: data.map((d) => (d.total_grosze || 0) / 100),
              backgroundColor: palette.slice(0, data.length),
              borderWidth: 0,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { position: "bottom" },
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const grosze = ctx.parsed * 100;
                  const pct = total ? ((grosze / total) * 100).toFixed(1) : "0";
                  return `${ctx.label}: ${PLN.format(ctx.parsed)} (${pct}%)`;
                },
              },
            },
          },
        },
      });
    },

    renderGenderPie() {
      const el = document.getElementById("chart-gender");
      if (!el || !this.stats) return;
      this.destroyChart("gender");
      const g = this.stats.by_gender || {};
      const order = ["F", "M", "company", "unknown"];
      const items = order
        .map((k) => ({ key: k, ...(g[k] || {}) }))
        .filter((d) => (d.total_grosze || 0) > 0);
      const palette = { F: "#ec4899", M: "#3b82f6", company: "#f59e0b", unknown: "#71717a" };
      const total = items.reduce((s, d) => s + (d.total_grosze || 0), 0);
      this.charts.gender = new Chart(el, {
        type: "doughnut",
        data: {
          labels: items.map((d) => d.label),
          datasets: [
            {
              data: items.map((d) => (d.total_grosze || 0) / 100),
              backgroundColor: items.map((d) => palette[d.key]),
              borderWidth: 0,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { position: "bottom" },
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const grosze = ctx.parsed * 100;
                  const pct = total ? ((grosze / total) * 100).toFixed(1) : "0";
                  const item = items[ctx.dataIndex];
                  return [
                    `${ctx.label}: ${PLN.format(ctx.parsed)} (${pct}%)`,
                    `Wpłat: ${INT.format(item.count)} · średnio ${PLN.format(item.avg_grosze / 100)}`,
                  ];
                },
              },
            },
          },
        },
      });
    },

    async loadCsv() {
      if (this.csv.loading || this.csv.loaded) return;
      this.csv.loading = true;
      this.csv.error = null;
      try {
        const conn = await getDuckConn();
        const totalRes = await conn.query(`SELECT count(*)::INTEGER AS n FROM payments`);
        this.csv.totalCount = Number(arrowToObjects(totalRes)[0].n) || 0;
        this.csv.loaded = true;
        await this.runFilterQuery();
      } catch (err) {
        this.csv.error = err?.message || String(err);
      } finally {
        this.csv.loading = false;
      }
    },

    buildWhere() {
      const f = this.filter;
      const where = [];
      const params = [];
      if (f.q && f.q.trim()) {
        where.push('("name" ILIKE ? OR "comment" ILIKE ?)');
        const like = `%${f.q.trim().replace(/[%_]/g, "")}%`;
        params.push(like, like);
      }
      if (f.min != null && f.min !== "" && !Number.isNaN(Number(f.min))) {
        where.push('"amount" >= ?');
        params.push(Number(f.min));
      }
      if (f.max != null && f.max !== "" && !Number.isNaN(Number(f.max))) {
        where.push('"amount" <= ?');
        params.push(Number(f.max));
      }
      if (f.onlyComments) where.push('length("comment") > 0');
      if (f.onlyCompanies) where.push('"company" = \'1\'');
      if (f.dateFrom) {
        where.push('substr("at", 1, 10) >= ?');
        params.push(f.dateFrom);
      }
      if (f.dateTo) {
        where.push('substr("at", 1, 10) <= ?');
        params.push(f.dateTo);
      }
      const sql = where.length ? `WHERE ${where.join(" AND ")}` : "";
      return { sql, params };
    },

    scheduleQuery(delay = 100) {
      if (!this.csv.loaded) return;
      if (this.csv._debounce) clearTimeout(this.csv._debounce);
      this.csv._debounce = setTimeout(() => this.runFilterQuery(), delay);
    },

    async runFilterQuery() {
      if (!this.csv.loaded) return;
      const seq = ++this.csv._seq;
      this.csv.querying = true;
      try {
        const conn = await getDuckConn();
        const { sql: whereSql, params } = this.buildWhere();
        const orderBy =
          SORT_COLUMNS[this.filter.sort] || SORT_COLUMNS.amount_desc;
        const offset = Math.max(0, (this.filter.page - 1) * PAGE_SIZE);

        const aggSql = `
          SELECT count(*)::INTEGER AS n,
                 COALESCE(sum("amount"), 0)::DOUBLE AS s
          FROM payments
          ${whereSql}
        `;
        const pageSql = `
          SELECT "amount","at","name","company","comment"
          FROM payments
          ${whereSql}
          ORDER BY ${orderBy}
          LIMIT ${PAGE_SIZE} OFFSET ${offset}
        `;

        const aggStmt = await conn.prepare(aggSql);
        const aggRes = await aggStmt.query(...params);
        await aggStmt.close();
        const pageStmt = await conn.prepare(pageSql);
        const pageRes = await pageStmt.query(...params);
        await pageStmt.close();
        const aggArr = arrowToObjects(aggRes);

        if (seq !== this.csv._seq) return;

        const agg = aggArr[0] || { n: 0, s: 0 };
        this.csv.matchCount = Number(agg.n) || 0;
        this.csv.matchSumGrosze = Math.round((Number(agg.s) || 0) * 100);
        this.csv.pageRows = arrowToObjects(pageRes).map((r, i) => ({
          k: i,
          amount: Number(r.amount) || 0,
          at: String(r.at ?? ""),
          name: String(r.name ?? ""),
          company: String(r.company ?? ""),
          comment: String(r.comment ?? ""),
        }));
        this.csv.error = null;
      } catch (err) {
        if (seq === this.csv._seq) {
          this.csv.error = err?.message || String(err);
        }
      } finally {
        if (seq === this.csv._seq) this.csv.querying = false;
      }
    },

    filtered() {
      return { length: this.csv.matchCount };
    },

    pageRows() {
      return this.csv.pageRows;
    },

    totalPages() {
      return Math.max(1, Math.ceil(this.csv.matchCount / PAGE_SIZE));
    },

    filteredSumGrosze() {
      return this.csv.matchSumGrosze;
    },

    resetFilter() {
      this.filter = {
        q: "",
        min: null,
        max: null,
        onlyComments: false,
        onlyCompanies: false,
        dateFrom: "",
        dateTo: "",
        sort: "amount_desc",
        page: 1,
      };
    },

    async downloadFiltered() {
      try {
        const conn = await getDuckConn();
        const { sql: whereSql, params } = this.buildWhere();
        const orderBy =
          SORT_COLUMNS[this.filter.sort] || SORT_COLUMNS.amount_desc;
        const stmt = await conn.prepare(
          `SELECT "amount","at","name","company","comment"
           FROM payments
           ${whereSql}
           ORDER BY ${orderBy}`,
        );
        const result = await stmt.query(...params);
        await stmt.close();
        const rows = arrowToObjects(result);
        if (!rows.length) return;
        const csv = toCsv(rows, [
          "amount",
          "at",
          "name",
          "company",
          "comment",
        ]);
        const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "latwogang-filtered.csv";
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
      } catch (err) {
        this.csv.error = err?.message || String(err);
      }
    },
  };
}

window.dashboard = dashboard;
