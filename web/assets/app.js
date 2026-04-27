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
    csv: {
      loading: false,
      loaded: false,
      rows: [],
      error: null,
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
      this.$watch("filter.q", () => (this.filter.page = 1));
      this.$watch("filter.min", () => (this.filter.page = 1));
      this.$watch("filter.max", () => (this.filter.page = 1));
      this.$watch("filter.onlyComments", () => (this.filter.page = 1));
      this.$watch("filter.onlyCompanies", () => (this.filter.page = 1));
      this.$watch("filter.dateFrom", () => (this.filter.page = 1));
      this.$watch("filter.dateTo", () => (this.filter.page = 1));
      this.$watch("filter.sort", () => (this.filter.page = 1));
    },

    fmtPLN,
    fmtInt,
    fmtDateTime,
    fmtDate,
    initial,
    colorFor,

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

    renderHourChart() {
      const el = document.getElementById("chart-hour");
      if (!el) return;
      this.destroyChart("hour");
      const data = this.stats.by_hour || [];
      this.charts.hour = new Chart(el, {
        type: "bar",
        data: {
          labels: data.map((d) => `${d.hour}:00`),
          datasets: [
            {
              label: "Wpłaty",
              data: data.map((d) => d.count),
              backgroundColor: "#3b82f6",
              borderRadius: 4,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { grid: { display: false } },
            y: { beginAtZero: true, ticks: { callback: (v) => INT.format(v) } },
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
            },
          ],
        },
        options: {
          indexAxis: "y",
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { beginAtZero: true, ticks: { callback: (v) => INT.format(v) } },
            y: { grid: { display: false } },
          },
        },
      });
    },

    loadCsv() {
      if (this.csv.loading || this.csv.loaded) return;
      this.csv.loading = true;
      this.csv.error = null;
      Papa.parse("data/payments.csv", {
        download: true,
        header: true,
        skipEmptyLines: true,
        worker: true,
        complete: (res) => {
          this.csv.rows = res.data.filter(
            (r) => r && r.state === "confirmed" && r.amount,
          );
          this.csv.loaded = true;
          this.csv.loading = false;
        },
        error: (err) => {
          this.csv.error = err?.message || String(err);
          this.csv.loading = false;
        },
      });
    },

    matchesFilter(r) {
      const f = this.filter;
      const amount = parseFloat(r.amount);
      if (Number.isNaN(amount)) return false;
      const pln = amount / 100;
      if (f.min != null && f.min !== "" && pln < Number(f.min)) return false;
      if (f.max != null && f.max !== "" && pln > Number(f.max)) return false;
      if (f.onlyComments && !(r.comment_text && r.comment_text.trim()))
        return false;
      if (f.onlyCompanies && r.payer_company !== "True") return false;
      if (f.dateFrom && r.state_changed_at < f.dateFrom) return false;
      if (f.dateTo && r.state_changed_at.slice(0, 10) > f.dateTo) return false;
      if (f.q) {
        const q = f.q.toLowerCase();
        const hay = (
          (r.payer_name || "") +
          " " +
          (r.comment_text || "")
        ).toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    },

    sortRows(rows) {
      const sort = this.filter.sort;
      const sorted = [...rows];
      if (sort === "amount_desc")
        sorted.sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
      else if (sort === "amount_asc")
        sorted.sort((a, b) => parseFloat(a.amount) - parseFloat(b.amount));
      else if (sort === "date_desc")
        sorted.sort((a, b) =>
          (b.state_changed_at || "").localeCompare(a.state_changed_at || ""),
        );
      else if (sort === "date_asc")
        sorted.sort((a, b) =>
          (a.state_changed_at || "").localeCompare(b.state_changed_at || ""),
        );
      return sorted;
    },

    filtered() {
      const matches = this.csv.rows.filter((r) => this.matchesFilter(r));
      return this.sortRows(matches);
    },

    pageRows() {
      const all = this.filtered();
      const start = (this.filter.page - 1) * PAGE_SIZE;
      return all.slice(start, start + PAGE_SIZE);
    },

    totalPages() {
      return Math.max(1, Math.ceil(this.filtered().length / PAGE_SIZE));
    },

    filteredSumGrosze() {
      return this.filtered().reduce(
        (acc, r) => acc + Math.round(parseFloat(r.amount) || 0),
        0,
      );
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

    downloadFiltered() {
      const rows = this.filtered();
      if (!rows.length) return;
      const csv = Papa.unparse(rows);
      const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "latwogang-filtered.csv";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    },
  };
}

window.dashboard = dashboard;
