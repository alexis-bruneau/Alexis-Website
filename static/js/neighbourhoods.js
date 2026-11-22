/************** NEIGHBOURHOOD MODE (sold map and active map) **************
 * A second way to pick the area on /ottawa_map and /ottawa_active: instead of dragging a circle, click one of Ottawa's
 * 116 fixed neighbourhoods (Ottawa Neighbourhood Study, City of Ottawa open data).
 *
 *  - the polygons are fetched once from /neighbourhood-boundaries.geojson;
 *  - a POST to the page's summary endpoint returns one row per neighbourhood for the current filters: the table and the
 *    colours (/neighbourhood-summary for sales, /active-neighbourhood-summary for listings);
 *  - a selected neighbourhood goes through the page's own points request (/filtered-points or /active-points) as the
 *    same call as the circle, with area = {type: "neighbourhood", id}.
 *
 * The one thing that differs per page is the NB object below (wording, metrics, columns, which request to repeat).
 * Loaded after map.js / active.js, whose globals it uses (map, circle, handle, setView, areaMode, selectedNeighbourhood,
 * updateMapMarkers, updateListingsSidebar, ...). Those files call refreshNeighbourhoodSummary,
 * showNoNeighbourhoodSelected, appendAreaBanner and refreshNeighbourhoodViews, which are defined here.
 */

// Kept short (it sits on the map); the full wording is in the table footer and in redfin/data/neighbourhoods/README.md
const NB_ATTRIBUTION =
  'Boundaries: <a href="https://open.ottawa.ca" target="_blank" rel="noopener">Ottawa Neighbourhood Study</a>, ' +
  '<a href="https://ottawa.ca/en/city-hall/get-know-your-city/open-data#open-data-licence-version-2-0" ' +
  'target="_blank" rel="noopener">OGL – City of Ottawa</a>';

// Colours (dataviz method): one-hue sequential ramp for magnitudes, blue <-> red with a grey midpoint for % vs asking,
// grey for areas with too few sales. The ramp is the blue 100 -> 700 scale.
const SEQ_STOPS = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"];
const DIV_MID = "#f0efec";
const DIV_OVER = "#1c5cab";    // sold over asking
const DIV_UNDER = "#d03b3b";   // sold under asking
const LOW_FILL = "#9ca3af";

const NB_ACTIVE = document.body.classList.contains("active-map");

/** Everything that differs between the sold page and the active page. */
const NB = NB_ACTIVE ? {
  noun: { one: "listing", many: "listings" },
  summaryUrl: "/active-neighbourhood-summary",
  minSample: 5,   // until the first summary says (ACTIVE_MIN_SAMPLE on the server)
  defaultMetric: "n",
  defaultSort: { key: "n", dir: -1 },
  // `exact`: the value is a count, not an estimate, so it is coloured whatever the sample size.
  metrics: {
    n: { label: "Listing count", kind: "seq", exact: true, value: r => r.n, fmt: v => Math.round(v).toLocaleString() },
    median_price: { label: "Median asking price", kind: "seq", value: r => r.median_price, fmt: v => fmtNbPrice(v) },
    avg_dom: { label: "Avg days on market", kind: "seq", value: r => r.avg_dom, fmt: v => `${Math.round(v)} d` },
    cut_pct: { label: "% with a price cut", kind: "seq", value: r => r.cut_pct, fmt: v => `${Math.round(v)}%` },
  },
  // `stat`: the cell is an estimate that is greyed out when the neighbourhood has too few listings
  columns: [
    { key: "name", label: "Neighbourhood", cls: "c-name", width: "", stat: false },
    { key: "n", label: "Listed", cls: "c-num", width: "54px", metric: "n", stat: false, title: "Active listings for sale",
      show: r => r.n.toLocaleString() },
    { key: "median_price", label: "Median", cls: "c-num", width: "66px", metric: "median_price", title: "Median asking price",
      show: r => fmtNbPrice(r.median_price) },
    { key: "avg_dom", label: "DOM", cls: "c-num", width: "48px", metric: "avg_dom", title: "Average days on market so far",
      show: r => nbIsNum(r.avg_dom) ? Math.round(r.avg_dom) : "–" },
    { key: "cut_pct", label: "Cuts", cls: "c-num", width: "52px", metric: "cut_pct",
      title: "Share of the listings whose last asking-price change was a cut",
      show: r => nbIsNum(r.cut_pct) ? `${Math.round(r.cut_pct)}%` : "–" },
  ],
  metaText: (total, withRows, areas, min) =>
    `${total.toLocaleString()} listings in ${withRows} of ${areas} neighbourhoods. Counts are exact; the median and averages are greyed in rows with fewer than ${min} listings.`,
  footText: missing => (missing ? `${missing.toLocaleString()} more listing${missing === 1 ? " has" : "s have"} no coordinates, so ${missing === 1 ? "it is" : "they are"} on no map. ` : "") +
    "Boundaries: Ottawa Neighbourhood Study (Gen 3), City of Ottawa open data, Open Government Licence – City of Ottawa. Median asking price; DOM is the average days on market so far; Cuts is the share of listings whose last price change was a cut.",
  refetch: () => fetchActivePoints(),
  showNothing: () => showNoNeighbourhoodSelectedActive(),
} : {
  noun: { one: "sale", many: "sales" },
  summaryUrl: "/neighbourhood-summary",
  minSample: 10,
  defaultMetric: "median_price",
  defaultSort: { key: "n", dir: -1 },
  metrics: {
    median_price: { label: "Median price", kind: "seq", value: r => r.median_price, fmt: v => fmtNbPrice(v) },
    avg_dom: { label: "Avg days on market", kind: "seq", value: r => r.avg_dom, fmt: v => `${Math.round(v)} d` },
    diff: { label: "Avg sold vs ask", kind: "div", value: r => r[`avg_diff_pct_${diffBasis}`], fmt: v => fmtNbDiff(v) },
  },
  columns: [
    { key: "name", label: "Neighbourhood", cls: "c-name", width: "" },
    { key: "n", label: "Sales", cls: "c-num", width: "50px", title: "Number of sales", show: r => r.n.toLocaleString() },
    { key: "median_price", label: "Median", cls: "c-num", width: "66px", metric: "median_price", title: "Median sold price",
      show: r => fmtNbPrice(r.median_price) },
    { key: "avg_dom", label: "DOM", cls: "c-num", width: "50px", metric: "avg_dom", title: "Average days on market",
      show: r => nbIsNum(r.avg_dom) ? Math.round(r.avg_dom) : "–" },
    { key: "diff", label: "vs ask", cls: "c-num", width: "62px", metric: "diff",
      title: () => `Average sold price vs the ${diffBasis === "last" ? "final" : "original"} asking price`,
      show: r => fmtNbDiff(nbMetricValue(r, "diff")) },
  ],
  metaText: (total, withRows, areas, min) =>
    `${total.toLocaleString()} sales in ${withRows} of ${areas} neighbourhoods. Greyed rows have fewer than ${min} sales.`,
  footText: missing => (missing ? `${missing.toLocaleString()} more sale${missing === 1 ? " has" : "s have"} no coordinates, so ${missing === 1 ? "it is" : "they are"} on no map. ` : "") +
    `Boundaries: Ottawa Neighbourhood Study (Gen 3), City of Ottawa open data, Open Government Licence – City of Ottawa. Median price; averages for days on market and sold vs ${diffBasis === "last" ? "final" : "original"} ask.`,
  refetch: () => fetchFilteredPoints(),
  showNothing: () => showNoNeighbourhoodSelectedSold(),
};
const NB_METRICS = NB.metrics;
const AREA_COLUMNS = NB.columns;

const nbState = {
  rows: [],                 // rows of the last summary
  byId: new Map(),
  meta: { minSample: NB.minSample, total: 0, noCoordinates: 0 },
  status: "loading",        // "loading" | "ready" | "error"
  layer: null,              // L.geoJSON of the polygons
  layers: new Map(),        // id -> polygon layer
  boundariesPromise: null,
  metric: NB.defaultMetric, // what the map is coloured by (a key of NB.metrics)
  sort: { ...NB.defaultSort },
  filterText: "",
  summaryRequestId: 0,
  scale: null,              // {kind, lo, hi} of the current colour scale
  hoverId: null,            // the polygon under the pointer
};

// ─── formatting ────────────────────────────────────────────────
const nbIsNum = v => v !== null && v !== undefined && !isNaN(v);
function fmtNbPrice(v) {
  if (!nbIsNum(v)) return "–";
  return v >= 995000 ? `$${(v / 1e6).toFixed(2)}M` : `$${Math.round(v / 1000)}k`;   // $995k and up reads as $1.00M, never "$1000k"
}
function fmtNbDiff(v) {
  if (!nbIsNum(v)) return "–";
  return `${v > 0 ? "+" : ""}${v.toFixed(1)}%`;
}
function nbEsc(text) {
  return String(text).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
function nbMetricValue(row, metric = nbState.metric) {
  const v = NB_METRICS[metric].value(row);
  return nbIsNum(v) ? v : null;
}
/** A row's statistics are only trusted when it has enough sales; a row without the value has none either way. */
function nbUsable(row, metric = nbState.metric) {
  if (!row || nbMetricValue(row, metric) === null) return false;
  return NB.metrics[metric].exact ? row.n > 0 : !row.low_sample;
}

// ─── colour scale ──────────────────────────────────────────────
function lerpHex(a, b, t) {
  const pa = [1, 3, 5].map(i => parseInt(a.slice(i, i + 2), 16));
  const pb = [1, 3, 5].map(i => parseInt(b.slice(i, i + 2), 16));
  return "#" + pa.map((x, i) => Math.round(x + (pb[i] - x) * t).toString(16).padStart(2, "0")).join("");
}
function seqColor(t) {
  const x = Math.min(Math.max(t, 0), 1) * (SEQ_STOPS.length - 1);
  const i = Math.min(Math.floor(x), SEQ_STOPS.length - 2);
  return lerpHex(SEQ_STOPS[i], SEQ_STOPS[i + 1], x - i);
}
function divColor(t) {   // t in [-1, 1]: -1 = most under asking, +1 = most over
  return t >= 0 ? lerpHex(DIV_MID, DIV_OVER, Math.min(t, 1)) : lerpHex(DIV_MID, DIV_UNDER, Math.min(-t, 1));
}
function quantile(sorted, q) {
  const pos = (sorted.length - 1) * q, lo = Math.floor(pos), hi = Math.ceil(pos);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}
/** Scale from the areas with enough sales, clipped at the 5th/95th percentile so one outlier cannot wash out the rest. */
function computeScale() {
  const metric = NB_METRICS[nbState.metric];
  const values = nbState.rows.filter(r => r.id !== 0 && nbUsable(r)).map(r => nbMetricValue(r)).sort((a, b) => a - b);
  if (!values.length) return null;
  let lo = values.length >= 8 ? quantile(values, 0.05) : values[0];
  let hi = values.length >= 8 ? quantile(values, 0.95) : values[values.length - 1];
  if (metric.kind === "div") {
    const m = Math.max(Math.abs(lo), Math.abs(hi), 0.5);
    return { kind: "div", lo: -m, hi: m };
  }
  if (hi - lo < 1e-9) hi = lo + 1;
  return { kind: "seq", lo, hi };
}
function colorFor(value) {
  const s = nbState.scale;
  if (!s) return LOW_FILL;
  return s.kind === "div" ? divColor(value / s.hi) : seqColor((value - s.lo) / (s.hi - s.lo));
}

// ─── the polygons ──────────────────────────────────────────────
// Hover is a glow, not a darker outline: a white edge plus a warm halo (CSS .nb-hover) and a stronger fill, so the area under
// the pointer is obvious before the click. The selected area keeps a white edge with a blue halo (.nb-selected).
const NB_CAN_HOVER = window.matchMedia("(hover: hover)").matches;   // fingers have no hover: a tap just selects

function styleFor(feature) {
  const id = feature.properties.id;
  const row = nbState.byId.get(id);
  const selected = !!selectedNeighbourhood && selectedNeighbourhood.id === id;
  const hovered = nbState.hoverId === id;
  const lit = selected || hovered;
  const style = { weight: selected ? 4 : hovered ? 3 : 1, color: selected ? "#2563eb" : hovered ? "#ffffff" : "#475569", opacity: lit ? 1 : 0.7 };
  // The selected area is only an outline: its middle is clear so the map (and the listings on it) shows through.
  // (fillOpacity 0, not fill:false, so a click inside it still counts as a click on the area.)
  if (selected) {
    return { ...style, fillColor: "#ffffff", fillOpacity: hovered ? 0.1 : 0, dashArray: null };
  }
  if (nbUsable(row)) {
    return { ...style, fillColor: colorFor(nbMetricValue(row)), fillOpacity: hovered ? 0.85 : 0.6, dashArray: null };
  }
  if (!row && nbState.status === "loading") {
    return { ...style, fillColor: "#cbd5e1", fillOpacity: hovered ? 0.55 : 0.3, dashArray: null };
  }
  // too few to colour: greyed out (still lights up under the pointer, so it can be clicked)
  return { ...style, fillColor: LOW_FILL, fillOpacity: hovered ? 0.5 : 0.22, dashArray: lit ? null : "3 3" };
}

/** Style a polygon and set its glow classes (the classes live on the SVG element, which Leaflet's setStyle leaves alone). */
function paintPolygon(id) {
  const layer = nbState.layers.get(id);
  if (!layer) return;
  layer.setStyle(styleFor(layer.feature));
  const el = layer.getElement && layer.getElement();
  if (el) {
    el.classList.toggle("nb-hover", nbState.hoverId === id);
    el.classList.toggle("nb-selected", !!selectedNeighbourhood && selectedNeighbourhood.id === id);
  }
}

function tooltipHtml(id, name) {
  const row = nbState.byId.get(id);
  if (!row) return `<strong>${nbEsc(name)}</strong>`;
  const metric = NB.metrics[nbState.metric];
  const many = NB.noun.many;
  let line;
  if (metric.exact) {
    line = row.n === 0 ? `No ${many} with these filters` : `<strong>${row.n.toLocaleString()}</strong> ${row.n === 1 ? NB.noun.one : many}`;
  } else if (row.low_sample) {
    line = row.n === 0 ? `No ${many} with these filters`
      : `Only ${row.n} ${row.n === 1 ? NB.noun.one : many} (fewer than ${nbState.meta.minSample}): too few to colour`;
  } else {
    const v = nbMetricValue(row);
    line = `${metric.label}: <strong>${v === null ? "–" : metric.fmt(v)}</strong> · ${row.n.toLocaleString()} ${many}`;
  }
  return `<strong>${nbEsc(name)}</strong><br>${line}`;
}

function loadBoundaries() {
  if (!nbState.boundariesPromise) {
    nbState.boundariesPromise = fetch("/neighbourhood-boundaries.geojson")
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(data => {
        nbState.layer = L.geoJSON(data, {
          style: styleFor,
          attribution: NB_ATTRIBUTION,
          onEachFeature: (feature, layer) => {
            const { id, name } = feature.properties;
            nbState.layers.set(id, layer);
            layer.bindTooltip(() => tooltipHtml(id, name), { sticky: true, className: "area-tip" });
            if (NB_CAN_HOVER) {
              layer.on("mouseover", () => {
                nbState.hoverId = id;
                paintPolygon(id);
                layer.bringToFront();   // so the glow is not clipped by a neighbour drawn later
              });
              layer.on("mouseout", () => {
                if (nbState.hoverId === id) nbState.hoverId = null;
                paintPolygon(id);
                if (selectedNeighbourhood) bringSelectedToFront();
              });
            }
            layer.on("click", e => {
              L.DomEvent.stopPropagation(e);   // not a click on the map background
              hideFilterPanel();
              selectNeighbourhood(id, { name });
            });
          },
        });
        return nbState.layer;
      })
      .catch(err => {
        console.error("Failed to load the neighbourhood boundaries:", err);
        nbState.boundariesPromise = null;   // let a later switch to Neighbourhood mode try again
        return null;
      });
  }
  return nbState.boundariesPromise;
}

function bringSelectedToFront() {
  if (selectedNeighbourhood && nbState.layers.has(selectedNeighbourhood.id)) {
    nbState.layers.get(selectedNeighbourhood.id).bringToFront();
  }
}

function restylePolygons() {
  if (nbState.layer) nbState.layer.setStyle(styleFor);
  nbState.layers.forEach((layer, id) => {
    const el = layer.getElement && layer.getElement();
    if (el) {
      el.classList.toggle("nb-hover", nbState.hoverId === id);
      el.classList.toggle("nb-selected", !!selectedNeighbourhood && selectedNeighbourhood.id === id);
    }
  });
  bringSelectedToFront();
}

// ─── legend (map control) ──────────────────────────────────────
let nbLegend = null;
function createLegend() {
  const control = L.control({ position: "bottomleft" });
  control.onAdd = function () {
    const div = L.DomUtil.create("div", "area-legend");
    div.innerHTML = `
      <label class="area-legend-metric">Colour by
        <select id="areaMetricSelect" aria-label="Colour the map by">
          ${Object.entries(NB.metrics).map(([key, m]) => `<option value="${key}">${nbEsc(m.label)}</option>`).join("")}
        </select>
      </label>
      <div class="area-legend-bar" id="areaLegendBar"></div>
      <div class="area-legend-scale" id="areaLegendScale"></div>
      <div class="area-legend-low"><i class="area-legend-swatch"></i><span id="areaLegendLow"></span></div>`;
    L.DomEvent.disableClickPropagation(div);
    L.DomEvent.disableScrollPropagation(div);
    div.querySelector("select").addEventListener("change", e => setMetric(e.target.value));
    return div;
  };
  return control;
}

function renderLegend() {
  const bar = document.getElementById("areaLegendBar");
  const scale = document.getElementById("areaLegendScale");
  const low = document.getElementById("areaLegendLow");
  const select = document.getElementById("areaMetricSelect");
  if (!bar) return;
  select.value = nbState.metric;
  const metric = NB_METRICS[nbState.metric];
  const s = nbState.scale;
  low.textContent = metric.exact ? `No ${NB.noun.many}` : `Fewer than ${nbState.meta.minSample} ${NB.noun.many}`;
  if (nbState.status === "loading") {
    bar.style.background = "#e2e8f0";
    scale.innerHTML = "<span>Loading…</span>";
    return;
  }
  if (!s) {
    bar.style.background = "#e2e8f0";
    scale.innerHTML = `<span>No area has enough ${NB.noun.many}</span>`;
    return;
  }
  if (s.kind === "div") {
    bar.style.background = `linear-gradient(to right, ${DIV_UNDER}, ${DIV_MID}, ${DIV_OVER})`;
    scale.innerHTML = `<span>${nbEsc(fmtNbDiff(s.lo))} under ask</span><span>0</span><span>${nbEsc(fmtNbDiff(s.hi))} over ask</span>`;
  } else {
    bar.style.background = `linear-gradient(to right, ${SEQ_STOPS.join(", ")})`;
    scale.innerHTML = `<span>≤ ${nbEsc(metric.fmt(s.lo))}</span><span>≥ ${nbEsc(metric.fmt(s.hi))}</span>`;
  }
}

function setMetric(metric) {
  nbState.metric = metric;
  nbState.scale = computeScale();
  restylePolygons();
  renderLegend();
  renderAreaTables();
}

// ─── the summary table (desktop panel + mobile tab share one markup) ────
const colTitle = c => (typeof c.title === "function" ? c.title() : c.title);

function areaContainers() {
  return ["areasBodyDesktop", "areasBodyMobile"].map(id => document.getElementById(id)).filter(Boolean);
}

function initAreaTables() {
  areaContainers().forEach(container => {
    container.innerHTML = `
      <div class="areas-head">
        <input type="search" class="areas-filter" placeholder="Filter neighbourhoods…" autocomplete="off" aria-label="Filter neighbourhoods by name" />
        <p class="areas-meta" aria-live="polite"></p>
      </div>
      <div class="areas-scroll">
        <table class="areas-table">
          <colgroup>${AREA_COLUMNS.map(c => `<col${c.width ? ` style="width:${c.width}"` : ""}>`).join("")}</colgroup>
          <thead><tr>${AREA_COLUMNS.map(c =>
            `<th class="${c.cls}" scope="col"><button type="button" class="th-btn" data-key="${c.key}"${c.metric ? ` data-metric="${c.metric}"` : ""}
               title="${(colTitle(c) ? colTitle(c) + ". " : "") + (c.metric ? "Click to sort by this and colour the map by it" : "Click to sort by this")}"><span class="th-label"></span><span class="th-arrow" aria-hidden="true"></span></button></th>`).join("")}
          </tr></thead>
          <tbody></tbody>
        </table>
      </div>
      <p class="areas-foot"></p>`;
    container.querySelector(".areas-filter").addEventListener("input", e => {
      nbState.filterText = e.target.value.trim().toLowerCase();
      areaContainers().forEach(c => { const i = c.querySelector(".areas-filter"); if (i !== e.target) i.value = e.target.value; });
      renderAreaTables();
    });
    container.querySelector("thead").addEventListener("click", e => {
      const btn = e.target.closest(".th-btn");
      if (!btn) return;
      const key = btn.dataset.key;
      nbState.sort = nbState.sort.key === key
        ? { key, dir: -nbState.sort.dir }
        : { key, dir: key === "name" ? 1 : -1 };       // names A-Z first, numbers biggest first
      if (btn.dataset.metric && btn.dataset.metric !== nbState.metric) setMetric(btn.dataset.metric);
      else renderAreaTables();
    });
    const tbody = container.querySelector("tbody");
    const pick = e => {
      const tr = e.target.closest("tr.area-row");
      if (tr) selectNeighbourhood(Number(tr.dataset.id), { zoom: true, name: tr.dataset.name });
    };
    tbody.addEventListener("click", pick);
    tbody.addEventListener("keydown", e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); pick(e); } });
  });
}

function sortKeyValue(row, key) {
  if (key === "name") return row.name.toLowerCase();
  const col = AREA_COLUMNS.find(c => c.key === key);
  const v = col && col.metric ? nbMetricValue(row, col.metric) : row[key];
  return nbIsNum(v) ? v : null;
}

function sortedRows() {
  const { key, dir } = nbState.sort;
  const unassigned = nbState.rows.find(r => r.id === 0);
  const rows = nbState.rows.filter(r => r.id !== 0 && (!nbState.filterText || r.name.toLowerCase().includes(nbState.filterText)));
  // Estimates (not exact counts) of areas with too few rows rank after the reliable ones, whatever the direction
  const byEstimate = AREA_COLUMNS.some(c => c.key === key && c.metric && !NB.metrics[c.metric].exact);
  rows.sort((a, b) => {
    if (byEstimate && a.low_sample !== b.low_sample) return a.low_sample ? 1 : -1;
    const va = sortKeyValue(a, key), vb = sortKeyValue(b, key);
    if (va === null && vb === null) return a.name.localeCompare(b.name);
    if (va === null) return 1;                          // rows with no value sink to the bottom either way
    if (vb === null) return -1;
    const cmp = typeof va === "string" ? va.localeCompare(vb) : va - vb;
    return cmp * dir || a.name.localeCompare(b.name);
  });
  // The unassigned bucket is always listed, last, so the rows add up to the total
  if (unassigned && (!nbState.filterText || unassigned.name.toLowerCase().includes(nbState.filterText))) rows.push(unassigned);
  return rows;
}

function rowHtml(r) {
  const selected = selectedNeighbourhood && selectedNeighbourhood.id === r.id;
  const cls = ["area-row", r.low_sample ? "is-low" : "", selected ? "is-selected" : "", r.id === 0 ? "is-unassigned" : ""].join(" ").trim();
  const many = NB.noun.many, one = NB.noun.one;
  const title = r.low_sample ? ` title="${r.n === 0 ? `No ${many}` : `Only ${r.n} ${r.n === 1 ? one : many}`} with these filters (fewer than ${nbState.meta.minSample}): too few to rely on"` : "";
  const cells = AREA_COLUMNS.map(c => {
    const cellCls = `${c.cls}${c.stat === false ? "" : " is-stat"}`;
    return `<td class="${cellCls}">${c.key === "name" ? nbEsc(r.name) : c.show(r)}</td>`;
  }).join("");
  return `<tr class="${cls}" data-id="${r.id}" data-name="${nbEsc(r.name)}" tabindex="0" aria-selected="${selected ? "true" : "false"}"${title}>${cells}</tr>`;
}

function renderAreaTables() {
  const containers = areaContainers();
  if (!containers.length) return;
  const rows = sortedRows();
  const withSales = nbState.rows.filter(r => r.id !== 0 && r.n > 0).length;
  const total = nbState.rows.filter(r => r.id !== 0).length;
  let body, meta, foot;
  if (nbState.status === "error") {
    body = `<tr><td colspan="5" class="areas-empty">Could not load the neighbourhood summary. Change a filter to try again.</td></tr>`;
    meta = "";
    foot = "";
  } else if (nbState.status === "loading" && !nbState.rows.length) {
    body = `<tr><td colspan="5" class="areas-empty">Loading…</td></tr>`;
    meta = "";
    foot = "";
  } else {
    body = rows.length ? rows.map(rowHtml).join("") : `<tr><td colspan="5" class="areas-empty">No neighbourhood matches “${nbEsc(nbState.filterText)}”.</td></tr>`;
    meta = NB.metaText(nbState.meta.total, withSales, total, nbState.meta.minSample);
    foot = NB.footText(nbState.meta.noCoordinates);
  }
  containers.forEach(c => {
    c.querySelector("tbody").innerHTML = body;
    c.querySelector(".areas-meta").textContent = meta;
    c.querySelector(".areas-foot").textContent = foot;
    c.querySelectorAll(".th-btn").forEach(btn => {
      const col = AREA_COLUMNS.find(x => x.key === btn.dataset.key);
      btn.querySelector(".th-label").textContent = col.label;
      if (typeof col.title === "function") {   // wording that follows a setting (sold: final or original asking price)
        btn.title = `${colTitle(col)}. Click to sort by this and colour the map by it`;
      }
      const sorted = nbState.sort.key === col.key;
      btn.querySelector(".th-arrow").textContent = sorted ? (nbState.sort.dir > 0 ? "▲" : "▼") : "";
      btn.closest("th").setAttribute("aria-sort", sorted ? (nbState.sort.dir > 0 ? "ascending" : "descending") : "none");
      btn.classList.toggle("on-map", !!col.metric && col.metric === nbState.metric);
    });
  });
}

function scrollSelectedRowIntoView() {
  areaContainers().forEach(c => {
    const row = c.querySelector("tr.is-selected");
    if (row && row.offsetParent !== null) row.scrollIntoView({ block: "nearest" });
  });
}

// ─── data: the summary for the current filters ─────────────────
async function refreshNeighbourhoodSummary(filters) {
  const requestId = ++nbState.summaryRequestId;
  if (!nbState.rows.length) { nbState.status = "loading"; renderAreaTables(); }
  try {
    const res = await fetch(NB.summaryUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filters }),
    });
    const data = await res.json();
    if (requestId !== nbState.summaryRequestId) return;   // a newer request is on its way
    if (data.error) throw new Error(data.error);
    nbState.rows = data.rows;
    nbState.byId = new Map(data.rows.map(r => [r.id, r]));
    nbState.meta = { minSample: data.min_sample, total: data.total, noCoordinates: data.no_coordinates };
    nbState.status = "ready";
  } catch (err) {
    if (requestId !== nbState.summaryRequestId) return;
    console.error("Failed to load the neighbourhood summary:", err);
    nbState.status = "error";
  }
  refreshNeighbourhoodViews();
}

/** Redraw everything that depends on the summary rows or on the "vs ask" basis (no request). */
function refreshNeighbourhoodViews() {
  if (areaMode !== "neighbourhood") return;
  nbState.scale = computeScale();
  restylePolygons();
  renderLegend();
  renderAreaTables();
}

// ─── selection ─────────────────────────────────────────────────
function selectNeighbourhood(id, { zoom = false, name = null } = {}) {
  const row = nbState.byId.get(id);
  selectedNeighbourhood = { id, name: name || (row && row.name) || `Neighbourhood ${id}` };
  restylePolygons();
  renderAreaTables();
  scrollSelectedRowIntoView();
  if (zoom && nbState.layers.has(id)) {
    map.fitBounds(nbState.layers.get(id).getBounds(), { padding: [24, 24], maxZoom: 15 });
  }
  updateAreaTitle();
  NB.refetch();
}

function clearNeighbourhoodSelection() {
  selectedNeighbourhood = null;
  restylePolygons();
  renderAreaTables();
  NB.refetch();   // shows the "pick one" state
}

function updateAreaTitle() {
  const title = document.getElementById("areaTitle");
  const hint = document.getElementById("areaHint");
  const inNb = areaMode === "neighbourhood";
  title.hidden = !(inNb && selectedNeighbourhood);
  hint.hidden = !(inNb && !selectedNeighbourhood);
  if (inNb && selectedNeighbourhood) document.getElementById("areaTitleName").textContent = selectedNeighbourhood.name;
}

/** Neighbourhood mode with nothing selected: no markers, no listings, empty cards. */
function showNoNeighbourhoodSelected() {
  updateAreaTitle();
  NB.showNothing();
}

function showNoNeighbourhoodSelectedSold() {
  updateMapMarkers([]);
  updateStats(null, []);
  currentPoints = [];
  currentPage = 1;
  selectedLocationKey = null;
  selectedMarkerRef = null;
  updateListingsSidebar([]);
  if (window._refreshMobileListings) window._refreshMobileListings();
}

function showNoNeighbourhoodSelectedActive() {
  basePoints = [];
  serverSummary = { count: 0 };
  soldInfo = { curve: null, sample: 0, window: null };
  resetChartSelection();
  focusedMls = null;
  refreshView();   // markers, cards, list and charts all draw their empty state
}

/** Banner at the top of the listings (desktop sidebar and mobile tab). Nothing in Circle mode. */
function appendAreaBanner(container, total) {
  if (areaMode !== "neighbourhood") return;
  const banner = document.createElement("div");
  banner.className = "selection-banner";
  if (selectedNeighbourhood) {
    banner.innerHTML = `<span>${total.toLocaleString()} ${total === 1 ? NB.noun.one : NB.noun.many} in <strong>${nbEsc(selectedNeighbourhood.name)}</strong></span>
      <button type="button" class="area-banner-clear">✕ Clear</button>`;
    banner.querySelector("button").addEventListener("click", clearNeighbourhoodSelection);
  } else {
    banner.innerHTML = `<span>Pick a neighbourhood on the map, or in the table, to list its ${NB.noun.many}.</span>`;
  }
  container.appendChild(banner);
}

// ─── switching between Circle and Neighbourhood ────────────────
async function setAreaMode(mode) {
  if (mode === areaMode) return;
  areaMode = mode;
  document.querySelectorAll("#areaToggle .area-btn").forEach(b => {
    const on = b.dataset.mode === mode;
    b.classList.toggle("active", on);
    b.setAttribute("aria-pressed", on ? "true" : "false");
  });
  const radiusGroup = document.getElementById("radiusGroup");
  const areasBtn = document.getElementById("toggleAreas");

  if (mode === "neighbourhood") {
    if (radiusGroup) radiusGroup.hidden = true;
    if (areasBtn) areasBtn.hidden = false;
    map.removeLayer(circle);
    map.removeLayer(handle);
    if (!nbLegend) nbLegend = createLegend();
    nbLegend.addTo(map);
    nbState.status = nbState.rows.length ? "ready" : "loading";
    renderLegend();
    // Polygons and summary load side by side; the polygons are fetched once per page load
    loadBoundaries().then(layer => {
      if (layer && areaMode === "neighbourhood") {
        layer.addTo(map);
        restylePolygons();
      }
    });
    updateAreaTitle();
    NB.refetch();                                        // requests the summary, shows the "pick one" state
    setView("areas");                                    // table beside the map (desktop) / Areas tab (phones)
    if (window._syncMobileTabs) window._syncMobileTabs("areas");
  } else {
    if (nbState.layer) map.removeLayer(nbState.layer);
    if (nbLegend) map.removeLayer(nbLegend);
    selectedNeighbourhood = null;
    if (radiusGroup) radiusGroup.hidden = false;
    if (areasBtn) areasBtn.hidden = true;
    circle.addTo(map);
    handle.addTo(map);
    nbState.hoverId = null;
    updateAreaTitle();
    if (currentView === "areas") setView("insights");
    if (window._syncMobileTabs) window._syncMobileTabs();   // drops the Areas tab
    NB.refetch();
  }
}

document.querySelectorAll("#areaToggle .area-btn").forEach(btn =>
  btn.addEventListener("click", () => setAreaMode(btn.dataset.mode)));
document.getElementById("toggleAreas").addEventListener("click", () => setView(currentView === "areas" ? "insights" : "areas"));
document.getElementById("areaClear").addEventListener("click", clearNeighbourhoodSelection);
initAreaTables();
renderAreaTables();
