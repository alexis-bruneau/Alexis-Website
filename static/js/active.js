/* Active listings map. Same layout and behaviour as map.js (the sold map), reading /active-points.
   Each listing shows an arrow for its last asking-price change: ▼ cut, ▲ increase.
   The area is a draggable circle or one of Ottawa's neighbourhoods (neighbourhoods.js, loaded after this file). */

/************** STATE **************/
let currentPoints = [];
let currentPage = 1;
let searchQuery = "";
let selectedBeds = [];       // [2,3,4,5]
let rangeMode = false;       // first bed click = "X or more", second = "exactly X"
let selectedPTypes = [];
let selectedChanges = [];    // ["down"], ["up"], both, or none
let selectedLocationKey = null;
let currentView = "insights";   // "insights" | "listings" | "areas" (the neighbourhood table, Neighbourhood mode only)
// How the area is picked (see neighbourhoods.js): a draggable "circle", or a fixed "neighbourhood".
let areaMode = "circle";
let selectedNeighbourhood = null;   // {id, name} in Neighbourhood mode once one is clicked
let basePoints = [];         // what the server returned, after the price-change filter, before the chart selections
let serverSummary = null;    // the server's summary of the points it returned
let soldInfo = { curve: null, sample: 0, window: null };   // sell-time curve for the current circle and filters
let focusedMls = null;       // the listing marked on the sell-time curve (last one clicked on the map, list or scatter)
// What is picked in the insight charts. One object, so the scatter brush and both histograms cross-filter through
// applySelection(). dom / cut hold bucket indexes (see the BUCKETS section); brush is a rectangle in data units.
const chartSelection = { dom: new Set(), cut: new Set(), brush: null };   // brush: { xMin, xMax, yMin, yMax }
const ITEMS_PER_PAGE = 20;
const MIN_PRICE = 0;
const MAX_PRICE = 2000000;
const MIN_DOM = 0;
const MAX_DOM = 180;         // the slider's top end means "180 or more", i.e. no upper limit

const sortSelect = document.getElementById("sortSelect");
const listingsPanel = document.getElementById("listingsPanel");
const insightsEl = document.getElementById("insights");
const mainPanel = document.getElementById("mainPanel");
const areasPanel = document.getElementById("areasPanel");

/************** FORMATTING **************/
const money = (n) => (n === null || n === undefined || isNaN(n)) ? "–" : `$${Math.round(n).toLocaleString()}`;
const compactPrice = (n) => n >= 1e6 ? `$${(n / 1e6).toFixed(1)}M` : `$${Math.round(n / 1000)}K`;
const escapeHtml = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const locationKey = (pt) => `${Number(pt.latitude).toFixed(7)},${Number(pt.longitude).toFixed(7)}`;
const hasChange = (pt) => pt.last_change_direction === "up" || pt.last_change_direction === "down";

function formatDate(iso) {
  if (!iso) return "";
  const d = new Date(`${iso}T00:00:00`);
  return d.toLocaleDateString("en-CA", { year: "numeric", month: "short", day: "numeric" });
}

/** "▼ $14,100 (6.6%)" (red) / "▲ $25,000 (5.0%)" (green) / "No price change" (grey) next to a listing's price. */
function changeBadge(pt) {
  if (!hasChange(pt)) {
    return `<span class="chg flat" title="The asking price has not changed since it was listed">No price change</span>`;
  }
  const up = pt.last_change_direction === "up";
  const amount = Math.abs(pt.last_change_amount);
  const pct = Math.abs(pt.last_change_pct);
  const previous = pt.price - pt.last_change_amount;
  let title = `Last change${pt.last_change_date ? " on " + formatDate(pt.last_change_date) : ""}: `
    + `${money(previous)} → ${money(pt.price)}`;
  if (pt.price_changes > 1) title += `. ${pt.price_changes} changes since listing, first asking price ${money(pt.original_price)}`;
  return `<span class="chg ${up ? "up" : "down"}" title="${title}">${up ? "▲" : "▼"} ${money(amount)} (${pct.toFixed(1)}%)</span>`;
}

/************** MAP **************/
const map = L.map("map", { minZoom: 10, scrollWheelZoom: true, zoomControl: false }).setView([45.4215, -75.6972], 13);
// Bottom right, like on phones: the top left belongs to the sidebars and the controls bar
L.control.zoom({ position: "bottomright" }).addTo(map);
// The map follows its own box: any layout change (a panel opening, a footer growing, a font loading) resizes it
if (window.ResizeObserver) new ResizeObserver(() => map.invalidateSize()).observe(document.getElementById("map"));
const ottawaBounds = L.latLngBounds([45.25, -76.0], [45.75, -75.4]);
map.setMaxBounds(ottawaBounds);
map.on("drag", () => map.panInsideBounds(ottawaBounds, { animate: false }));
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 19, attribution: "© OpenStreetMap" }).addTo(map);

const markersCluster = L.markerClusterGroup({
  showCoverageOnHover: false,
  maxClusterRadius: 90,           // the labels are ~80px wide: closer than this they pile on top of each other
  disableClusteringAtZoom: 17,
  spiderfyOnMaxZoom: true,
  removeOutsideVisibleBounds: true,
  animate: true,
  iconCreateFunction: (cluster) => L.divIcon({
    html: `<div class="cluster-label">${cluster.getChildCount()} listings</div>`,
    className: "cluster-marker",
    iconSize: [80, 24],
    iconAnchor: [40, 12]
  })
});
map.addLayer(markersCluster);

/************** CIRCLE AND DRAGGABLE HANDLE **************/
const radiusInput = document.getElementById("radiusInput");
const currentRadius = parseFloat(radiusInput.value) || 5;
const handle = L.marker([45.4215, -75.6972], {
  draggable: true,
  icon: L.divIcon({ className: "drag-handle", iconSize: [24, 24], iconAnchor: [12, 12] }),
  zIndexOffset: 10000,
  title: "Drag to move search area"
}).addTo(map);
handle.bindTooltip("Drag to move search area", { permanent: false, direction: "right", offset: [15, 0] });
const circle = L.circle(handle.getLatLng(), {
  radius: currentRadius * 1000, color: "blue", fillColor: "blue", fillOpacity: 0.2, weight: 1
}).addTo(map);
handle.on("drag", () => circle.setLatLng(handle.getLatLng()));
handle.on("dragend", () => fetchActivePoints());
radiusInput.addEventListener("change", () => {
  const r = Math.min(parseFloat(radiusInput.value) || 0, 60);
  radiusInput.value = r;
  circle.setRadius(r * 1000);
  fetchActivePoints();
});

/************** VIEWS (insights footer <-> listings sidebar) **************/
function setView(view) {
  currentView = view;
  if (view === "listings") {
    listingsPanel.classList.add("open");
    listingsPanel.classList.remove("hidden");
    insightsEl.classList.add("hidden");
    mainPanel.classList.add("full-map");
  } else {
    listingsPanel.classList.remove("open");
    setTimeout(() => listingsPanel.classList.add("hidden"), 300);
    insightsEl.classList.remove("hidden");
    mainPanel.classList.remove("full-map");
  }
  // The neighbourhood table slides in on the left like the listings; the insights stay visible beside it
  if (areasPanel) {
    if (view === "areas") {
      areasPanel.classList.remove("hidden");
      requestAnimationFrame(() => areasPanel.classList.add("open"));
    } else {
      areasPanel.classList.remove("open");
      setTimeout(() => { if (currentView !== "areas") areasPanel.classList.add("hidden"); }, 300);
    }
  }
  const areasBtn = document.getElementById("toggleAreas");
  if (areasBtn) areasBtn.classList.toggle("is-on", view === "areas");
  document.body.classList.toggle("side-open", view === "listings" || view === "areas");   // CSS keeps the controls bar off the sidebar
  setTimeout(() => map.invalidateSize(), 310);
  if (view === "insights" || view === "areas") setTimeout(resizeCharts, 50);   // the charts were laid out while hidden
}
document.getElementById("toggleListings").addEventListener("click", () => setView("listings"));
/** Where closing the listings sidebar goes back to: the neighbourhood table in Neighbourhood mode, else the insights. */
function homeView() { return areaMode === "neighbourhood" ? "areas" : "insights"; }
document.getElementById("viewInsightsBtn").addEventListener("click", () => setView(currentView === "listings" ? homeView() : "listings"));

function hideFilterPanel() {
  const panel = document.getElementById("filterPanel");
  if (panel) {
    panel.classList.remove("show");
    panel.style.display = "none";
  }
}

map.on("click", () => {
  if (selectedLocationKey) clearLocationSelection();
  hideFilterPanel();
  if (currentView === "listings") setView(homeView());
});


/** The filter panel never runs off the screen: on a phone it ends above the tab bar (the bottom of the map), on a desktop at the
 *  bottom of the window. Longer than that, it scrolls inside itself. */
function fitFilterPanel() {
  const panel = document.getElementById("filterPanel");
  if (!panel || panel.style.display === "none") return;
  const top = panel.getBoundingClientRect().top;
  const mapBottom = document.getElementById("map").getBoundingClientRect().bottom;
  const phone = window.innerWidth <= 900 || (window.innerHeight <= 500 && window.innerWidth > window.innerHeight);
  const limit = phone ? mapBottom : window.innerHeight;
  panel.style.maxHeight = `${Math.max(120, limit - top - 12)}px`;
}
window.addEventListener("resize", () => fitFilterPanel());

/** Axis label for a dollar amount, with only as many decimals as the tick spacing needs ($1.0M three times never happens). */
function fmtMoneyTick(v, ticks) {
  const step = ticks && ticks.length > 1 ? Math.abs(ticks[1].value - ticks[0].value) : 1000;
  const abs = Math.abs(v);
  if (abs >= 1e6) {
    const d = step >= 1e6 ? 0 : step >= 1e5 ? 1 : 2;
    return `$${Number((v / 1e6).toFixed(d)).toFixed(d === 0 ? 0 : Math.max(1, d))}M`;
  }
  if (abs >= 1000) return `$${Number((v / 1000).toFixed(step >= 1000 ? 0 : 1))}k`;
  return `$${Math.round(v)}`;
}

/** Shows (or, with null, hides) a centred message over a chart's box, so a chart with nothing to draw says why. */
function setChartEmpty(box, message) {
  if (!box) return;
  let el = box.querySelector(".chart-empty");
  if (!message) { if (el) el.hidden = true; return; }
  if (!el) {
    el = document.createElement("div");
    el.className = "chart-empty";
    box.appendChild(el);
  }
  el.textContent = message;
  el.hidden = false;
}

/************** FILTERS **************/
const priceSlider = document.getElementById("priceRangeSlider");
noUiSlider.create(priceSlider, { start: [MIN_PRICE, MAX_PRICE], connect: true, range: { min: MIN_PRICE, max: MAX_PRICE }, step: 10000 });
priceSlider.noUiSlider.on("update", (values) => {
  const [lo, hi] = values.map(Math.round);
  document.getElementById("labelPriceMin").textContent = money(lo);
  document.getElementById("labelPriceMax").textContent = hi >= MAX_PRICE ? `${money(hi)}+` : money(hi);
});
priceSlider.noUiSlider.on("change", () => fetchActivePoints());

// Days on market: finer steps for the first weeks, where most of the movement is.
const domSlider = document.getElementById("domRangeSlider");
noUiSlider.create(domSlider, {
  start: [MIN_DOM, MAX_DOM], connect: true, step: 1,
  range: { min: MIN_DOM, "30%": 14, "60%": 60, max: MAX_DOM }
});
domSlider.noUiSlider.on("update", (values) => {
  const [lo, hi] = values.map(Math.round);
  document.getElementById("labelDomMin").textContent = `${lo} day${lo === 1 ? "" : "s"}`;
  document.getElementById("labelDomMax").textContent = hi >= MAX_DOM ? `${hi}+ days` : `${hi} day${hi === 1 ? "" : "s"}`;
});
domSlider.noUiSlider.on("change", () => fetchActivePoints());

document.getElementById("filterToggle").addEventListener("click", (e) => {
  L.DomEvent.stopPropagation(e);
  const panel = document.getElementById("filterPanel");
  if (panel.classList.contains("show")) {
    panel.classList.remove("show");
    setTimeout(() => { if (!panel.classList.contains("show")) panel.style.display = "none"; }, 200);
  } else {
    panel.style.display = "block";
    fitFilterPanel();
    requestAnimationFrame(() => panel.classList.add("show"));
  }
});

document.getElementById("clearFilters").addEventListener("click", () => {
  selectedBeds = []; rangeMode = false; selectedPTypes = []; selectedChanges = [];
  document.querySelectorAll(".bed-btn, .ptype-btn").forEach((b) => b.classList.remove("selected"));
  priceSlider.noUiSlider.set([MIN_PRICE, MAX_PRICE]);
  domSlider.noUiSlider.set([MIN_DOM, MAX_DOM]);
  fetchActivePoints();
});

document.querySelectorAll(".bed-btn").forEach((btn) => btn.addEventListener("click", () => {
  const clicked = Number(btn.dataset.bed);
  const all = document.querySelectorAll(".bed-btn");
  if (!rangeMode) {
    all.forEach((b) => b.classList.toggle("selected", Number(b.dataset.bed) >= clicked));
    selectedBeds = [1, 2, 3, 4, 5].filter((n) => n >= clicked);
    rangeMode = true;
  } else {
    all.forEach((b) => b.classList.remove("selected"));
    btn.classList.add("selected");
    selectedBeds = [clicked];
    rangeMode = false;
  }
  fetchActivePoints();
}));

function toggleInList(list, value, btn) {
  const i = list.indexOf(value);
  if (i >= 0) list.splice(i, 1); else list.push(value);
  btn.classList.toggle("selected", i < 0);
}
document.querySelectorAll("#ptypeFilter .ptype-btn").forEach((btn) => btn.addEventListener("click", () => {
  toggleInList(selectedPTypes, btn.dataset.ptype, btn);
  fetchActivePoints();
}));
document.querySelectorAll("#changeFilter .ptype-btn").forEach((btn) => btn.addEventListener("click", () => {
  toggleInList(selectedChanges, btn.dataset.change, btn);
  fetchActivePoints();
}));

/************** DATA **************/
/** The filter panel as the server's `filters` object (the price-change buttons are applied here, not by /active-points). */
function currentFilters() {
  const [minPrice, maxPrice] = priceSlider.noUiSlider.get().map((v) => Math.round(v));
  const filters = {};
  if (minPrice > MIN_PRICE) filters.min_price = minPrice;
  if (maxPrice < MAX_PRICE) filters.max_price = maxPrice;
  const [minDom, maxDom] = domSlider.noUiSlider.get().map((v) => Math.round(v));
  if (minDom > MIN_DOM) filters.min_dom = minDom;
  if (maxDom < MAX_DOM) filters.max_dom = maxDom;
  if (selectedBeds.length) filters.beds = selectedBeds;
  if (selectedPTypes.length) filters.ptypes = selectedPTypes;
  return filters;
}

let fetchSeq = 0;
async function fetchActivePoints() {
  const filters = currentFilters();
  const seq = ++fetchSeq;

  let area;
  if (areaMode === "neighbourhood") {
    // The table and the colours follow the filters too, whether or not a neighbourhood is selected. They also follow the
    // price-change buttons, so the table counts the same listings as the map.
    refreshNeighbourhoodSummary(selectedChanges.length ? { ...filters, changes: selectedChanges } : filters);
    if (!selectedNeighbourhood) {
      showNoNeighbourhoodSelected();
      return;
    }
    area = { type: "neighbourhood", id: selectedNeighbourhood.id };
  } else {
    const center = circle.getLatLng();
    area = { type: "circle", center: [center.lat, center.lng], radius_km: circle.getRadius() / 1000 };
  }

  try {
    const res = await fetch("/active-points", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ area, filters })
    });
    const { points, summary, error, sold_curve, sold_sample, sold_window } = await res.json();
    if (error) throw new Error(error);
    if (seq !== fetchSeq) return;   // a newer search (circle dragged, filter changed) is already on its way
    // The price-change filter is applied here: it only needs fields already on each point.
    basePoints = selectedChanges.length ? points.filter((p) => selectedChanges.includes(p.last_change_direction)) : points;
    serverSummary = summary;
    soldInfo = { curve: sold_curve, sample: sold_sample ?? 0, window: sold_window };
    // The circle or a filter changed, so the listings did. A brush rectangle, bucket picks or a marked listing made
    // on the old set would hide or point at the wrong things in the new one, so every fetch starts clean.
    resetChartSelection();
    focusedMls = null;
    finishLoading();
    clearDataStatus();
    refreshView();
    if (!summary || summary.count === 0) {
      setDataStatus("empty", "No active listings match these filters.", [
        { label: "Clear filters", run: () => document.getElementById("clearFilters").click() }
      ]);
    }
  } catch (err) {
    console.error("Failed to fetch active listings:", err);
    if (seq !== fetchSeq) return;
    finishLoading();
    setDataStatus("error", "Couldn't load the listings.", [{ label: "Try again", run: () => fetchActivePoints() }]);
    document.querySelectorAll(".chart-box").forEach((box) => setChartEmpty(box, "Couldn't load the chart."));
  }
}

/** Applies the chart selections to basePoints and redraws everything that shows listings: markers, cards, list, charts. */
function refreshView() {
  currentPoints = applySelection(basePoints);
  currentPage = 1;
  selectedLocationKey = null;
  const narrowed = selectedChanges.length > 0 || hasChartSelection();
  updateMapMarkers(currentPoints);
  updateInsights(narrowed ? summarise(currentPoints, serverSummary) : serverSummary, currentPoints);
  updateListingsSidebar();
  updateCharts();
}

/************** BUCKETS AND CHART SELECTION **************/
// Each bucket includes its lower bound and excludes its upper one: a listing at exactly 7 days is in "7–30".
const DOM_BUCKETS = [
  { label: "<7", desc: "Under 7 days", test: (v) => v < 7 },
  { label: "7–30", desc: "7 to under 30 days", test: (v) => v >= 7 && v < 30 },
  { label: "30–60", desc: "30 to under 60 days", test: (v) => v >= 30 && v < 60 },
  { label: "60–90", desc: "60 to under 90 days", test: (v) => v >= 60 && v < 90 },
  { label: "90+", desc: "90 days or more", test: (v) => v >= 90 }
];
const CUT_BUCKETS = [
  { label: "No cut", desc: "No cut (or priced at or above the first asking price)", test: (v) => v <= 0 },
  { label: "<3%", desc: "Under 3% below the first asking price", test: (v) => v > 0 && v < 3 },
  { label: "3–5%", desc: "3% to under 5% below the first asking price", test: (v) => v >= 3 && v < 5 },
  { label: "5–10%", desc: "5% to under 10% below the first asking price", test: (v) => v >= 5 && v < 10 },
  { label: ">10%", desc: "10% or more below the first asking price", test: (v) => v >= 10 }
];
/** Cut depth: how far the asking price is below the FIRST asking price (original_price), as a % of it. This is the
 *  total cut over the listing's life, not last_change_pct (only the latest change): a listing cut twice counts once,
 *  at its total. Null when there is no first asking price, so such a listing is in no cut bucket. */
const cutPct = (pt) => (pt.original_price > 0 && pt.price != null) ? (pt.original_price - pt.price) / pt.original_price * 100 : null;
const bucketIndex = (buckets, value) => value == null ? -1 : buckets.findIndex((b) => b.test(value));
const inBrush = (pt, b) => pt.dom != null && pt.price != null
  && pt.dom >= b.xMin && pt.dom <= b.xMax && pt.price >= b.yMin && pt.price <= b.yMax;
const hasChartSelection = () => chartSelection.dom.size > 0 || chartSelection.cut.size > 0 || chartSelection.brush !== null;

function resetChartSelection() {
  chartSelection.dom.clear();
  chartSelection.cut.clear();
  chartSelection.brush = null;
}

/** The one place the chart selections become a list of listings. Buckets within a chart OR together, the three
 *  selections (days, cut, brush) AND together. `skip` ("dom", "cut" or "brush") leaves that one selection out: each
 *  chart draws the listings that pass the OTHER selections, so it can show what picking something in it would give. */
function applySelection(points, skip) {
  const { dom, cut, brush } = chartSelection;
  return points.filter((pt) => {
    if (skip !== "dom" && dom.size && !dom.has(bucketIndex(DOM_BUCKETS, pt.dom))) return false;
    if (skip !== "cut" && cut.size && !cut.has(bucketIndex(CUT_BUCKETS, cutPct(pt)))) return false;
    if (skip !== "brush" && brush && !inBrush(pt, brush)) return false;
    return true;
  });
}

const mean = (xs) => xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
const median = (xs) => {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
/** Insights for the points left after the price-change filter (the server summarised the unfiltered set). */
function summarise(points, base) {
  const prices = points.map((p) => p.price).filter((v) => v != null);
  const doms = points.map((p) => p.dom).filter((v) => v != null);
  return {
    ...base,
    count: points.length,
    average_price: mean(prices), median_price: median(prices),
    min_price: prices.length ? Math.min(...prices) : null, max_price: prices.length ? Math.max(...prices) : null,
    avg_dom: mean(doms), median_dom: median(doms),
    with_price_cut: points.filter((p) => p.last_change_direction === "down").length,
    with_price_increase: points.filter((p) => p.last_change_direction === "up").length
  };
}

/************** MARKERS **************/
let markerByKey = new Map();                    // location key -> its Leaflet marker, rebuilt with the markers
const emphasised = { "is-linked": [], "is-pulsing": [] };   // marker elements currently carrying each highlight class
let pulseKey = null;                            // "dom:2" while the markers of that histogram bar pulse

/** Highlights the markers of `points` with a CSS class; replaces whatever the class was on before (pass [] to clear).
 *  A marker inside a cluster has no element of its own, so the cluster that holds it is highlighted instead. */
function setEmphasis(cls, points) {
  emphasised[cls].forEach((el) => el.classList.remove(cls));
  emphasised[cls] = [];
  points.forEach((pt) => {
    const marker = markerByKey.get(locationKey(pt));
    const shown = marker && markersCluster.getVisibleParent(marker);
    const el = shown && shown.getElement();
    if (el) { el.classList.add(cls); emphasised[cls].push(el); }
  });
}

function updateMapMarkers(points) {
  markersCluster.clearLayers();
  markerByKey = new Map();
  emphasised["is-linked"] = []; emphasised["is-pulsing"] = []; pulseKey = null;   // the old elements are gone
  const byLocation = new Map();
  points.forEach((pt) => {
    if (pt.latitude == null || pt.longitude == null) return;
    const key = locationKey(pt);
    if (!byLocation.has(key)) byLocation.set(key, []);
    byLocation.get(key).push(pt);
  });

  byLocation.forEach((group, key) => {
    const first = group[0];
    let icon;
    if (group.length === 1 && first.price) {
      const arrow = hasChange(first)
        ? `<span class="arrow ${first.last_change_direction}">${first.last_change_direction === "up" ? "▲" : "▼"}</span> ` : "";
      icon = L.divIcon({
        className: "price-marker",
        html: `<div class="price-label">${arrow}${compactPrice(first.price)}</div>`,
        iconSize: [80, 24], iconAnchor: [40, 12]
      });
    } else {
      icon = L.divIcon({
        className: "count-marker",
        html: `<div class="count-label">${group.length} listings</div>`,
        iconSize: [80, 24], iconAnchor: [40, 12]
      });
    }
    const marker = L.marker([first.latitude, first.longitude], { icon });
    marker.on("click", (e) => {
      L.DomEvent.stopPropagation(e);
      currentPage = 1;
      selectedLocationKey = group.length > 1 ? key : null;
      if (currentView !== "listings") setView("listings");
      updateListingsSidebar();
      if (group.length === 1) setTimeout(() => highlightListing(first.mls), 150);
    });
    markerByKey.set(key, marker);
    markersCluster.addLayer(marker);
  });
}

/************** SIDEBAR **************/
function sortedList(sortValue = sortSelect.value) {
  let list = currentPoints.slice();
  if (selectedLocationKey) list = list.filter((pt) => locationKey(pt) === selectedLocationKey);
  if (searchQuery) {
    list = list.filter((pt) => (pt.address || "").toLowerCase().includes(searchQuery)
      || money(pt.price).toLowerCase().includes(searchQuery) || String(pt.price).includes(searchQuery));
  }
  const num = (v, fallback) => (v == null ? fallback : v);
  switch (sortValue) {
    case "asc-price": list.sort((a, b) => a.price - b.price); break;
    case "desc-price": list.sort((a, b) => b.price - a.price); break;
    case "oldest": list.sort((a, b) => num(b.dom, -1) - num(a.dom, -1)); break;
    case "recent-change":
      list.sort((a, b) => (b.last_change_date || "").localeCompare(a.last_change_date || "")); break;
    case "biggest-drop":
      list.sort((a, b) => num(a.last_change_pct, 0) - num(b.last_change_pct, 0)); break;
    default: list.sort((a, b) => num(a.dom, 1e9) - num(b.dom, 1e9));   // newest on market first
  }
  return list;
}

/** Opens the page of the list that holds a listing, highlights its row and marks it on the sell-time curve. */
function highlightListing(mls) {
  const list = sortedList();
  const idx = list.findIndex((pt) => pt.mls === mls);
  if (idx === -1) return;
  focusedMls = mls;
  currentPage = Math.floor(idx / ITEMS_PER_PAGE) + 1;
  updateListingsSidebar();   // draws the row highlighted (markFocusedRow)
  updateCurve();
  const row = document.getElementById(`listing-${mls}`);
  if (row) row.scrollIntoView({ block: "center", behavior: "smooth" });
}

const focusedPoint = () => focusedMls ? basePoints.find((p) => p.mls === focusedMls) : undefined;

/** Highlights the focused listing's row (if it is on this page) and adds the "x% of comparable homes sold by..." note. */
function markFocusedRow() {
  document.querySelectorAll("#listingRows .highlight").forEach((el) => el.classList.remove("highlight"));
  document.querySelectorAll("#listingRows .sell-note").forEach((el) => el.remove());
  const pt = focusedPoint();
  const row = pt && document.getElementById(`listing-${pt.mls}`);
  if (!row) return;
  row.classList.add("highlight");
  const note = document.createElement("p");
  note.className = "sell-note";
  note.textContent = sellNote(pt);
  row.querySelector(".listing-info").insertBefore(note, row.querySelector(".view-btn"));
}

function clearLocationSelection() {
  selectedLocationKey = null;
  updateListingsSidebar();
}
window.clearLocationSelection = clearLocationSelection;

/** One listing card. The desktop list marks the clicked row on the sell-time curve; the phone list has no curve beside it. */
function listingRow(pt, { mobile = false } = {}) {
  const row = document.createElement("div");
  row.className = "listing-row";
  if (!mobile) {
    row.id = `listing-${pt.mls}`;
    row.style.cursor = "pointer";
  }
  const dom = pt.dom == null ? "" : `${pt.dom} day${pt.dom === 1 ? "" : "s"} on market`;
  const address = pt.address ? escapeHtml(pt.address) : "Address not shown";
  row.innerHTML = `
    <div class="image-wrapper">
      <img src="${escapeHtml(pt.photo || "")}" referrerpolicy="no-referrer" loading="lazy"
           onerror="this.outerHTML = '<div class=&quot;no-image-placeholder&quot;></div>';" alt="" />
    </div>
    <div class="listing-info">
      <strong>${money(pt.price)}</strong>
      ${changeBadge(pt)}
      <p>${address}</p>
      <p>${pt.beds ?? "–"} bd · ${pt.baths ?? "–"} ba · <span class="ptype-text">${escapeHtml(pt.ptype || "Unknown Type")}</span></p>
      <p>${dom}${pt.original_price && pt.original_price !== pt.price ? ` · listed at ${money(pt.original_price)}` : ""}</p>
      <a href="${escapeHtml(pt.url)}" target="_blank" rel="noopener" class="view-btn">View Details</a>
    </div>`;
  if (!mobile) {
    // Clicking a row marks it on the sell-time curve; "View Details" is what opens the listing page.
    row.addEventListener("click", (e) => {
      if (e.target.closest(".view-btn")) return;
      focusedMls = pt.mls;
      markFocusedRow();
      updateCurve();
    });
  }
  return row;
}

/** The banners above the list: which neighbourhood (Neighbourhood mode) and which address (a marker with several listings). */
function appendListBanners(container, count) {
  appendAreaBanner(container, currentPoints.length);
  if (selectedLocationKey) {
    const banner = document.createElement("div");
    banner.className = "selection-banner";
    banner.innerHTML = `<span>Showing ${count} listing${count !== 1 ? "s" : ""} at this address</span>
      <button onclick="clearLocationSelection()">✕ Show All</button>`;
    container.appendChild(banner);
  }
}

function updateListingsSidebar() {
  const list = sortedList();
  const totalPages = Math.max(1, Math.ceil(list.length / ITEMS_PER_PAGE));
  if (currentPage > totalPages) currentPage = 1;
  const pageItems = list.slice((currentPage - 1) * ITEMS_PER_PAGE, currentPage * ITEMS_PER_PAGE);

  const container = document.getElementById("listingRows");
  container.innerHTML = "";
  appendListBanners(container, list.length);
  if (!pageItems.length) {
    container.insertAdjacentHTML("beforeend", `<p class="empty-note">No active listings match.</p>`);
  }
  pageItems.forEach((pt) => container.appendChild(listingRow(pt)));
  markFocusedRow();

  renderPagination(document.getElementById("paginationControls"), currentPage, totalPages, list.length, (page) => {
    currentPage = page;
    updateListingsSidebar();
    container.scrollTop = 0;
  });
  if (window._refreshMobileListings) window._refreshMobileListings();   // the phone's Listings tab shows the same list
}

function renderPagination(container, current, totalPages, totalItems, onPageClick) {
  container.innerHTML = "";
  if (totalPages <= 1) return;
  const button = (label, page, { active = false, disabled = false, nav = false } = {}) => {
    const b = document.createElement("button");
    b.textContent = label;
    b.className = `page-btn${nav ? " nav-btn" : ""}${active ? " active" : ""}`;
    b.disabled = disabled;
    b.addEventListener("click", () => onPageClick(page));
    container.appendChild(b);
  };
  button("«", current - 1, { disabled: current === 1, nav: true });
  for (let p = 1; p <= totalPages; p++) button(p, p, { active: p === current });
  button("»", current + 1, { disabled: current === totalPages, nav: true });
  const info = document.createElement("span");
  info.className = "pagination-info";
  info.textContent = `${totalItems.toLocaleString()} total`;
  container.appendChild(info);
}

const searchInput = document.getElementById("listingSearch");
searchInput.addEventListener("input", (e) => {
  searchQuery = e.target.value.toLowerCase();
  const mobileSearch = document.getElementById("mobileListingSearch");
  if (mobileSearch) mobileSearch.value = e.target.value;
  currentPage = 1;
  updateListingsSidebar();
});
sortSelect.addEventListener("change", () => { currentPage = 1; updateListingsSidebar(); });

/************** INSIGHTS **************/
function updateInsights(s, points) {
  const set = (id, text) => { document.getElementById(id).textContent = text; };
  const days = (n) => n == null ? "–" : `${Math.round(n)} days`;
  set("stat-count", s.count ?? 0);
  set("stat-cuts", s.count ? `${s.with_price_cut} (${Math.round(100 * s.with_price_cut / s.count)}%)` : "–");
  set("stat-avg", money(s.average_price));
  set("stat-median", money(s.median_price));
  set("stat-dom", days(s.avg_dom));
  set("stat-dom-median", days(s.median_dom));
  set("stat-min", money(s.min_price));
  set("stat-max", money(s.max_price));
  document.getElementById("asOf").textContent = s.as_of ? `Snapshot of ${formatDate(s.as_of)}` : "";

  const recent = points.filter(hasChange)
    .sort((a, b) => (b.last_change_date || "").localeCompare(a.last_change_date || "") || Math.abs(b.last_change_pct) - Math.abs(a.last_change_pct))
    .slice(0, 8);
  const box = document.getElementById("recentChanges");
  box.innerHTML = recent.length ? "" : `<p class="empty-note">No price changes in this area.</p>`;
  recent.forEach((pt) => {
    const a = document.createElement("a");
    a.className = "recent-row";
    a.href = pt.url; a.target = "_blank"; a.rel = "noopener";
    a.innerHTML = `${changeBadge(pt)}<span class="recent-addr" title="${pt.address ? escapeHtml(pt.address) : "Address not shown"}">${pt.address ? escapeHtml(pt.address) : "Address not shown"}</span>
      <span class="recent-date">${formatDate(pt.last_change_date)}</span>`;
    box.appendChild(a);
  });
}

/************** INSIGHT CHARTS **************/
// Chart.js comes from a CDN. If it is blocked the map and the list still work and the chart panes say so.
const chartsReady = typeof Chart !== "undefined";
// Colours come from the page's design tokens (map.css :root); the fallbacks are the original values
const CHANGE_COLORS = { down: cssVar("--down", "#dc2626"), up: cssVar("--up", "#047857"), none: cssVar("--flat", "#94a3b8") };   // the colours of the .chg price-change badges
const ACCENT = cssVar("--accent", "#2563eb");
const ACCENT_DIM = cssVar("--accent-dim", "#bfdbfe");
const INK = cssVar("--ink", "#1e293b");
const GRID = cssVar("--chart-grid", "#f1f5f9");
const charts = {};   // scatter, dom, cut, curve
const changeKind = (pt) => hasChange(pt) ? pt.last_change_direction : "none";
const withAlpha = (hex, a) => { const n = parseInt(hex.slice(1), 16); return `rgba(${n >> 16}, ${(n >> 8) & 255}, ${n & 255}, ${a})`; };
/** Plain-text twin of changeBadge(), for chart tooltips (canvas text, not HTML). */
const changeText = (pt) => hasChange(pt)
  ? `${pt.last_change_direction === "up" ? "▲" : "▼"} ${money(Math.abs(pt.last_change_amount))} (${Math.abs(pt.last_change_pct).toFixed(1)}%)`
  : "No price change";

function resizeCharts() { Object.values(charts).forEach((c) => c.resize()); }

function updateCharts() {
  updateSelectionBar();
  updateCurve();   // its text works without Chart.js
  if (!chartsReady) return;
  updateScatter();
  updateBuckets();
}

/* ---- Tabs ---- */
const insightTabs = [...document.querySelectorAll(".insight-tab")];
function showPane(name) {
  insightTabs.forEach((tab) => {
    const on = tab.dataset.pane === name;
    tab.classList.toggle("active", on);
    tab.setAttribute("aria-selected", on);
    tab.tabIndex = on ? 0 : -1;
    document.getElementById(`pane-${tab.dataset.pane}`).hidden = !on;
  });
  setEmphasis("is-linked", []); setEmphasis("is-pulsing", []); pulseKey = null;
  resizeCharts();
}
insightTabs.forEach((tab, i) => {
  tab.addEventListener("click", () => showPane(tab.dataset.pane));
  tab.addEventListener("keydown", (e) => {
    const step = { ArrowRight: 1, ArrowLeft: -1 }[e.key];
    if (!step) return;
    const next = insightTabs[(i + step + insightTabs.length) % insightTabs.length];
    next.focus();
    showPane(next.dataset.pane);
  });
});

/* ---- Selection bar: what the charts are filtering by, and how to undo it ---- */
function updateSelectionBar() {
  const bar = document.getElementById("selectionBar");
  bar.hidden = !hasChartSelection();
  if (bar.hidden) return;
  document.getElementById("selectionSummary").textContent = `${currentPoints.length} of ${basePoints.length} listings`;
  const chips = document.getElementById("selectionChips");
  chips.replaceChildren();
  const addChip = (text, clear) => {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "chip-btn chip";
    b.title = "Remove this selection";
    b.textContent = `${text} ✕`;
    b.addEventListener("click", () => { clear(); refreshView(); });
    chips.appendChild(b);
  };
  const labels = (set, buckets) => [...set].sort((a, b) => a - b).map((i) => buckets[i].label).join(", ");
  const { dom, cut, brush } = chartSelection;
  if (dom.size) addChip(`Days: ${labels(dom, DOM_BUCKETS)}`, () => dom.clear());
  if (cut.size) addChip(`Cut: ${labels(cut, CUT_BUCKETS)}`, () => cut.clear());
  if (brush) {
    addChip(`Area: ${Math.round(brush.xMin)}–${Math.round(brush.xMax)} days, ${compactPrice(brush.yMin)}–${compactPrice(brush.yMax)}`,
      () => { chartSelection.brush = null; });
  }
}
document.getElementById("clearSelection").addEventListener("click", () => { resetChartSelection(); refreshView(); });

/* ---- 1. Scatter: asking price vs days on market ---- */
let brushDrag = null;       // { x0, y0, x1, y1 } in canvas pixels while a selection rectangle is being dragged
let brushMode = false;      // touch screens: "Select area" turns a finger drag into a selection instead of a page scroll
let suppressClick = false;  // the click that ends a drag must not also select the dot under the cursor

/** Draws the selection rectangle: the one being dragged, else the current brush (kept in data units so it survives resizes). */
const brushPlugin = {
  id: "brush",
  afterDatasetsDraw(chart) {
    const a = chart.chartArea;
    let r = null;
    if (brushDrag) {
      r = { l: Math.min(brushDrag.x0, brushDrag.x1), r: Math.max(brushDrag.x0, brushDrag.x1),
            t: Math.min(brushDrag.y0, brushDrag.y1), b: Math.max(brushDrag.y0, brushDrag.y1) };
    } else if (chartSelection.brush) {
      const { xMin, xMax, yMin, yMax } = chartSelection.brush;
      const { x, y } = chart.scales;
      r = { l: x.getPixelForValue(xMin), r: x.getPixelForValue(xMax), t: y.getPixelForValue(yMax), b: y.getPixelForValue(yMin) };
    }
    if (!r) return;
    const left = Math.max(r.l, a.left), right = Math.min(r.r, a.right), top = Math.max(r.t, a.top), bottom = Math.min(r.b, a.bottom);
    if (right <= left || bottom <= top) return;
    const { ctx } = chart;
    ctx.save();
    ctx.fillStyle = withAlpha(ACCENT, 0.1);
    ctx.strokeStyle = ACCENT;
    ctx.setLineDash([5, 3]);
    ctx.fillRect(left, top, right - left, bottom - top);
    ctx.strokeRect(left, top, right - left, bottom - top);
    ctx.restore();
  }
};

function scatterDataset(kind, label, pointStyle, pointRotation) {
  // Dots outside the brush stay on the chart (so the rectangle can be adjusted) but fade.
  const faded = (ctx) => ctx.raw && chartSelection.brush && !inBrush(ctx.raw.pt, chartSelection.brush);
  return {
    kind, label, pointStyle, pointRotation, data: [],
    pointRadius: 5, pointHoverRadius: 7, pointHitRadius: 8, pointBorderWidth: 1.5,
    pointBackgroundColor: (ctx) => withAlpha(CHANGE_COLORS[kind], faded(ctx) ? 0.12 : 0.75),
    pointBorderColor: (ctx) => withAlpha(CHANGE_COLORS[kind], faded(ctx) ? 0.25 : 1)
  };
}

function initScatter() {
  const canvas = document.getElementById("scatterChart");
  // The shapes match the badges (▼ cut, ▲ increase) so the three kinds are not told apart by red/green alone.
  charts.scatter = new Chart(canvas, {
    type: "scatter",
    data: { datasets: [
      scatterDataset("down", "Price cut", "triangle", 180),
      scatterDataset("up", "Price increase", "triangle", 0),
      scatterDataset("none", "No change", "circle", 0)
    ] },
    options: {
      responsive: true, maintainAspectRatio: false, animation: { duration: 150 },
      interaction: { mode: "nearest", intersect: true },
      plugins: {
        legend: { display: false },   // an HTML legend sits in the toolbar above the chart
        tooltip: {
          callbacks: {
            title: (items) => items[0].raw.pt.address || "Address not shown",
            label: (item) => {
              const pt = item.raw.pt;
              const lines = [`${money(pt.price)} · ${pt.dom} day${pt.dom === 1 ? "" : "s"} on market`, changeText(pt)];
              if (pt.original_price && pt.original_price !== pt.price) lines.push(`Listed at ${money(pt.original_price)}`);
              return lines;
            }
          }
        }
      },
      scales: {
        x: { type: "linear", min: 0, suggestedMax: 90, title: { display: true, text: "Days on market" }, grid: { color: GRID } },
        y: { title: { display: true, text: "Asking price" }, min: 0, suggestedMax: 1000000, grace: "5%", ticks: { callback: (v, i, ticks) => fmtMoneyTick(v, ticks) }, grid: { color: GRID } }
      }
    },
    plugins: [brushPlugin]
  });
  const chart = charts.scatter;

  const xy = (e) => { const r = canvas.getBoundingClientRect(); return { x: e.clientX - r.left, y: e.clientY - r.top }; };
  const inArea = (p) => p.x >= chart.chartArea.left && p.x <= chart.chartArea.right && p.y >= chart.chartArea.top && p.y <= chart.chartArea.bottom;
  const clamp = (p) => ({ x: Math.min(Math.max(p.x, chart.chartArea.left), chart.chartArea.right),
                          y: Math.min(Math.max(p.y, chart.chartArea.top), chart.chartArea.bottom) });
  const pointAt = (e) => {
    const hit = chart.getElementsAtEventForMode(e, "nearest", { intersect: true }, false)[0];
    return hit ? chart.data.datasets[hit.datasetIndex].data[hit.index].pt : null;
  };

  // Hover: the listing's marker lights up on the map. Click: open it in the list.
  canvas.addEventListener("mousemove", (e) => {
    if (brushDrag) return;
    const pt = pointAt(e);
    canvas.style.cursor = pt ? "pointer" : "crosshair";
    setEmphasis("is-linked", pt ? [pt] : []);
  });
  canvas.addEventListener("mouseleave", () => setEmphasis("is-linked", []));
  canvas.addEventListener("click", (e) => {
    if (suppressClick) return;
    const pt = pointAt(e);
    if (pt) openListing(pt);
  });
  canvas.addEventListener("dblclick", () => {
    if (chartSelection.brush) { chartSelection.brush = null; refreshView(); }
  });

  // Drag: select an area. Fingers scroll the page unless "Select area" is on (CSS: .brushing sets touch-action: none).
  canvas.addEventListener("pointerdown", (e) => {
    if (e.button !== 0 || (e.pointerType === "touch" && !brushMode)) return;
    const p = xy(e);
    if (!inArea(p)) return;
    brushDrag = { x0: p.x, y0: p.y, x1: p.x, y1: p.y };
    canvas.setPointerCapture(e.pointerId);
    setEmphasis("is-linked", []);
  });
  canvas.addEventListener("pointermove", (e) => {
    if (!brushDrag) return;
    const p = clamp(xy(e));
    brushDrag.x1 = p.x; brushDrag.y1 = p.y;
    chart.draw();
  });
  canvas.addEventListener("pointerup", () => {
    if (!brushDrag) return;
    const d = brushDrag;
    brushDrag = null;
    if (Math.abs(d.x1 - d.x0) < 5 || Math.abs(d.y1 - d.y0) < 5) { chart.draw(); return; }   // a click, not a drag
    suppressClick = true;
    setTimeout(() => { suppressClick = false; }, 50);
    const { x, y } = chart.scales;
    const [xa, xb] = [x.getValueForPixel(d.x0), x.getValueForPixel(d.x1)];
    const [ya, yb] = [y.getValueForPixel(d.y0), y.getValueForPixel(d.y1)];
    chartSelection.brush = { xMin: Math.min(xa, xb), xMax: Math.max(xa, xb), yMin: Math.min(ya, yb), yMax: Math.max(ya, yb) };
    refreshView();
  });
  canvas.addEventListener("pointercancel", () => { brushDrag = null; chart.draw(); });

  const toggle = document.getElementById("brushToggle");
  toggle.addEventListener("click", () => {
    brushMode = !brushMode;
    toggle.setAttribute("aria-pressed", brushMode);
    toggle.classList.toggle("on", brushMode);
    canvas.classList.toggle("brushing", brushMode);
  });
}

function updateScatter() {
  // Every listing that passes the bucket picks; the brush only fades dots, it does not remove them.
  const shown = applySelection(basePoints, "brush").filter((pt) => pt.dom != null && pt.price != null);
  charts.scatter.data.datasets.forEach((ds) => {
    ds.data = shown.filter((pt) => changeKind(pt) === ds.kind).map((pt) => ({ x: pt.dom, y: pt.price, pt }));
  });
  // Nothing to draw (no neighbourhood picked yet): give the empty axes a sensible range instead of $0K ticks
  charts.scatter.options.scales.x.suggestedMax = shown.length ? undefined : 90;
  charts.scatter.options.scales.y.suggestedMax = shown.length ? undefined : 1000000;
  charts.scatter.update("none");
  setChartEmpty(charts.scatter.canvas.parentElement, shown.length ? null
    : (areaMode === "neighbourhood" && !selectedNeighbourhood ? "Pick a neighbourhood to see its listings." : "No listings to plot."));
  charts.scatter.canvas.setAttribute("aria-label",
    `Scatter chart of asking price against days on market: ${shown.length} active listings`);
}

/** Clicking a dot: show the list, scroll to the listing and highlight it (highlightListing also marks it on the curve). */
function openListing(pt) {
  if (!currentPoints.some((p) => p.mls === pt.mls)) {   // a faded dot outside the brush: drop the brush so the row exists
    chartSelection.brush = null;
    refreshView();
  }
  // Anything else that could hide the row in the list
  selectedLocationKey = null;
  if (searchQuery) { searchQuery = ""; searchInput.value = ""; }
  setEmphasis("is-linked", []);
  if (currentView !== "listings") setView("listings");
  updateListingsSidebar();
  setTimeout(() => highlightListing(pt.mls), 150);
}

/* ---- 2. Histograms: days on market and cut depth. Click a bar to filter. ---- */
const BUCKET_CHARTS = [
  { key: "dom", canvasId: "domChart", buckets: DOM_BUCKETS, valueOf: (pt) => pt.dom },
  { key: "cut", canvasId: "cutChart", buckets: CUT_BUCKETS, valueOf: cutPct }
];

/** Draws each bar's count just above it. */
const barCountPlugin = {
  id: "barCount",
  afterDatasetsDraw(chart) {
    const { ctx } = chart;
    ctx.save();
    ctx.fillStyle = INK;
    ctx.font = `600 11px ${Chart.defaults.font.family}`;
    ctx.textAlign = "center";
    ctx.textBaseline = "bottom";
    chart.getDatasetMeta(0).data.forEach((bar, i) => ctx.fillText(String(chart.data.datasets[0].data[i]), bar.x, bar.y - 3));
    ctx.restore();
  }
};

/** Pulses the markers of the listings in one histogram bar (i = -1 stops). Only listings that are on the map pulse. */
function pulseBucket(cfg, i) {
  const id = i < 0 ? null : `${cfg.key}:${i}`;
  if (id === pulseKey) return;   // re-adding the class on every mouse move would restart the animation
  pulseKey = id;
  if (id === null) { setEmphasis("is-pulsing", []); return; }
  const onMap = new Set(currentPoints.map((p) => p.mls));
  setEmphasis("is-pulsing", applySelection(basePoints, cfg.key)
    .filter((pt) => bucketIndex(cfg.buckets, cfg.valueOf(pt)) === i && onMap.has(pt.mls)));
}

function initBuckets() {
  BUCKET_CHARTS.forEach((cfg) => {
    const canvas = document.getElementById(cfg.canvasId);
    const chart = new Chart(canvas, {
      type: "bar",
      data: { labels: cfg.buckets.map((b) => b.label), datasets: [{ data: cfg.buckets.map(() => 0), borderRadius: 3, maxBarThickness: 56 }] },
      options: {
        responsive: true, maintainAspectRatio: false, animation: { duration: 150 },
        layout: { padding: { top: 18 } },   // room for the count above the tallest bar
        interaction: { mode: "index", intersect: false },   // the whole column is the target, also for short or empty bars
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: {
            title: (items) => cfg.buckets[items[0].dataIndex].desc,
            label: (item) => `${item.raw} listing${item.raw === 1 ? "" : "s"}`
          } }
        },
        scales: {
          x: { grid: { display: false }, ticks: { maxRotation: 0, autoSkip: false, font: { size: 10 } } },
          y: { beginAtZero: true, ticks: { precision: 0 }, grid: { color: GRID } }
        }
      },
      plugins: [barCountPlugin]
    });
    charts[cfg.key] = chart;

    const bucketAt = (e) => {
      const hit = chart.getElementsAtEventForMode(e, "index", { intersect: false }, false)[0];
      return hit ? hit.index : -1;
    };
    canvas.style.cursor = "pointer";
    canvas.addEventListener("mousemove", (e) => pulseBucket(cfg, bucketAt(e)));
    canvas.addEventListener("mouseleave", () => pulseBucket(cfg, -1));
    canvas.addEventListener("click", (e) => {
      const i = bucketAt(e);
      const picked = chartSelection[cfg.key];
      // An empty bar can't be added (it would just blank the map) but a picked one can always be removed.
      if (i < 0 || (!picked.has(i) && chart.data.datasets[0].data[i] === 0)) return;
      if (!picked.delete(i)) picked.add(i);
      refreshView();
      pulseKey = null;
      pulseBucket(cfg, i);   // refreshView rebuilt the markers; light up the ones this bar now stands for
    });
  });
}

function updateBuckets() {
  BUCKET_CHARTS.forEach((cfg) => {
    // Counts are of the listings that pass the OTHER selections, so a bar shows what picking it would add.
    const counts = cfg.buckets.map(() => 0);
    applySelection(basePoints, cfg.key).forEach((pt) => {
      const i = bucketIndex(cfg.buckets, cfg.valueOf(pt));
      if (i >= 0) counts[i]++;
    });
    const picked = chartSelection[cfg.key];
    const ds = charts[cfg.key].data.datasets[0];
    ds.data = counts;
    ds.backgroundColor = counts.map((_, i) => (!picked.size || picked.has(i)) ? ACCENT : ACCENT_DIM);
    charts[cfg.key].update("none");
    setChartEmpty(charts[cfg.key].canvas.parentElement, counts.some((n) => n > 0) ? null
      : (areaMode === "neighbourhood" && !selectedNeighbourhood ? "Pick a neighbourhood." : "No listings."));
  });
}

/* ---- 3. How long do homes like these take to sell (sold data) ---- */
/** "80% of comparable homes sold by day 52; this one is at day 71." */
function sellNote(pt) {
  const { curve } = soldInfo;
  if (!curve) return "Too few comparable sales nearby to compare this listing's time on market.";
  if (pt.dom == null) return "Days on market is not known for this listing.";
  const day = Math.round(pt.dom);
  const at = day > curve.max_days ? `day ${day} (past the ${curve.max_days}-day chart)` : `day ${day}`;
  const head = curve.p80_days == null
    ? `Fewer than 80% of comparable homes sold within ${curve.max_days} days`
    : `80% of comparable homes sold by day ${curve.p80_days}`;
  return `${head}; this one is at ${at}.`;
}

/** Vertical marker at the focused listing's days on market, with a dot where it meets the curve. Also the empty state. */
const curvePlugin = {
  id: "curveMarker",
  afterDatasetsDraw(chart) {
    const { curve } = soldInfo;
    const pt = focusedPoint();
    if (!curve || !pt || pt.dom == null) return;
    const { ctx, chartArea: a, scales: { x, y } } = chart;
    const day = Math.min(Math.round(pt.dom), curve.max_days);
    const px = x.getPixelForValue(day);
    ctx.save();
    ctx.strokeStyle = INK;
    ctx.fillStyle = INK;
    ctx.lineWidth = 1.5;
    ctx.setLineDash([5, 4]);
    ctx.beginPath(); ctx.moveTo(px, a.top); ctx.lineTo(px, a.bottom); ctx.stroke();
    ctx.setLineDash([]);
    ctx.beginPath(); ctx.arc(px, y.getPixelForValue(curve.share[day] * 100), 4, 0, 2 * Math.PI); ctx.fill();
    ctx.font = `600 11px ${Chart.defaults.font.family}`;
    ctx.textBaseline = "top";
    ctx.textAlign = px > (a.left + a.right) / 2 ? "right" : "left";
    ctx.fillText(`this listing: day ${Math.round(pt.dom)}`, px + (ctx.textAlign === "right" ? -6 : 6), a.top + 4);
    ctx.restore();
  },
  afterDraw(chart) {
    if (soldInfo.curve) return;
    const { ctx, chartArea: a } = chart;
    ctx.save();
    ctx.fillStyle = "#94a3b8";
    ctx.font = `13px ${Chart.defaults.font.family}`;
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText("Not enough comparable sales to draw a curve", (a.left + a.right) / 2, (a.top + a.bottom) / 2);
    ctx.restore();
  }
};

function initCurve() {
  charts.curve = new Chart(document.getElementById("curveChart"), {
    type: "line",
    data: { datasets: [{ data: [], borderColor: ACCENT, backgroundColor: withAlpha(ACCENT, 0.12), fill: true,
                         pointRadius: 0, pointHitRadius: 6, borderWidth: 2, tension: 0 }] },
    options: {
      responsive: true, maintainAspectRatio: false, animation: { duration: 150 },
      interaction: { mode: "nearest", axis: "x", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: {
          title: () => "",
          label: (item) => `${Math.round(item.parsed.y)}% of comparable homes sold by day ${item.parsed.x}`
        } }
      },
      scales: {
        x: { type: "linear", min: 0, max: 180, ticks: { stepSize: 30 }, title: { display: true, text: "Days on market" }, grid: { color: GRID } },
        y: { min: 0, max: 100, ticks: { stepSize: 25, callback: (v) => `${v}%` }, title: { display: true, text: "Sold by then" }, grid: { color: GRID } }
      }
    },
    plugins: [curvePlugin]
  });
}

function updateCurve() {
  const { curve, sample, window: win } = soldInfo;
  const pt = focusedPoint();
  const place = areaMode === "neighbourhood" ? "neighbourhood" : "search circle";
  let note = "Click a listing on the map, in the list or in the scatter to see where it sits.";
  if (areaMode === "neighbourhood" && !selectedNeighbourhood) note = "Pick a neighbourhood to see how long comparable homes took to sell there.";
  else if (pt) note = sellNote(pt);
  else if (!curve) note = sample ? `Only ${sample} comparable sale${sample === 1 ? "" : "s"} in this ${place}; at least 20 are needed for a curve.`
                              : `No comparable sales in this ${place}.`;
  document.getElementById("curveNote").textContent = note;
  const period = win && win.start ? `${formatDate(win.start)} to ${formatDate(win.end)} (the last ${win.days} days of sold data)` : "";
  let foot = "";
  if (period && curve) {
    foot = `Based on ${sample} sales inside the ${place}, ${period}, with the same price, bedroom and property-type filters. `
      + `Only homes that sold are counted: unsold and withdrawn listings are not in the data, so real waits run longer than this curve shows.`;
  } else if (period) {
    foot = `${sample} sale${sample === 1 ? "" : "s"} inside the ${place}, ${period}, with the same price, bedroom and property-type filters.`;
  }
  document.getElementById("curveFoot").textContent = foot;
  if (!chartsReady) return;
  charts.curve.data.datasets[0].data = curve ? curve.share.map((s, n) => ({ x: n, y: s * 100 })) : [];
  charts.curve.options.scales.x.max = curve ? curve.max_days : 180;
  charts.curve.update("none");
}

if (chartsReady) {
  Chart.defaults.font.family = getComputedStyle(document.body).fontFamily;
  Chart.defaults.font.size = 11;
  Chart.defaults.color = cssVar("--chart-text", "#526071");
  initScatter();
  initBuckets();
  initCurve();
} else {
  console.warn("Chart.js did not load; the insight charts are unavailable.");
  document.querySelectorAll(".chart-box").forEach((box) => {
    box.className = "empty-note";
    box.textContent = "The charts could not be loaded (Chart.js is blocked or offline). The map and list still work.";
  });
}

/************** PHONE: TABBED PANEL BELOW THE MAP (Insights | Listings | Areas) **************/
// Same pattern as the sold map. On a phone the desktop side panels are hidden by CSS, so the list and the neighbourhood
// table are panels under the map, picked with the tab bar. The Areas tab only exists in Neighbourhood mode.
(function initMobileTabs() {
  const tabBar = document.getElementById("mobileTabBar");
  const listingsEl = document.getElementById("mobileListingsPanel");
  const rowsEl = document.getElementById("mobileListingRows");
  const sortEl = document.getElementById("mobileSortSelect");
  const areasEl = document.getElementById("mobileAreasPanel");
  const areasTab = document.getElementById("mobileAreasTab");
  const mobileSearch = document.getElementById("mobileListingSearch");
  if (!tabBar || !listingsEl) return;

  let activeTab = "insights";
  let mobilePage = 1;
  const isMobile = () => window.innerWidth <= 900 || (window.innerHeight <= 500 && window.innerWidth > window.innerHeight);

  function applyTab() {
    if (!isMobile()) {   // desktop: back to the side panels, no leftovers from the phone layout
      delete document.body.dataset.mtab;
      tabBar.style.display = "none";
      listingsEl.style.display = "none";
      if (areasEl) areasEl.style.display = "none";
      insightsEl.classList.remove("mobile-hidden");
      insightsEl.style.display = "";
      return;
    }
    if (areasTab) areasTab.hidden = areaMode !== "neighbourhood";
    if (activeTab === "areas" && areaMode !== "neighbourhood") activeTab = window._phoneHomeTab || "insights";
    // Phone: what is on screen is decided by CSS from body[data-mtab] (map | listings | insights | areas), not by inline styles
    document.body.dataset.mtab = activeTab;
    [tabBar, listingsEl, areasEl, insightsEl].forEach((el) => { if (el) el.style.display = ""; });
    insightsEl.classList.remove("mobile-hidden", "hidden");
    tabBar.querySelectorAll(".mobile-tab").forEach((btn) => btn.classList.toggle("active", btn.dataset.tab === activeTab));
    // (neighbourhoods.js, which defines the banner the list starts with, loads after this file: the first call comes with the data)
    if (typeof appendAreaBanner === "function") renderMobileListings();   // cheap (one page of rows); keeps the list current whichever view shows it
    setTimeout(() => { map.invalidateSize(); resizeCharts(); }, 60);   // the charts and the map were laid out while hidden
  }

  function renderMobileListings() {
    if (!rowsEl) return;
    const list = sortedList(sortEl ? sortEl.value : "newest");
    const totalPages = Math.max(1, Math.ceil(list.length / ITEMS_PER_PAGE));
    if (mobilePage > totalPages) mobilePage = 1;
    rowsEl.innerHTML = "";
    appendListBanners(rowsEl, list.length);
    if (!list.length) rowsEl.insertAdjacentHTML("beforeend", `<p class="empty-note">No active listings match.</p>`);
    list.slice((mobilePage - 1) * ITEMS_PER_PAGE, mobilePage * ITEMS_PER_PAGE).forEach((pt) => rowsEl.appendChild(listingRow(pt, { mobile: true })));
    renderPagination(document.getElementById("mobilePaginationControls"), mobilePage, totalPages, list.length, (page) => {
      mobilePage = page;
      renderMobileListings();
      rowsEl.scrollTop = 0;
    });
  }

  // Straight to the view: the bottom bar also has Map and Filters tabs, which the desktop view state knows nothing about
  tabBar.querySelectorAll(".mobile-tab").forEach((btn) => btn.addEventListener("click", () => { activeTab = btn.dataset.tab; applyTab(); }));
  if (sortEl) sortEl.addEventListener("change", () => { mobilePage = 1; renderMobileListings(); });
  if (mobileSearch) mobileSearch.addEventListener("input", (e) => {
    searchQuery = e.target.value.toLowerCase();
    searchInput.value = e.target.value;
    mobilePage = 1;
    renderMobileListings();
  });

  // New data: redraw the phone list when it is the one on screen (new data starts at page 1)
  window._refreshMobileListings = () => { if (isMobile()) { mobilePage = 1; renderMobileListings(); } };

  // A marker tap (or the scatter) opens the list: on a phone that means the Listings tab
  const desktopSetView = setView;
  window.setView = function (view) {
    desktopSetView(view);
    if (isMobile()) {
      activeTab = view === "listings" ? "listings" : view === "areas" ? "areas" : "insights";
      applyTab();
    }
  };
  // neighbourhoods.js calls this when the area mode changes (shows or hides the Areas tab)
  window._syncMobileTabs = (openTab) => { if (openTab) activeTab = openTab; applyTab(); };

  window.addEventListener("resize", () => { applyTab(); setTimeout(() => map.invalidateSize(), 100); });
  window.addEventListener("orientationchange", () => setTimeout(() => { applyTab(); map.invalidateSize(); }, 300));
  applyTab();
})();

fetchActivePoints();
