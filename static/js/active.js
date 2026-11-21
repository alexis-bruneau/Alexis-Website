/* Active listings map. Same layout and behaviour as map.js (the sold map), reading /active-points.
   Each listing shows an arrow for its last asking-price change: ▼ cut, ▲ increase. */

/************** STATE **************/
let currentPoints = [];
let currentPage = 1;
let searchQuery = "";
let selectedBeds = [];       // [2,3,4,5]
let rangeMode = false;       // first bed click = "X or more", second = "exactly X"
let selectedPTypes = [];
let selectedChanges = [];    // ["down"], ["up"], both, or none
let selectedLocationKey = null;
let currentView = "insights";
const ITEMS_PER_PAGE = 20;
const MIN_PRICE = 0;
const MAX_PRICE = 2000000;

const sortSelect = document.getElementById("sortSelect");
const listingsPanel = document.getElementById("listingsPanel");
const insightsEl = document.getElementById("insights");
const mainPanel = document.getElementById("mainPanel");

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
const map = L.map("map", { minZoom: 10, scrollWheelZoom: true }).setView([45.4215, -75.6972], 13);
const ottawaBounds = L.latLngBounds([45.25, -76.0], [45.75, -75.4]);
map.setMaxBounds(ottawaBounds);
map.on("drag", () => map.panInsideBounds(ottawaBounds, { animate: false }));
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 19, attribution: "© OpenStreetMap" }).addTo(map);

const markersCluster = L.markerClusterGroup({
  showCoverageOnHover: false,
  maxClusterRadius: 30,
  disableClusteringAtZoom: 15,
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
  setTimeout(() => map.invalidateSize(), 310);
}
document.getElementById("toggleListings").addEventListener("click", () => setView("listings"));
document.getElementById("viewInsightsBtn").addEventListener("click", () => setView(currentView === "insights" ? "listings" : "insights"));

map.on("click", () => {
  if (selectedLocationKey) clearLocationSelection();
  const panel = document.getElementById("filterPanel");
  panel.classList.remove("show");
  panel.style.display = "none";
  if (currentView === "listings") setView("insights");
});

/************** FILTERS **************/
const priceSlider = document.getElementById("priceRangeSlider");
noUiSlider.create(priceSlider, { start: [MIN_PRICE, MAX_PRICE], connect: true, range: { min: MIN_PRICE, max: MAX_PRICE }, step: 10000 });
priceSlider.noUiSlider.on("update", (values) => {
  const [lo, hi] = values.map(Math.round);
  document.getElementById("labelPriceMin").textContent = money(lo);
  document.getElementById("labelPriceMax").textContent = hi >= MAX_PRICE ? `${money(hi)}+` : money(hi);
});
priceSlider.noUiSlider.on("change", () => fetchActivePoints());

document.getElementById("filterToggle").addEventListener("click", (e) => {
  L.DomEvent.stopPropagation(e);
  const panel = document.getElementById("filterPanel");
  if (panel.classList.contains("show")) {
    panel.classList.remove("show");
    setTimeout(() => { if (!panel.classList.contains("show")) panel.style.display = "none"; }, 200);
  } else {
    panel.style.display = "block";
    requestAnimationFrame(() => panel.classList.add("show"));
  }
});

document.getElementById("clearFilters").addEventListener("click", () => {
  selectedBeds = []; rangeMode = false; selectedPTypes = []; selectedChanges = [];
  document.querySelectorAll(".bed-btn, .ptype-btn").forEach((b) => b.classList.remove("selected"));
  priceSlider.noUiSlider.set([MIN_PRICE, MAX_PRICE]);
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
async function fetchActivePoints() {
  const center = circle.getLatLng();
  const [minPrice, maxPrice] = priceSlider.noUiSlider.get().map((v) => Math.round(v));
  const filters = {};
  if (minPrice > MIN_PRICE) filters.min_price = minPrice;
  if (maxPrice < MAX_PRICE) filters.max_price = maxPrice;
  if (selectedBeds.length) filters.beds = selectedBeds;
  if (selectedPTypes.length) filters.ptypes = selectedPTypes;

  try {
    const res = await fetch("/active-points", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ center: [center.lat, center.lng], radius_km: circle.getRadius() / 1000, filters })
    });
    const { points, summary, error } = await res.json();
    if (error) throw new Error(error);
    // The price-change filter is applied here: it only needs fields already on each point.
    currentPoints = selectedChanges.length ? points.filter((p) => selectedChanges.includes(p.last_change_direction)) : points;
    currentPage = 1;
    selectedLocationKey = null;
    updateMapMarkers(currentPoints);
    updateInsights(selectedChanges.length ? summarise(currentPoints, summary) : summary, currentPoints);
    updateListingsSidebar();
  } catch (err) {
    console.error("Failed to fetch active listings:", err);
  }
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
function updateMapMarkers(points) {
  markersCluster.clearLayers();
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
      if (currentView === "insights") setView("listings");
      updateListingsSidebar();
      if (group.length === 1) setTimeout(() => highlightListing(first.mls), 150);
    });
    markersCluster.addLayer(marker);
  });
}

/************** SIDEBAR **************/
function sortedList() {
  let list = currentPoints.slice();
  if (selectedLocationKey) list = list.filter((pt) => locationKey(pt) === selectedLocationKey);
  if (searchQuery) {
    list = list.filter((pt) => (pt.address || "").toLowerCase().includes(searchQuery)
      || money(pt.price).toLowerCase().includes(searchQuery) || String(pt.price).includes(searchQuery));
  }
  const num = (v, fallback) => (v == null ? fallback : v);
  switch (sortSelect.value) {
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

function highlightListing(mls) {
  const list = sortedList();
  const idx = list.findIndex((pt) => pt.mls === mls);
  if (idx === -1) return;
  currentPage = Math.floor(idx / ITEMS_PER_PAGE) + 1;
  updateListingsSidebar();
  const row = document.getElementById(`listing-${mls}`);
  if (!row) return;
  document.querySelectorAll("#listingRows .highlight").forEach((el) => el.classList.remove("highlight"));
  row.classList.add("highlight");
  row.scrollIntoView({ block: "center", behavior: "smooth" });
}

function clearLocationSelection() {
  selectedLocationKey = null;
  updateListingsSidebar();
}
window.clearLocationSelection = clearLocationSelection;

function updateListingsSidebar() {
  const list = sortedList();
  const totalPages = Math.max(1, Math.ceil(list.length / ITEMS_PER_PAGE));
  if (currentPage > totalPages) currentPage = 1;
  const pageItems = list.slice((currentPage - 1) * ITEMS_PER_PAGE, currentPage * ITEMS_PER_PAGE);

  const container = document.getElementById("listingRows");
  container.innerHTML = "";
  if (selectedLocationKey) {
    const banner = document.createElement("div");
    banner.className = "selection-banner";
    banner.innerHTML = `<span>Showing ${list.length} listing${list.length !== 1 ? "s" : ""} at this address</span>
      <button onclick="clearLocationSelection()">✕ Show All</button>`;
    container.appendChild(banner);
  }
  if (!pageItems.length) {
    container.insertAdjacentHTML("beforeend", `<p class="empty-note">No active listings match.</p>`);
  }

  pageItems.forEach((pt) => {
    const row = document.createElement("div");
    row.className = "listing-row";
    row.id = `listing-${pt.mls}`;
    row.style.cursor = "pointer";
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
        <p>${pt.beds ?? "–"} 🛏 | ${pt.baths ?? "–"} 🛁 | <span class="ptype-text">${escapeHtml(pt.ptype || "Unknown Type")}</span></p>
        <p>${dom}${pt.original_price && pt.original_price !== pt.price ? ` · listed at ${money(pt.original_price)}` : ""}</p>
        <a href="${escapeHtml(pt.url)}" target="_blank" rel="noopener" class="view-btn">View Details</a>
      </div>`;
    row.addEventListener("click", (e) => {
      if (e.target.closest(".view-btn")) return;
      window.open(pt.url, "_blank", "noopener,noreferrer");
    });
    container.appendChild(row);
  });

  renderPagination(document.getElementById("paginationControls"), currentPage, totalPages, list.length, (page) => {
    currentPage = page;
    updateListingsSidebar();
    container.scrollTop = 0;
  });
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
searchInput.addEventListener("input", (e) => { searchQuery = e.target.value.toLowerCase(); currentPage = 1; updateListingsSidebar(); });
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
    a.innerHTML = `${changeBadge(pt)}<span class="recent-addr">${pt.address ? escapeHtml(pt.address) : "Address not shown"}</span>
      <span class="recent-date">${formatDate(pt.last_change_date)}</span>`;
    box.appendChild(a);
  });
}

fetchActivePoints();
