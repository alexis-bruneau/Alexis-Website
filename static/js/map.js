/************** 0. GLOBAL CONSTANTS **************/
const sortSelect = document.getElementById("sortSelect");
let currentPoints = [];   // cache the last fetch so we can re-sort on demand
sortSelect.addEventListener("change", () => {
  updateListingsSidebar(currentPoints);
});

const IMG_BASE = "/redfin-images/";   // Local Flask route for development

// ─── Search state ───────────────────────────────────────────────
let searchQuery = "";
const listingSearchInput = document.getElementById("listingSearch");
const mobileSearchInput = document.getElementById("mobileListingSearch");

function handleSearchInput(e) {
  searchQuery = e.target.value.toLowerCase();
  // Keep both inputs in sync
  if (listingSearchInput && listingSearchInput !== e.target) listingSearchInput.value = e.target.value;
  if (mobileSearchInput && mobileSearchInput !== e.target) mobileSearchInput.value = e.target.value;
  currentPage = 1;
  updateListingsSidebar(currentPoints);
}

if (listingSearchInput) listingSearchInput.addEventListener("input", handleSearchInput);
if (mobileSearchInput) mobileSearchInput.addEventListener("input", handleSearchInput);

// ─── view-state helpers ─────────────────────────────────────────
const listingsPanel = document.getElementById("listingsPanel");
const insightsEl = document.getElementById("insights");
const mainPanel = document.getElementById("mainPanel");
const areasPanel = document.getElementById("areasPanel");
let currentView = "insights";   // "insights" | "listings" | "areas" (the neighbourhood table, Neighbourhood mode only)

// How the area is picked (see neighbourhoods.js): a draggable "circle", or a fixed "neighbourhood".
let areaMode = "circle";
let selectedNeighbourhood = null;   // {id, name} in Neighbourhood mode once one is clicked

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
}

// Global function for onclick handlers in HTML
function toggleView() {
  (window.setView || setView)(currentView === "listings" ? homeView() : "listings");
}

/** Where closing the listings sidebar goes back to: the neighbourhood table in Neighbourhood mode, else the insights. */
function homeView() { return areaMode === "neighbourhood" ? "areas" : "insights"; }

document.getElementById("toggleListings")
  .addEventListener("click", () => setView("listings"));


/************** BED-FILTER STATE **************/
let selectedBeds = [];    // e.g. [2,3,4,5]
let rangeMode = false; // first click = “range”, second = “exact”

/************** PTYPE-FILTER STATE **************/
let selectedPTypes = [];  // Array of property types


/************** 1. MAP SETUP **************/
const map = L.map("map", { minZoom: 10, scrollWheelZoom: true, zoomControl: false })
  .setView([45.4215, -75.6972], 13);
// Bottom right, like on phones: the top left belongs to the sidebars and the controls bar
L.control.zoom({ position: "bottomright" }).addTo(map);
// The map follows its own box: any layout change (a panel opening, a footer growing, a font loading) resizes it
if (window.ResizeObserver) new ResizeObserver(() => map.invalidateSize()).observe(document.getElementById("map"));

const ottawaBounds = L.latLngBounds([45.25, -76.0], [45.75, -75.4]);
map.setMaxBounds(ottawaBounds);
map.on("drag", () => map.panInsideBounds(ottawaBounds, { animate: false }));

L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: "© OpenStreetMap"
}).addTo(map);


/************** 2. MARKER CLUSTERING **************/
const redDotIcon = L.divIcon({
  className: "red-dot",
  iconSize: [14, 14],
  iconAnchor: [7, 7]
});

const markersCluster = L.markerClusterGroup({
  showCoverageOnHover: false,
  maxClusterRadius: 90,           // the labels are ~80px wide: closer than this they pile on top of each other
  disableClusteringAtZoom: 17,
  spiderfyOnMaxZoom: true,
  removeOutsideVisibleBounds: true,
  animate: true,
  iconCreateFunction: function (cluster) {
    const count = cluster.getChildCount();
    return L.divIcon({
      html: `<div class="cluster-label">${count} listings</div>`,
      className: 'cluster-marker',
      iconSize: [80, 24],
      iconAnchor: [40, 12]
    });
  }
});

const markers = [];
const locationMarkersMap = new Map(); // Map "lat,lng" to marker

fetch("/points.json")
  .then(r => r.json())
  .then(data => {
    // Support both {points: [...], date_range: {...}} and flat array
    const points = data.points || data;

    // Update date slider with actual data range
    if (data.date_range && data.date_range.min_date) {
      dateOrigin = new Date(data.date_range.min_date);
      const maxDate = data.date_range.max_date ? new Date(data.date_range.max_date) : new Date();
      totalDays = Math.floor((maxDate - dateOrigin) / msPerDay);
      slider.noUiSlider.updateOptions({
        range: { min: 0, max: totalDays },
        start: [0, totalDays]
      });
      updateDateLabels();
    }

    // Group properties by coordinates to create one marker per unique location
    const locationGroups = new Map();
    points.forEach((point) => {
      const lat = point.latitude;
      const lng = point.longitude;
      if (!isNaN(lat) && !isNaN(lng)) {
        const key = `${lat.toFixed(7)},${lng.toFixed(7)}`;
        if (!locationGroups.has(key)) {
          locationGroups.set(key, []);
        }
        locationGroups.get(key).push(point);
      }
    });

    // Create one marker per unique location
    locationGroups.forEach((group, key) => {
      const point = group[0];
      const marker = L.marker([point.latitude, point.longitude], { icon: redDotIcon });
      marker._locationKey = key;

      markersCluster.addLayer(marker);
      markers.push(marker);
      locationMarkersMap.set(key, marker);
    });

    map.addLayer(markersCluster);
    updateListingsCount();
    fetchFilteredPoints();
  })
  .catch((err) => {
    // The first fetch only sizes the date slider: the page still works without it
    console.error("Failed to load the date range:", err);
    fetchFilteredPoints();
  });


/************** 3. CIRCLE AND DRAGGABLE HANDLE **************/
const radiusInput = document.getElementById("radiusInput");
let currentRadius = parseFloat(radiusInput.value) || 5;

const handleIcon = L.divIcon({
  className: "drag-handle",
  iconSize: [24, 24],
  iconAnchor: [12, 12]
});

const handle = L.marker([45.4215, -75.6972], {
  draggable: true,
  icon: handleIcon,
  zIndexOffset: 10000, // Stay on top of clustered markers
  title: "Drag to move search area"
}).addTo(map);

handle.bindTooltip("Drag to move search area", {
  permanent: false,
  direction: "right",
  offset: [15, 0]
});

let circle = L.circle(handle.getLatLng(), {
  radius: currentRadius * 1000,
  color: "blue",
  fillColor: "blue",
  fillOpacity: 0.2,
  weight: 1
}).addTo(map);

handle.on("drag", () => circle.setLatLng(handle.getLatLng()));
handle.on("dragend", () => fetchFilteredPoints());

radiusInput.addEventListener("change", () => {
  const r = Math.min(parseFloat(radiusInput.value) || 0, 60);
  radiusInput.value = r;
  circle.setRadius(r * 1000);
  fetchFilteredPoints();
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

/************** 4. FILTER HANDLERS **************/
document.getElementById("filterToggle").addEventListener("click", (e) => {
  L.DomEvent.stopPropagation(e); // Prevent map click from firing immediately
  const panel = document.getElementById("filterPanel");

  if (panel.classList.contains("show")) {
    panel.classList.remove("show");
    // We don't need to set display: none here if the map click does it, 
    // but for consistency let's ensure it's hidden.
    setTimeout(() => { if (!panel.classList.contains("show")) panel.style.display = "none"; }, 200);
  } else {
    // Opening: Clear inline display and add class
    panel.style.display = "block";
    fitFilterPanel();
    // We need a tiny delay for the transition to work if it was just display: none
    requestAnimationFrame(() => {
      panel.classList.add("show");
    });
  }
});

// Clear All button
document.getElementById("clearFilters").addEventListener("click", () => {
  // Reset bed filter
  selectedBeds = [];
  rangeMode = false;
  document.querySelectorAll(".bed-btn").forEach(b => b.classList.remove("selected"));

  // Reset property type filter
  selectedPTypes = [];
  document.querySelectorAll(".ptype-btn").forEach(b => b.classList.remove("selected"));

  // Reset date range slider to full range
  slider.noUiSlider.set([0, totalDays]);

  // Reset price range slider to full range
  priceSlider.noUiSlider.set([MIN_PRICE, MAX_PRICE]);

  // Fetch filtered points with reset filters
  fetchFilteredPoints();
});

// Bed buttons
document.querySelectorAll(".bed-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    const clicked = Number(btn.dataset.bed);
    const allBtns = document.querySelectorAll(".bed-btn");

    if (!rangeMode) {
      // First click: select "X or more" - highlight clicked and all higher
      allBtns.forEach(b => {
        const val = Number(b.dataset.bed);
        b.classList.toggle("selected", val >= clicked);
      });
      selectedBeds = [1, 2, 3, 4, 5].filter(n => n >= clicked);
      rangeMode = true;
    } else {
      // Second click: select "exactly X" - highlight only the clicked button
      allBtns.forEach(b => b.classList.remove("selected"));
      btn.classList.add("selected");
      selectedBeds = [clicked];
      rangeMode = false;
    }

    fetchFilteredPoints();
  });
});

// Property Type buttons
document.querySelectorAll(".ptype-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    const ptype = btn.dataset.ptype;

    // Toggle state
    if (selectedPTypes.includes(ptype)) {
      selectedPTypes = selectedPTypes.filter(t => t !== ptype);
      btn.classList.remove("selected");
    } else {
      selectedPTypes.push(ptype);
      btn.classList.add("selected");
    }

    fetchFilteredPoints();
  });
});

function highlightSidebarListing(mls) {
  // Check if listing is already on the current page
  let row = document.getElementById(`listing-${mls}`);
  if (!row) {
    // Listing is on a different page - find which page it's on
    const idx = getSortedList().findIndex(pt => pt.mls === mls);
    if (idx === -1) return;
    const targetPage = Math.floor(idx / ITEMS_PER_PAGE) + 1;
    currentPage = targetPage;
    updateListingsSidebar(currentPoints);
    row = document.getElementById(`listing-${mls}`);
    if (!row) return;
  }
  document
    .querySelectorAll("#listingRows .highlight")
    .forEach(el => el.classList.remove("highlight"));
  row.classList.add("highlight");
  row.scrollIntoView({ block: "center", behavior: "smooth" });
}

// Helper: returns the current sorted (and optionally filtered) list
function getSortedList() {
  let list = currentPoints.slice();
  if (selectedLocationKey) {
    list = list.filter(pt => {
      const key = `${Number(pt.latitude).toFixed(7)},${Number(pt.longitude).toFixed(7)}`;
      return key === selectedLocationKey;
    });
  }
  switch (sortSelect.value) {
    case 'asc-price': list.sort((a, b) => a.price - b.price); break;
    case 'desc-price': list.sort((a, b) => b.price - a.price); break;
    case 'newest': list.sort((a, b) => new Date(b.sold_date) - new Date(a.sold_date)); break;
    case 'oldest': list.sort((a, b) => new Date(a.sold_date) - new Date(b.sold_date)); break;
    default:
      const center = circle.getLatLng();
      list.sort((a, b) => {
        const da = center.distanceTo(L.latLng(a.latitude, a.longitude));
        const db = center.distanceTo(L.latLng(b.latitude, b.longitude));
        return da - db;
      });
  }
  return list;
}

// ─── Multi-listing selection state ──────────────────────────────
let selectedLocationKey = null;  // When set, sidebar shows only this location's listings
let selectedMarkerRef = null;    // Reference to the currently selected marker

function clearLocationSelection() {
  selectedLocationKey = null;
  if (selectedMarkerRef) {
    // Remove selected styling from marker
    const el = selectedMarkerRef.getElement();
    if (el) el.classList.remove("marker-selected");
    selectedMarkerRef = null;
  }
  // Restore full listings
  updateListingsSidebar(currentPoints);
}

function hideFilterPanel() {
  const panel = document.getElementById("filterPanel");
  if (panel) {
    panel.classList.remove("show");
    // Also explicitly hide it in case CSS hasn't loaded properly
    panel.style.display = "none";
  }
}

// Click on map background clears selection, hides filter panel, and returns to insights view
map.on("click", (e) => {
  if (selectedLocationKey) {
    clearLocationSelection();
  }

  hideFilterPanel();

  // If sidebar is open, close it (returning to insights)
  if (currentView === "listings") {
    setView(homeView());
  }
});

// ─── Pagination state ───────────────────────────────────────────
const ITEMS_PER_PAGE = 20;
let currentPage = 1;

/**
 * Generate smart pagination into a target container.
 * Shows: « 1 2 3 ... N » with window around current page.
 */
function renderPagination(container, currentPg, totalPages, totalItems, onPageClick) {
  container.innerHTML = "";
  if (totalPages <= 1) return;

  // Prev button
  const prevBtn = document.createElement("button");
  prevBtn.textContent = "«";
  prevBtn.className = "page-btn nav-btn";
  prevBtn.disabled = currentPg === 1;
  prevBtn.addEventListener("click", () => onPageClick(currentPg - 1));
  container.appendChild(prevBtn);

  // Determine which page numbers to show
  const pages = getPageNumbers(currentPg, totalPages);
  pages.forEach(p => {
    if (p === "...") {
      const ellipsis = document.createElement("span");
      ellipsis.className = "page-ellipsis";
      ellipsis.textContent = "…";
      container.appendChild(ellipsis);
    } else {
      const btn = document.createElement("button");
      btn.textContent = p;
      btn.className = `page-btn${p === currentPg ? ' active' : ''}`;
      btn.addEventListener("click", () => onPageClick(p));
      container.appendChild(btn);
    }
  });

  // Next button
  const nextBtn = document.createElement("button");
  nextBtn.textContent = "»";
  nextBtn.className = "page-btn nav-btn";
  nextBtn.disabled = currentPg === totalPages;
  nextBtn.addEventListener("click", () => onPageClick(currentPg + 1));
  container.appendChild(nextBtn);

  // Total count
  const info = document.createElement("span");
  info.className = "pagination-info";
  info.textContent = `${totalItems.toLocaleString()} total`;
  container.appendChild(info);
}

/**
 * Returns array of page numbers and "..." ellipsis markers.
 * Always shows first, last, and a window around current page.
 */
function getPageNumbers(current, total) {
  if (total <= 7) {
    return Array.from({ length: total }, (_, i) => i + 1);
  }

  const pages = [];
  // Always show page 1
  pages.push(1);

  // Start of window around current
  const windowStart = Math.max(2, current - 1);
  const windowEnd = Math.min(total - 1, current + 1);

  if (windowStart > 2) pages.push("...");

  for (let i = windowStart; i <= windowEnd; i++) {
    pages.push(i);
  }

  if (windowEnd < total - 1) pages.push("...");

  // Always show last page
  pages.push(total);

  return pages;
}

function updateListingsSidebar(points) {
  let list = points.slice();

  // If a location is selected, filter to only those listings
  if (selectedLocationKey) {
    list = list.filter(pt => {
      const key = `${Number(pt.latitude).toFixed(7)},${Number(pt.longitude).toFixed(7)}`;
      return key === selectedLocationKey;
    });
  }

  // Search filter
  if (searchQuery) {
    list = list.filter(pt => {
      const addr = (pt.address || "").toLowerCase();
      const price = `$${parseInt(pt.price).toLocaleString()}`;
      return addr.includes(searchQuery) || price.includes(searchQuery);
    });
  }

  // sort
  switch (sortSelect.value) {
    case 'asc-price':
      list.sort((a, b) => a.price - b.price);
      break;
    case 'desc-price':
      list.sort((a, b) => b.price - a.price);
      break;
    case 'newest':
      list.sort((a, b) => new Date(b.sold_date) - new Date(a.sold_date));
      break;
    case 'oldest':
      list.sort((a, b) => new Date(a.sold_date) - new Date(b.sold_date));
      break;
    default:
      const center = circle.getLatLng();
      list.sort((a, b) => {
        const da = center.distanceTo(L.latLng(a.latitude, a.longitude));
        const db = center.distanceTo(L.latLng(b.latitude, b.longitude));
        return da - db;
      });
  }

  // Pagination
  const totalItems = list.length;
  const totalPages = Math.ceil(totalItems / ITEMS_PER_PAGE);
  if (currentPage > totalPages) currentPage = 1;
  const startIdx = (currentPage - 1) * ITEMS_PER_PAGE;
  const pageItems = list.slice(startIdx, startIdx + ITEMS_PER_PAGE);

  // Render listings
  const container = document.getElementById("listingRows");
  container.innerHTML = "";

  appendAreaBanner(container, points.length);

  // Show selection banner if location is selected
  if (selectedLocationKey) {
    const banner = document.createElement("div");
    banner.className = "selection-banner";
    banner.innerHTML = `
      <span>Showing ${list.length} listing${list.length !== 1 ? 's' : ''} at this address</span>
      <button onclick="clearLocationSelection()">✕ Show All</button>
    `;
    container.appendChild(banner);
  }

  pageItems.forEach(pt => {
    const row = document.createElement("div");
    row.className = "listing-row";
    row.id = `listing-${pt.mls}`;

    row.innerHTML = `
      <div class="image-wrapper">
        <img
          src="${pt.photo || ''}"
          referrerpolicy="no-referrer"
          loading="lazy"
          onerror="this.outerHTML = '<div class=&quot;no-image-placeholder&quot;></div>';"
          alt=""
        />
      </div>
      <div class="listing-info">
        <strong>$${parseInt(pt.price).toLocaleString()}</strong>
        ${askingBadge(pt)}
        <p>${pt.address}</p>
        <p>${pt.beds ?? "–"} bd · ${pt.baths ?? "–"} ba · <span class="ptype-text">${pt.ptype || 'Unknown Type'}</span></p>
        <p>${getTimeSinceSold(pt.sold_date)}</p>
        <a
          href="${pt.url}"
          target="_blank"
          rel="noopener"
          class="view-btn"
        >View Details</a>
      </div>
    `;

    // Make the entire row clickable
    row.style.cursor = 'pointer';
    row.addEventListener('click', (e) => {
      // Don't trigger if clicking the "View Details" button directly
      if (e.target.classList.contains('view-btn') || e.target.closest('.view-btn')) {
        return;
      }
      // Open the URL in a new tab
      window.open(pt.url, '_blank', 'noopener,noreferrer');
    });

    container.appendChild(row);
  });

  // Render pagination into the dedicated sticky container
  const paginationContainer = document.getElementById("paginationControls");
  renderPagination(paginationContainer, currentPage, totalPages, totalItems, (page) => {
    currentPage = page;
    updateListingsSidebar(currentPoints);
    container.scrollTop = 0;
  });
}


/************** 5. FILTERED DATA REQUEST **************/
let dateOrigin = new Date("2019-01-01");  // default, updated when data loads
const msPerDay = 1000 * 60 * 60 * 24;
let totalDays = Math.floor((new Date() - dateOrigin) / msPerDay);

const MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

function updateDateLabels() {
  const [startOffset, endOffset] = slider.noUiSlider.get().map(v => Math.round(v));
  const startDate = new Date(dateOrigin.getTime() + startOffset * msPerDay);
  const endDate = new Date(dateOrigin.getTime() + endOffset * msPerDay);
  labelStart.textContent = `${MONTH_NAMES[startDate.getMonth()]} ${startDate.getFullYear()}`;
  labelEnd.textContent = `${MONTH_NAMES[endDate.getMonth()]} ${endDate.getFullYear()}`;
}

const slider = document.getElementById("dateRangeSlider");
const labelStart = document.getElementById("labelStart");
const labelEnd = document.getElementById("labelEnd");

noUiSlider.create(slider, {
  start: [0, totalDays],
  connect: true,
  range: { min: 0, max: totalDays },
  step: 1,
  tooltips: false
});
slider.noUiSlider.on("update", updateDateLabels);
slider.noUiSlider.on("change", () => fetchFilteredPoints());
updateDateLabels();


const priceSlider = document.getElementById("priceRangeSlider");
const labelPriceMin = document.getElementById("labelPriceMin");
const labelPriceMax = document.getElementById("labelPriceMax");
const MIN_PRICE = 0;
const MAX_PRICE = 2000000;

noUiSlider.create(priceSlider, {
  start: [MIN_PRICE, MAX_PRICE],
  connect: true,
  range: { min: MIN_PRICE, max: MAX_PRICE },
  step: 10000,
  tooltips: false
});
priceSlider.noUiSlider.on("update", (values) => {
  const [minPrice, maxPrice] = values.map(v => Math.round(v));
  labelPriceMin.textContent = `$${minPrice.toLocaleString()}`;
  labelPriceMax.textContent = `$${maxPrice.toLocaleString()}`;
});
priceSlider.noUiSlider.on("change", () => fetchFilteredPoints());

// The filters the page sends: the same for the circle, the neighbourhood and the neighbourhood summary.
function currentFilters() {
  const [startOffset, endOffset] = slider.noUiSlider.get().map(v => Math.round(v));
  const soldStart = offsetToDateStr(startOffset);
  const soldEnd = offsetToDateStr(endOffset);

  const [minPrice, maxPrice] = priceSlider.noUiSlider.get().map(v => Math.round(v));

  const filters = {};
  if (soldStart) filters.sold_start = soldStart;
  if (soldEnd) filters.sold_end = soldEnd;
  if (minPrice > MIN_PRICE) filters.min_price = minPrice;
  if (maxPrice < MAX_PRICE) filters.max_price = maxPrice;
  if (selectedBeds.length) filters.beds = selectedBeds;
  if (selectedPTypes.length) filters.ptypes = selectedPTypes;
  return filters;
}

let pointsRequestId = 0;   // only the newest request may update the page

async function fetchFilteredPoints() {
  const filters = currentFilters();
  const requestId = ++pointsRequestId;

  let area;
  if (areaMode === "neighbourhood") {
    // The table and the colours follow the filters too, whether or not a neighbourhood is selected
    refreshNeighbourhoodSummary(filters);
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
    const res = await fetch("/filtered-points", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ area, filters }),
    });
    const { points, summary, error } = await res.json();
    if (requestId !== pointsRequestId) return;   // a newer request is on its way
    if (error) throw new Error(error);

    finishLoading();
    clearDataStatus();
    updateMapMarkers(points); // Update markers with filtered points
    updateStats(summary, points); // Pass points to calculate detailed stats client-side
    currentPoints = points;
    currentPage = 1; // Reset to page 1 on new data
    selectedLocationKey = null; // Clear location selection
    selectedMarkerRef = null;
    updateListingsSidebar(points); // Only filtered points in sidebar
    if (window._refreshMobileListings) window._refreshMobileListings(); // Also refresh mobile

    if (!summary || summary.count === 0) {
      setDataStatus("empty", "No sales match these filters.", [
        { label: "Clear filters", run: () => document.getElementById("clearFilters").click() }
      ]);
    }
  } catch (err) {
    console.error("Failed to fetch filtered points:", err);
    if (requestId !== pointsRequestId) return;
    finishLoading();
    setDataStatus("error", "Couldn't load the sales.", [{ label: "Try again", run: () => fetchFilteredPoints() }]);
    setChartEmpty(document.querySelector(".chart-container-full"), "Couldn't load the chart.");
  }
}


/************** 6. MAP & UI UPDATES **************/
function updateMapMarkers(filteredPoints) {
  // Clear all markers from cluster and re-add only filtered ones
  markersCluster.clearLayers();

  // Group filtered points by location (same logic as marker creation)
  const filteredByLocation = new Map();
  filteredPoints.forEach(pt => {
    if (pt.latitude != null && pt.longitude != null) {
      const key = `${Number(pt.latitude).toFixed(7)},${Number(pt.longitude).toFixed(7)}`;
      if (!filteredByLocation.has(key)) {
        filteredByLocation.set(key, []);
      }
      filteredByLocation.get(key).push(pt);
    }
  });

  // Match markers to filtered location groups
  markers.forEach((marker) => {
    const key = marker._locationKey;
    const propertiesHere = filteredByLocation.get(key);

    if (propertiesHere && propertiesHere.length > 0) {
      const count = propertiesHere.length;
      const firstProperty = propertiesHere[0];

      // Create custom icon with price label or count badge
      let iconHTML;
      let className;

      if (count === 1 && firstProperty.price) {
        // Single property - show price label
        const price = firstProperty.price;
        let formattedPrice;
        if (price >= 1000000) {
          formattedPrice = `$${(price / 1000000).toFixed(1)}M`;
        } else {
          formattedPrice = `$${Math.round(price / 1000)}K`;
        }
        className = "price-marker";
        iconHTML = `
          <div class="price-label">${formattedPrice}</div>
        `;
      } else {
        // Multiple properties - show count badge
        className = "count-marker";
        iconHTML = `
          <div class="count-label">${count} listings</div>
        `;
      }

      // Update marker icon
      const newIcon = L.divIcon({
        className: className,
        html: iconHTML,
        iconSize: [80, 24],
        iconAnchor: [40, 12]
      });

      marker.setIcon(newIcon);
      markersCluster.addLayer(marker);

      // Update click handler
      marker.off('click');
      marker.on("click", (e) => {
        L.DomEvent.stopPropagation(e); // Prevent map click from clearing selection

        if (count > 1) {
          // Multi-listing: select this location and filter sidebar
          // Clear previous selection
          if (selectedMarkerRef) {
            const prevEl = selectedMarkerRef.getElement();
            if (prevEl) prevEl.classList.remove("marker-selected");
          }

          selectedLocationKey = key;
          selectedMarkerRef = marker;

          // Add selected styling
          const el = marker.getElement();
          if (el) el.classList.add("marker-selected");

          // Switch to listings view and show filtered listings
          if (currentView !== "listings") {
            setView("listings");
          }
          currentPage = 1;
          updateListingsSidebar(currentPoints);
        } else {
          // Single listing: clear any active selection and restore full view
          if (selectedMarkerRef) {
            const prevEl = selectedMarkerRef.getElement();
            if (prevEl) prevEl.classList.remove("marker-selected");
          }
          selectedLocationKey = null;
          selectedMarkerRef = null;
          currentPage = 1;

          // Switch to listings if needed
          if (currentView !== "listings") {
            setView("listings");
          }

          // Re-render full listings then highlight clicked one
          updateListingsSidebar(currentPoints);
          if (firstProperty.mls) {
            setTimeout(() => highlightSidebarListing(firstProperty.mls), 150);
          }
        }
      });
    }
  });
}

let soldChart = null;
let currentChartMetric = "price"; // default

// What "% diff" compares the sold price with: the asking price when it sold ("last") or the first one ("original").
let diffBasis = "last";
const DIFF_BASIS_LABEL = { last: "Final ask", original: "Original ask" };

// Globally available for HTML buttons
window.renderChart = function (metric) {
  currentChartMetric = metric;
  // The basis switch only means something for the % diff chart
  const basisGroup = document.getElementById("diffBasisGroup");
  if (basisGroup) basisGroup.hidden = metric !== "diff";
  if (lastSummaryData) {
    updateStats(lastSummaryData, lastPointsData);
  }
};

window.setDiffBasis = function (basis) {
  diffBasis = basis;
  document.querySelectorAll("#diffBasisGroup .chart-btn").forEach(b =>
    b.classList.toggle("active", b.dataset.basis === basis));
  if (lastSummaryData) {
    updateStats(lastSummaryData, lastPointsData);
  }
  if (typeof refreshNeighbourhoodViews === "function") refreshNeighbourhoodViews();   // table and colours use the basis too
};

document.querySelectorAll("#diffBasisGroup .chart-btn").forEach(btn =>
  btn.addEventListener("click", () => window.setDiffBasis(btn.dataset.basis)));

const isNumber = v => v !== null && v !== undefined && !isNaN(v);

// "▲ 2.3% over asking" (green) / "▼ 1.5% under asking" (red) under a listing's price, against the final asking price.
// The tooltip gives the dollars and the comparison with the original asking price.
function askingBadge(pt) {
  if (!isNumber(pt.diff_pct_last)) return "";
  const describe = (dollars, pct, what) => {
    const dir = pct > 0 ? "over" : "under";
    return `${Math.abs(pct).toFixed(1)}% ($${Math.abs(Math.round(dollars)).toLocaleString()}) ${dir} ${what}`;
  };
  const pct = pt.diff_pct_last;
  const flat = Math.abs(pct) < 0.05;
  const cls = flat ? "flat" : pct > 0 ? "up" : "down";
  const text = flat ? "At asking" : `${pct > 0 ? "▲" : "▼"} ${Math.abs(pct).toFixed(1)}% ${pct > 0 ? "over" : "under"} asking`;
  let title = flat ? "Sold at the final asking price" : "Sold " + describe(pt.diff_last, pct, "the final asking price");
  if (isNumber(pt.diff_pct_original) && Math.abs(pt.diff_pct_original) >= 0.05) {
    title += `. Original asking price: ${describe(pt.diff_original, pt.diff_pct_original, "it")}`;
  }
  return `<span class="ask-diff ${cls}" title="${title}">${text}</span>`;
}

let lastSummaryData = null;
let lastPointsData = null;

function updateStats(summary, points) {
  lastSummaryData = summary;
  lastPointsData = points;

  if (!summary || summary.count === 0) {
    ["count", "avg", "max", "min", "dom", "diff"].forEach(id => {
      const el = document.getElementById(`stat-${id}`);
      if (el) { el.textContent = "–"; if (id === "diff") el.style.color = ""; }
    });
    if (soldChart) {
      soldChart.data.labels = [];
      soldChart.data.datasets[0].data = [];
      soldChart.data.datasets[1].data = [];
      // nothing to draw: sensible empty axes instead of a column of "$0k"
      soldChart.options.scales.y.suggestedMax = 10;
      soldChart.options.scales.y1.suggestedMax = 1000000;
      soldChart.update();
    }
    const nothingPicked = areaMode === "neighbourhood" && !selectedNeighbourhood;
    setChartEmpty(document.querySelector(".chart-container-full"),
      nothingPicked ? "Pick a neighbourhood to see its trend." : "No sales to chart.");
    return;
  }

  // Calculate metrics client-side if points are available
  let clientAvgDom = null;
  let clientAvgDiff = null;

  if (points && points.length > 0) {
    let domSum = 0;
    let domCount = 0;
    let diffSum = 0;
    let diffCount = 0;

    points.forEach(pt => {
      if (pt.dom !== null && pt.dom !== undefined && !isNaN(pt.dom)) {
        domSum += pt.dom;
        domCount++;
      }
      if (isNumber(pt[`diff_pct_${diffBasis}`])) {
        diffSum += pt[`diff_pct_${diffBasis}`];
        diffCount++;
      }
    });

    if (domCount > 0) clientAvgDom = domSum / domCount;
    if (diffCount > 0) clientAvgDiff = diffSum / diffCount;

  }

  const { count, average_price, max_price, min_price, by_month } = summary;
  // Use client-side calculations if available, otherwise fallback to summary (or null)
  const avg_dom = clientAvgDom;
  const avg_diff_pct = clientAvgDiff;

  document.getElementById("stat-count").textContent = count.toLocaleString();
  document.getElementById("stat-avg").textContent = `$${Math.round(average_price).toLocaleString()}`;
  document.getElementById("stat-max").textContent = `$${Math.round(max_price).toLocaleString()}`;
  document.getElementById("stat-min").textContent = `$${Math.round(min_price).toLocaleString()}`;

  // New metrics
  const domEl = document.getElementById("stat-dom");
  if (domEl) domEl.textContent = avg_dom ? Math.round(avg_dom) : "–";

  const diffLabelEl = document.getElementById("stat-diff-label");
  if (diffLabelEl) diffLabelEl.textContent = `Avg vs ${DIFF_BASIS_LABEL[diffBasis]}`;

  const diffEl = document.getElementById("stat-diff");
  if (diffEl) {
    if (avg_diff_pct !== null && avg_diff_pct !== undefined) {
      const sign = avg_diff_pct > 0 ? "+" : "";
      diffEl.textContent = `${sign}${Number(avg_diff_pct).toFixed(1)}%`;
      diffEl.style.color = avg_diff_pct >= 0 ? "#047857" : "#dc2626"; // Green if positive (sold over list), Red if negative (both pass AA on white)
    } else {
      diffEl.textContent = "–";
      diffEl.style.color = "";
    }
  }

  setChartEmpty(document.querySelector(".chart-container-full"), by_month && by_month.length ? null : "No monthly data for these sales.");
  if (by_month && by_month.length) {
    if (soldChart) { soldChart.options.scales.y.suggestedMax = undefined; soldChart.options.scales.y1.suggestedMax = undefined; }
    const labels = by_month.map(item => item.month);
    const counts = by_month.map(item => item.count);

    // Determine which metric to show on the line chart
    let lineData = [];
    let lineLabel = "";
    let lineColor = cssVar("--chart-line", "red");
    let lineBg = cssVar("--chart-line-fill", "rgba(255,0,0,0.2)");
    let yAxisFormat = (val) => val; // default formatter

    if (currentChartMetric === "price") {
      lineData = by_month.map(item => item.avg_price || 0);
      lineLabel = "Average Price";
      yAxisFormat = (val) => `$${Math.round(val / 1000)}k`;
    } else if (currentChartMetric === "dom") {
      lineData = by_month.map(item => item.avg_dom || 0);
      lineLabel = "Avg Days on Market";
      lineColor = cssVar("--chart-line-dom", "#f59e0b"); // Amber
      lineBg = cssVar("--chart-line-dom-fill", "rgba(245, 158, 11, 0.2)");
      yAxisFormat = (val) => `${Math.round(val)}d`;
    } else if (currentChartMetric === "diff") {
      lineData = by_month.map(item => item[`avg_diff_pct_${diffBasis}`] || 0);
      lineLabel = `Avg Sold vs ${DIFF_BASIS_LABEL[diffBasis]} %`;
      lineColor = cssVar("--chart-line-diff", "#8b5cf6"); // Violet
      lineBg = cssVar("--chart-line-diff-fill", "rgba(139, 92, 246, 0.2)");
      yAxisFormat = (val) => `${val.toFixed(1)}%`;
    }

    if (soldChart) {
      soldChart.data.labels = labels;
      soldChart.data.datasets[0].data = counts;

      // Update line dataset
      soldChart.data.datasets[1].label = lineLabel;
      soldChart.data.datasets[1].data = lineData;
      soldChart.data.datasets[1].borderColor = lineColor;
      soldChart.data.datasets[1].backgroundColor = lineBg;

      soldChart.update();
    } else {
      const ctx = document.getElementById("soldByMonthChart").getContext("2d");
      soldChart = new Chart(ctx, {
        type: "bar",
        data: {
          labels,
          datasets: [
            {
              label: "# Sold",
              data: counts,
              backgroundColor: cssVar("--chart-bar-fill", "rgba(54, 162, 235, 0.2)"),
              borderColor: cssVar("--chart-bar", "rgba(54, 162, 235, 1)"),
              borderWidth: 1,
              yAxisID: "y",
              order: 2
            },
            {
              label: lineLabel,
              data: lineData,
              type: "line",
              fill: false,
              tension: 0.3,
              yAxisID: "y1",
              borderColor: lineColor,
              backgroundColor: lineBg,
              order: 1
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          color: cssVar("--chart-text", "#526071"),
          scales: {
            x: {
              grid: { display: false },
              // "2021-04" -> "Apr ’21", flat and thinned out: tilted, crammed month codes were unreadable
              ticks: {
                maxRotation: 0, autoSkip: true, maxTicksLimit: 8,
                callback: function (value) {
                  const [y, m] = String(this.getLabelForValue(value)).split("-");
                  return m ? `${MONTH_NAMES[Number(m) - 1]} ’${y.slice(2)}` : this.getLabelForValue(value);
                }
              }
            },
            y: {
              type: "linear",
              position: "left",
              beginAtZero: true,
              title: { display: true, text: "Listings Sold" },
              ticks: { precision: 0 },
              grid: { display: false }
            },
            y1: {
              type: "linear",
              position: "right",
              grid: { drawOnChartArea: true, color: "rgba(0,0,0,0.05)" },
              title: { display: false },
              ticks: {
                callback: function (value, index, ticks) {
                  if (currentChartMetric === "price") return fmtMoneyTick(value, ticks);
                  if (currentChartMetric === "dom") return `${Math.round(value)}d`;
                  if (currentChartMetric === "diff") {
                    // Only as many decimals as the tick spacing needs (0.5 step -> 1, 0.25 -> 2, 2 -> 0),
                    // so float noise like -0.4000000000000001 never reaches the label.
                    const step = ticks.length > 1 ? Math.abs(ticks[1].value - ticks[0].value) : 1;
                    let decimals = 0;
                    while (decimals < 3 && Math.abs(step * 10 ** decimals - Math.round(step * 10 ** decimals)) > 1e-6) decimals++;
                    const rounded = Number(value.toFixed(decimals));
                    return `${Object.is(rounded, -0) ? 0 : rounded.toFixed(decimals)}%`;
                  }
                  return value;
                }
              }
            }
          },
          plugins: {
            legend: { position: "top", labels: { usePointStyle: true, boxWidth: 8, boxHeight: 8 } },
            tooltip: {
              callbacks: {
                label: ctx => {
                  const label = ctx.dataset.label;
                  const val = ctx.raw;
                  if (label === "Average Price") return `${label}: $${Math.round(val).toLocaleString()}`;
                  if (label === "Avg Days on Market") return `${label}: ${Math.round(val)} days`;
                  if (label.startsWith("Avg Sold vs")) return `${label}: ${val.toFixed(2)}%`;
                  return `${label}: ${val}`;
                }
              }
            }
          }
        }
      });
    }
  }
}

function updateListingsCount() {
  const circleCenter = circle.getLatLng();
  const circleRadius = circle.getRadius();
  let count = 0;
  markers.forEach(marker => {
    if (circleCenter.distanceTo(marker.getLatLng()) <= circleRadius) count++;
  });
  console.log(`Listings within circle: ${count}`);
}

// ═══════════════════════════════════════════════════════════
// TIME UTILITIES
// ═══════════════════════════════════════════════════════════

function getTimeSinceSold(dateStr) {
  const soldDate = new Date(dateStr);
  const now = new Date();
  const diffMin = Math.floor((now - soldDate) / 60000);
  const plural = (n, unit) => `${n} ${unit}${n === 1 ? "" : "s"} ago`;
  if (diffMin < 60) return plural(Math.max(diffMin, 0), "min");
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return plural(diffHr, "hr");
  const diffDays = Math.floor(diffHr / 24);
  if (diffDays < 60) return plural(diffDays, "day");
  if (diffDays < 730) return plural(Math.floor(diffDays / 30), "month");
  return plural(Math.floor(diffDays / 365), "year");
}

// ═══════════════════════════════════════════════════════════
// CHART & STATS
// ═══════════════════════════════════════════════════════════
function offsetToDateStr(offset) {
  const d = new Date(dateOrigin.getTime() + offset * msPerDay);
  return d.toISOString().split("T")[0];
}

// ═══════════════════════════════════════════════════════════
// MOBILE: TABBED BOTTOM PANEL (Insights / Listings)
// ═══════════════════════════════════════════════════════════
(function initMobileTabs() {
  const tabBar = document.getElementById("mobileTabBar");
  const mobileListingsPanel = document.getElementById("mobileListingsPanel");
  const mobileListingRows = document.getElementById("mobileListingRows");
  const mobileSortSelect = document.getElementById("mobileSortSelect");
  const mobileAreasPanel = document.getElementById("mobileAreasPanel");
  const mobileAreasTab = document.getElementById("mobileAreasTab");
  if (!tabBar || !mobileListingsPanel) return;

  let activeMobileTab = "insights"; // default

  function isMobile() {
    // Portrait phone
    if (window.innerWidth <= 900) return true;
    // Landscape phone (short height, wider than tall)
    if (window.innerHeight <= 500 && window.innerWidth > window.innerHeight) return true;
    return false;
  }

  function applyMobileTab() {
    if (!isMobile()) {
      // Desktop: hide mobile elements, restore desktop insights
      tabBar.style.display = "none";
      mobileListingsPanel.style.display = "none";
      if (mobileAreasPanel) mobileAreasPanel.style.display = "none";
      insightsEl.classList.remove("mobile-hidden");
      return;
    }

    // The Areas tab only exists in Neighbourhood mode
    if (mobileAreasTab) mobileAreasTab.hidden = areaMode !== "neighbourhood";
    if (activeMobileTab === "areas" && areaMode !== "neighbourhood") activeMobileTab = "insights";

    // Show tab bar
    tabBar.style.display = "flex";

    // Update tab active states
    tabBar.querySelectorAll(".mobile-tab").forEach(btn => {
      btn.classList.toggle("active", btn.dataset.tab === activeMobileTab);
    });

    if (mobileAreasPanel) mobileAreasPanel.style.display = "none";
    if (activeMobileTab === "insights") {
      insightsEl.classList.remove("mobile-hidden");
      insightsEl.style.display = "flex";
      mobileListingsPanel.style.display = "none";
    } else if (activeMobileTab === "areas") {
      insightsEl.classList.add("mobile-hidden");
      insightsEl.style.display = "none";
      mobileListingsPanel.style.display = "none";
      if (mobileAreasPanel) mobileAreasPanel.style.display = "flex";
    } else {
      insightsEl.classList.add("mobile-hidden");
      insightsEl.style.display = "none";
      mobileListingsPanel.style.display = "flex";
      renderMobileListings();
    }

    setTimeout(() => map.invalidateSize(), 50);
  }

  // Render listings into the mobile panel
  function renderMobileListings() {
    if (!mobileListingRows) return;

    let list = currentPoints.slice();

    // If a location is selected, filter
    if (selectedLocationKey) {
      list = list.filter(pt => {
        const key = `${Number(pt.latitude).toFixed(7)},${Number(pt.longitude).toFixed(7)}`;
        return key === selectedLocationKey;
      });
    }

    // Search filter (uses shared searchQuery variable)
    if (searchQuery) {
      list = list.filter(pt => {
        const addr = (pt.address || "").toLowerCase();
        const price = `$${parseInt(pt.price).toLocaleString()}`;
        return addr.includes(searchQuery) || price.includes(searchQuery);
      });
    }

    // Sort using mobile sort select
    const sortVal = mobileSortSelect ? mobileSortSelect.value : "newest";
    switch (sortVal) {
      case 'asc-price': list.sort((a, b) => a.price - b.price); break;
      case 'desc-price': list.sort((a, b) => b.price - a.price); break;
      case 'newest': list.sort((a, b) => new Date(b.sold_date) - new Date(a.sold_date)); break;
      case 'oldest': list.sort((a, b) => new Date(a.sold_date) - new Date(b.sold_date)); break;
      default:
        const center = circle.getLatLng();
        list.sort((a, b) => {
          const da = center.distanceTo(L.latLng(a.latitude, a.longitude));
          const db = center.distanceTo(L.latLng(b.latitude, b.longitude));
          return da - db;
        });
    }

    // Pagination
    const MOBILE_PER_PAGE = 20;
    const totalItems = list.length;
    const totalPages = Math.ceil(totalItems / MOBILE_PER_PAGE);
    if (window._mobileCurrentPage > totalPages) window._mobileCurrentPage = 1;
    if (!window._mobileCurrentPage) window._mobileCurrentPage = 1;
    const startIdx = (window._mobileCurrentPage - 1) * MOBILE_PER_PAGE;
    const pageItems = list.slice(startIdx, startIdx + MOBILE_PER_PAGE);

    mobileListingRows.innerHTML = "";

    appendAreaBanner(mobileListingRows, currentPoints.length);

    // Selection banner
    if (selectedLocationKey) {
      const banner = document.createElement("div");
      banner.className = "selection-banner";
      banner.innerHTML = `
        <span>Showing ${list.length} listing${list.length !== 1 ? 's' : ''} at this address</span>
        <button onclick="clearLocationSelection()">✕ Show All</button>
      `;
      mobileListingRows.appendChild(banner);
    }

    pageItems.forEach(pt => {
      const row = document.createElement("div");
      row.className = "listing-row";
      row.innerHTML = `
        <div class="image-wrapper">
          <img
            src="${pt.photo || ''}"
            onerror="this.outerHTML = '<div class=&quot;no-image-placeholder&quot;></div>';"
            alt=""
          />
        </div>
        <div class="listing-info">
          <strong>$${parseInt(pt.price).toLocaleString()}</strong>
          ${askingBadge(pt)}
          <p>${pt.address}</p>
          <p>${pt.beds ?? "–"} bd · ${pt.baths ?? "–"} ba</p>
          <p>${getTimeSinceSold(pt.sold_date)}</p>
        </div>
      `;
      row.style.cursor = 'pointer';
      row.addEventListener('click', () => {
        window.open(pt.url, '_blank', 'noopener,noreferrer');
      });
      mobileListingRows.appendChild(row);
    });

    // Render pagination into the dedicated sticky container
    const mobilePagContainer = document.getElementById("mobilePaginationControls");
    renderPagination(mobilePagContainer, window._mobileCurrentPage, totalPages, totalItems, (page) => {
      window._mobileCurrentPage = page;
      renderMobileListings();
      mobileListingRows.scrollTop = 0;
    });

  }

  // Tab clicks
  tabBar.querySelectorAll(".mobile-tab").forEach(btn => {
    btn.addEventListener("click", () => {
      activeMobileTab = btn.dataset.tab;
      applyMobileTab();
    });
  });

  // Mobile sort select change
  if (mobileSortSelect) {
    mobileSortSelect.addEventListener("change", () => {
      window._mobileCurrentPage = 1;
      renderMobileListings();
    });
  }

  // Mobile search input
  const mobileSearch = document.getElementById("mobileListingSearch");
  if (mobileSearch) {
    mobileSearch.addEventListener("input", (e) => {
      searchQuery = e.target.value.toLowerCase();
      // Keep desktop search in sync
      if (listingSearchInput) listingSearchInput.value = e.target.value;
      window._mobileCurrentPage = 1;
      renderMobileListings();
    });
  }

  // When data updates, refresh mobile listings if visible
  const origUpdateListingsSidebar = window.updateListingsSidebar || updateListingsSidebar;
  // We expose a hook: after desktop sidebar updates, also refresh mobile
  window._refreshMobileListings = function () {
    if (isMobile() && activeMobileTab === "listings") {
      renderMobileListings();
    }
  };

  // When a marker is clicked and view switches to listings on desktop,
  // also switch the mobile tab
  const origSetView = setView;
  window.setView = function (view) {
    origSetView(view);
    if (isMobile()) {
      activeMobileTab = view === "listings" ? "listings" : view === "areas" ? "areas" : "insights";
      applyMobileTab();
    }
  };

  // Called by neighbourhoods.js when the area mode changes (shows or hides the Areas tab)
  window._syncMobileTabs = function (openTab) {
    if (openTab) activeMobileTab = openTab;
    applyMobileTab();
  };

  // Re-evaluate on resize & orientation change
  window.addEventListener("resize", () => {
    applyMobileTab();
    setTimeout(() => map.invalidateSize(), 100);
  });

  // Handle phone rotation specifically
  window.addEventListener("orientationchange", () => {
    setTimeout(() => {
      applyMobileTab();
      map.invalidateSize();
    }, 300); // Delay for browser to complete rotation
  });

  // Initial setup
  applyMobileTab();

  // Expose renderMobileListings so it can be called after data loads
  window._renderMobileListings = renderMobileListings;
})();

