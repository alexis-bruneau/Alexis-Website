/************** 0. GLOBAL CONSTANTS **************/
const sortSelect = document.getElementById("sortSelect");
let currentPoints = [];   // cache the last fetch so we can re-sort on demand
sortSelect.addEventListener("change", () => {
  updateListingsSidebar(currentPoints);
});

const IMG_BASE = "/redfin-images/";   // Local Flask route for development

// ─── view-state helpers ─────────────────────────────────────────
const listingsPanel = document.getElementById("listingsPanel");
const insightsEl = document.getElementById("insights");
const mainPanel = document.getElementById("mainPanel");
let currentView = "insights";

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

// Global function for onclick handlers in HTML
function toggleView() {
  setView(currentView === "insights" ? "listings" : "insights");
}

document.getElementById("toggleListings")
  .addEventListener("click", () => setView("listings"));


/************** BED-FILTER STATE **************/
let selectedBeds = [];    // e.g. [2,3,4,5]
let rangeMode = false; // first click = “range”, second = “exact”


/************** 1. MAP SETUP **************/
const map = L.map("map", { minZoom: 10, scrollWheelZoom: true })
  .setView([45.4215, -75.6972], 13);

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
  maxClusterRadius: 30,
  disableClusteringAtZoom: 15,
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
  .then(points => {
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
  .catch(console.error);


/************** 3. CIRCLE AND DRAGGABLE HANDLE **************/
const radiusInput = document.getElementById("radiusInput");
let currentRadius = parseFloat(radiusInput.value) || 5;

const handleIcon = L.divIcon({
  className: "drag-handle",
  iconSize: [14, 14],
  iconAnchor: [7, 7]
});

const handle = L.marker([45.4215, -75.6972], {
  draggable: true,
  icon: handleIcon
}).addTo(map);

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
  const r = Math.min(parseFloat(radiusInput.value) || 0, 15);
  radiusInput.value = r;
  circle.setRadius(r * 1000);
  fetchFilteredPoints();
});



/************** 4. FILTER HANDLERS **************/
document.getElementById("filterToggle").addEventListener("click", () => {
  const panel = document.getElementById("filterPanel");
  panel.style.display = panel.style.display === "none" ? "block" : "none";
});

// Clear All button
document.getElementById("clearFilters").addEventListener("click", () => {
  // Reset bed filter
  selectedBeds = [];
  rangeMode = false;
  document.querySelectorAll(".bed-btn").forEach(b => b.classList.remove("selected"));

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

function highlightSidebarListing(mls) {
  // Check if listing is already on the current page
  let row = document.getElementById(`listing-${mls}`);
  if (!row) {
    // Listing is on a different page - find which page it's on
    const idx = getSortedList().findIndex(pt => pt.mls === mls);
    if (idx === -1) return;
    const targetPage = Math.floor(idx / ITEMS_PER_PAGE) + 1;
    if (targetPage > MAX_PAGES) return; // Beyond max pages
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

// Click on map background clears selection
map.on("click", () => {
  if (selectedLocationKey) {
    clearLocationSelection();
  }
});

// ─── Pagination state ───────────────────────────────────────────
const ITEMS_PER_PAGE = 20;
const MAX_PAGES = 5;
let currentPage = 1;

function updateListingsSidebar(points) {
  let list = points.slice();

  // If a location is selected, filter to only those listings
  if (selectedLocationKey) {
    list = list.filter(pt => {
      const key = `${Number(pt.latitude).toFixed(7)},${Number(pt.longitude).toFixed(7)}`;
      return key === selectedLocationKey;
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
  const totalPages = Math.min(Math.ceil(totalItems / ITEMS_PER_PAGE), MAX_PAGES);
  if (currentPage > totalPages) currentPage = 1;
  const startIdx = (currentPage - 1) * ITEMS_PER_PAGE;
  const pageItems = list.slice(startIdx, startIdx + ITEMS_PER_PAGE);

  // Render listings
  const container = document.getElementById("listingRows");
  container.innerHTML = "";

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
          onerror="this.outerHTML = '<div class=&quot;no-image-placeholder&quot;></div>';"
          alt=""
        />
      </div>
      <div class="listing-info">
        <strong>$${parseInt(pt.price).toLocaleString()}</strong>
        <p>${pt.address}</p>
        <p>${pt.beds} 🛏 | ${pt.baths} 🛁</p>
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

  // Render pagination controls
  if (totalPages > 1) {
    const paginationDiv = document.createElement("div");
    paginationDiv.className = "pagination-controls";

    for (let i = 1; i <= totalPages; i++) {
      const btn = document.createElement("button");
      btn.textContent = i;
      btn.className = `page-btn${i === currentPage ? ' active' : ''}`;
      btn.addEventListener("click", () => {
        currentPage = i;
        updateListingsSidebar(currentPoints);
        // Scroll to top of listings
        container.scrollTop = 0;
      });
      paginationDiv.appendChild(btn);
    }

    const info = document.createElement("span");
    info.className = "pagination-info";
    info.textContent = `${totalItems} total`;
    paginationDiv.appendChild(info);

    container.appendChild(paginationDiv);
  }
}


/************** 5. FILTERED DATA REQUEST **************/
const dateOrigin = new Date("2019-01-01");
const msPerDay = 1000 * 60 * 60 * 24;
const today = new Date();
const totalDays = Math.floor((today - dateOrigin) / msPerDay);

function updateDateLabels() {
  const [startOffset, endOffset] = slider.noUiSlider.get().map(v => Math.round(v));
  labelStart.textContent = offsetToDateStr(startOffset);
  labelEnd.textContent = offsetToDateStr(endOffset);
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

async function fetchFilteredPoints() {
  const center = circle.getLatLng();
  const radius_km = circle.getRadius() / 1000;

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

  const payload = {
    center: [center.lat, center.lng],
    radius_km,
    filters
  };

  try {
    const res = await fetch("/filtered-points", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const { points, summary } = await res.json();
    console.log("DEBUG: Received Summary:", summary);

    updateMapMarkers(points); // Update markers with filtered points
    updateStats(summary, points); // Pass points to calculate detailed stats client-side
    currentPoints = points;
    currentPage = 1; // Reset to page 1 on new data
    selectedLocationKey = null; // Clear location selection
    selectedMarkerRef = null;
    updateListingsSidebar(points); // Only filtered points in sidebar

  } catch (err) {
    console.error("Failed to fetch filtered points:", err);
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
          if (currentView === "insights") {
            toggleView();
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
          if (currentView === "insights") {
            toggleView();
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

// Globally available for HTML buttons
window.renderChart = function (metric) {
  currentChartMetric = metric;
  if (lastSummaryData) {
    updateStats(lastSummaryData, lastPointsData);
  }
};

let lastSummaryData = null;
let lastPointsData = null;

function updateStats(summary, points) {
  lastSummaryData = summary;
  lastPointsData = points;

  if (!summary || summary.count === 0) {
    ["count", "avg", "max", "min", "dom", "diff"].forEach(id => {
      const el = document.getElementById(`stat-${id}`);
      if (el) el.textContent = "–";
    });
    if (soldChart) {
      soldChart.data.labels = [];
      soldChart.data.datasets[0].data = [];
      soldChart.data.datasets[1].data = [];
      soldChart.update();
    }
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
      if (pt.price_diff_pct !== null && pt.price_diff_pct !== undefined && !isNaN(pt.price_diff_pct)) {
        diffSum += pt.price_diff_pct;
        diffCount++;
      }
    });

    if (domCount > 0) clientAvgDom = domSum / domCount;
    if (diffCount > 0) clientAvgDiff = diffSum / diffCount;

    console.log(`DEBUG Client Calc: DOM=${clientAvgDom}, Diff=${clientAvgDiff}, Count=${points.length}`);
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

  const diffEl = document.getElementById("stat-diff");
  if (diffEl) {
    if (avg_diff_pct !== null && avg_diff_pct !== undefined) {
      const sign = avg_diff_pct > 0 ? "+" : "";
      diffEl.textContent = `${sign}${Number(avg_diff_pct).toFixed(1)}%`;
      diffEl.style.color = avg_diff_pct >= 0 ? "#10b981" : "#ef4444"; // Green if positive (sold over list), Red if negative
    } else {
      diffEl.textContent = "–";
      diffEl.style.color = "";
    }
  }

  if (by_month && by_month.length) {
    const labels = by_month.map(item => item.month);
    const counts = by_month.map(item => item.count);

    // Determine which metric to show on the line chart
    let lineData = [];
    let lineLabel = "";
    let lineColor = "red";
    let lineBg = "rgba(255,0,0,0.2)";
    let yAxisFormat = (val) => val; // default formatter

    if (currentChartMetric === "price") {
      lineData = by_month.map(item => item.avg_price || 0);
      lineLabel = "Average Price";
      yAxisFormat = (val) => `$${Math.round(val / 1000)}k`;
    } else if (currentChartMetric === "dom") {
      lineData = by_month.map(item => item.avg_dom || 0);
      lineLabel = "Avg Days on Market";
      lineColor = "#f59e0b"; // Amber
      lineBg = "rgba(245, 158, 11, 0.2)";
      yAxisFormat = (val) => `${Math.round(val)}d`;
    } else if (currentChartMetric === "diff") {
      lineData = by_month.map(item => item.avg_diff_pct || 0);
      lineLabel = "Avg Sold vs List %";
      lineColor = "#8b5cf6"; // Violet
      lineBg = "rgba(139, 92, 246, 0.2)";
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
              backgroundColor: "rgba(54, 162, 235, 0.2)",
              borderColor: "rgba(54, 162, 235, 1)",
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
          scales: {
            y: {
              type: "linear",
              position: "left",
              title: { display: true, text: "Listings Sold" },
              ticks: { stepSize: 1, precision: 0 },
              grid: { display: false }
            },
            y1: {
              type: "linear",
              position: "right",
              grid: { drawOnChartArea: true, color: "rgba(0,0,0,0.05)" },
              title: { display: false },
              ticks: {
                callback: function (value) {
                  if (currentChartMetric === "price") return `$${Math.round(value / 1000)}k`;
                  if (currentChartMetric === "dom") return `${Math.round(value)}d`;
                  if (currentChartMetric === "diff") return `${value}%`;
                  return value;
                }
              }
            }
          },
          plugins: {
            legend: { position: "top" },
            tooltip: {
              callbacks: {
                label: ctx => {
                  const label = ctx.dataset.label;
                  const val = ctx.raw;
                  if (label === "Average Price") return `${label}: $${Math.round(val).toLocaleString()}`;
                  if (label === "Avg Days on Market") return `${label}: ${Math.round(val)} days`;
                  if (label === "Avg Sold vs List %") return `${label}: ${val.toFixed(2)}%`;
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
  if (diffMin < 60) return `${diffMin} min ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr} hrs ago`;
  const diffDays = Math.floor(diffHr / 24);
  return `${diffDays} days ago`;
}

// ═══════════════════════════════════════════════════════════
// CHART & STATS
// ═══════════════════════════════════════════════════════════
function offsetToDateStr(offset) {
  const d = new Date(dateOrigin.getTime() + offset * msPerDay);
  return d.toISOString().split("T")[0];
}
