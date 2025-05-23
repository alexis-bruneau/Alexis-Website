/************** 0. GLOBAL CONSTANTS **************/
const sortSelect = document.getElementById("sortSelect");
let currentPoints = [];   // cache the last fetch so we can re-sort on demand
sortSelect.addEventListener("change", () => {
  updateListingsSidebar(currentPoints);
});

const IMG_BASE   = "https://redfinstorage.blob.core.windows.net/images/";   // include SAS query-string if private

// ─── view-state helpers ─────────────────────────────────────────
const listingsPanel = document.getElementById("listingsPanel");
const insightsEl    = document.getElementById("insights");
const mainPanel     = document.getElementById("mainPanel");
let currentView     = "insights";

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

document.getElementById("toggleListings")
        .addEventListener("click", () => setView("listings"));
document.getElementById("showInsightsBtn")
        .addEventListener("click", () => setView("insights"));


/************** BED-FILTER STATE **************/
let selectedBeds = [];    // e.g. [2,3,4,5]
let rangeMode    = false; // first click = “range”, second = “exact”


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
  maxClusterRadius: 50
});

const markers = [];

fetch("/points.json")
  .then(r => r.json())
  .then(points => {
    points.forEach(({ latitude: lat, longitude: lng }) => {
      if (!isNaN(lat) && !isNaN(lng)) {
        const marker = L.marker([lat, lng], { icon: redDotIcon });
        markersCluster.addLayer(marker);
        markers.push(marker);
      }
    });
    map.addLayer(markersCluster);
    updateListingsCount();
    /* 👇 kick off the “real” data load so each point gets its photo */
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

handle.on("drag",    () => circle.setLatLng(handle.getLatLng()));
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

// Bed buttons
document.querySelectorAll(".bed-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    const clicked = Number(btn.dataset.bed);
    const allBtns = document.querySelectorAll(".bed-btn");

    if (!rangeMode) {
      allBtns.forEach(b => {
        const val = Number(b.dataset.bed);
        b.classList.toggle("selected", val >= clicked);
      });
      selectedBeds = [1,2,3,4,5].filter(n => n >= clicked);
      rangeMode = true;
    } else {
      allBtns.forEach(b => b.classList.remove("selected"));
      btn.classList.add("selected");
      selectedBeds = [clicked];
      rangeMode = false;
    }

    fetchFilteredPoints();
  });
});

function highlightSidebarListing(mls) {
  const row = document.getElementById(`listing-${mls}`);
  if (!row) return;
  document
    .querySelectorAll("#listingRows .highlight")
    .forEach(el => el.classList.remove("highlight"));
  row.classList.add("highlight");
  row.scrollIntoView({ block: "center", behavior: "smooth" });
}

function updateListingsSidebar(points) {
  let list = points.slice();

  // sort
  switch (sortSelect.value) {
    case 'asc-price':
      list.sort((a,b) => a.price - b.price);
      break;
    case 'desc-price':
      list.sort((a,b) => b.price - a.price);
      break;
    case 'recent':
      list.sort((a,b) => new Date(b.sold_date) - new Date(a.sold_date));
      break;
    default:
      const center = circle.getLatLng();
      list.sort((a,b) => {
        const da = center.distanceTo(L.latLng(a.latitude,a.longitude));
        const db = center.distanceTo(L.latLng(b.latitude,b.longitude));
        return da - db;
      });
  }

  // render top 10
  const container = document.getElementById("listingRows");
  container.innerHTML = "";

  list.slice(0, 10).forEach(pt => {
    const row = document.createElement("div");
    row.className = "listing-row";
    row.id = `listing-${pt.mls}`;

    row.innerHTML = `
      <div class="image-wrapper">
        <img
          src="${IMG_BASE}${pt.mls}_1.jpg"
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

    container.appendChild(row);
  });
}


/************** 5. FILTERED DATA REQUEST **************/
const dateOrigin = new Date("2019-01-01");
const msPerDay   = 1000 * 60 * 60 * 24;
const today      = new Date();
const totalDays  = Math.floor((today - dateOrigin) / msPerDay);

function updateDateLabels() {
  const [startOffset, endOffset] = slider.noUiSlider.get().map(v => Math.round(v));
  labelStart.textContent = offsetToDateStr(startOffset);
  labelEnd.textContent   = offsetToDateStr(endOffset);
}

const slider     = document.getElementById("dateRangeSlider");
const labelStart = document.getElementById("labelStart");
const labelEnd   = document.getElementById("labelEnd");

noUiSlider.create(slider, {
  start:  [0, totalDays],
  connect:true,
  range:  { min:0, max:totalDays },
  step:   1,
  tooltips:false
});
slider.noUiSlider.on("update", updateDateLabels);
slider.noUiSlider.on("change", () => fetchFilteredPoints());
updateDateLabels();

const priceSlider   = document.getElementById("priceRangeSlider");
const labelPriceMin = document.getElementById("labelPriceMin");
const labelPriceMax = document.getElementById("labelPriceMax");
const MIN_PRICE     = 0;
const MAX_PRICE     = 2000000;

noUiSlider.create(priceSlider, {
  start:  [MIN_PRICE, MAX_PRICE],
  connect:true,
  range:  { min:MIN_PRICE, max:MAX_PRICE },
  step:   10000,
  tooltips:false
});
priceSlider.noUiSlider.on("update", (values) => {
  const [minPrice, maxPrice] = values.map(v => Math.round(v));
  labelPriceMin.textContent = `$${minPrice.toLocaleString()}`;
  labelPriceMax.textContent = `$${maxPrice.toLocaleString()}`;
});
priceSlider.noUiSlider.on("change", () => fetchFilteredPoints());

async function fetchFilteredPoints() {
  const center    = circle.getLatLng();
  const radius_km = circle.getRadius() / 1000;

  const [startOffset, endOffset] = slider.noUiSlider.get().map(v => Math.round(v));
  const soldStart = offsetToDateStr(startOffset);
  const soldEnd   = offsetToDateStr(endOffset);

  const [minPrice, maxPrice] = priceSlider.noUiSlider.get().map(v => Math.round(v));

  const filters = {};
  if (soldStart)             filters.sold_start = soldStart;
  if (soldEnd)               filters.sold_end   = soldEnd;
  if (minPrice > MIN_PRICE)  filters.min_price = minPrice;
  if (maxPrice < MAX_PRICE)  filters.max_price = maxPrice;
  if (selectedBeds.length)   filters.beds      = selectedBeds;

  const payload = {
    center:     [center.lat, center.lng],
    radius_km,
    filters
  };

  try {
    const res    = await fetch("/filtered-points", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(payload),
    });
    const { points, summary } = await res.json();

    updateMapMarkers(points);
    updateStats(summary);
    currentPoints = points;
    updateListingsSidebar(points);

  } catch (err) {
    console.error("Failed to fetch filtered points:", err);
  }
}


/************** 6. MAP & UI UPDATES **************/
function updateMapMarkers(points) {
  markersCluster.clearLayers();
  markers.length = 0;

  points.forEach(point => {
    const { latitude: lat, longitude: lng, mls } = point;
    if (!isNaN(lat) && !isNaN(lng)) {
      const marker = L.marker([lat, lng], { icon: redDotIcon });
      marker.on("click", () => {
        if (currentView === "insights") {
          updatePropertyCard(point);
        } else {
          setView("listings");
          updateListingsSidebar(currentPoints);
          setTimeout(() => highlightSidebarListing(mls), 350);
          updatePropertyCard(point);
        }
      });
      markersCluster.addLayer(marker);
      markers.push(marker);
    }
  });

  map.addLayer(markersCluster);
}

let soldChart = null;
function updateStats(summary) {
  if (!summary || summary.count === 0) {
    ["count","avg","max","min"].forEach(id => document.getElementById(`stat-${id}`).textContent = "–");
    if (soldChart) {
      soldChart.data.labels           = [];
      soldChart.data.datasets[0].data = [];
      soldChart.data.datasets[1].data = [];
      soldChart.update();
    }
    return;
  }

  const { count, average_price, max_price, min_price, by_month } = summary;
  document.getElementById("stat-count").textContent = count.toLocaleString();
  document.getElementById("stat-avg").textContent   = `$${Math.round(average_price).toLocaleString()}`;
  document.getElementById("stat-max").textContent   = `$${Math.round(max_price).toLocaleString()}`;
  document.getElementById("stat-min").textContent   = `$${Math.round(min_price).toLocaleString()}`;

  if (by_month && by_month.length) {
    const labels    = by_month.map(item => item.month);
    const counts    = by_month.map(item => item.count);
    const avgPrices = by_month.map(item => item.avg_price || 0);

    if (soldChart) {
      soldChart.data.labels               = labels;
      soldChart.data.datasets[0].data     = counts;
      soldChart.data.datasets[1].data     = avgPrices;
      soldChart.update();
    } else {
      const ctx = document.getElementById("soldByMonthChart").getContext("2d");
      soldChart = new Chart(ctx, {
        type: "bar",
        data: {
          labels,
          datasets: [
            {
              label: "# Sold per Month",
              data: counts,
              backgroundColor: "rgba(54, 162, 235, 0.6)",
              borderColor: "rgba(54, 162, 235, 1)",
              borderWidth: 1,
              yAxisID: "y"
            },
            {
              label: "Average Sold Price",
              data: avgPrices,
              type:  "line",
              fill:  false,
              tension: 0.3,
              yAxisID: "y1"
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          scales: {
            y:  { type: "linear", position: "left", title: { display: true, text: "Listings Sold" }, ticks: { stepSize: 1, precision: 0 } },
            y1: { type: "linear", position: "right", grid: { drawOnChartArea: false }, title: { display: true, text: "Average Price ($)" } }
          },
          plugins: {
            legend: { position: "top" },
            tooltip: {
              callbacks: {
                label: ctx => {
                  const label = ctx.dataset.label;
                  const val   = ctx.raw;
                  return label === "Average Sold Price"
                    ? `${label}: $${Math.round(val).toLocaleString()}`
                    : `${label}: ${val}`;
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

let carouselIndex = 0;
let carouselImages = [];

function updatePropertyCard(point) {
  const { price, address, beds, baths, sold_date, mls, url } = point;

  carouselImages = [];
  carouselIndex  = 0;
  for (let i = 1; i <= 5; i++) {
    carouselImages.push(`${IMG_BASE}${mls}_${i}.jpg`);
  }

  document.getElementById("carousel-price").textContent   = `$${parseInt(price).toLocaleString()}`;
  document.getElementById("carousel-address").textContent = address || "Unknown address";
  document.getElementById("carousel-details").textContent = `${beds ?? "?"} 🛏 | ${baths ?? "?"} 🛁`;
  document.getElementById("carousel-note").textContent    = getTimeSinceSold(sold_date);

  const viewBtn = document.getElementById("viewDetailsBtn");
  viewBtn.href = url || "#";
  viewBtn.style.pointerEvents = url ? "auto" : "none";

  showCarouselImage();
}

function getTimeSinceSold(dateStr) {
  const soldDate = new Date(dateStr);
  const now      = new Date();
  const diffMin  = Math.floor((now - soldDate) / 60000);
  if (diffMin < 60)  return `${diffMin} min ago`;
  const diffHr   = Math.floor(diffMin / 60);
  if (diffHr < 24)  return `${diffHr} hrs ago`;
  const diffDays = Math.floor(diffHr / 24);
  return `${diffDays} days ago`;
}

function showCarouselImage() {
  const imgEl = document.getElementById("carouselImage");
  const src   = carouselImages[carouselIndex] || "";
  imgEl.onerror = () => {
    imgEl.onerror = null;
    imgEl.removeAttribute("src");
    imgEl.classList.add("no-image-carousel");
  };
  imgEl.classList.remove("no-image-carousel");
  imgEl.src = src;
}

function nextCarouselImage() {
  if (carouselImages.length <= 1) return;
  carouselIndex = (carouselIndex + 1) % carouselImages.length;
  showCarouselImage();
}

function prevCarouselImage() {
  if (carouselImages.length <= 1) return;
  carouselIndex = (carouselIndex - 1 + carouselImages.length) % carouselImages.length;
  showCarouselImage();
}

document.addEventListener("DOMContentLoaded", () => {
  showCarouselImage();
});

function showArrows(visible) {
  const left  = document.getElementById("leftArrow");
  const right = document.getElementById("rightArrow");
  if (left && right) {
    left.style.display  = visible ? "block" : "none";
    right.style.display = visible ? "block" : "none";
  }
}

// Utility: convert slider offset to ISO date
function offsetToDateStr(offset) {
  const d = new Date(dateOrigin.getTime() + offset * msPerDay);
  return d.toISOString().split("T")[0];
}
