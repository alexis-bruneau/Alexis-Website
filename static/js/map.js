/************** 0. GLOBAL CONSTANTS **************/
const IMG_BASE   = "https://redfinstorage.blob.core.windows.net/images/";   // include SAS query-string if your container is Private
const DEFAULT_IMG = IMG_BASE + "no-image.jpg";                              // same container, or any public URL

/************** BED-FILTER STATE **************/
let selectedBeds = [];      // e.g. [2,3,4,5]
let rangeMode    = false;   // first click = “range”, second = “exact”


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

    /* 👇  kick off the “real” data load so each point gets its photo */
    fetchFilteredPoints();              // <-- ADD THIS LINE
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

handle.on("drag", () => {
  circle.setLatLng(handle.getLatLng());
});

handle.on("dragend", () => {
  fetchFilteredPoints();
});

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
});/************** BED BUTTON LOGIC **************/
document.querySelectorAll(".bed-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    const clicked = Number(btn.dataset.bed);
    const allBtns = document.querySelectorAll(".bed-btn");

    if (!rangeMode) {
      /* first click → highlight clicked & everything above it */
      allBtns.forEach(b => {
        const val = Number(b.dataset.bed);
        b.classList.toggle("selected", val >= clicked);
      });
      selectedBeds = [1, 2, 3, 4, 5].filter(n => n >= clicked);
      rangeMode = true;          // next click switches to “exact”
    } else {
      /* second click → keep only the one clicked */
      allBtns.forEach(b => b.classList.remove("selected"));
      btn.classList.add("selected");
      selectedBeds = [clicked];
      rangeMode = false;         // ready for a new range later
    }

    fetchFilteredPoints();       // refresh results
  });
});


/************** 5. FILTERED DATA REQUEST **************/

const dateOrigin = new Date("2019-01-01");
const msPerDay = 1000 * 60 * 60 * 24;

// Calculate total days between 2019-01-01 and today
const today = new Date();
const totalDays = Math.floor((today - dateOrigin) / msPerDay);



// Show readable labels
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
  range: {
    min: 0,
    max: totalDays
  },
  step: 1,
  tooltips: false,
});

function offsetToDateStr(offset) {
  const d = new Date(dateOrigin.getTime() + offset * msPerDay);
  return d.toISOString().split("T")[0];
}

slider.noUiSlider.on("update", function (values, handle) {
  const [startOffset, endOffset] = values.map(v => Math.round(v));
  labelStart.textContent = offsetToDateStr(startOffset);
  labelEnd.textContent = offsetToDateStr(endOffset);
});

slider.noUiSlider.on("change", function () {
  fetchFilteredPoints();
});


updateDateLabels(); // initialize

const priceSlider = document.getElementById("priceRangeSlider");
const labelPriceMin = document.getElementById("labelPriceMin");
const labelPriceMax = document.getElementById("labelPriceMax");

const MIN_PRICE = 0;
const MAX_PRICE = 2000000;

noUiSlider.create(priceSlider, {
  start: [MIN_PRICE, MAX_PRICE],
  connect: true,
  range: {
    min: MIN_PRICE,
    max: MAX_PRICE
  },
  step: 10000,
  tooltips: false
});

priceSlider.noUiSlider.on("update", function (values, handle) {
  const [minPrice, maxPrice] = values.map(v => Math.round(v));
  labelPriceMin.textContent = `$${minPrice.toLocaleString()}`;
  labelPriceMax.textContent = `$${maxPrice.toLocaleString()}`;
});

priceSlider.noUiSlider.on("change", function () {
  fetchFilteredPoints(); // trigger data refresh
});



async function fetchFilteredPoints() {
  const center = circle.getLatLng();
  const radius_km = circle.getRadius() / 1000;

  const [startOffset, endOffset] = slider.noUiSlider.get().map(v => Math.round(v));
  const soldStart = offsetToDateStr(startOffset);
  const soldEnd = offsetToDateStr(endOffset);

  const filters = {};
  if (soldStart) filters.sold_start = soldStart;
  if (soldEnd) filters.sold_end = soldEnd;

  if (selectedBeds.length) {          
    filters.beds = selectedBeds;      
  }


  const payload = {
    center: [center.lat, center.lng],
    radius_km: radius_km,
    filters: filters,
  };

  try {
    const res = await fetch("/filtered-points", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const data = await res.json();
    const { points, summary } = data;

    updateMapMarkers(points);
    updateStats(summary);

    if (!points.length) {
  // Clear the property card
  document.getElementById("carousel-price").textContent = "";
  document.getElementById("carousel-address").textContent = "";
  document.getElementById("carousel-details").textContent = "";
  document.getElementById("carousel-note").textContent = "";
  document.getElementById("carouselImage").src = DEFAULT_IMG;
}


  } catch (err) {
    console.error("Failed to fetch filtered points:", err);
  }
}


/************** 6. MAP & UI UPDATES **************/
function updateMapMarkers(points) {
  markersCluster.clearLayers();
  markers.length = 0;

  points.forEach(point => {
    const { latitude: lat, longitude: lng } = point;
    if (!isNaN(lat) && !isNaN(lng)) {
      const marker = L.marker([lat, lng], { icon: redDotIcon });

      marker.on("click", () => updatePropertyCard(point)); // pass whole point
      markersCluster.addLayer(marker);
      markers.push(marker);
    }
  });

  map.addLayer(markersCluster);
}



let soldChart = null;

function updateStats(summary) {
  if (!summary || summary.count === 0) {
    // Reset all stats
    document.getElementById("stat-count").textContent = "–";
    document.getElementById("stat-avg").textContent = "–";
    document.getElementById("stat-max").textContent = "–";
    document.getElementById("stat-min").textContent = "–";

    // Clear the chart if it exists
    if (soldChart) {
      soldChart.data.labels = [];
      soldChart.data.datasets[0].data = [];
      soldChart.data.datasets[1].data = [];
      soldChart.update();
    }

    return; // Exit early
  }

  // Existing logic if there is data
  const { count, average_price, max_price, min_price, by_month } = summary;

  document.getElementById("stat-count").textContent =
    count ? count.toLocaleString() : "–";
  document.getElementById("stat-avg").textContent =
    average_price ? `$${Math.round(average_price).toLocaleString()}` : "–";
  document.getElementById("stat-max").textContent =
    max_price ? `$${Math.round(max_price).toLocaleString()}` : "–";
  document.getElementById("stat-min").textContent =
    min_price ? `$${Math.round(min_price).toLocaleString()}` : "–";

  if (by_month && by_month.length > 0) {
    const labels = by_month.map(item => item.month);
    const counts = by_month.map(item => item.count);
    const avgPrices = by_month.map(item => item.avg_price || 0);

    if (soldChart) {
      soldChart.data.labels = labels;
      soldChart.data.datasets[0].data = counts;
      soldChart.data.datasets[1].data = avgPrices;
      soldChart.update();
    } else {
      const ctx = document.getElementById("soldByMonthChart").getContext("2d");
      soldChart = new Chart(ctx, {
        type: "bar",
        data: {
          labels: labels,
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
              type: "line",
              borderColor: "rgba(255, 99, 132, 0.9)",
              backgroundColor: "rgba(255, 99, 132, 0.3)",
              fill: false,
              tension: 0.3,
              yAxisID: "y1"
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: {
            mode: 'index',
            intersect: false
          },
          scales: {
            y: {
              type: "linear",
              position: "left",
              title: { display: true, text: "Listings Sold" },
              ticks: {
                stepSize: 1,       // always increment by 1
                precision: 0       // no decimals
              }
            },
            y1: {
              type: "linear",
              position: "right",
              grid: { drawOnChartArea: false },
              title: { display: true, text: "Average Price ($)" },
              ticks: {
                callback: value => `$${value.toLocaleString()}`
              }
            }
          },
          plugins: {
            legend: { position: "top" },
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  if (ctx.dataset.label === "Average Sold Price") {
                    return `${ctx.dataset.label}: $${Math.round(ctx.raw).toLocaleString()}`;
                  }
                  return `${ctx.dataset.label}: ${ctx.raw}`;
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
    const distance = circleCenter.distanceTo(marker.getLatLng());
    if (distance <= circleRadius) count++;
  });

  console.log(`Listings within circle: ${count}`);
}

let imageIndex = 1;
let currentMLS = null;

function updatePropertyCard(point) {
  const { price, address, beds, baths, sold_date, mls, url } = point;

  /* ---------- build the image list ----------------------------- */
  carouselImages.length = 0;
  carouselIndex = 0;

  // try the first 5 photos following <MLS>_1.jpg, _2.jpg…
  for (let i = 1; i <= 5; i++) {
    carouselImages.push(`${IMG_BASE}${mls}_${i}.jpg`);
  }

  /* ---------- the rest of your card logic ---------------------- */
  document.getElementById("carousel-price").textContent =
    `$${parseInt(price).toLocaleString()}`;
  document.getElementById("carousel-address").textContent =
    address || "Unknown address";
  document.getElementById("carousel-details").textContent =
    `${beds ?? "?"} 🛏 | ${baths ?? "?"} 🛁`;
  document.getElementById("carousel-note").textContent =
    getTimeSinceSold(sold_date);

  const viewBtn = document.getElementById("viewDetailsBtn");
  viewBtn.href = url || "#";
  viewBtn.style.pointerEvents = url ? "auto" : "none";

  showCarouselImage();
}


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

let carouselIndex = 0;
let carouselImages = [];

function showCarouselImage() {
  const imgEl = document.getElementById("carouselImage");
  const src   = carouselImages[carouselIndex];

  imgEl.onerror = () => { imgEl.src = DEFAULT_IMG; };
  imgEl.src     = src;
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
  const left = document.getElementById("leftArrow");
  const right = document.getElementById("rightArrow");

  if (left && right) {
    left.style.display = visible ? "block" : "none";
    right.style.display = visible ? "block" : "none";
  }
}
