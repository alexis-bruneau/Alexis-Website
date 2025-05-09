/************** 1. MAP SETUP **************/
const map = L.map("map", { minZoom: 10, scrollWheelZoom: true })
             .setView([45.4215, -75.6972], 13);

const ottawaBounds = L.latLngBounds([45.25, -76.0], [45.75, -75.4]);
map.setMaxBounds(ottawaBounds);
map.on("drag", () => map.panInsideBounds(ottawaBounds, { animate: false }));

let selectedMinBeds = null;

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
});

document.getElementById("soldStart").addEventListener("change", fetchFilteredPoints);
document.getElementById("soldEnd").addEventListener("change", fetchFilteredPoints);

document.querySelectorAll(".bed-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".bed-btn").forEach(b => b.classList.remove("selected"));
    btn.classList.add("selected");
    selectedMinBeds = parseInt(btn.dataset.minbeds);
    fetchFilteredPoints();
  });
});

/************** 5. FILTERED DATA REQUEST **************/
async function fetchFilteredPoints() {
  const center = circle.getLatLng();
  const radius_km = circle.getRadius() / 1000;

  const soldStart = document.getElementById("soldStart").value;
  const soldEnd = document.getElementById("soldEnd").value;

  const filters = {};
  if (soldStart) filters.sold_start = soldStart;
  if (soldEnd) filters.sold_end = soldEnd;
  if (selectedMinBeds !== null) {
    filters.beds = [1, 2, 3, 4, 5].filter(n => n >= selectedMinBeds);
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

  } catch (err) {
    console.error("Failed to fetch filtered points:", err);
  }
}

/************** 6. MAP & UI UPDATES **************/
function updateMapMarkers(points) {
  markersCluster.clearLayers();
  markers.length = 0;

  points.forEach(({ latitude: lat, longitude: lng }) => {
    if (!isNaN(lat) && !isNaN(lng)) {
      const marker = L.marker([lat, lng], { icon: redDotIcon });
      markersCluster.addLayer(marker);
      markers.push(marker);
    }
  });

  map.addLayer(markersCluster);
}

let soldChart = null;

function updateStats(summary) {
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
              title: { display: true, text: "Listings Sold" }
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
