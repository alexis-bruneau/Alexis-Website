/************** 1. MAP SETUP **************/
const map = L.map("map", { minZoom: 10, scrollWheelZoom: true })
             .setView([45.4215, -75.6972], 13);

const ottawaBounds = L.latLngBounds([45.25, -76.0], [45.75, -75.4]);
map.setMaxBounds(ottawaBounds);
map.on("drag", () => map.panInsideBounds(ottawaBounds, { animate:false }));

let selectedMinBeds = null;


L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: "© OpenStreetMap"
}).addTo(map);

/************** 2.  PLOT RED DOTS FROM CSV WITH CLUSTERING **************/
const redDotIcon = L.divIcon({
  className  : "red-dot",
  iconSize   : [14, 14],
  iconAnchor : [7, 7]
});

// Initialize MarkerClusterGroup
const markersCluster = L.markerClusterGroup({
  showCoverageOnHover: false,
  maxClusterRadius: 50,  // Adjust as needed
});

// Load points and add to cluster
// Load points and add to cluster
const markers = [];  // new array to hold markers

fetch("/points.json")
  .then(r => r.json())
  .then(points => {
    points.forEach(({ latitude: lat, longitude: lng }) => {
      if (!isNaN(lat) && !isNaN(lng)) {
        const marker = L.marker([lat, lng], { icon: redDotIcon });
        markersCluster.addLayer(marker);
        markers.push(marker); // Save reference
      } else {
        console.warn("Invalid point:", lat, lng);
      }
    });

    map.addLayer(markersCluster);

    // Initial count
    updateListingsCount();
  })
  .catch(console.error);


/************** 3. AREA‑OF‑INTEREST (AOI) HANDLE + CIRCLE **************/
const radiusInput = document.getElementById("radiusInput");
let currentRadius = parseFloat(radiusInput.value) || 5; // km

/* tiny draggable dot */
const handleIcon = L.divIcon({
  className : "drag-handle",
  iconSize  : [14, 14],
  iconAnchor: [7, 7]
});

/* handle starts at map centre */
const handle = L.marker([45.4215, -75.6972], {
  draggable : true,
  icon      : handleIcon
}).addTo(map);

/* blue circle centred on that handle */
let circle = L.circle(handle.getLatLng(), {
  radius      : currentRadius * 1000, // km → m
  color       : "blue",
  fillColor   : "blue",
  fillOpacity : 0.2,
  weight      : 1
}).addTo(map);

// keep circle following the handle and update count
handle.on("drag", () => {
  circle.setLatLng(handle.getLatLng());
});

handle.on("dragend", () => {
  fetchFilteredPoints();
});




// resize circle when radius box changes and update count
radiusInput.addEventListener("change", () => {
  const r = Math.min(parseFloat(radiusInput.value) || 0, 15);
  radiusInput.value = r;
  circle.setRadius(r * 1000);
  fetchFilteredPoints();
});



// Function to update and log count of listings within circle
function updateListingsCount() {
  const circleCenter = circle.getLatLng();
  const circleRadius = circle.getRadius(); // in meters

  let count = 0;
  markers.forEach(marker => {
    const distance = circleCenter.distanceTo(marker.getLatLng());
    if (distance <= circleRadius) {
      count++;
    }
  });

  console.log(`Listings within circle: ${count}`);
}

// Toggle filter panel on button click
document.getElementById("filterToggle").addEventListener("click", () => {
  const panel = document.getElementById("filterPanel");
  panel.style.display = panel.style.display === "none" ? "block" : "none";
});

soldStart.addEventListener("change", fetchFilteredPoints);
soldEnd.addEventListener("change", fetchFilteredPoints);

document.querySelectorAll(".bed-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    // Unselect all buttons
    document.querySelectorAll(".bed-btn").forEach(b => b.classList.remove("selected"));

    // Select current one
    btn.classList.add("selected");

    // Update global value
    selectedMinBeds = parseInt(btn.dataset.minbeds);

    // Trigger filtering
    fetchFilteredPoints();
  });
});



// Helper function to log current values
function logSoldDateRange() {
  console.log("Sold Date Range:");
  console.log("  Start:", soldStart.value || "(not set)");
  console.log("  End  :", soldEnd.value || "(not set)");
}

// Add event listeners
soldStart.addEventListener("change", logSoldDateRange);
soldEnd.addEventListener("change", logSoldDateRange);

async function fetchFilteredPoints() {
  // Get center and radius from the circle
  const center = circle.getLatLng();
  const radius_km = circle.getRadius() / 1000; // meters → km

  // Get filter values from UI
  const soldStart = document.getElementById("soldStart").value;
  const soldEnd = document.getElementById("soldEnd").value;

  // Build filter object
  const filters = {};
  if (soldStart) filters.sold_start = soldStart;
  if (soldEnd) filters.sold_end = soldEnd;

  if (selectedMinBeds !== null) {
    filters.beds = [1, 2, 3, 4, 5].filter(n => n >= selectedMinBeds);
  }
  

  // Build full request payload
  const payload = {
    center: [center.lat, center.lng],
    radius_km: radius_km,
    filters: filters,
  };

  try {
    const res = await fetch("/filtered-points", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    const data = await res.json();

    // Destructure response
    const { points, summary } = data;

    // Update map
    updateMapMarkers(points);

    // Log summary for now
    console.log("Summary:", summary);

    // Later: update DOM with summary stats
    updateStats(summary);

  } catch (err) {
    console.error("Failed to fetch filtered points:", err);
  }
}

function updateMapMarkers(points) {
  markersCluster.clearLayers(); // Remove old markers
  markers.length = 0; // Reset marker array

  points.forEach(({ latitude: lat, longitude: lng, price }) => {
    if (!isNaN(lat) && !isNaN(lng)) {
      const marker = L.marker([lat, lng], { icon: redDotIcon });
      markersCluster.addLayer(marker);
      markers.push(marker);
    }
  });

  map.addLayer(markersCluster);
}
