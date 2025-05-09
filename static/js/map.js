/************** 1. MAP SETUP **************/
const map = L.map("map", { minZoom: 10, scrollWheelZoom: true })
             .setView([45.4215, -75.6972], 13);

const ottawaBounds = L.latLngBounds([45.25, -76.0], [45.75, -75.4]);
map.setMaxBounds(ottawaBounds);
map.on("drag", () => map.panInsideBounds(ottawaBounds, { animate:false }));

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
  updateListingsCount();
});

// resize circle when radius box changes and update count
radiusInput.addEventListener("input", e => {
  let r = Math.min(parseFloat(e.target.value) || 0, 15);
  e.target.value = r;
  circle.setRadius(r * 1000);
  updateListingsCount();
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
