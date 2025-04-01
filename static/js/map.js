// Initialize the map centered on Ottawa with a minimum zoom level
var map = L.map("map", {
    minZoom: 10,
    scrollWheelZoom: true
  }).setView([45.4215, -75.6972], 13);
  
  // Define bounds for Ottawa (adjust as needed)
  var ottawaBounds = L.latLngBounds(
    [45.25, -76.0], // southwest corner
    [45.75, -75.4]  // northeast corner
  );
  map.setMaxBounds(ottawaBounds);
  map.on("drag", function() {
    map.panInsideBounds(ottawaBounds, { animate: false });
  });
  
  // Add OpenStreetMap tile layer
  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution:
      '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
  }).addTo(map);
  
  // Create a draggable marker representing the center of the circle
  var marker = L.marker([45.4215, -75.6972], { draggable: true }).addTo(map);
  
  // Get the initial radius from the input (in km) and convert to meters
  var radiusInput = document.getElementById("radiusInput");
  var initialRadius = parseFloat(radiusInput.value);
  var circle = L.circle(marker.getLatLng(), {
    radius: initialRadius * 1000, // convert km to meters
    color: "blue",
    fillOpacity: 0.2
  }).addTo(map);
  
// Update circle center continuously while dragging
marker.on("drag", function(e) {
    circle.setLatLng(e.target.getLatLng());
  });
  
  // When the marker dragging ends, log the new coordinates and send them to the backend
  marker.on("dragend", function(e) {
    var center = marker.getLatLng();
    console.log("Marker moved to:", center);
    
    // Optionally, send the new coordinates to your backend
    fetch("/update-coordinates", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lat: center.lat, lng: center.lng })
    })
    .then(response => response.json())
    .then(data => console.log("Server response:", data))
    .catch(error => console.error("Error sending coordinates:", error));
  });
  
  
  // Update circle radius when the input value changes
  radiusInput.addEventListener("input", function(e) {
    var newRadius = parseFloat(e.target.value);
    // If the value is higher than 15 km, set it to 15
    if (newRadius > 15) {
      newRadius = 15;
      e.target.value = 15;
    }
    circle.setRadius(newRadius * 1000); // update radius in meters
  });
  