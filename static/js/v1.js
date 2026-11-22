/* V1 · analyst dashboard: switches the right-hand rail on when it fits.
 * The rail needs about 1100px; with a 420px sidebar open (listings or the neighbourhood table) it needs 1500px, otherwise the
 * figures drop back under the map, as in the standard layout. */
(function () {
  const body = document.body;
  function apply() {
    const sideOpen = body.classList.contains("side-open");
    const fits = window.innerWidth >= 1100 && window.innerHeight > 500 && (!sideOpen || window.innerWidth >= 1500);
    if (body.classList.contains("rail") === fits) return;
    body.classList.toggle("rail", fits);
    // the map and the charts were laid out for the other shape
    setTimeout(() => {
      if (window.map) map.invalidateSize();
      if (typeof resizeCharts === "function") resizeCharts();
    }, 60);
  }
  new MutationObserver(apply).observe(body, { attributes: true, attributeFilter: ["class"] });
  window.addEventListener("resize", apply);
  apply();
})();
