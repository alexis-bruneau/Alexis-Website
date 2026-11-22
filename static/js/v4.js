/* V4 · calm. The headline sentence is written from the same stat elements the cards use (no number is calculated twice). */
(function () {
  const body = document.body;
  const ACTIVE = body.classList.contains("active-map");
  const $ = (id) => document.getElementById(id);
  const text = (id) => ($(id) ? $(id).textContent.trim() : "");
  const has = (v) => v && v !== "–" && v !== "-";
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const em = (s) => `<strong>${esc(s)}</strong>`;

  const SOLD_WHEN = { "1w": "in the last week", "1m": "in the last month", "3m": "in the last three months", "6m": "in the last six months", "1y": "in the last year", all: "since 2021", custom: "in the range you picked" };
  const ACTIVE_WHEN = { "1w": "listed in the last week", "1m": "listed in the last month", "3m": "listed in the last three months", "6m": "listed in the last six months", "1y": "", all: "", custom: "with the time on market you picked" };

  function where() {
    if (typeof areaMode !== "undefined" && areaMode === "neighbourhood") {
      return typeof selectedNeighbourhood !== "undefined" && selectedNeighbourhood ? `in ${selectedNeighbourhood.name}` : null;
    }
    const r = $("radiusInput");
    return r && r.value ? `within ${r.value} km of the pin` : "near the pin";
  }

  function write() {
    const lead = $("hlLead"), sub = $("hlSub");
    const count = text("stat-count");
    const place = where();
    const when = (ACTIVE ? ACTIVE_WHEN : SOLD_WHEN)[document.body.dataset.period || "all"];
    if (place === null) {
      lead.innerHTML = "Pick a neighbourhood to read its market.";
      sub.textContent = "Click an area on the map or a row in the table.";
      return;
    }
    if (!has(count) || count === "0") {
      lead.textContent = ACTIVE ? "No homes for sale match these filters." : "No sales match these filters.";
      sub.textContent = "Widen the time period or clear the filters.";
      return;
    }
    if (ACTIVE) {
      const cuts = text("stat-cuts"), pct = (cuts.match(/\((\d+)%\)/) || [])[1];
      lead.innerHTML = `${em(count)} ${count === "1" ? "home is" : "homes are"} for sale ${place}${when ? ", " + when : ""}` + (pct !== undefined ? `; ${em(pct + "%")} have cut their price.` : ".");
      const parts = [];
      if (has(text("stat-median"))) parts.push(`The median asking price is ${em(text("stat-median"))}`);
      if (has(text("stat-dom"))) parts.push(`homes have been listed ${em(text("stat-dom"))} on average`);
      sub.innerHTML = parts.length ? parts.join(", and ") + "." : "";
    } else {
      lead.innerHTML = `${em(count)} ${count === "1" ? "home" : "homes"} sold ${place} ${when}.`;
      const diff = parseFloat(text("stat-diff").replace("−", "-"));
      const parts = [];
      if (has(text("stat-dom"))) parts.push(`took ${em(text("stat-dom") + " days")} to sell`);
      if (has(text("stat-avg"))) parts.push(`averaged ${em(text("stat-avg"))}`);
      if (!isNaN(diff)) parts.push(Math.abs(diff) < 0.05 ? "closed right at asking" : `closed ${em(Math.abs(diff).toFixed(1) + "% " + (diff < 0 ? "under" : "over"))} asking`);
      sub.innerHTML = parts.length ? "They " + parts.join(", ") + "." : "";
    }
  }

  const mo = new MutationObserver(write);
  const watch = [document.querySelector(".summary-cards"), document.getElementById("areaTitle")].filter(Boolean);
  watch.forEach((el) => mo.observe(el, { childList: true, characterData: true, subtree: true, attributes: true, attributeFilter: ["hidden"] }));
  new MutationObserver(write).observe(body, { attributes: true, attributeFilter: ["data-period"] });
  const radius = $("radiusInput");
  if (radius) radius.addEventListener("change", write);
  write();
})();
