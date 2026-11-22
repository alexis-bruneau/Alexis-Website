/* V3 · guided cards
 *  1. the chart panel moves beside the map when it fits (body.rail), as a column under the map otherwise;
 *  2. the answer cards say the figures as sentences. They read the same stat elements as the standard cards (which stay in
 *     the page, hidden, because map.js / active.js write to them), so no number is calculated twice. */
(function () {
  const body = document.body;
  const ACTIVE = body.classList.contains("active-map");

  // ---- 1. rail ----
  function applyRail() {
    const sideOpen = body.classList.contains("side-open");
    const fits = window.innerWidth >= 960 && window.innerHeight > 500 && (!sideOpen || window.innerWidth >= 1500);
    if (body.classList.contains("rail") === fits) return;
    body.classList.toggle("rail", fits);
    setTimeout(() => {
      if (window.map) map.invalidateSize();
      if (typeof resizeCharts === "function") resizeCharts();
    }, 60);
  }
  new MutationObserver(applyRail).observe(body, { attributes: true, attributeFilter: ["class"] });
  window.addEventListener("resize", applyRail);
  applyRail();

  // ---- 2. answers ----
  const $ = (id) => document.getElementById(id);
  const text = (id) => ($(id) ? $(id).textContent.trim() : "");
  const has = (v) => v && v !== "–" && v !== "-";
  const set = (key, value, cls) => {
    const el = document.querySelector(`[data-ans="${key}"]`);
    if (!el) return;
    el.textContent = value;
    if (cls !== undefined) { el.classList.remove("is-up", "is-down"); if (cls) el.classList.add(cls); }
  };
  const periodLabel = () => (document.querySelector("[data-period-label]") || {}).textContent || "";

  function soldAnswers() {
    const count = text("stat-count"), avg = text("stat-avg"), dom = text("stat-dom"), diff = text("stat-diff");
    const min = text("stat-min"), max = text("stat-max");
    const basis = /original/i.test(text("stat-diff-label")) ? "original" : "final";
    set("big1", has(count) ? count : "–");
    set("text1", has(count) ? (count === "1" ? "home sold" : "homes sold") : "No sales");
    set("sub1", periodLabel());
    set("big2", has(dom) ? `${dom} days` : "–");
    set("text2", "to sell, on average");
    set("sub2", "Time on the market, listing to sale");
    const n = parseFloat(diff.replace("−", "-"));
    if (has(diff) && !isNaN(n)) {
      const under = n < 0;
      set("big3", `${Math.abs(n).toFixed(1)}%`, under ? "is-down" : "is-up");
      set("text3", Math.abs(n) < 0.05 ? "right at asking" : `${under ? "under" : "over"} asking`);
      set("sub3", `Average sold price against the ${basis} asking price`);
    } else {
      set("big3", "–", ""); set("text3", "vs asking price"); set("sub3", "");
    }
    set("big4", has(avg) ? avg : "–");
    set("text4", "average sale price");
    set("sub4", has(min) && has(max) ? `From ${min} to ${max}` : "");
  }

  function activeAnswers() {
    const count = text("stat-count"), cuts = text("stat-cuts"), avg = text("stat-avg"), median = text("stat-median");
    const dom = text("stat-dom"), domMedian = text("stat-dom-median"), min = text("stat-min"), max = text("stat-max");
    set("big1", has(count) ? count : "–");
    set("text1", count === "1" ? "home for sale" : "homes for sale");
    set("sub1", periodLabel());
    const pct = (cuts.match(/\((\d+)%\)/) || [])[1];
    set("big2", pct !== undefined ? `${pct}%` : "–");
    set("text2", "have cut their price");
    set("sub2", has(cuts) ? `${cuts.replace(/\s*\(.*\)/, "")} of ${count} listings` : "");
    set("big3", has(median) ? median : "–");
    set("text3", "median asking price");
    set("sub3", has(avg) ? `Average ${avg}, from ${min} to ${max}` : "");
    set("big4", has(dom) ? dom.replace(/ days?/, "") : "–");
    set("text4", "days on the market, on average");
    set("sub4", has(domMedian) ? `Half were listed ${domMedian.replace(/ days?/, "")} days ago or less` : "");
  }

  const update = ACTIVE ? activeAnswers : soldAnswers;
  const watched = [...document.querySelectorAll(".summary-cards, [data-period-label], #stat-diff-label")];
  const mo = new MutationObserver(update);
  watched.forEach((el) => mo.observe(el, { childList: true, characterData: true, subtree: true }));
  update();
})();
