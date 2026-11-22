/************** TIME SELECTION (sold map and active map) **************
 * Quick picks for the period the page describes: 1 week, 1 month, 3 months, 6 months, 1 year, All, Custom.
 *
 *  - Sold map: sets the existing "Sold Date Range" slider to the last N days of sold data (counted back from the latest sale in
 *    the data, not from today's date, so it still has sales when the scrape is a few days behind), then repeats the page's own
 *    /filtered-points request. Custom shows that same slider.
 *  - Active map: a listing has no "posted date" of its own, so the period is the time on market: "listed within N days" sets the
 *    existing Days on Market slider. 1 year and All both mean "no limit" (the slider tops out at 180+ days).
 *
 * No endpoint or filter changes: this file only moves the existing sliders and asks the page to fetch again.
 *
 * Markup it looks for (any layout can host it):
 *   [data-period="1w|1m|3m|6m|1y|all|custom"]   buttons: get .is-active and aria-pressed
 *   [data-period-label]                         text: "Last 3 months · Aug 22 – Nov 21, 2026"
 *   [data-period-custom]                        dock that shows the existing slider while Custom is picked
 *   [data-period-count="1w"]                    optional: how many sales (listings) fall in that period for the current area and
 *                                               filters (one extra request to the same endpoint, without the period)
 *   [data-period-note]                          optional: the active page's wording ("Listed within ...")
 */
(function () {
  const ACTIVE = document.body.classList.contains("active-map");
  const KEYS = ["1w", "1m", "3m", "6m", "1y", "all"];
  const NAMES = { "1w": "Last week", "1m": "Last month", "3m": "Last 3 months", "6m": "Last 6 months", "1y": "Last year", all: "All time", custom: "Custom range" };
  const ACTIVE_NAMES = { "1w": "Listed within a week", "1m": "Listed within a month", "3m": "Listed within 3 months", "6m": "Listed within 6 months", "1y": "Any time on market", all: "Any time on market", custom: "Custom time on market" };
  const NO_LIMIT = ACTIVE ? MAX_DOM : 180;   // MAX_DOM = "180 or more" = no limit (only the active page has this slider)
  const ACTIVE_DAYS = { "1w": 7, "1m": 30, "3m": 90, "6m": 179, "1y": NO_LIMIT, all: NO_LIMIT };
  const $$ = (s) => [...document.querySelectorAll(s)];
  const hasUI = $$("[data-period]").length > 0;
  if (!hasUI) return;

  const slider = ACTIVE ? domSlider : document.getElementById("dateRangeSlider");
  let current = "all";

  const UTC_DAY = 86400000;
  const fmtDate = (d) => d.toLocaleDateString("en-CA", { year: "numeric", month: "short", day: "numeric", timeZone: "UTC" });
  const fmtShort = (d) => d.toLocaleDateString("en-CA", { month: "short", day: "numeric", timeZone: "UTC" });
  const dayDate = (offset) => new Date(dateOrigin.getTime() + offset * UTC_DAY);

  /** [startOffset, endOffset] of a preset on the sold date slider (days after the first sale in the data). */
  function soldWindow(key) {
    if (key === "all") return [0, totalDays];
    const end = dayDate(totalDays), start = new Date(end);
    if (key === "1w") start.setUTCDate(start.getUTCDate() - 6);
    else { start.setUTCMonth(start.getUTCMonth() - { "1m": 1, "3m": 3, "6m": 6, "1y": 12 }[key]); start.setUTCDate(start.getUTCDate() + 1); }
    return [Math.max(0, Math.round((start - dateOrigin) / UTC_DAY)), totalDays];
  }
  const windowFor = (key) => (ACTIVE ? [MIN_DOM, ACTIVE_DAYS[key]] : soldWindow(key));
  const sliderValues = () => slider.noUiSlider.get().map((v) => Math.round(v));

  /** Which presets the slider currently matches (1 year and All can coincide). */
  function matching() {
    const [a, b] = sliderValues();
    return KEYS.filter((k) => { const w = windowFor(k); return w[0] === a && w[1] === b; });
  }

  /** {name, range}: "Last 3 months" and "Aug 22 – Nov 21, 2026" (the active page: "Listed within 3 months" and ""). */
  function labelParts() {
    const [a, b] = sliderValues();
    if (ACTIVE) {
      if (current === "custom") return { name: "Custom time on market", range: b >= MAX_DOM ? `${a}+ days on market` : `${a}–${b} days on market` };
      return { name: ACTIVE_NAMES[current], range: "" };
    }
    return { name: NAMES[current], range: `${fmtShort(dayDate(a))} – ${fmtDate(dayDate(b))}` };
  }
  function labelText() {
    const { name, range } = labelParts();
    return range ? `${name} · ${range}` : name;
  }

  function render() {
    $$("[data-period]").forEach((el) => {
      const on = el.dataset.period === current;
      el.classList.toggle("is-active", on);
      el.setAttribute("aria-pressed", on ? "true" : "false");
    });
    $$("[data-period-label]").forEach((el) => { el.textContent = labelText(); });
    const parts = labelParts();
    $$("[data-period-name]").forEach((el) => { el.textContent = parts.name; });
    $$("[data-period-range]").forEach((el) => { el.textContent = parts.range; });
    $$("[data-period-custom]").forEach((el) => { el.hidden = current !== "custom"; });
    document.body.dataset.period = current;
  }

  function fetchAgain() { return ACTIVE ? fetchActivePoints() : fetchFilteredPoints(); }

  function pick(key) {
    current = key;
    if (key !== "custom") {
      slider.noUiSlider.set(windowFor(key));   // fires "set" -> sync() keeps the highlight right
      fetchAgain();
    }
    render();
  }

  // The slider moved (dragged, Clear All, data loaded): the highlight follows it
  function sync() {
    const m = matching();
    if (current === "custom" && !m.length) { render(); return; }
    current = m.includes(current) ? current : m.length ? m[0] : "custom";
    render();
  }

  // The existing slider lives in the dock when there is one
  const dock = document.querySelector("[data-period-custom]");
  if (dock) {
    const fs = slider.closest("fieldset");
    if (fs) dock.appendChild(fs);
  }

  $$("[data-period]").forEach((el) => el.addEventListener("click", () => pick(el.dataset.period)));
  slider.noUiSlider.on("set", sync);
  // The sold slider's range is replaced when the data's dates arrive: re-sync a moment later
  setTimeout(sync, 1500);
  setTimeout(sync, 4000);
  render();

  // ---- optional: counts per period ------------------------------------------------------------------------
  const countEls = $$("[data-period-count]");
  if (!countEls.length) return;
  let countSeq = 0;

  async function refreshCounts() {
    const seq = ++countSeq;
    const set = (fn) => countEls.forEach((el) => { el.textContent = fn(el.dataset.periodCount); });
    try {
      let area;
      if (areaMode === "neighbourhood") {
        if (!selectedNeighbourhood) { set(() => "–"); return; }
        area = { type: "neighbourhood", id: selectedNeighbourhood.id };
      } else {
        const c = circle.getLatLng();
        area = { type: "circle", center: [c.lat, c.lng], radius_km: circle.getRadius() / 1000 };
      }
      const filters = { ...currentFilters() };
      delete filters.sold_start; delete filters.sold_end; delete filters.min_dom; delete filters.max_dom;
      const res = await fetch(ACTIVE ? "/active-points" : "/filtered-points", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ area, filters })
      });
      const data = await res.json();
      if (seq !== countSeq || data.error) return;
      const pts = data.points || [];
      if (ACTIVE) {
        const doms = pts.map((p) => p.dom);
        set((k) => { const n = ACTIVE_DAYS[k]; return (k === "1y" || k === "all" ? pts.length : doms.filter((d) => d != null && d <= n).length).toLocaleString(); });
      } else {
        const times = pts.map((p) => new Date(p.sold_date).getTime());
        set((k) => {
          const [a] = soldWindow(k), from = dateOrigin.getTime() + a * UTC_DAY;
          return times.filter((t) => t >= from).length.toLocaleString();
        });
      }
    } catch (e) { /* counts are decoration: the page keeps working without them */ }
  }
  const fetchName = ACTIVE ? "fetchActivePoints" : "fetchFilteredPoints";
  const original = window[fetchName];
  window[fetchName] = function () {
    const result = original.apply(this, arguments);
    Promise.resolve(result).then(refreshCounts);
    return result;
  };
  refreshCounts();   // the page's first request was made before this wrapper existed
})();
