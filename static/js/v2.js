/* V2 · map first: the time-period menu and the insights drawer. */
(function () {
  const body = document.body;

  // ---- time-period menu ----
  const btn = document.getElementById("periodMenuBtn");
  const pop = document.getElementById("periodMenu");
  function setMenu(open) {
    if (!btn || !pop) return;
    pop.hidden = !open;
    btn.setAttribute("aria-expanded", open ? "true" : "false");
  }
  if (btn && pop) {
    btn.addEventListener("click", (e) => { e.stopPropagation(); setMenu(pop.hidden); });
    // picking a preset closes the menu; "Custom" keeps it open so the slider can be moved
    pop.querySelectorAll("[data-period]").forEach((opt) =>
      opt.addEventListener("click", () => { if (opt.dataset.period !== "custom") setMenu(false); }));
    document.addEventListener("click", (e) => { if (!pop.hidden && !pop.contains(e.target) && e.target !== btn) setMenu(false); });
    document.addEventListener("keydown", (e) => { if (e.key === "Escape" && !pop.hidden) { setMenu(false); btn.focus(); } });
    // a click on the map and on the other controls also closes it (Leaflet stops some clicks from bubbling)
    if (window.map) map.on("click", () => setMenu(false));
    const filterBtn = document.getElementById("filterToggle");
    if (filterBtn) filterBtn.addEventListener("click", () => setMenu(false));
  }

  // ---- insights drawer ----
  const handle = document.getElementById("drawerToggle");
  if (handle) {
    const label = handle.querySelector(".dt-label");
    handle.addEventListener("click", () => {
      const collapsed = body.classList.toggle("drawer-collapsed");
      handle.setAttribute("aria-expanded", collapsed ? "false" : "true");
      label.textContent = collapsed ? "Show insights" : "Hide insights";
      setTimeout(() => { if (typeof resizeCharts === "function") resizeCharts(); }, 80);
    });
  }
})();

/* ═══════════════════════════════════════════════════════════
   PHONE · B · LIST FIRST
   The header (filter chips, summary strip), the Map / List switch and the two bottom sheets (a chip's control, the insights).
   Nothing here runs on a desktop: the elements it drives are display: none there.
   ═══════════════════════════════════════════════════════════ */
(function phoneListFirst() {
  const body = document.body;
  const header = document.getElementById("mHeader");
  if (!header) return;
  const PHONE = window.matchMedia("(max-width: 900px), (max-height: 500px) and (orientation: landscape)");
  const ACTIVE = body.classList.contains("active-map");
  const $ = (id) => document.getElementById(id);
  const viewBtn = $("mViewBtn"), scrim = $("mScrim"), controls = $("controls");

  // ── views: "listings" / "areas" (the list view) or "map" ──
  let lastList = "listings";
  window._phoneHomeTab = "listings";
  function goTab(tab) {
    if (typeof window._syncMobileTabs === "function") window._syncMobileTabs(tab);
    else body.dataset.mtab = tab;
  }
  const mapShowing = () => body.dataset.mtab === "map";
  function syncViewBtn() {
    const onMap = mapShowing();
    $("mViewIcon").textContent = onMap ? "☰" : "🗺️";
    $("mViewText").textContent = onMap ? "List" : "Map";
  }
  new MutationObserver(() => {
    if (!PHONE.matches) return;
    const t = body.dataset.mtab;
    if (t === "listings" || t === "areas") lastList = t;
    if (t === "insights") { goTab(lastList); return; }   // the insights are a sheet here, not a view
    syncViewBtn();
    body.classList.remove("m-scrolled");
    measure();
  }).observe(body, { attributes: true, attributeFilter: ["data-mtab"] });
  viewBtn.addEventListener("click", () => goTab(mapShowing() ? lastList : "map"));

  // A marker tap or a closed list asks for a "home" view: keep to the view that is showing
  const prevSetView = window.setView;
  window.setView = function (view) {
    prevSetView(view);
    if (!PHONE.matches) return;
    if (view === "insights") goTab(mapShowing() ? "map" : lastList);
  };

  // ── header height → the list starts right under it ──
  function measure() {
    const h = Math.ceil(header.getBoundingClientRect().height);
    if (h) document.documentElement.style.setProperty("--m-head-h", `${h}px`);
  }
  if (window.ResizeObserver) new ResizeObserver(measure).observe(header);
  window.addEventListener("resize", measure);

  // the summary strip tucks away while the list scrolls down, and returns at the top
  ["mobileListingsPanel", "mobileAreasPanel"].forEach((id) => {
    const el = $(id);
    if (!el) return;
    el.addEventListener("scroll", () => {
      const down = el.scrollTop > 60, up = el.scrollTop < 8;
      if (down && !body.classList.contains("m-scrolled")) body.classList.add("m-scrolled");
      else if (up && body.classList.contains("m-scrolled")) body.classList.remove("m-scrolled");
    }, { passive: true });
  });

  // ── the summary strip ──
  const compact = (txt) => {
    const n = Number(String(txt).replace(/[^0-9.]/g, ""));
    if (!n) return "–";
    return n >= 1e6 ? `$${(n / 1e6).toFixed(2).replace(/\.?0+$/, "")}M` : `$${Math.round(n / 1000)}k`;
  };
  const txt = (id) => ($(id).textContent || "").trim();
  function refreshStrip() {
    const n = txt("stat-count");
    $("mS1").textContent = n || "–";
    $("mS2").textContent = compact(txt("stat-avg"));
    const dom = txt("stat-dom");
    $("mS3").textContent = dom && dom !== "–" ? dom.replace(/\s*days?$/, "") : "–";
    $("mS4").textContent = (ACTIVE ? txt("stat-cuts") : txt("stat-diff")) || "–";
    const el = $("mS4");
    el.className = !ACTIVE && txt("stat-diff").startsWith("-") ? "is-down" : "";
    refreshChips();
  }
  ["stat-count", "stat-avg", "stat-dom", ACTIVE ? "stat-cuts" : "stat-diff"].forEach((id) =>
    new MutationObserver(refreshStrip).observe($(id), { childList: true, characterData: true, subtree: true }));
  $("mStrip").addEventListener("click", openInsights);

  // the page's "no results" / "couldn't load" note, repeated under the chips
  const status = $("dataStatus"), mirror = $("mSumStatus");
  function refreshStatus() {
    if (status.hidden || !status.firstChild) { mirror.hidden = true; mirror.replaceChildren(); return; }
    mirror.className = `m-sum-status ${status.className.includes("is-error") ? "is-error" : ""}`;
    mirror.replaceChildren();
    const t = status.querySelector(".data-status-text");
    if (t) { const s = document.createElement("span"); s.textContent = t.textContent; mirror.appendChild(s); }
    status.querySelectorAll("button").forEach((orig) => {
      const b = document.createElement("button");
      b.type = "button"; b.textContent = orig.textContent;
      b.addEventListener("click", () => orig.click());
      mirror.appendChild(b);
    });
    mirror.hidden = false;
  }
  new MutationObserver(refreshStatus).observe(status, { childList: true, attributes: true, subtree: true });

  // ── filter chips: each one says what it holds and opens a sheet with that one control ──
  const TITLES = { area: "Area", price: ACTIVE ? "Asking price" : "Price", beds: "Bedrooms", ptype: "Property type", period: ACTIVE ? "Time on market" : "Sold when", change: "Price changes" };
  const chipEls = [...document.querySelectorAll(".m-chip[data-chip]")];
  const chip = (name) => chipEls.find((c) => c.dataset.chip === name);
  function sliderState(id) {
    const el = $(id);
    if (!el || !el.noUiSlider) return null;
    const r = el.noUiSlider.options.range, v = el.noUiSlider.get().map((x) => Math.round(Number(x)));
    return { lo: v[0], hi: v[1], min: Math.round(r.min), max: Math.round(r.max), changed: v[0] > Math.round(r.min) || v[1] < Math.round(r.max) };
  }
  function setChip(name, label, on) {
    const c = chip(name);
    if (!c) return;
    c.textContent = label;
    c.classList.toggle("is-on", !!on);
    c.setAttribute("aria-label", `${TITLES[name]}: ${label}`);
  }
  function refreshChips() {
    body.classList.toggle("m-no-area", areaMode === "neighbourhood" && !selectedNeighbourhood);   // no figures to show until an area is picked
    // area
    if (areaMode === "neighbourhood") setChip("area", selectedNeighbourhood ? selectedNeighbourhood.name : "Neighbourhood", true);
    else setChip("area", `Circle · ${$("radiusInput").value || 15} km`, false);
    // price
    const pr = sliderState("priceRangeSlider");
    if (pr) {
      const label = !pr.changed ? (ACTIVE ? "Asking price" : "Price")
        : pr.lo <= pr.min ? `Up to ${compact(pr.hi)}` : pr.hi >= pr.max ? `${compact(pr.lo)}+` : `${compact(pr.lo)}–${compact(pr.hi)}`;
      setChip("price", label, pr.changed);
    }
    // beds: "2+ beds" or "3 beds"
    const beds = [...document.querySelectorAll("#bedFilter .bed-btn.selected")].map((b) => Number(b.dataset.bed));
    if (!beds.length) setChip("beds", "Beds", false);
    else if (beds.length === 1) setChip("beds", `${beds[0]}${beds[0] === 5 ? "+" : ""} bed${beds[0] === 1 ? "" : "s"}`, true);
    else setChip("beds", `${Math.min(...beds)}+ beds`, true);
    // property type
    const types = [...document.querySelectorAll("#ptypeFilter .ptype-btn.selected")].map((b) => b.textContent.trim());
    setChip("ptype", !types.length ? "Type" : types.length === 1 ? types[0] : `${types[0]} +${types.length - 1}`, types.length > 0);
    // period
    const pName = document.querySelector("[data-period-name]");
    const period = body.dataset.period || "all";
    setChip("period", pName ? pName.textContent : "Period", ACTIVE ? !["all", "1y"].includes(period) : period !== "all");
    // price changes (active page)
    const ch = [...document.querySelectorAll("#changeFilter .ptype-btn.selected")].map((b) => b.textContent.replace(/[▼▲]\s*/, "").trim());
    if (chip("change")) setChip("change", ch.length ? ch.join(" + ") : "Price changes", ch.length > 0);
    // clear
    const anyFilter = chipEls.some((c) => c.dataset.chip !== "area" && c.classList.contains("is-on"));
    $("mChipsClear").hidden = !anyFilter;
  }
  $("mChipsClear").addEventListener("click", () => { $("clearFilters").click(); setTimeout(refreshChips, 120); });
  controls.addEventListener("click", () => setTimeout(refreshChips, 80));
  document.querySelectorAll(".noUi-target").forEach((el) => { if (el.noUiSlider) el.noUiSlider.on("set", () => setTimeout(refreshChips, 0)); });
  document.querySelectorAll("#areaToggle .area-btn").forEach((b) => b.addEventListener("click", () => setTimeout(refreshChips, 80)));
  const radius = $("radiusInput");
  if (radius) radius.addEventListener("change", refreshChips);
  const areaTitle = $("areaTitle");
  if (areaTitle) new MutationObserver(refreshChips).observe(areaTitle, { childList: true, characterData: true, subtree: true, attributes: true });
  const periodLabel = document.querySelector("[data-period-name]");
  if (periodLabel) new MutationObserver(refreshChips).observe(periodLabel, { childList: true, characterData: true, subtree: true });

  // ── sheets: one at a time, scrim behind, Escape / scrim / ✕ / Done close ──
  let opener = null;
  function openChip(name, from) {
    closeInsights(true);
    opener = from || null;
    body.dataset.chip = name;
    $("mFilterTitle").textContent = TITLES[name];
    scrim.hidden = false;
    const done = $("mFilterApply");
    const n = txt("stat-count");
    done.textContent = n && n !== "–" && !(areaMode === "neighbourhood" && !selectedNeighbourhood) ? `Show ${n} ${ACTIVE ? "listings" : "sales"}` : "Done";
    $("mFilterClose").focus({ preventScroll: true });
  }
  function closeChip() {
    if (!body.dataset.chip) return;
    delete body.dataset.chip;
    scrim.hidden = !body.classList.contains("m-insights-open");
    refreshChips();
    if (opener) opener.focus({ preventScroll: true });
    setTimeout(() => map.invalidateSize(), 60);
  }
  function openInsights() {
    closeChip();
    body.classList.add("m-insights-open");
    scrim.hidden = false;
    $("mInsightsClose").focus({ preventScroll: true });
    setTimeout(() => { if (typeof resizeCharts === "function") resizeCharts(); }, 80);
  }
  function closeInsights(quiet) {
    if (!body.classList.contains("m-insights-open")) return;
    body.classList.remove("m-insights-open");
    if (!quiet) { scrim.hidden = true; $("mStrip").focus({ preventScroll: true }); }
  }
  chipEls.forEach((c) => c.addEventListener("click", () => openChip(c.dataset.chip, c)));
  $("mFilterClose").addEventListener("click", closeChip);
  $("mFilterApply").addEventListener("click", closeChip);
  $("mInsightsClose").addEventListener("click", () => closeInsights(false));
  scrim.addEventListener("click", () => { closeChip(); closeInsights(false); });
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") { closeChip(); closeInsights(false); } });
  // choosing the neighbourhood mode in the Area sheet closes it onto the neighbourhood table; choosing a preset closes nothing (Done does)
  PHONE.addEventListener("change", () => { if (!PHONE.matches) { delete body.dataset.chip; body.classList.remove("m-insights-open", "m-scrolled"); scrim.hidden = true; } });

  // start on the list
  goTab("listings");
  syncViewBtn();
  refreshStrip();
  refreshStatus();
  refreshChips();
  measure();
})();
