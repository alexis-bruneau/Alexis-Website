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
   PHONE · C · MINIMAL
   The dock (three figures, Price, Period, List), the list sheet and the Price / Period sheet. Nothing here runs on a desktop:
   the elements it drives are display: none there.
   ═══════════════════════════════════════════════════════════ */
(function phoneMinimal() {
  const body = document.body;
  const dock = document.getElementById("mDock");
  if (!dock) return;
  const PHONE = window.matchMedia("(max-width: 900px), (max-height: 500px) and (orientation: landscape)");
  const ACTIVE = body.classList.contains("active-map");
  const $ = (id) => document.getElementById(id);
  const scrim = $("mScrim");

  // ── views: the map (nothing open) or the list sheet ──
  window._phoneHomeTab = "map";
  function goTab(tab) {
    if (typeof window._syncMobileTabs === "function") window._syncMobileTabs(tab);
    else body.dataset.mtab = tab;
  }
  const listOpen = () => body.dataset.mtab === "listings";
  let listOpener = null;
  function openList(from) {
    closeChip();
    listOpener = from || null;
    goTab("listings");
  }
  function closeList() {
    if (!listOpen()) return;
    goTab("map");
    if (listOpener) listOpener.focus({ preventScroll: true });
  }
  new MutationObserver(() => {
    if (!PHONE.matches) return;
    const t = body.dataset.mtab;
    if (t && t !== "map" && t !== "listings") { goTab("map"); return; }   // there are no insights or areas on this layout
    scrim.hidden = !(listOpen() || body.dataset.chip);
    if (listOpen()) {
      $("mListTitle").textContent = `Listings · ${txt("stat-count")}`;
      $("mListClose").focus({ preventScroll: true });
    }
    setTimeout(() => map.invalidateSize(), 60);
  }).observe(body, { attributes: true, attributeFilter: ["data-mtab"] });

  // a marker tap opens the list (the page asks for it); a tap on the map closes it again
  const prevSetView = window.setView;
  window.setView = function (view) {
    prevSetView(view);
    if (PHONE.matches && view !== "listings") goTab("map");
  };
  map.on("click", () => { if (PHONE.matches && listOpen()) goTab("map"); });
  $("mListBtn").addEventListener("click", (e) => openList(e.currentTarget));
  $("mStats").addEventListener("click", (e) => openList(e.currentTarget));
  $("mListClose").addEventListener("click", closeList);

  // ── the dock's height → Leaflet's corners sit above it ──
  function measure() {
    const h = Math.ceil(dock.getBoundingClientRect().height);
    if (h) document.documentElement.style.setProperty("--m-dock-h", `${h}px`);
  }
  if (window.ResizeObserver) new ResizeObserver(measure).observe(dock);
  window.addEventListener("resize", measure);

  // ── the three figures ──
  const txt = (id) => ($(id).textContent || "").trim();
  const compact = (t) => {
    const n = Number(String(t).replace(/[^0-9.]/g, ""));
    if (!n) return "–";
    return n >= 1e6 ? `$${(n / 1e6).toFixed(2).replace(/\.?0+$/, "")}M` : `$${Math.round(n / 1000)}k`;
  };
  function refresh() {
    $("mS1").textContent = txt("stat-count") || "–";
    $("mS2").textContent = compact(txt("stat-avg"));
    const dom = txt("stat-dom");
    $("mS3").textContent = dom && dom !== "–" ? dom.replace(/\s*days?$/, "") : "–";
    if (listOpen()) $("mListTitle").textContent = `Listings · ${txt("stat-count")}`;
    refreshChips();
  }
  ["stat-count", "stat-avg", "stat-dom"].forEach((id) => new MutationObserver(refresh).observe($(id), { childList: true, characterData: true, subtree: true }));

  // the page's "no results" / "couldn't load" note, with its buttons, above the figures
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

  // ── Price and Period buttons: each says what it holds and opens a sheet with that one control ──
  const TITLES = { price: ACTIVE ? "Asking price" : "Price", period: ACTIVE ? "Time on market" : "Sold when" };
  const chipEls = [...document.querySelectorAll(".m-chip[data-chip]")];
  const chip = (name) => chipEls.find((c) => c.dataset.chip === name);
  function sliderState(id) {
    const el = $(id);
    if (!el || !el.noUiSlider) return null;
    const r = el.noUiSlider.options.range, v = el.noUiSlider.get().map((x) => Math.round(Number(x)));
    return { lo: v[0], hi: v[1], min: Math.round(r.min), max: Math.round(r.max), changed: v[0] > Math.round(r.min) || v[1] < Math.round(r.max) };
  }
  function refreshChips() {
    const pr = sliderState("priceRangeSlider"), pc = chip("price");
    if (pr) {
      const label = !pr.changed ? "Price" : pr.lo <= pr.min ? `Up to ${compact(pr.hi)}` : pr.hi >= pr.max ? `${compact(pr.lo)}+` : `${compact(pr.lo)}–${compact(pr.hi)}`;
      pc.textContent = label; pc.classList.toggle("is-on", pr.changed);
    }
    const pName = document.querySelector("[data-period-name]"), period = body.dataset.period || "all", pp = chip("period");
    pp.textContent = pName ? pName.textContent.replace(/^Listed within /, "≤ ").replace("Any time on market", "Any time") : "Period";   // short enough for the dock
    pp.classList.toggle("is-on", ACTIVE ? !["all", "1y"].includes(period) : period !== "all");
  }
  $("controls").addEventListener("click", () => setTimeout(refreshChips, 80));
  document.querySelectorAll(".noUi-target").forEach((el) => { if (el.noUiSlider) el.noUiSlider.on("set", () => setTimeout(refreshChips, 0)); });
  const periodLabel = document.querySelector("[data-period-name]");
  if (periodLabel) new MutationObserver(refreshChips).observe(periodLabel, { childList: true, characterData: true, subtree: true });

  let opener = null;
  function openChip(name, from) {
    if (listOpen()) goTab("map");
    opener = from || null;
    body.dataset.chip = name;
    $("mFilterTitle").textContent = TITLES[name];
    scrim.hidden = false;
    const n = txt("stat-count");
    $("mFilterApply").textContent = n && n !== "–" ? `Show ${n} ${ACTIVE ? "listings" : "sales"}` : "Done";
    $("mFilterClose").focus({ preventScroll: true });
  }
  function closeChip() {
    if (!body.dataset.chip) return;
    delete body.dataset.chip;
    scrim.hidden = !listOpen();
    refreshChips();
    if (opener) opener.focus({ preventScroll: true });
    setTimeout(() => map.invalidateSize(), 60);
  }
  chipEls.forEach((c) => c.addEventListener("click", () => openChip(c.dataset.chip, c)));
  $("mFilterClose").addEventListener("click", closeChip);
  $("mFilterApply").addEventListener("click", closeChip);
  scrim.addEventListener("click", () => { closeChip(); closeList(); });
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") { closeChip(); closeList(); } });
  PHONE.addEventListener("change", () => { if (!PHONE.matches) { delete body.dataset.chip; scrim.hidden = true; } });

  // start on the map
  goTab("map");
  refresh();
  refreshStatus();
  measure();
})();
