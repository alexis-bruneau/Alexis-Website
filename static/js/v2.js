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
   PHONE · D · BOTTOM TAB NAVIGATION
   The tab bar (Map | Areas | Listings | Insights | Filters): titles, badges, the button on the Filters page. The views themselves
   are the panels the page already has; body[data-mtab] picks which one is on screen. Nothing here runs on a desktop.
   ═══════════════════════════════════════════════════════════ */
(function phoneTabs() {
  const body = document.body;
  const nav = document.getElementById("mobileTabBar");
  const bar = document.getElementById("mTopBar");
  if (!nav || !bar) return;
  const PHONE = window.matchMedia("(max-width: 900px), (max-height: 500px) and (orientation: landscape)");
  const ACTIVE = body.classList.contains("active-map");
  const $ = (id) => document.getElementById(id);
  const txt = (id) => ($(id).textContent || "").trim();

  // ── views ──
  window._phoneHomeTab = "map";
  function goTab(tab) {
    if (typeof window._syncMobileTabs === "function") window._syncMobileTabs(tab);
    else body.dataset.mtab = tab;
  }
  const TITLES = { listings: "Listings", areas: "Neighbourhoods", insights: "Insights", filters: "Filters" };
  function titleFor(tab) {
    if (tab === "listings") { const n = txt("stat-count"); return n && n !== "–" ? `Listings · ${n}` : "Listings"; }
    return TITLES[tab] || "";
  }
  function showTitle() { $("mBarTitle").textContent = titleFor(body.dataset.mtab); }
  new MutationObserver(() => {
    if (!PHONE.matches) return;
    showTitle();
    // the map, and the tab's page, are laid out while hidden: tell them
    setTimeout(() => { map.invalidateSize(); if (typeof resizeCharts === "function") resizeCharts(); }, 80);
    refreshApply();
  }).observe(body, { attributes: true, attributeFilter: ["data-mtab"] });

  // A tap on the map's background (the page asks for its "home" view) leaves the map as it is. On the Filters page the area switch
  // may ask for the insights: stay where you are.
  const prevSetView = window.setView;
  window.setView = function (view) {
    const before = body.dataset.mtab;
    if (view === "map") { prevSetView("insights"); if (PHONE.matches) goTab("map"); return; }
    prevSetView(view);
    if (PHONE.matches && view === "insights" && before && before !== "insights") goTab(before);
  };
  window.homeView = () => (areaMode === "neighbourhood" ? "areas" : PHONE.matches ? "map" : "insights");

  // ── badges: the number of listings, the number of active filters ──
  const compactCount = (t) => { const n = Number(String(t).replace(/[^0-9]/g, "")); return !n ? "" : n >= 10000 ? `${Math.round(n / 1000)}k` : n >= 1000 ? `${(n / 1000).toFixed(1).replace(/\.0$/, "")}k` : String(n); };
  function activeFilterCount() {
    let n = 0;
    if (document.querySelector("#bedFilter .selected")) n++;
    if (document.querySelector("#ptypeFilter .selected")) n++;
    if (document.querySelector("#changeFilter .selected")) n++;
    document.querySelectorAll(".noUi-target").forEach((el) => {
      const api = el.noUiSlider;
      if (!api || !api.options || !api.options.range) return;
      const r = api.options.range, v = api.get().map((x) => Math.round(Number(x)));
      if (v[0] > Math.round(r.min) || v[1] < Math.round(r.max)) n++;
    });
    return n;
  }
  function refreshBadges() {
    const lb = $("mListBadge"), c = compactCount(txt("stat-count"));
    lb.hidden = !c; lb.textContent = c;
    const fb = $("mFiltersBadge"), f = activeFilterCount();
    fb.hidden = f === 0; fb.textContent = String(f);
    showTitle();
    refreshApply();
  }
  ["stat-count"].forEach((id) => new MutationObserver(refreshBadges).observe($(id), { childList: true, characterData: true, subtree: true }));
  document.querySelectorAll(".noUi-target").forEach((el) => { if (el.noUiSlider) el.noUiSlider.on("set", () => setTimeout(refreshBadges, 0)); });
  $("controls").addEventListener("click", () => setTimeout(refreshBadges, 80));

  // ── the button at the bottom of the Filters page: shows the result on the map ──
  function refreshApply() {
    const n = txt("stat-count");
    $("mFilterApply").textContent = n && n !== "–" && !(areaMode === "neighbourhood" && !selectedNeighbourhood) ? `Show ${n} ${ACTIVE ? "listings" : "sales"} on the map` : "Show the map";
  }
  $("mFilterApply").addEventListener("click", () => goTab("map"));

  // the page's "no results" / "couldn't load" note, with its buttons
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

  // start on the map
  goTab("map");
  refreshBadges();
  refreshStatus();
})();
