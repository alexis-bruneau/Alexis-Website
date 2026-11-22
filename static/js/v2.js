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
   PHONE · A · MAP + BOTTOM SHEET
   The bottom sheet (peek / half / full), its summary line, and the full-screen Filters page. Nothing here runs on a desktop:
   the elements it drives are display: none there.
   ═══════════════════════════════════════════════════════════ */
(function phoneSheet() {
  const body = document.body;
  const sheet = document.getElementById("mSheet");
  const grip = document.getElementById("mGrip");
  const fab = document.getElementById("mFiltersBtn");
  if (!sheet || !grip || !fab) return;
  const PHONE = window.matchMedia("(max-width: 900px), (max-height: 500px) and (orientation: landscape)");
  const ACTIVE = body.classList.contains("active-map");
  const $ = (id) => document.getElementById(id);
  const SNAPS = ["peek", "half", "full"];

  // Closing things (a tap on the map) lands on the summary, not on the insights
  window._phoneHomeTab = "map";

  // ── snap heights, measured from the CSS (they use safe-area insets and dvh) ──
  const probes = {};
  SNAPS.forEach((s) => {
    const p = document.createElement("div");
    p.setAttribute("aria-hidden", "true");
    p.style.cssText = `position:fixed;left:-9px;top:0;width:1px;visibility:hidden;pointer-events:none;height:var(--m-${s})`;
    body.appendChild(p);
    probes[s] = p;
  });
  const snapH = (s) => probes[s].getBoundingClientRect().height;

  // ── the sheet follows body[data-mtab]: map = peek, listings / areas = half, insights = full ──
  let snap = "peek";
  let halfTab = "listings";
  const snapForTab = (t) => (t === "insights" ? "full" : t === "listings" || t === "areas" ? "half" : "peek");
  const tabForSnap = (s) => (s === "full" ? "insights" : s === "half" ? (halfTab === "areas" && areaMode === "neighbourhood" ? "areas" : "listings") : "map");
  function applySnap(s) {
    snap = s;
    body.dataset.snap = s;
    grip.setAttribute("aria-valuetext", s === "peek" ? "summary" : s === "half" ? "list" : "insights");
  }
  function goTab(tab) {
    if (typeof window._syncMobileTabs === "function") window._syncMobileTabs(tab);
    else body.dataset.mtab = tab;
  }
  new MutationObserver(() => {
    if (!PHONE.matches) return;
    const t = body.dataset.mtab;
    if (!t) return;
    if (t === "listings" || t === "areas") halfTab = t;
    applySnap(snapForTab(t));
    if (t === "insights") setTimeout(() => { if (typeof resizeCharts === "function") resizeCharts(); }, 320);   // the sheet is still growing
  }).observe(body, { attributes: true, attributeFilter: ["data-mtab"] });

  // A tap on the map's background closes the list (the page asks for its "home" view): back to the summary, not to the insights.
  // While the Filters page is open it may switch the area mode: that must not slide the sheet up to the insights either.
  const prevSetView = window.setView;
  window.setView = function (view) {
    if (view === "map") { prevSetView("insights"); if (PHONE.matches) goTab("map"); return; }
    prevSetView(view);
    if (PHONE.matches && view === "insights" && body.classList.contains("filters-open")) goTab("map");
  };
  window.homeView = () => (areaMode === "neighbourhood" ? "areas" : PHONE.matches ? "map" : "insights");
  // any tap on the map (not on a marker) puts the sheet back down to the summary
  map.on("click", () => { if (PHONE.matches && snap !== "peek" && !body.classList.contains("filters-open")) goTab("map"); });

  // ── drag, tap and keys on the grip ──
  let drag = null;
  grip.addEventListener("pointerdown", (e) => {
    drag = { id: e.pointerId, y0: e.clientY, h0: sheet.getBoundingClientRect().height, moved: false, lastY: e.clientY, lastT: performance.now(), v: 0 };
    try { grip.setPointerCapture(e.pointerId); } catch (_) { /* old browsers */ }
  });
  grip.addEventListener("pointermove", (e) => {
    if (!drag || e.pointerId !== drag.id) return;
    const dy = e.clientY - drag.y0;
    if (!drag.moved && Math.abs(dy) < 6) return;
    if (!drag.moved) { drag.moved = true; body.classList.add("sheet-dragging"); }
    const h = Math.min(snapH("full"), Math.max(snapH("peek"), drag.h0 - dy));
    body.style.setProperty("--sheet-h", `${h}px`);
    const now = performance.now();
    drag.v = (e.clientY - drag.lastY) / Math.max(1, now - drag.lastT);   // px per ms, positive = moving down
    drag.lastY = e.clientY; drag.lastT = now;
  });
  function endDrag(e, cancelled) {
    if (!drag || e.pointerId !== drag.id) return;
    const d = drag; drag = null;
    if (!d.moved) {
      if (!cancelled) goTab(tabForSnap(SNAPS[(SNAPS.indexOf(snap) + 1) % 3]));   // a tap: next height
      return;
    }
    const cur = sheet.getBoundingClientRect().height;
    const projected = cur - d.v * 160;   // a flick carries on
    const best = SNAPS.reduce((a, s) => (Math.abs(snapH(s) - projected) < Math.abs(snapH(a) - projected) ? s : a), "peek");
    body.classList.remove("sheet-dragging");
    body.style.removeProperty("--sheet-h");
    goTab(tabForSnap(best));
    if (best === snap) applySnap(best);
  }
  grip.addEventListener("pointerup", (e) => endDrag(e, false));
  grip.addEventListener("pointercancel", (e) => endDrag(e, true));
  grip.addEventListener("keydown", (e) => {
    const i = SNAPS.indexOf(snap);
    if (e.key === "ArrowUp" && i < 2) { e.preventDefault(); goTab(tabForSnap(SNAPS[i + 1])); }
    if (e.key === "ArrowDown" && i > 0) { e.preventDefault(); goTab(tabForSnap(SNAPS[i - 1])); }
  });
  // a grip tap fires pointerup then click: the click is ignored, the pointer events did the work
  grip.addEventListener("click", (e) => e.preventDefault());

  // ── summary line: "312 sales · avg $642k" ──
  const compact = (txt) => {
    const n = Number(String(txt).replace(/[^0-9.]/g, ""));
    if (!n) return "–";
    return n >= 1e6 ? `$${(n / 1e6).toFixed(2).replace(/\.?0+$/, "")}M` : `$${Math.round(n / 1000)}k`;
  };
  function summaryText() {
    const n = ($("stat-count").textContent || "").trim();
    if (areaMode === "neighbourhood" && !selectedNeighbourhood) return "Pick a neighbourhood";
    if (!n || n === "–") return "Loading…";
    const noun = ACTIVE ? (n === "1" ? "listing" : "listings") : (n === "1" ? "sale" : "sales");
    const where = areaMode === "neighbourhood" && selectedNeighbourhood ? `${selectedNeighbourhood.name} · ` : "";
    return `${where}${Number(n.replace(/,/g, "")).toLocaleString()} ${noun} · avg ${compact($("stat-avg").textContent)}`;
  }
  function refreshSummary() {
    $("mSumMain").textContent = summaryText();
    const apply = $("mFilterApply");
    const n = ($("stat-count").textContent || "").trim();
    const noun = ACTIVE ? "listings" : "sales";
    apply.textContent = n && n !== "–" && !(areaMode === "neighbourhood" && !selectedNeighbourhood) ? `Show ${n} ${noun}` : "Show results";
    refreshBadge();
  }
  ["stat-count", "stat-avg"].forEach((id) => new MutationObserver(refreshSummary).observe($(id), { childList: true, characterData: true, subtree: true }));
  const areaTitle = $("areaTitle");
  if (areaTitle) new MutationObserver(refreshSummary).observe(areaTitle, { childList: true, characterData: true, subtree: true, attributes: true });
  document.querySelectorAll("#areaToggle .area-btn").forEach((b) => b.addEventListener("click", () => setTimeout(refreshSummary, 60)));

  // the page's "no results" / "couldn't load" note, with its buttons, repeated where it can be seen
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

  // ── the Filters page ──
  const badge = $("mFiltersBadge");
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
  function refreshBadge() {
    const n = activeFilterCount();
    badge.hidden = n === 0;
    badge.textContent = String(n);
    fab.setAttribute("aria-label", n ? `Filters, ${n} active` : "Filters");
  }
  document.querySelectorAll(".noUi-target").forEach((el) => { if (el.noUiSlider) el.noUiSlider.on("set", () => setTimeout(refreshBadge, 0)); });
  $("controls").addEventListener("click", () => setTimeout(refreshBadge, 80));

  function openFilters() {
    body.classList.add("filters-open");
    $("mFilterClose").focus({ preventScroll: true });
    refreshSummary();
  }
  function closeFilters() {
    body.classList.remove("filters-open");
    fab.focus({ preventScroll: true });
    setTimeout(() => map.invalidateSize(), 60);
  }
  fab.addEventListener("click", openFilters);
  $("mFilterClose").addEventListener("click", closeFilters);
  $("mFilterApply").addEventListener("click", closeFilters);
  document.addEventListener("keydown", (e) => { if (e.key === "Escape" && body.classList.contains("filters-open")) closeFilters(); });
  PHONE.addEventListener("change", () => { if (!PHONE.matches) body.classList.remove("filters-open", "sheet-dragging"); });

  // start on the summary
  goTab("map");
  applySnap("peek");
  refreshSummary();
  refreshStatus();
})();
