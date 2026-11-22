/************** DATA STATUS (sold map and active map) **************
 * One place for the three states a page can be in besides "showing results":
 *   loading  the first request is on its way        -> body.is-loading (CSS draws skeleton cards and a chart message)
 *   empty    the filters match nothing              -> a note above the cards with a "Clear filters" button
 *   error    a request failed                       -> a note above the cards with a "Try again" button
 * Loaded first on both pages; map.js / active.js / neighbourhoods.js call these functions.
 */
/** A CSS custom property of the page (design tokens live in map.css), with a fallback. */
window.cssVar = function (name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
};

(function () {
  const bar = () => document.getElementById("dataStatus");

  /** kind: "empty" | "error" | "loading"; actions: [{label, run}] */
  window.setDataStatus = function (kind, message, actions) {
    const el = bar();
    if (!el) return;
    el.className = `data-status is-${kind}`;
    el.hidden = false;
    el.setAttribute("role", kind === "error" ? "alert" : "status");
    el.replaceChildren();
    const text = document.createElement("span");
    text.className = "data-status-text";
    text.textContent = message;
    el.appendChild(text);
    (actions || []).forEach((a) => {
      const b = document.createElement("button");
      b.type = "button";
      b.className = "data-status-btn";
      b.textContent = a.label;
      b.addEventListener("click", a.run);
      el.appendChild(b);
    });
  };

  window.clearDataStatus = function () {
    const el = bar();
    if (el) { el.hidden = true; el.replaceChildren(); }
  };

  /** The first answer (or failure) arrived: stop the skeleton. */
  window.finishLoading = function () {
    document.body.classList.remove("is-loading");
  };
})();
