(function () {
  const form = document.getElementById("qform");
  if (!form) return;

  const clearBtn = document.getElementById("clearBtn");
  const clearSortsBtn = document.getElementById("clearSortsBtn");
  const chips = document.getElementById("chips");

  function setParam(name, value) {
    const url = new URL(window.location.href);
    if (!value) url.searchParams.delete(name);
    else url.searchParams.set(name, value);
    window.location.href = url.toString();
  }

  function clearAll() {
    const url = new URL(window.location.href);
    for (const k of Array.from(url.searchParams.keys())) {
      if (k.startsWith("f_") || k.startsWith("s_") || k === "limit") url.searchParams.delete(k);
    }
    window.location.href = url.toString();
  }

  function clearSorts() {
    const url = new URL(window.location.href);
    for (const k of Array.from(url.searchParams.keys())) {
      if (k.startsWith("s_")) url.searchParams.delete(k);
    }
    window.location.href = url.toString();
  }

  if (clearBtn) clearBtn.addEventListener("click", clearAll);
  if (clearSortsBtn) clearSortsBtn.addEventListener("click", clearSorts);

  document.querySelectorAll("th[data-col]").forEach((th) => {
    th.addEventListener("click", (e) => {
      const actionEl = e.target.closest("[data-action='sort']");
      if (!actionEl) return;

      const col = th.getAttribute("data-col");
      const current = th.getAttribute("data-sort") || "";
      const next = current === "" ? "asc" : current === "asc" ? "desc" : "";
      const url = new URL(window.location.href);
      url.searchParams.set(`s_${col}`, next);
      window.location.href = url.toString();
    });
  });

  if (chips) {
    chips.addEventListener("click", (e) => {
      const x = e.target.closest(".x");
      if (!x) return;
      const chip = e.target.closest(".chip");
      if (!chip) return;
      const kind = chip.getAttribute("data-kind");
      const col = chip.getAttribute("data-col");
      const url = new URL(window.location.href);
      if (kind === "filter") url.searchParams.delete(`f_${col}`);
      if (kind === "sort") url.searchParams.delete(`s_${col}`);
      window.location.href = url.toString();
    });
  }
})();
