// ============================
// FILE 3: static/storage.js
// ============================

(function () {
  // If the page has no data, don't render chart
  const labels = window.STORAGE_LABELS || [];
  const rootPct = window.STORAGE_ROOT_PCT || [];
  const volPct = window.STORAGE_VOL_PCT || [];

  const canvas = document.getElementById("usageChart");
  if (!canvas) return;
  if (!labels.length) return;

  new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Root % Used",
          data: rootPct,
          borderWidth: 2,
          pointRadius: 3,
          tension: 0.25,
        },
        {
          label: "Volume % Used",
          data: volPct,
          borderWidth: 2,
          pointRadius: 3,
          tension: 0.25,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "nearest", intersect: false },
      plugins: {
        legend: { display: true },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(2)}%`,
          },
        },
      },
      scales: {
        x: {
          title: { display: true, text: "Date" },
          ticks: { maxRotation: 0 },
        },
        y: {
          title: { display: true, text: "Percent Used (%)" },
          beginAtZero: true,
          suggestedMax: 100,
        },
      },
    },
  });
})();
