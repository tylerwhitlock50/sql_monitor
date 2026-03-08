function kv(targetId, data) {
  const target = document.getElementById(targetId);
  target.innerHTML = "";
  const entries = Object.entries(data || {});
  if (!entries.length) {
    target.innerHTML = '<div class="kv-row"><span>no data</span></div>';
    return;
  }

  for (const [key, value] of entries) {
    const row = document.createElement("div");
    row.className = "kv-row";
    row.innerHTML = `<span>${key.replaceAll("_", " ")}</span><b>${value ?? "-"}</b>`;
    target.appendChild(row);
  }
}

function renderProblems(problems) {
  const list = document.getElementById("problems");
  list.innerHTML = "";

  if (!problems || problems.length === 0) {
    list.innerHTML = "<li>No active problems detected.</li>";
    return;
  }

  for (const item of problems) {
    const li = document.createElement("li");
    li.className = `problem-${item.level}`;
    li.textContent = `[${item.level}] ${item.message}`;
    list.appendChild(li);
  }
}

function renderBlocking(rows) {
  const wrap = document.getElementById("blocking");
  if (!rows || rows.length === 0) {
    wrap.innerHTML = "No blocking rows found.";
    return;
  }

  const header = [
    "capture_time",
    "waiting_session_id",
    "blocking_session_id",
    "wait_type",
    "wait_duration_ms",
    "waiting_database_name"
  ];

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  thead.innerHTML = `<tr>${header.map((h) => `<th>${h}</th>`).join("")}</tr>`;
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = header.map((h) => `<td>${row[h] ?? ""}</td>`).join("");
    tbody.appendChild(tr);
  }

  table.appendChild(tbody);
  wrap.innerHTML = "";
  wrap.appendChild(table);
}

function setStatusPill(status) {
  const el = document.getElementById("overallStatus");
  el.className = `pill ${status}`;
  el.textContent = status;
}

function average(values) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function trendIsRising(buckets, key) {
  if (!buckets || buckets.length < 6) return false;
  const recent = buckets.slice(-3).map((row) => row[key] || 0);
  const previous = buckets.slice(-6, -3).map((row) => row[key] || 0);
  return average(recent) > average(previous) && average(recent) > 0;
}

function renderVerdict(data, timeseries) {
  const summary = document.getElementById("verdictSummary");
  const checks = document.getElementById("verdictChecks");
  const cause = document.getElementById("verdictCause");

  const latest = data.latest_attempt || {};
  const lastHour = data.rollups?.last_hour || {};
  const xevents = data.xevents_last_hour || {};
  const problemCodes = new Set((data.problems || []).map((problem) => problem.code));

  const checkRows = [];

  const stale = problemCodes.has("stale_capture") || problemCodes.has("no_capture_attempts");
  checkRows.push({
    level: stale ? "critical" : "ok",
    label: "Telemetry freshness",
    detail: stale ? "stale or missing capture data" : "fresh capture data is available"
  });

  const latestStatus = (latest.status || "unknown").toLowerCase();
  let collectorLevel = "warning";
  let collectorDetail = "collector status unknown";
  if (latestStatus === "success") {
    collectorLevel = "ok";
    collectorDetail = "latest collector run succeeded";
  } else if (latestStatus === "partial_failure") {
    collectorLevel = "warning";
    collectorDetail = "latest collector run partially failed";
  } else if (latestStatus === "failed") {
    collectorLevel = "critical";
    collectorDetail = "latest collector run failed";
  }
  checkRows.push({ level: collectorLevel, label: "Collector runtime", detail: collectorDetail });

  const failedCount = lastHour.failed_count || 0;
  checkRows.push({
    level: failedCount > 0 ? "warning" : "ok",
    label: "Failed captures (1h)",
    detail: failedCount > 0 ? `${failedCount} failures in last hour` : "no failed captures in last hour"
  });

  const blockingRows = lastHour.blocking_rows || 0;
  checkRows.push({
    level: blockingRows > 0 ? "warning" : "ok",
    label: "Blocking pressure (1h)",
    detail: blockingRows > 0 ? `${blockingRows} blocking rows captured` : "no blocking rows captured"
  });

  const severeXevents = xevents.severe_events || 0;
  checkRows.push({
    level: severeXevents > 0 ? "warning" : "ok",
    label: "Severe SQL events (1h)",
    detail: severeXevents > 0 ? `${severeXevents} severe xevents detected` : "no severe xevents detected"
  });

  const risingPressure =
    trendIsRising(timeseries?.buckets || [], "blocking_events")
    || trendIsRising(timeseries?.buckets || [], "xevent_severe")
    || trendIsRising(timeseries?.buckets || [], "failed_count");
  checkRows.push({
    level: risingPressure ? "warning" : "ok",
    label: "Pressure trend (recent)",
    detail: risingPressure ? "signals are rising over recent buckets" : "no rising pressure trend"
  });

  checks.innerHTML = checkRows
    .map((check) => `<li class="check-${check.level}"><b>${check.label}:</b> ${check.detail}</li>`)
    .join("");

  if (data.overall_status === "ok") {
    summary.textContent = "Healthy: current signals indicate the system is running normally.";
  } else if (data.overall_status === "warning") {
    summary.textContent = "Warning: monitor is running, but workload pressure or partial failures are present.";
  } else {
    summary.textContent = "Critical: monitoring is failing or stale, and findings may be incomplete.";
  }

  if (stale || latestStatus === "failed") {
    cause.textContent = "Likely cause: collector/runtime or connectivity issue (fix this first).";
  } else if (blockingRows > 0 || (xevents.locking_events || 0) > 0) {
    cause.textContent = "Likely cause: lock contention/blocking in SQL workload.";
  } else if (severeXevents > 0) {
    cause.textContent = "Likely cause: SQL errors from application/database operations.";
  } else if (risingPressure) {
    cause.textContent = "Likely cause: increasing workload pressure trend, monitor closely.";
  } else {
    cause.textContent = "Likely cause: no active issue signal at this time.";
  }
}

function renderTimeseries(data) {
  kv("timeseriesSummary", {
    window_minutes: data.window_minutes,
    bucket_minutes: data.bucket_minutes,
    points: (data.buckets || []).length
  });

  const wrap = document.getElementById("timeseries");
  const legend = document.getElementById("timeseriesLegend");
  const caption = document.getElementById("timeseriesCaption");
  const buckets = data.buckets || [];
  if (!buckets.length) {
    wrap.innerHTML = "No timeseries data.";
    legend.innerHTML = "";
    caption.textContent = "";
    return;
  }

  const points = buckets.slice(-72);
  const metrics = [
    { key: "blocking_events", label: "Blocking events", color: "#ffb347" },
    { key: "failed_count", label: "Failed attempts", color: "#ff5f56" },
    { key: "xevent_severe", label: "Severe xevents", color: "#35d0ff" }
  ];
  const maxValue = Math.max(
    1,
    ...points.flatMap((point) => metrics.map((metric) => point[metric.key] || 0))
  );

  const width = 960;
  const height = 280;
  const pad = 28;
  const usableW = width - pad * 2;
  const usableH = height - pad * 2;

  const pointX = (index) => {
    if (points.length === 1) {
      return pad + usableW / 2;
    }
    return pad + (index / (points.length - 1)) * usableW;
  };

  const pointY = (value) => pad + usableH - (value / maxValue) * usableH;

  const lines = [];
  for (const metric of metrics) {
    let d = "";
    for (let i = 0; i < points.length; i += 1) {
      const p = points[i];
      const x = pointX(i).toFixed(2);
      const y = pointY(p[metric.key] || 0).toFixed(2);
      d += `${i === 0 ? "M" : " L"}${x} ${y}`;
    }
    lines.push(`<path d="${d}" fill="none" stroke="${metric.color}" stroke-width="2.5" />`);
  }

  const grid = [];
  for (let i = 0; i <= 4; i += 1) {
    const y = pad + (usableH * i) / 4;
    const value = Math.round(maxValue - (maxValue * i) / 4);
    grid.push(
      `<line x1="${pad}" y1="${y}" x2="${width - pad}" y2="${y}" stroke="rgba(255,255,255,0.15)" stroke-width="1" />`
    );
    grid.push(
      `<text x="${pad - 6}" y="${y + 4}" text-anchor="end" font-size="10" fill="#9bb1c7">${value}</text>`
    );
  }

  const start = new Date(points[0].bucket_start);
  const end = new Date(points[points.length - 1].bucket_start);
  const timeLabel = `${start.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })} - ${end.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;

  const svg = `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Operational issues timeseries">
      ${grid.join("")}
      ${lines.join("")}
      <text x="${width - pad}" y="${height - 8}" text-anchor="end" font-size="11" fill="#9bb1c7">${timeLabel}</text>
    </svg>
  `;

  wrap.innerHTML = "";
  wrap.innerHTML = svg;

  legend.innerHTML = metrics
    .map((metric) => {
      const latest = points[points.length - 1][metric.key] || 0;
      return `
        <span class="legend-item">
          <span class="legend-swatch" style="background:${metric.color}"></span>
          <span>${metric.label}: ${latest}</span>
        </span>
      `;
    })
    .join("");
  caption.textContent = "Chart shows recent pressure indicators over time buckets.";
}

async function refresh() {
  try {
    const [dashboardResp, timeseriesResp] = await Promise.all([
      fetch("/api/dashboard", { cache: "no-store" }),
      fetch("/api/timeseries?window_minutes=360&bucket_minutes=5", { cache: "no-store" })
    ]);
    const data = await dashboardResp.json();
    const timeseries = await timeseriesResp.json();

    document.getElementById("generatedAt").textContent = `generated ${data.generated_at || "-"}`;
    setStatusPill(data.overall_status || "critical");
    kv("latestAttempt", data.latest_attempt || {});
    kv("lastHour", data.rollups?.last_hour || {});
    kv("lastDay", data.rollups?.last_24_hours || {});
    kv("xevents", data.xevents_last_hour || {});
    renderProblems(data.problems || []);
    renderBlocking(data.recent_blocking || []);
    renderVerdict(data, timeseries || {});
    renderTimeseries(timeseries || {});
  } catch (error) {
    setStatusPill("critical");
    document.getElementById("generatedAt").textContent = `dashboard fetch failed: ${error}`;
  }
}

refresh();
setInterval(refresh, 10000);
