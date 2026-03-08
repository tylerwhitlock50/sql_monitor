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

function renderTimeseries(data) {
  kv("timeseriesSummary", {
    window_minutes: data.window_minutes,
    bucket_minutes: data.bucket_minutes,
    points: (data.buckets || []).length
  });

  const wrap = document.getElementById("timeseries");
  const buckets = data.buckets || [];
  if (!buckets.length) {
    wrap.innerHTML = "No timeseries data.";
    return;
  }

  const rows = buckets.slice(-24);
  const header = [
    "bucket_start",
    "attempts",
    "failed_count",
    "rows_blocking",
    "max_wait_duration_ms",
    "xevent_severe"
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
    renderTimeseries(timeseries || {});
  } catch (error) {
    setStatusPill("critical");
    document.getElementById("generatedAt").textContent = `dashboard fetch failed: ${error}`;
  }
}

refresh();
setInterval(refresh, 10000);
