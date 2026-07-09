const SOURCE_FILTER_KEY = "manga-manager.hiddenSources";
const state = {
  jobs: null,
  expandedJobs: new Set(),
  drawerSections: new Map(),
  infoJobPages: {active: 1, failed: 1, completed: 1},
  infoJobPageSize: 25,
  jobStatusErrors: 0,
  unloading: false,
  filter: new URLSearchParams(window.location.search).get("filter") || "all",
  view: new URLSearchParams(window.location.search).get("view") || "list",
};

function csrfToken() {
  const input = document.querySelector('input[name="csrf_token"]');
  return input ? input.value : "";
}

function jsonHeaders() {
  const headers = {"Accept": "application/json"};
  const token = csrfToken();
  if (token) headers["X-CSRF-Token"] = token;
  return headers;
}

function toast(message, type = "info") {
  if (!message) return;
  const stack = document.getElementById("toast-stack");
  if (!stack) return;
  const item = document.createElement("div");
  item.className = `toast toast-${type}`;
  item.textContent = message;
  stack.append(item);
  setTimeout(() => item.classList.add("leaving"), 4200);
  setTimeout(() => item.remove(), 4800);
}

function hiddenSources() {
  try {
    return new Set(JSON.parse(localStorage.getItem(SOURCE_FILTER_KEY) || "[]"));
  } catch {
    return new Set();
  }
}

function saveHiddenSources(sources) {
  localStorage.setItem(SOURCE_FILTER_KEY, JSON.stringify([...sources].sort()));
}

function activeSources(card) {
  return (card.dataset.sources || "").split(/\s+/).filter(Boolean);
}

function cardMatchesSource(card, hidden) {
  const sources = activeSources(card);
  return sources.length === 0 || sources.some((source) => !hidden.has(source));
}

function cardMatchesLibraryFilter(card) {
  if (!card.matches(".library-item")) return true;
  if (state.filter === "ready") return card.dataset.readyState === "ready";
  if (state.filter === "pending-sync") return card.dataset.readyState === "pending-sync";
  if (state.filter === "unread") return card.dataset.unread === "true";
  if (state.filter === "reading") return card.dataset.progress === "reading";
  if (state.filter === "caught-up") return card.dataset.progress === "caught_up";
  if (state.filter === "failed") return card.dataset.failed === "true";
  return true;
}

function applyFilters() {
  const hidden = hiddenSources();
  document.querySelectorAll("[data-source-toggle]").forEach((toggle) => {
    toggle.checked = !hidden.has(toggle.value);
  });
  document.querySelectorAll("[data-source-row], .sources [data-source]").forEach((row) => {
    const source = row.dataset.sourceRow || row.dataset.source || "";
    row.hidden = Boolean(source && hidden.has(source));
  });
  document.querySelectorAll("[data-series-card]").forEach((card) => {
    card.hidden = !cardMatchesSource(card, hidden) || !cardMatchesLibraryFilter(card);
  });
  document.querySelectorAll("[data-library-filter]").forEach((link) => {
    const active = link.dataset.libraryFilter === state.filter;
    link.classList.toggle("button-link", active);
    link.classList.toggle("filter-link", !active);
  });
  document.querySelectorAll("[data-view-mode]").forEach((link) => {
    const active = link.dataset.viewMode === state.view;
    link.classList.toggle("button-link", active);
    link.classList.toggle("filter-link", !active);
  });
  document.querySelectorAll(".library-item").forEach((card) => {
    card.classList.toggle("cover-card", state.view === "cover");
  });
}

function syncLibraryUrl() {
  if (!document.querySelector("[data-library-filter]")) return;
  const params = new URLSearchParams(window.location.search);
  params.set("filter", state.filter);
  params.set("view", state.view);
  window.history.replaceState(null, "", `/library?${params.toString()}`);
}

function setupFilters() {
  document.querySelectorAll("[data-source-toggle]").forEach((toggle) => {
    toggle.addEventListener("change", () => {
      const hidden = hiddenSources();
      if (toggle.checked) hidden.delete(toggle.value);
      else hidden.add(toggle.value);
      saveHiddenSources(hidden);
      applyFilters();
    });
  });
  document.querySelectorAll("[data-library-filter]").forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      state.filter = link.dataset.libraryFilter || "all";
      syncLibraryUrl();
      applyFilters();
    });
  });
  document.querySelectorAll("[data-view-mode]").forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      state.view = link.dataset.viewMode || "list";
      syncLibraryUrl();
      applyFilters();
    });
  });
  applyFilters();
}

function jobLabel(job) {
  if (job.kind === "download_series") return `DL: ${job.series_title || "Unknown series"}`;
  if (job.kind === "pull") return `${job.source || "source"} pull`;
  if (job.kind === "kavita") return job.series_title ? `Kavita: ${job.series_title}` : `Kavita #${job.id}`;
  if (job.series_title && job.chapter_number) {
    return `${job.series_title} Ch. ${job.chapter_number}`;
  }
  return `Download #${job.id}`;
}

function jobDetail(job) {
  const parts = [];
  if (job.kind === "download_series" && job.total) parts.push(`${job.processed || 0}/${job.total}`);
  if (job.source) parts.push(job.source);
  if (job.kind === "pull" && job.total) parts.push(`${job.processed}/${job.total}`);
  if (job.job_type && job.job_type !== "normal") parts.push(job.job_type);
  if (job.retry_after) parts.push(`retry ${job.retry_after}`);
  if (job.error) parts.push(shortText(job.error));
  return parts.join(" · ");
}

function shortText(value, max = 96) {
  if (!value) return "";
  const text = String(value);
  return text.length > max ? `${text.slice(0, max - 1)}...` : text;
}

function retryUrl(job) {
  if (job.kind === "download") return `/download-jobs/${job.id}/retry`;
  if (job.kind === "kavita") return `/kavita-sync-jobs/${job.id}/retry`;
  return "";
}

async function postJson(url, formData = null) {
  const options = {method: "POST", headers: jsonHeaders()};
  if (formData) options.body = formData;
  const response = await fetch(url, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.message || `Request failed (${response.status})`);
  }
  return payload;
}

async function retryJob(job) {
  if (job.kind === "download_series") {
    const failed = (job.chapters || []).filter((chapter) => chapter.retryable);
    for (const chapter of failed) {
      await postJson(retryUrl(chapter));
    }
    toast(failed.length ? `Queued ${failed.length} chapter retries.` : "No failed chapters to retry.");
    await refreshJobs();
    return;
  }
  const url = retryUrl(job);
  if (!url) return;
  const payload = await postJson(url);
  toast(payload.message || "Retry queued.");
  await refreshJobs();
}

function jobKey(job) {
  return `${job.kind}:${job.id}`;
}

function detailRow(label, value) {
  if (value === undefined || value === null || value === "") return null;
  const row = document.createElement("div");
  row.className = "job-detail-row";
  const key = document.createElement("span");
  key.textContent = label;
  const body = document.createElement("span");
  body.textContent = String(value);
  row.append(key, body);
  return row;
}

function renderJobDetails(job, options = {}) {
  const details = document.createElement("div");
  details.className = "job-details";
  [
    detailRow("Series", job.series_title),
    detailRow("Source", job.source),
    detailRow("Progress", job.total ? `${job.processed || 0}/${job.total}` : ""),
    detailRow("Attempts", job.attempts),
    detailRow("Job type", job.job_type),
    detailRow("Retry after", job.retry_after),
    detailRow("Updated", job.updated_at),
    detailRow("Created", job.created_at),
    detailRow("Error", job.error),
  ].filter(Boolean).forEach((row) => details.append(row));

  if (job.counts) {
    const counts = Object.entries(job.counts)
      .filter(([, value]) => value)
      .map(([key, value]) => `${key} ${value}`)
      .join(" · ");
    const row = detailRow("Chapters", counts);
    if (row) details.append(row);
  }

  if (options.showChapters && job.kind === "download_series") {
    const chapterList = document.createElement("div");
    chapterList.className = "job-chapter-list";
    (job.chapters || []).forEach((chapter) => {
      const chapterRow = document.createElement("div");
      chapterRow.className = `job-chapter-row status-${chapter.status}`;
      const label = document.createElement("span");
      label.textContent = `Ch. ${chapter.chapter_number || "?"}`;
      const source = document.createElement("span");
      source.textContent = chapter.source || "";
      const status = document.createElement("span");
      status.textContent = chapter.status;
      const attempts = document.createElement("span");
      attempts.textContent = chapter.attempts ? `${chapter.attempts}x` : "";
      const retry = document.createElement("span");
      retry.textContent = chapter.retry_after || "";
      const error = document.createElement("span");
      error.textContent = shortText(chapter.error);
      error.title = chapter.error || "";
      chapterRow.append(label, source, status, attempts, retry, error);
      if (chapter.retryable) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "secondary";
        button.textContent = "Retry";
        button.addEventListener("click", (event) => {
          event.stopPropagation();
          retryJob(chapter).catch((retryError) => toast(retryError.message, "error"));
        });
        chapterRow.append(button);
      }
      chapterList.append(chapterRow);
    });
    details.append(chapterList);
  }
  return details;
}

function renderJob(job, options = {}) {
  const row = document.createElement("div");
  row.className = `job drawer-job status-${job.status}`;
  row.dataset.jobKind = job.kind;
  row.dataset.jobId = job.id;
  const key = jobKey(job);
  row.dataset.jobKey = key;
  const expanded = state.expandedJobs.has(key);
  row.classList.toggle("expanded", expanded);

  const title = document.createElement("span");
  title.textContent = jobLabel(job);
  row.append(title);

  const status = document.createElement("span");
  status.textContent = job.status;
  row.append(status);

  const detail = document.createElement("span");
  detail.textContent = jobDetail(job);
  row.append(detail);

  if ((job.kind === "pull" || job.kind === "download_series") && job.total) {
    const progress = document.createElement("progress");
    progress.max = job.total;
    progress.value = job.processed || 0;
    row.append(progress);
  }

  if (job.retryable) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "secondary";
    button.textContent = "Retry";
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      retryJob(job).catch((error) => toast(error.message, "error"));
    });
    row.append(button);
  }
  row.addEventListener("click", () => {
    if (state.expandedJobs.has(key)) state.expandedJobs.delete(key);
    else state.expandedJobs.add(key);
    renderJobs(state.jobs);
  });
  if (expanded) row.append(renderJobDetails(job, options));
  return row;
}

function renderSection(title, jobs, options = {}) {
  const section = document.createElement("details");
  section.className = "drawer-section";
  const key = options.key || title.toLowerCase();
  section.open = state.drawerSections.has(key)
    ? state.drawerSections.get(key)
    : options.open !== false;
  const heading = document.createElement("summary");
  const count = options.count ?? jobs.length;
  heading.textContent = `${title} (${count})`;
  section.append(heading);
  section.addEventListener("toggle", () => {
    if (!section.isConnected) return;
    state.drawerSections.set(key, section.open);
  });
  const body = document.createElement("div");
  body.className = "job-list";
  if (!jobs.length) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No recent jobs.";
    body.append(empty);
  } else {
    jobs.forEach((job) => body.append(renderJob(job, options)));
  }
  section.append(body);
  return section;
}

function jobsStatusUrl() {
  if (!document.querySelector("[data-jobs-status]")) return "/api/jobs/status";
  const params = new URLSearchParams();
  params.set("active_page", String(state.infoJobPages.active || 1));
  params.set("failed_page", String(state.infoJobPages.failed || 1));
  params.set("completed_page", String(state.infoJobPages.completed || 1));
  params.set("page_size", String(state.infoJobPageSize || 25));
  return `/api/jobs/status?${params.toString()}`;
}

function allJobs(payload) {
  return [...(payload.pulls || []), ...(payload.downloads || []), ...(payload.kavita || [])];
}

function splitJobsByStatus(payload) {
  const jobs = allJobs(payload);
  return {
    active: jobs
      .filter((job) => ["queued", "running", "delayed"].includes(job.status))
      .sort((a, b) => (b.updated_at || "").localeCompare(a.updated_at || "")),
    failed: jobs
      .filter((job) => job.status === "failed")
      .sort((a, b) => (b.updated_at || "").localeCompare(a.updated_at || "")),
    completed: jobs
      .filter((job) => ["complete", "skipped"].includes(job.status))
      .sort((a, b) => (b.updated_at || "").localeCompare(a.updated_at || "")),
  };
}

function statusCounts(payload) {
  const downloads = (payload.counts && payload.counts.downloads) || {};
  const pulls = (payload.counts && payload.counts.pulls) || {};
  const kavita = (payload.counts && payload.counts.kavita) || {};
  const sum = (source, names) => names.reduce((total, name) => total + (source[name] || 0), 0);
  return {
    active:
      sum(downloads, ["queued", "running", "delayed"]) +
      sum(pulls, ["queued", "running"]) +
      sum(kavita, ["queued", "running"]),
    failed: sum(downloads, ["failed"]) + sum(pulls, ["failed"]) + sum(kavita, ["failed"]),
    completed:
      sum(downloads, ["complete", "skipped"]) +
      sum(pulls, ["complete", "skipped"]) +
      sum(kavita, ["complete", "skipped"]),
  };
}

function renderOverall(payload) {
  const targets = [
    document.getElementById("jobs-overall"),
    document.getElementById("info-jobs-overall"),
  ].filter(Boolean);
  const button = document.getElementById("jobs-toggle");
  if (!button) return;
  const overall = payload.overall || {};
  const total = overall.total || 0;
  const processed = overall.processed || 0;
  const active = Boolean(overall.active);
  button.classList.toggle("active", active);
  if (total) {
    button.textContent = `Jobs ${processed}/${total}`;
    targets.forEach((target) => {
      target.innerHTML = `<span>Overall ${processed}/${total}</span><progress max="${total}" value="${processed}"></progress>`;
    });
  } else {
    const count = (overall.active_downloads || 0) + (overall.active_kavita || 0);
    button.textContent = active ? `Jobs ${count || "active"}` : "Jobs";
    targets.forEach((target) => {
      target.textContent = active ? `${count} queue jobs active` : "No active jobs";
    });
  }
}

function renderJobs(payload) {
  state.jobs = payload;
  if (payload.sections) {
    Object.entries(payload.sections).forEach(([status, page]) => {
      state.infoJobPages[status] = page.page || state.infoJobPages[status] || 1;
    });
  }
  renderOverall(payload);
  const drawer = document.getElementById("jobs-drawer-content");
  if (drawer) {
    const byStatus = splitJobsByStatus(payload);
    const counts = statusCounts(payload);
    drawer.replaceChildren(
      renderSection("Queued", byStatus.active, {key: "queued", count: counts.active}),
      renderSection("Completed", byStatus.completed.slice(0, 10), {key: "completed", count: counts.completed, open: false}),
      renderSection("Failed", byStatus.failed.slice(0, 10), {key: "failed", count: counts.failed, open: false}),
    );
  }
  const byStatus = splitJobsByStatus(payload);
  const counts = statusCounts(payload);
  Object.entries(byStatus).forEach(([status, jobs]) => {
    document.querySelectorAll(`[data-jobs-status="${status}"]`).forEach((target) => {
      const sectionPage = payload.sections && payload.sections[status];
      if (sectionPage) renderInfoJobSection(target, status, sectionPage);
      else renderInfoJobSection(target, status, {
        jobs,
        page: 1,
        page_size: jobs.length || state.infoJobPageSize,
        total: counts[status],
        total_pages: 1,
      });
    });
  });
  ["pulls", "downloads", "kavita"].forEach((kind) => {
    document.querySelectorAll(`[data-jobs-list="${kind}"]`).forEach((target) => {
      target.replaceChildren(
        ...(payload[kind] || []).map((job) => renderJob(job, {showChapters: kind === "downloads"})),
      );
      if (!(payload[kind] || []).length) {
        const empty = document.createElement("p");
        empty.className = "empty";
        empty.textContent = "No recent jobs.";
        target.append(empty);
      }
    });
  });
}

function renderInfoJobSection(target, status, page) {
  const jobs = page.jobs || [];
  const list = document.createElement("div");
  list.className = "job-list";
  if (!jobs.length) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No recent jobs.";
    list.append(empty);
  } else {
    jobs.forEach((job) => list.append(renderJob(job, {showChapters: job.kind === "download_series"})));
  }
  const pager = renderInfoPager(status, page);
  target.replaceChildren(list, pager);
}

function renderInfoPager(status, page) {
  const pager = document.createElement("div");
  pager.className = "job-pager";
  const current = page.page || 1;
  const totalPages = page.total_pages || 1;
  const total = page.total || 0;
  const label = document.createElement("span");
  label.className = "muted";
  label.textContent = `Page ${current} of ${totalPages} · ${total} jobs`;
  const buttons = [
    ["First", 1],
    ["Prev", Math.max(1, current - 1)],
    ["Next", Math.min(totalPages, current + 1)],
    ["Last", totalPages],
  ];
  buttons.slice(0, 2).forEach(([text, nextPage]) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "secondary";
    button.textContent = text;
    button.disabled = nextPage === current || totalPages <= 1;
    button.addEventListener("click", () => {
      state.infoJobPages[status] = nextPage;
      refreshJobs().catch((error) => toast(error.message, "error"));
    });
    pager.append(button);
  });
  pager.append(label);
  buttons.slice(2).forEach(([text, nextPage]) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "secondary";
    button.textContent = text;
    button.disabled = nextPage === current || totalPages <= 1;
    button.addEventListener("click", () => {
      state.infoJobPages[status] = nextPage;
      refreshJobs().catch((error) => toast(error.message, "error"));
    });
    pager.append(button);
  });
  return pager;
}

async function refreshJobs() {
  try {
    const response = await fetch(jobsStatusUrl(), {headers: {"Accept": "application/json"}});
    if (!response.ok) return;
    renderJobs(await response.json());
    state.jobStatusErrors = 0;
  } catch {
    state.jobStatusErrors += 1;
    if (!state.unloading && document.visibilityState === "visible" && state.jobStatusErrors >= 3) {
      toast("Job status unavailable.", "error");
      state.jobStatusErrors = 0;
    }
  }
}

function setupJobsDrawer() {
  const drawer = document.getElementById("jobs-drawer");
  const toggle = document.getElementById("jobs-toggle");
  const close = document.getElementById("jobs-close");
  if (!drawer || !toggle) return;
  const setOpen = (open) => {
    drawer.classList.toggle("open", open);
    drawer.setAttribute("aria-hidden", open ? "false" : "true");
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
    if (open) refreshJobs();
  };
  toggle.addEventListener("click", () => setOpen(!drawer.classList.contains("open")));
  if (close) close.addEventListener("click", () => setOpen(false));
}

function setupJobEvents() {
  if (!window.EventSource) {
    refreshJobs();
    setInterval(refreshJobs, 3000);
    return;
  }
  const events = new EventSource("/api/jobs/events");
  events.addEventListener("snapshot", (event) => {
    if (document.querySelector("[data-jobs-status]")) refreshJobs();
    else renderJobs(JSON.parse(event.data));
  });
  events.addEventListener("new-job", (event) => {
    const payload = JSON.parse(event.data);
    if (payload.job) toast(`New job: ${jobLabel(payload.job)}`);
  });
  events.onerror = () => {
    refreshJobs();
  };
}

function formSubmittedValue(form, submitter) {
  if (!submitter || !submitter.name) return "";
  return `${submitter.name}:${submitter.value}`;
}

function shouldRemoveFormCard(form, payload, submitter) {
  if (form.dataset.removeOnSuccess === "true") return true;
  const statuses = (form.dataset.removeStatuses || "").split(/\s+/).filter(Boolean);
  if (!statuses.length) return Boolean(payload.remove);
  return statuses.includes(submitter?.value || payload.status || "");
}

function updateCardAfterAction(form, payload, submitter) {
  const card = form.closest("[data-series-card]");
  if (form.action.includes("/rescan") || form.action.includes("/caught-up")) {
    window.location.reload();
    return;
  }
  if (!card) return;
  if (shouldRemoveFormCard(form, payload, submitter)) {
    card.classList.add("removing");
    setTimeout(() => card.remove(), 180);
    return;
  }
  if (payload.status && form.action.includes("/progress")) {
    card.dataset.progress = payload.status;
    card.querySelectorAll(".library-card-main p").forEach((line) => {
      if (line.textContent.includes("Progress:")) {
        line.textContent = line.textContent.replace(/Progress: [^ ·]+/, `Progress: ${payload.status}`);
      }
    });
    applyFilters();
  }
}

function setupAsyncForms() {
  document.querySelectorAll("form[data-async-form]").forEach((form) => {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const submitter = event.submitter;
      const formData = new FormData(form);
      if (submitter && submitter.name) formData.set(submitter.name, submitter.value);
      form.dataset.pendingSubmit = formSubmittedValue(form, submitter);
      form.querySelectorAll("button").forEach((button) => {
        button.disabled = true;
      });
      try {
        const payload = await postJson(form.action, formData);
        toast(payload.message || "Updated.");
        updateCardAfterAction(form, payload, submitter);
        await refreshJobs();
      } catch (error) {
        toast(error.message, "error");
      } finally {
        form.querySelectorAll("button").forEach((button) => {
          button.disabled = false;
        });
      }
    });
  });
}

setupFilters();
setupJobsDrawer();
setupJobEvents();
setupAsyncForms();

window.addEventListener("beforeunload", () => {
  state.unloading = true;
});
