(function () {
  function readJsonScript(id) {
    const node = document.getElementById(id);
    if (!node) {
      return "";
    }
    try {
      return JSON.parse(node.textContent || '""');
    } catch (_error) {
      return "";
    }
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function setHidden(id, hidden) {
    const node = document.getElementById(id);
    if (!node) {
      return;
    }
    node.classList.toggle("hidden", hidden);
  }

  function setText(id, value) {
    const node = document.getElementById(id);
    if (node) {
      node.textContent = value;
    }
  }

  function buildLinePath(values, width, height, padding, maxValue) {
    const usableWidth = width - padding * 2;
    const usableHeight = height - padding * 2;
    if (!values.length) {
      return "";
    }
    if (values.length === 1) {
      const x = padding + usableWidth / 2;
      const y = padding + usableHeight - (values[0] / maxValue) * usableHeight;
      return `M ${x} ${y}`;
    }
    return values
      .map((value, index) => {
        const x = padding + (usableWidth / (values.length - 1)) * index;
        const y = padding + usableHeight - (value / maxValue) * usableHeight;
        return `${index === 0 ? "M" : "L"} ${x} ${y}`;
      })
      .join(" ");
  }

  function buildDots(values, width, height, padding, maxValue, color) {
    const usableWidth = width - padding * 2;
    const usableHeight = height - padding * 2;
    if (!values.length) {
      return "";
    }
    if (values.length === 1) {
      const x = padding + usableWidth / 2;
      const y = padding + usableHeight - (values[0] / maxValue) * usableHeight;
      return `<circle class="chart-dot" cx="${x}" cy="${y}" r="4" fill="${color}"></circle>`;
    }
    return values
      .map((value, index) => {
        const x = padding + (usableWidth / (values.length - 1)) * index;
        const y = padding + usableHeight - (value / maxValue) * usableHeight;
        return `<circle class="chart-dot" cx="${x}" cy="${y}" r="4" fill="${color}"></circle>`;
      })
      .join("");
  }

  function renderTrend(trend) {
    if (!trend || !Array.isArray(trend.categories) || !trend.categories.length) {
      return "";
    }
    const width = 640;
    const height = 240;
    const padding = 28;
    const values = (trend.series || []).flatMap((series) => series.values || []);
    const maxValue = Math.max(...values, 1);
    const gridLines = [0.25, 0.5, 0.75, 1]
      .map((ratio) => {
        const y = padding + (height - padding * 2) * (1 - ratio);
        return `<line class="chart-grid-line" x1="${padding}" y1="${y}" x2="${width - padding}" y2="${y}"></line>`;
      })
      .join("");
    const lineMarkup = (trend.series || [])
      .map((series) => {
        const path = buildLinePath(series.values || [], width, height, padding, maxValue);
        const dots = buildDots(series.values || [], width, height, padding, maxValue, series.color || "#0f766e");
        return `<path class="chart-line" d="${path}" stroke="${series.color || "#0f766e"}"></path>${dots}`;
      })
      .join("");
    const axisLabels = [
      trend.categories[0] || "",
      trend.categories[Math.floor(trend.categories.length / 2)] || "",
      trend.categories[trend.categories.length - 1] || "",
    ];
    const legend = (trend.series || [])
      .map(
        (series) =>
          `<span class="legend-item"><span class="legend-swatch" style="background:${escapeHtml(series.color || "#0f766e")}"></span>${escapeHtml(series.label || "")}</span>`,
      )
      .join("");

    return `
      <div class="panel">
        <h3>${escapeHtml(trend.label || "Trend")}</h3>
        <p>Trend over the most recent reporting buckets returned by the API.</p>
        <div class="chart-shell">
          <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(trend.label || "Trend chart")}">
            ${gridLines}
            ${lineMarkup}
          </svg>
          <div class="chart-axis">
            ${axisLabels.map((label) => `<span>${escapeHtml(label)}</span>`).join("")}
          </div>
        </div>
        <div class="legend-row">${legend}</div>
      </div>
    `;
  }

  function renderBreakdown(rows) {
    if (!Array.isArray(rows) || !rows.length) {
      return `
        <div class="panel">
          <h3>Adoption Breakdown</h3>
          <p>No clinic adoption data is available yet for this campaign.</p>
        </div>
      `;
    }
    const rowMarkup = rows
      .map((row) => {
        const rate = Number(row.adoption_rate || 0);
        const width = rate > 0 ? Math.min(100, rate) : 0;
        return `
          <div class="breakdown-row">
            <div class="breakdown-topline">
              <strong>${escapeHtml(row.label || row.system_key || "System")}</strong>
              <span>${escapeHtml((rate || 0).toFixed(1))}%</span>
            </div>
            <div class="bar-track" aria-hidden="true">
              <div class="bar-fill" style="width:${width}%"></div>
            </div>
            <div class="breakdown-footnote">
              ${escapeHtml(String(row.participating_clinics || 0))} participating / ${escapeHtml(String(row.eligible_clinics || 0))} tracked clinics
            </div>
          </div>
        `;
      })
      .join("");
    return `
      <div class="panel">
        <h3>Adoption Breakdown</h3>
        <p>Clinic participation and adoption rate by available system.</p>
        <div class="breakdown-list">${rowMarkup}</div>
      </div>
    `;
  }

  function renderMetric(metric) {
    return `
      <article class="metric-card">
        <p class="metric-label">${escapeHtml(metric.label || "")}</p>
        <p class="metric-value">${escapeHtml(metric.display_value || metric.value || "0")}</p>
        <p class="metric-helper">${escapeHtml(metric.helper_text || "")}</p>
      </article>
    `;
  }

  function renderMeta(meta) {
    const filtered = (Array.isArray(meta) ? meta : []).filter(Boolean);
    if (!filtered.length) {
      return "";
    }
    return filtered
      .map((item) => `<span class="meta-chip">${escapeHtml(item.label || "")}: ${escapeHtml(item.value || "")}</span>`)
      .join("");
  }

  function renderSection(section) {
    const metrics = (section.metrics || []).map(renderMetric).join("");
    const panels = [];
    const trendMarkup = renderTrend(section.trend);
    if (trendMarkup) {
      panels.push(trendMarkup);
    }
    if (section.breakdown) {
      panels.push(renderBreakdown(section.breakdown));
    }
    const panelMarkup = panels.length ? `<div class="section-panels">${panels.join("")}</div>` : "";
    return `
      <article class="section-card card">
        <header class="section-header">
          <div>
            <h2>${escapeHtml(section.label || "")}</h2>
            <p class="section-subtitle">${escapeHtml(section.subtitle || "")}</p>
          </div>
          <div class="section-meta">${renderMeta(section.meta)}</div>
        </header>
        <div class="metric-grid">${metrics}</div>
        ${panelMarkup}
      </article>
    `;
  }

  function renderPills(systems) {
    if (!Array.isArray(systems) || !systems.length) {
      return '<span class="system-pill">No systems available yet</span>';
    }
    return systems.map((system) => `<span class="system-pill">${escapeHtml(system.label || "")}</span>`).join("");
  }

  function showError(message) {
    setHidden("loading-state", true);
    setHidden("empty-state", true);
    setHidden("report-sections", true);
    const errorNode = document.getElementById("error-state");
    if (!errorNode) {
      return;
    }
    errorNode.innerHTML = `<h2>Unable to load campaign performance</h2><p>${escapeHtml(message || "Unexpected error")}</p>`;
    setHidden("error-state", false);
  }

  function showEmpty(message) {
    setHidden("loading-state", true);
    setHidden("error-state", true);
    setHidden("report-sections", true);
    const emptyNode = document.getElementById("empty-state");
    if (!emptyNode) {
      return;
    }
    emptyNode.innerHTML = `<h2>No connected systems found</h2><p>${escapeHtml(message || "This campaign does not have reportable activity yet.")}</p>`;
    setHidden("empty-state", false);
  }

  function renderPayload(payload) {
    const campaign = payload.campaign || {};
    setText("page-title", campaign.campaign_name || payload.requested_campaign_id || "Campaign Performance");
    setText(
      "page-subtitle",
      campaign.brand_name
        ? `${campaign.brand_name} unified reporting view across ${payload.system_count || 0} active system(s).`
        : `Unified reporting view across ${payload.system_count || 0} active system(s).`,
    );
    setText("campaign-id-label", campaign.campaign_id || payload.requested_campaign_id || "-");
    setText("brand-name-label", campaign.brand_name || "Not mapped");
    setText("system-count-label", String(payload.system_count || 0));
    const pillRow = document.getElementById("system-pill-row");
    if (pillRow) {
      pillRow.innerHTML = renderPills(payload.available_systems || []);
    }

    if (!payload.system_count) {
      showEmpty(payload.detail || "The campaign resolved successfully, but no system data is available yet.");
      return;
    }

    const sectionsNode = document.getElementById("report-sections");
    if (!sectionsNode) {
      return;
    }
    sectionsNode.innerHTML = (payload.sections || []).map(renderSection).join("");
    setHidden("loading-state", true);
    setHidden("error-state", true);
    setHidden("empty-state", true);
    setHidden("report-sections", false);
  }

  async function init() {
    const apiUrl = readJsonScript("campaign-performance-api-url");
    if (!apiUrl) {
      showError("Campaign performance API URL is missing.");
      return;
    }
    try {
      const response = await fetch(apiUrl, {
        headers: { Accept: "application/json" },
        credentials: "same-origin",
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.detail || "Campaign performance request failed.");
      }
      renderPayload(payload);
    } catch (error) {
      showError(error instanceof Error ? error.message : "Campaign performance request failed.");
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();
