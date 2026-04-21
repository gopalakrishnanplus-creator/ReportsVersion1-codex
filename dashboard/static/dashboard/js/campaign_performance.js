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

  function toNumber(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function formatNumber(value) {
    const number = toNumber(value);
    return Number.isInteger(number) ? number.toLocaleString() : number.toLocaleString(undefined, { maximumFractionDigits: 1 });
  }

  function formatCellValue(value) {
    if (value === null || value === undefined || value === "") {
      return "—";
    }
    if (typeof value === "number") {
      return formatNumber(value);
    }
    return String(value);
  }

  function computePoints(values, width, height, padding, maxValue) {
    const usableWidth = width - padding.left - padding.right;
    const usableHeight = height - padding.top - padding.bottom;
    if (!values.length) {
      return [];
    }
    if (values.length === 1) {
      return [
        {
          x: padding.left + usableWidth / 2,
          y: padding.top + usableHeight - (toNumber(values[0]) / maxValue) * usableHeight,
          value: toNumber(values[0]),
        },
      ];
    }
    return values.map((value, index) => ({
      x: padding.left + (usableWidth / (values.length - 1)) * index,
      y: padding.top + usableHeight - (toNumber(value) / maxValue) * usableHeight,
      value: toNumber(value),
    }));
  }

  function buildLinePath(points) {
    if (!points.length) {
      return "";
    }
    return points
      .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
      .join(" ");
  }

  function buildAreaPath(points, height, padding) {
    if (!points.length) {
      return "";
    }
    const baseY = height - padding.bottom;
    const start = points[0];
    const end = points[points.length - 1];
    return `${buildLinePath(points)} L ${end.x} ${baseY} L ${start.x} ${baseY} Z`;
  }

  function buildDots(points, color) {
    return points
      .map(
        (point) =>
          `<circle class="chart-dot" cx="${point.x}" cy="${point.y}" r="4.5" fill="${escapeHtml(color)}"></circle>`,
      )
      .join("");
  }

  function renderTrend(trend) {
    if (!trend || !Array.isArray(trend.categories) || !trend.categories.length) {
      return "";
    }

    const width = 760;
    const height = 300;
    const padding = { top: 24, right: 18, bottom: 52, left: 48 };
    const seriesList = (trend.series || []).filter((series) => Array.isArray(series.values));
    const values = seriesList.flatMap((series) => series.values.map(toNumber));
    const maxValue = Math.max(...values, 1);
    const usableHeight = height - padding.top - padding.bottom;
    const gridRatios = [0, 0.25, 0.5, 0.75, 1];
    const defs = [];
    const areas = [];
    const lines = [];
    const dots = [];
    const legend = [];
    const latestBadges = [];

    seriesList.forEach((series, index) => {
      const gradientId = `chart-fill-${escapeHtml(series.key || `series-${index}`)}`;
      const color = series.color || "#0f766e";
      const points = computePoints(series.values || [], width, height, padding, maxValue);
      const areaPath = buildAreaPath(points, height, padding);
      const linePath = buildLinePath(points);
      const latestValue = points.length ? points[points.length - 1].value : 0;
      defs.push(`
        <linearGradient id="${gradientId}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="${escapeHtml(color)}" stop-opacity="0.28"></stop>
          <stop offset="100%" stop-color="${escapeHtml(color)}" stop-opacity="0.02"></stop>
        </linearGradient>
      `);
      if (areaPath) {
        areas.push(`<path class="chart-area" d="${areaPath}" fill="url(#${gradientId})"></path>`);
      }
      if (linePath) {
        lines.push(`<path class="chart-line" d="${linePath}" stroke="${escapeHtml(color)}"></path>`);
        dots.push(buildDots(points, color));
      }
      legend.push(
        `<span class="legend-item"><span class="legend-swatch" style="background:${escapeHtml(color)}"></span>${escapeHtml(series.label || "")}</span>`,
      );
      latestBadges.push(
        `<span class="trend-pill"><span class="trend-pill-dot" style="background:${escapeHtml(color)}"></span>${escapeHtml(series.label || "")}: ${escapeHtml(formatNumber(latestValue))}</span>`,
      );
    });

    const horizontalGuides = gridRatios
      .map((ratio) => {
        const y = padding.top + usableHeight * (1 - ratio);
        return `
          <g>
            <line class="chart-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
            <text class="chart-y-label" x="${padding.left - 10}" y="${y + 4}" text-anchor="end">${formatNumber(maxValue * ratio)}</text>
          </g>
        `;
      })
      .join("");

    const categoryLabels = trend.categories
      .map((label) => `<span>${escapeHtml(label)}</span>`)
      .join("");

    return `
      <div class="panel panel-chart">
        <div class="panel-head">
          <div>
            <h3>${escapeHtml(trend.label || "Trend")}</h3>
            <p>Recent reporting buckets for this campaign.</p>
          </div>
        </div>
        <div class="trend-pill-row">${latestBadges.join("")}</div>
        <div class="chart-shell">
          <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(trend.label || "Trend chart")}">
            <defs>${defs.join("")}</defs>
            ${horizontalGuides}
            ${areas.join("")}
            ${lines.join("")}
            ${dots.join("")}
          </svg>
          <div class="chart-axis chart-axis-wide">${categoryLabels}</div>
        </div>
        <div class="legend-row">${legend.join("")}</div>
      </div>
    `;
  }

  function renderBarChart(chart) {
    if (!chart || !Array.isArray(chart.bars) || !chart.bars.length) {
      return "";
    }
    const maxValue = Math.max(...chart.bars.map((bar) => toNumber(bar.value)), 1);
    const rows = chart.bars
      .map((bar) => {
        const width = Math.max(4, (toNumber(bar.value) / maxValue) * 100);
        return `
          <div class="comparison-row">
            <div class="comparison-topline">
              <strong>${escapeHtml(bar.label || "")}</strong>
              <span>${escapeHtml(bar.display_value || formatNumber(bar.value))}</span>
            </div>
            <div class="comparison-track" aria-hidden="true">
              <div class="comparison-fill" style="width:${width}%;background:${escapeHtml(bar.color || "#0f766e")}"></div>
            </div>
          </div>
        `;
      })
      .join("");
    return `
      <div class="panel">
        <h3>${escapeHtml(chart.label || "Comparison")}</h3>
        <p>${escapeHtml(chart.description || "")}</p>
        <div class="comparison-list">${rows}</div>
      </div>
    `;
  }

  function renderTable(table) {
    if (!table) {
      return "";
    }
    const columns = Array.isArray(table.columns) ? table.columns : [];
    const rows = Array.isArray(table.rows) ? table.rows : [];
    const head = columns.map((column) => `<th class="align-${escapeHtml(column.align || "left")}">${escapeHtml(column.label || "")}</th>`).join("");
    const body = rows.length
      ? rows
          .map((row) => {
            const cells = columns
              .map((column) => {
                const raw = row ? row[column.key] : "";
                return `<td class="align-${escapeHtml(column.align || "left")}">${escapeHtml(formatCellValue(raw))}</td>`;
              })
              .join("");
            return `<tr>${cells}</tr>`;
          })
          .join("")
      : `<tr><td colspan="${Math.max(columns.length, 1)}" class="table-empty">${escapeHtml(table.empty_message || "No rows available.")}</td></tr>`;
    return `
      <div class="panel panel-table">
        <div class="panel-head">
          <div>
            <h3>${escapeHtml(table.label || "Detail Table")}</h3>
            <p>${escapeHtml(table.description || "")}</p>
          </div>
        </div>
        <div class="table-scroll">
          <table class="detail-table">
            <thead><tr>${head}</tr></thead>
            <tbody>${body}</tbody>
          </table>
        </div>
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
        const rate = toNumber(row.adoption_rate || 0);
        const width = rate > 0 ? Math.min(100, rate) : 0;
        return `
          <div class="breakdown-row">
            <div class="breakdown-topline">
              <strong>${escapeHtml(row.label || row.system_key || "System")}</strong>
              <span>${escapeHtml(rate.toFixed(1))}%</span>
            </div>
            <div class="bar-track" aria-hidden="true">
              <div class="bar-fill" style="width:${width}%"></div>
            </div>
            <div class="breakdown-footnote">
              ${escapeHtml(formatNumber(row.participating_clinics || 0))} active / ${escapeHtml(formatNumber(row.eligible_clinics || 0))} added
            </div>
          </div>
        `;
      })
      .join("");
    return `
      <div class="panel">
        <h3>Adoption Breakdown</h3>
        <p>Clinic participation and adoption rate by selected system.</p>
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
    const barMarkup = renderBarChart(section.bar_chart);
    const tableMarkup = renderTable(section.table);
    if (trendMarkup) {
      panels.push(trendMarkup);
    }
    if (barMarkup) {
      panels.push(barMarkup);
    }
    if (tableMarkup) {
      panels.push(tableMarkup);
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
    const systemCount = toNumber(payload.system_count || 0);
    setText("page-title", campaign.campaign_name || payload.requested_campaign_id || "Campaign Performance");
    setText(
      "page-subtitle",
      campaign.brand_name
        ? `${campaign.brand_name} unified reporting view across ${systemCount} selected system(s).`
        : `Unified reporting view across ${systemCount} selected system(s).`,
    );
    setText("campaign-id-label", campaign.campaign_id || payload.requested_campaign_id || "-");
    setText("brand-name-label", campaign.brand_name || "Not mapped");
    setText("system-count-label", String(systemCount));
    const pillRow = document.getElementById("system-pill-row");
    if (pillRow) {
      pillRow.innerHTML = renderPills(payload.available_systems || []);
    }

    if (!systemCount) {
      showEmpty(payload.detail || "The campaign resolved successfully, but no configured system data is available yet.");
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
