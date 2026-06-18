(function () {
  const pageLoading = document.getElementById('page-loading');
  const pageLoadingText = pageLoading?.querySelector('[data-loading-text]');
  function showPageLoading(message) {
    if (pageLoadingText && message) {
      pageLoadingText.textContent = message;
    }
    pageLoading?.classList.remove('hidden');
  }
  function hidePageLoading() {
    pageLoading?.classList.add('hidden');
    if (pageLoadingText) {
      pageLoadingText.textContent = 'Loading...';
    }
  }

  window.addEventListener('pageshow', hidePageLoading);
  document.querySelectorAll('a[href]').forEach((link) => {
    link.addEventListener('click', (event) => {
      const href = link.getAttribute('href') || '';
      if (
        event.defaultPrevented
        || event.metaKey
        || event.ctrlKey
        || event.shiftKey
        || event.altKey
        || link.target
        || link.hasAttribute('download')
        || link.matches('[data-collateral-switch-link]')
        || !href
        || href.startsWith('#')
        || href.startsWith('javascript:')
      ) {
        return;
      }
      showPageLoading();
    });
  });

  const labels = JSON.parse(document.getElementById('trend-labels')?.textContent || '[]');
  const opened = JSON.parse(document.getElementById('trend-opened')?.textContent || '[]');
  const reached = JSON.parse(document.getElementById('trend-reached')?.textContent || '[]');
  const pdf = JSON.parse(document.getElementById('trend-pdf')?.textContent || '[]');
  const video = JSON.parse(document.getElementById('trend-video')?.textContent || '[]');

  const canvas = document.getElementById('trend-chart');
  if (canvas && labels.length) {
    const ctx = canvas.getContext('2d');
    const width = canvas.clientWidth;
    const height = 260;
    canvas.width = width * window.devicePixelRatio;
    canvas.height = height * window.devicePixelRatio;
    ctx.scale(window.devicePixelRatio, window.devicePixelRatio);

    const margin = { top: 22, right: 18, bottom: 56, left: 46 };
    const chartW = width - margin.left - margin.right;
    const chartH = height - margin.top - margin.bottom;
    const series = [
      { name: 'Doctors Opened %', values: opened, color: '#18a668' },
      { name: 'Doctors Reached %', values: reached, color: '#3279db' },
      { name: 'PDF Downloads %', values: pdf, color: '#7a57d1' },
      { name: 'Video Viewed (>50%) %', values: video, color: '#f0aa12' },
    ];

    const rawMax = Math.max(...series.flatMap((s) => s.values).filter((value) => Number.isFinite(value)), 0);
    const axisSteps = [1, 2, 5, 10, 20, 40, 60, 80, 100];
    const maxVal = rawMax <= 0 ? 100 : axisSteps.find((step) => step >= rawMax * 1.15) || 100;
    const groupCount = labels.length;
    const groupWidth = chartW / Math.max(groupCount, 1);
    const barWidth = Math.min(20, groupWidth / 6);

    ctx.clearRect(0, 0, width, height);
    ctx.strokeStyle = '#d5dbe5';
    ctx.lineWidth = 1;

    for (let i = 0; i <= 5; i++) {
      const y = margin.top + (chartH * i / 5);
      ctx.beginPath();
      ctx.moveTo(margin.left, y);
      ctx.lineTo(margin.left + chartW, y);
      ctx.stroke();
      ctx.fillStyle = '#6b7280';
      ctx.font = '11px Arial';
      const tickValue = maxVal - (maxVal * i / 5);
      const pct = maxVal <= 5 ? tickValue.toFixed(1) : tickValue.toFixed(0);
      ctx.fillText(`${pct}`, 10, y + 4);
    }

    labels.forEach((label, idx) => {
      const xBase = margin.left + (idx * groupWidth) + groupWidth * 0.1;

      series.forEach((s, sIdx) => {
        const val = s.values[idx] || 0;
        const h = val > 0 ? Math.max((val / maxVal) * chartH, 3) : 0;
        const x = xBase + sIdx * (barWidth + 4);
        const y = margin.top + chartH - h;
        ctx.fillStyle = s.color;
        ctx.fillRect(x, y, barWidth, h);
      });

      ctx.fillStyle = '#374151';
      ctx.font = '12px Arial';
      ctx.fillText(label, xBase + barWidth, margin.top + chartH + 18);
    });

    // Legend is rendered in HTML for clarity and responsiveness.
  }

  const weekForm = document.getElementById('week-filter-form');
  const weekSelect = document.getElementById('week-select');
  if (weekForm && weekSelect) {
    weekForm.addEventListener('submit', showPageLoading);
    weekSelect.addEventListener('change', () => {
      showPageLoading();
      weekForm.submit();
    });
  }

  const fieldRepTile = document.getElementById('field_rep_tile');
  const fieldRepPanel = document.getElementById('field_rep_insights_panel');
  const fieldRepToggle = document.getElementById('field-rep-toggle');
  const fieldRepClose = document.getElementById('field-rep-close');

  function setFieldRepPanel(open) {
    if (!fieldRepTile || !fieldRepPanel) return;
    const currentFieldRepToggle = document.getElementById('field-rep-toggle') || fieldRepToggle;
    const currentFieldRepClose = document.getElementById('field-rep-close') || fieldRepClose;
    fieldRepPanel.classList.toggle('hidden', !open);
    document.body.classList.toggle('modal-open', currentModalOpen());
    fieldRepTile.setAttribute('aria-expanded', open ? 'true' : 'false');
    if (currentFieldRepToggle) {
      currentFieldRepToggle.textContent = open ? 'Hide insights' : 'View all reps';
    }
    if (open) {
      currentFieldRepClose?.focus();
    }
  }

  if (fieldRepTile && fieldRepPanel) {
    fieldRepTile.addEventListener('click', () => {
      setFieldRepPanel(fieldRepPanel.classList.contains('hidden'));
    });
    fieldRepTile.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        setFieldRepPanel(fieldRepPanel.classList.contains('hidden'));
      }
    });
  }
  document.addEventListener('click', (event) => {
    if (event.target?.closest?.('#field-rep-close')) {
      event.stopPropagation();
      setFieldRepPanel(false);
    }
  });
  if (fieldRepPanel) {
    fieldRepPanel.addEventListener('click', (event) => {
      if (event.target === fieldRepPanel) {
        setFieldRepPanel(false);
      }
    });
  }
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && fieldRepPanel && !fieldRepPanel.classList.contains('hidden')) {
      setFieldRepPanel(false);
    }
  });

  function escapeExcelHtml(value) {
    return String(value || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function parseDoctorPayload(button) {
    try {
      const doctors = JSON.parse(button?.dataset.doctors || '[]');
      return Array.isArray(doctors) ? doctors : [];
    } catch (error) {
      return [];
    }
  }

  function buildExcelTable(headers, rows) {
    const headerHtml = headers.map((header) => `<th>${escapeExcelHtml(header)}</th>`).join('');
    const bodyHtml = rows.map((row) => (
      `<tr>${row.map((cell) => `<td>${escapeExcelHtml(cell)}</td>`).join('')}</tr>`
    )).join('');
    return `<table border="1"><thead><tr>${headerHtml}</tr></thead><tbody>${bodyHtml}</tbody></table>`;
  }

  function isMissingDoctorName(name) {
    const normalized = String(name || '').trim().toLowerCase();
    return !normalized || ['unknown doctor', 'unknown', 'null', 'none', '-'].includes(normalized);
  }

  function campaignDownloadKey() {
    return (window.location.pathname.split('/')[2] || 'campaign').replace(/[^a-zA-Z0-9-_]/g, '_');
  }

  function shouldUseServerDownload(element) {
    return Boolean(
      element?.dataset?.serverDownload === 'true'
      || (element?.tagName === 'A' && element.getAttribute('href'))
    );
  }

  function downloadExcelWorkbook(prefix, workbook) {
    const blob = new Blob([workbook], { type: 'application/vnd.ms-excel;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${prefix}_${campaignDownloadKey()}.xls`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  }

  function contentDispositionFilename(headerValue) {
    const value = String(headerValue || '');
    const utfMatch = value.match(/filename\*=UTF-8''([^;]+)/i);
    if (utfMatch?.[1]) {
      try {
        return decodeURIComponent(utfMatch[1].replace(/"/g, ''));
      } catch (error) {
        return utfMatch[1].replace(/"/g, '');
      }
    }
    const plainMatch = value.match(/filename="?([^";]+)"?/i);
    return plainMatch?.[1] || '';
  }

  function triggerFileDownload(blob, filename) {
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename || `download_${campaignDownloadKey()}.xls`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  }

  async function downloadServerFile(link) {
    const href = link?.getAttribute('href') || '';
    if (!href) return;
    const fallbackFilename = link.getAttribute('download') || `download_${campaignDownloadKey()}.xls`;
    showPageLoading('Preparing download...');
    link.setAttribute('aria-busy', 'true');
    link.classList.add('is-loading');
    try {
      const response = await fetch(href, { credentials: 'same-origin' });
      if (!response.ok) {
        throw new Error(`Download failed with status ${response.status}`);
      }
      const blob = await response.blob();
      const filename = contentDispositionFilename(response.headers.get('content-disposition')) || fallbackFilename;
      triggerFileDownload(blob, filename);
    } catch (error) {
      window.location.href = href;
    } finally {
      hidePageLoading();
      link.removeAttribute('aria-busy');
      link.classList.remove('is-loading');
    }
  }

  function getFieldRepTable() {
    return document.getElementById('field-rep-insights-table');
  }

  function collectDoctorDetailRows() {
    const currentFieldRepTable = getFieldRepTable();
    if (!currentFieldRepTable) return [];
    const rows = [];
    Array.from(currentFieldRepTable.querySelectorAll('.doctor-count-btn')).forEach((button) => {
      const doctors = parseDoctorPayload(button);
      const state = button.dataset.state || button.closest('tr')?.children?.[2]?.textContent.trim() || 'UNKNOWN';
      doctors.forEach((doctor, index) => {
        rows.push({
          repId: button.dataset.repId || '',
          repName: button.dataset.repName || '',
          state,
          metric: button.dataset.metricLabel || '',
          index: index + 1,
          doctorName: doctor?.name || '',
          doctorPhone: doctor?.phone || '',
          doctorKey: doctor?.doctor_key || '',
        });
      });
    });
    return rows;
  }

  const fieldRepExcelBtn = document.getElementById('field-rep-excel-btn');
  const unmappedDoctorsExcelBtn = document.getElementById('unmapped-doctors-excel-btn');
  const fieldRepTable = getFieldRepTable();
  if (fieldRepExcelBtn && fieldRepTable) {
    fieldRepExcelBtn.addEventListener('click', (event) => {
      event.stopPropagation();
      if (shouldUseServerDownload(fieldRepExcelBtn)) return;
      const summaryHeaders = Array.from(fieldRepTable.querySelectorAll('thead th')).map((cell) => cell.textContent.trim());
      const summaryRows = Array.from(fieldRepTable.querySelectorAll('tbody tr')).map((row) => (
        Array.from(row.children).map((cell) => cell.textContent.trim())
      ));
      const doctorDetailRows = collectDoctorDetailRows().map((row) => [
        row.repId,
        row.repName,
        row.state,
        row.metric,
        row.index,
        row.doctorName,
        row.doctorPhone,
        row.doctorKey,
      ]);
      const workbook = `
        <html>
          <head><meta charset="UTF-8"></head>
          <body>
            <h2>Field Representative Summary</h2>
            ${buildExcelTable(summaryHeaders, summaryRows)}
            <br>
            <h2>Doctor Details</h2>
            ${buildExcelTable(
              ['Field Rep ID', 'Field Representative', 'State', 'Metric', 'S. No.', 'Doctor Name', 'Doctor Number', 'Doctor Key'],
              doctorDetailRows
            )}
          </body>
        </html>
      `;
      downloadExcelWorkbook('field_rep_insights', workbook);
    });
  }

  if (unmappedDoctorsExcelBtn && fieldRepTable) {
    unmappedDoctorsExcelBtn.addEventListener('click', (event) => {
      event.stopPropagation();
      if (shouldUseServerDownload(unmappedDoctorsExcelBtn)) return;
      const assignedDoctorKeysByRep = new Map();
      collectDoctorDetailRows().forEach((row) => {
        if (row.metric !== 'Doctors Assigned') return;
        const doctorKey = row.doctorKey || row.doctorPhone;
        if (!doctorKey) return;
        if (!assignedDoctorKeysByRep.has(row.repId)) {
          assignedDoctorKeysByRep.set(row.repId, new Set());
        }
        assignedDoctorKeysByRep.get(row.repId).add(doctorKey);
      });
      const correctionRowsByKey = new Map();
      collectDoctorDetailRows().forEach((row) => {
        const isUnmappedRep = row.repId === 'UNMAPPED_ACTIVITY';
        const isUnknownName = isMissingDoctorName(row.doctorName);
        const doctorKey = row.doctorKey || row.doctorPhone;
        const assignedKeys = assignedDoctorKeysByRep.get(row.repId) || new Set();
        const isActivityMetric = row.metric !== 'Doctors Assigned';
        const isOutsideRoster = isActivityMetric && !isUnmappedRep && doctorKey && !assignedKeys.has(doctorKey);
        if (!isUnmappedRep && !isUnknownName && !isOutsideRoster) return;
        let issue = 'Doctor name missing or unknown';
        if (isUnmappedRep) {
          issue = 'No campaign-roster field rep mapping';
        } else if (isOutsideRoster) {
          issue = 'Activity doctor is not in assigned roster for this rep';
        }
        const key = [row.repId, row.doctorKey, row.doctorPhone, issue].join('|');
        const existing = correctionRowsByKey.get(key) || {
          repId: row.repId,
          repName: row.repName,
          metrics: new Set(),
          doctorName: row.doctorName,
          doctorPhone: row.doctorPhone,
          doctorKey: row.doctorKey,
          issue,
        };
        existing.metrics.add(row.metric);
        correctionRowsByKey.set(key, existing);
      });
      const correctionRows = Array.from(correctionRowsByKey.values()).map((row, index) => [
        index + 1,
        row.issue,
        row.repId,
        row.repName,
        Array.from(row.metrics).sort().join(', '),
        row.doctorName,
        row.doctorPhone,
        row.doctorKey,
        '',
        '',
        '',
        row.doctorPhone,
      ]);
      const workbook = `
        <html>
          <head><meta charset="UTF-8"></head>
          <body>
            <h2>Doctors Requiring Manual Mapping</h2>
            ${buildExcelTable(
              [
                'S. No.',
                'Issue',
                'Current Field Rep ID',
                'Current Field Representative',
                'Metric(s)',
                'Current Doctor Name',
                'Current Doctor Number',
                'Doctor Key',
                'Correct Field Rep Brand Supplied ID',
                'Correct Field Rep Name / Email',
                'Correct Doctor Name',
                'Correct Doctor Number',
              ],
              correctionRows
            )}
          </body>
        </html>
      `;
      downloadExcelWorkbook('doctors_requiring_manual_mapping', workbook);
    });
  }

  document.addEventListener('click', (event) => {
    const link = event.target?.closest?.('a[data-server-download="true"]');
    if (!link) return;
    event.preventDefault();
    event.stopPropagation();
    downloadServerFile(link);
  });

  const oldCollateralsPanel = document.getElementById('old_collaterals_panel');
  const oldCollateralsClose = document.getElementById('old-collaterals-close');

  function setOldCollateralsPanel(open) {
    if (!oldCollateralsPanel) return;
    const currentOldCollateralsClose = document.getElementById('old-collaterals-close') || oldCollateralsClose;
    oldCollateralsPanel.classList.toggle('hidden', !open);
    document.body.classList.toggle('modal-open', currentModalOpen());
    if (open) {
      currentOldCollateralsClose?.focus();
    }
  }

  document.addEventListener('click', (event) => {
    if (event.target?.closest?.('[data-collateral-switch-trigger]')) {
      event.stopPropagation();
      setOldCollateralsPanel(true);
    }
  });
  document.addEventListener('click', (event) => {
    if (event.target?.closest?.('#old-collaterals-close')) {
      event.stopPropagation();
      setOldCollateralsPanel(false);
    }
  });

  function updateWeekFormCollateral(url) {
    const weekFilterForm = document.getElementById('week-filter-form');
    if (!weekFilterForm) return;
    const nextUrl = new URL(url, window.location.origin);
    const collateralId = nextUrl.searchParams.get('collateral_id') || '';
    let collateralInput = weekFilterForm.querySelector('input[name="collateral_id"]');
    if (!collateralId) {
      collateralInput?.remove();
      return;
    }
    if (!collateralInput) {
      collateralInput = document.createElement('input');
      collateralInput.type = 'hidden';
      collateralInput.name = 'collateral_id';
      weekFilterForm.appendChild(collateralInput);
    }
    collateralInput.value = collateralId;
  }

  function replaceInnerHtmlFromDocument(sourceDocument, selector) {
    const currentElement = document.querySelector(selector);
    const nextElement = sourceDocument.querySelector(selector);
    if (!currentElement || !nextElement) return false;
    currentElement.innerHTML = nextElement.innerHTML;
    return true;
  }

  async function switchCollateralInPlace(link) {
    const href = link?.getAttribute('href') || '';
    if (!href) return;
    showPageLoading('Refreshing insights...');
    link.setAttribute('aria-busy', 'true');
    link.classList.add('is-loading');
    try {
      const response = await fetch(href, {
        credentials: 'same-origin',
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
      });
      if (!response.ok) {
        throw new Error(`Collateral switch failed with status ${response.status}`);
      }
      const html = await response.text();
      const sourceDocument = new DOMParser().parseFromString(html, 'text/html');
      const updatedPanel = replaceInnerHtmlFromDocument(sourceDocument, '#field_rep_insights_panel .card');
      const updatedTile = replaceInnerHtmlFromDocument(sourceDocument, '#field_rep_tile');
      replaceInnerHtmlFromDocument(sourceDocument, '#old_collaterals_panel .old-collateral-list');
      if (!updatedPanel || !updatedTile) {
        throw new Error('Collateral response did not include Field Representative Insights.');
      }
      const nextUrl = new URL(href, window.location.origin);
      window.history.pushState({}, '', `${nextUrl.pathname}${nextUrl.search}`);
      updateWeekFormCollateral(nextUrl.toString());
      setOldCollateralsPanel(false);
      setFieldRepPanel(true);
    } catch (error) {
      console.error('Failed to switch collateral without reload', error);
      alert('Could not refresh Field Representative Insights for this collateral. Please try again.');
    } finally {
      hidePageLoading();
      link.removeAttribute('aria-busy');
      link.classList.remove('is-loading');
    }
  }

  document.addEventListener('click', (event) => {
    const link = event.target?.closest?.('a[data-collateral-switch-link]');
    if (
      !link
      || event.metaKey
      || event.ctrlKey
      || event.shiftKey
      || event.altKey
      || link.target
    ) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    switchCollateralInPlace(link);
  });

  if (oldCollateralsPanel) {
    oldCollateralsPanel.addEventListener('click', (event) => {
      if (event.target === oldCollateralsPanel) {
        setOldCollateralsPanel(false);
      }
    });
  }
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && oldCollateralsPanel && !oldCollateralsPanel.classList.contains('hidden')) {
      setOldCollateralsPanel(false);
    }
  });

  const doctorRosterPanel = document.getElementById('doctor_roster_panel');
  const doctorRosterClose = document.getElementById('doctor-roster-close');
  const doctorRosterTitle = document.getElementById('doctor-roster-title');
  const doctorRosterSubtitle = document.getElementById('doctor-roster-subtitle');
  const doctorRosterBody = document.getElementById('doctor-roster-body');
  const doctorRosterNameHeader = document.getElementById('doctor-roster-name-header');

  function escapeHtml(value) {
    return String(value || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  function currentModalOpen() {
    return Boolean(
      (fieldRepPanel && !fieldRepPanel.classList.contains('hidden'))
      || (oldCollateralsPanel && !oldCollateralsPanel.classList.contains('hidden'))
      || (doctorRosterPanel && !doctorRosterPanel.classList.contains('hidden')),
    );
  }

  function setDoctorRosterPanel(open) {
    if (!doctorRosterPanel) return;
    doctorRosterPanel.classList.toggle('hidden', !open);
    document.body.classList.toggle('modal-open', currentModalOpen());
    if (open) {
      doctorRosterClose?.focus();
    }
  }

  function doctorRowsHtml(doctors) {
    if (!Array.isArray(doctors) || !doctors.length) {
      return '<tr class="empty-roster-row"><td colspan="3">No doctor names are available for this count.</td></tr>';
    }
    return doctors.map((doctor, index) => `
      <tr>
        <td>${index + 1}</td>
        <td>${escapeHtml(isMissingDoctorName(doctor.name) ? 'Unknown Doctor' : doctor.name)}</td>
        <td>${escapeHtml(doctor.phone || '-')}</td>
      </tr>
    `).join('');
  }

  async function fetchDoctorPayload(button) {
    const detailUrl = button?.dataset?.detailUrl || '';
    const repId = button?.dataset?.repId || '';
    const metricKey = button?.dataset?.metricKey || 'assigned';
    if (!detailUrl || !repId) {
      return parseDoctorPayload(button);
    }
    const url = new URL(detailUrl, window.location.origin);
    url.searchParams.set('rep_id', repId);
    url.searchParams.set('metric', metricKey);
    const response = await fetch(url.toString(), { credentials: 'same-origin' });
    if (!response.ok) {
      throw new Error(`Doctor details failed with status ${response.status}`);
    }
    const payload = await response.json();
    return Array.isArray(payload?.doctors) ? payload.doctors : [];
  }

  document.addEventListener('click', async (event) => {
    const button = event.target?.closest?.('.doctor-count-btn');
    if (!button) return;
      event.stopPropagation();
      const repName = button.dataset.repName || button.dataset.repId || 'Field Representative';
      const metricLabel = button.dataset.metricLabel || 'Assigned Doctors';
      if (doctorRosterTitle) {
        doctorRosterTitle.textContent = `${metricLabel} - ${repName}`;
      }
      if (doctorRosterSubtitle) {
        doctorRosterSubtitle.textContent = 'Loading doctor details...';
      }
      if (doctorRosterNameHeader) {
        doctorRosterNameHeader.classList.remove('hidden');
      }
      if (doctorRosterBody) {
        doctorRosterBody.innerHTML = '<tr class="empty-roster-row"><td colspan="3">Loading doctor details...</td></tr>';
      }
      setDoctorRosterPanel(true);
      button.setAttribute('aria-busy', 'true');
      button.classList.add('is-loading');
      try {
        const doctors = await fetchDoctorPayload(button);
        if (doctorRosterSubtitle) {
          doctorRosterSubtitle.textContent = `${doctors.length} unique doctor${doctors.length === 1 ? '' : 's'} for ${metricLabel}.`;
        }
        if (doctorRosterBody) {
          doctorRosterBody.innerHTML = doctorRowsHtml(doctors);
        }
      } catch (error) {
        const fallbackDoctors = parseDoctorPayload(button);
        if (doctorRosterSubtitle) {
          doctorRosterSubtitle.textContent = fallbackDoctors.length
            ? `${fallbackDoctors.length} unique doctor${fallbackDoctors.length === 1 ? '' : 's'} for ${metricLabel}.`
            : 'Doctor details could not be loaded.';
        }
        if (doctorRosterBody) {
          doctorRosterBody.innerHTML = fallbackDoctors.length
            ? doctorRowsHtml(fallbackDoctors)
            : '<tr class="empty-roster-row"><td colspan="3">Doctor details could not be loaded. Please try the Excel download.</td></tr>';
        }
      } finally {
        button.removeAttribute('aria-busy');
        button.classList.remove('is-loading');
      }
  });

  if (doctorRosterClose) {
    doctorRosterClose.addEventListener('click', (event) => {
      event.stopPropagation();
      setDoctorRosterPanel(false);
    });
  }
  if (doctorRosterPanel) {
    doctorRosterPanel.addEventListener('click', (event) => {
      if (event.target === doctorRosterPanel) {
        setDoctorRosterPanel(false);
      }
    });
  }
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && doctorRosterPanel && !doctorRosterPanel.classList.contains('hidden')) {
      setDoctorRosterPanel(false);
    }
  });

  const downloadBtn = document.getElementById('download-pdf-btn');
  const reportRoot = document.getElementById('report-root');
  if (!downloadBtn || !reportRoot) return;
  if (shouldUseServerDownload(downloadBtn)) return;

  downloadBtn.addEventListener('click', async () => {
    const originalText = downloadBtn.textContent;
    downloadBtn.disabled = true;
    downloadBtn.textContent = 'Preparing PDF...';

    try {
      if (typeof window.html2canvas !== 'function' || !window.jspdf?.jsPDF) {
        throw new Error('Required PDF libraries are unavailable');
      }

      const { jsPDF } = window.jspdf;
      const pdfDoc = new jsPDF('p', 'mm', 'a4');
      const pageWidth = pdfDoc.internal.pageSize.getWidth();
      const pageHeight = pdfDoc.internal.pageSize.getHeight();
      const margin = 9;
      const contentWidth = pageWidth - margin * 2;
      const contentHeight = pageHeight - margin * 2;

      document.body.classList.add('pdf-exporting');
      await document.fonts?.ready;
      await new Promise((resolve) => setTimeout(resolve, 100));

      const captureCanvas = await window.html2canvas(reportRoot, {
        backgroundColor: '#f2f4f8',
        useCORS: true,
        scale: Math.min(2, window.devicePixelRatio || 1.5),
        scrollX: 0,
        scrollY: 0,
        windowWidth: Math.max(1240, reportRoot.scrollWidth, document.documentElement.scrollWidth),
        windowHeight: Math.max(reportRoot.scrollHeight, document.documentElement.scrollHeight),
      });

      if (!captureCanvas.width || !captureCanvas.height) {
        throw new Error('Report capture was empty');
      }

      const sliceCanvas = document.createElement('canvas');
      const sliceContext = sliceCanvas.getContext('2d');
      if (!sliceContext) {
        throw new Error('Unable to create PDF canvas context');
      }
      const maxSliceHeight = Math.max(1, Math.floor((captureCanvas.width * contentHeight) / contentWidth));
      let sourceY = 0;
      let pageIndex = 0;

      while (sourceY < captureCanvas.height) {
        const sliceHeight = Math.min(maxSliceHeight, captureCanvas.height - sourceY);
        sliceCanvas.width = captureCanvas.width;
        sliceCanvas.height = sliceHeight;
        sliceContext.clearRect(0, 0, sliceCanvas.width, sliceCanvas.height);
        sliceContext.drawImage(
          captureCanvas,
          0,
          sourceY,
          captureCanvas.width,
          sliceHeight,
          0,
          0,
          captureCanvas.width,
          sliceHeight,
        );

        if (pageIndex > 0) {
          pdfDoc.addPage();
        }
        const imgData = sliceCanvas.toDataURL('image/png');
        const imgHeight = (sliceHeight * contentWidth) / captureCanvas.width;
        pdfDoc.addImage(imgData, 'PNG', margin, margin, contentWidth, imgHeight);

        sourceY += sliceHeight;
        pageIndex += 1;
      }

      const safeCampaign = (window.location.pathname.split('/')[2] || 'campaign').replace(/[^a-zA-Z0-9-_]/g, '_');
      const params = new URLSearchParams(window.location.search);
      const week = params.get('week');
      const suffix = week ? `_week_${week}` : '_all_weeks';
      pdfDoc.save(`in_clinic_report_${safeCampaign}${suffix}.pdf`);
    } catch (err) {
      console.error('Failed to auto-download PDF', err);
      alert('PDF download failed in this browser. Please refresh and try again.');
    } finally {
      document.body.classList.remove('pdf-exporting');
      downloadBtn.disabled = false;
      downloadBtn.textContent = originalText;
    }
  });
})();
