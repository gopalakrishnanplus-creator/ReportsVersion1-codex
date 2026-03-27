(function () {
  const filterForm = document.getElementById('global-filters');
  const downloadButton = document.getElementById('download-pdf-btn');
  const reportRoot = document.getElementById('report-root');
  const csrfInput = document.querySelector('#pdf-export-csrf input[name=csrfmiddlewaretoken]');
  const chartCanvas = document.getElementById('trend-chart');

  if (filterForm) {
    filterForm.querySelectorAll('select').forEach((select) => {
      select.addEventListener('change', () => filterForm.submit());
    });
  }

  function readJsonScript(id) {
    const node = document.getElementById(id);
    if (!node) {
      return [];
    }
    try {
      return JSON.parse(node.textContent || '[]');
    } catch (error) {
      console.error(error);
      return [];
    }
  }

  function roundedRectPath(ctx, x, y, width, height, radius) {
    const safeRadius = Math.min(radius, width / 2, height / 2);
    ctx.beginPath();
    ctx.moveTo(x + safeRadius, y);
    ctx.lineTo(x + width - safeRadius, y);
    ctx.quadraticCurveTo(x + width, y, x + width, y + safeRadius);
    ctx.lineTo(x + width, y + height - safeRadius);
    ctx.quadraticCurveTo(x + width, y + height, x + width - safeRadius, y + height);
    ctx.lineTo(x + safeRadius, y + height);
    ctx.quadraticCurveTo(x, y + height, x, y + height - safeRadius);
    ctx.lineTo(x, y + safeRadius);
    ctx.quadraticCurveTo(x, y, x + safeRadius, y);
    ctx.closePath();
  }

  function drawTrendChart() {
    if (!chartCanvas || !chartCanvas.getContext) {
      return;
    }

    const labels = readJsonScript('pe-trend-labels');
    const series = [
      { label: 'Activation', values: readJsonScript('pe-trend-activation'), color: '#2AA7A1' },
      { label: 'Play Rate', values: readJsonScript('pe-trend-play'), color: '#3FB6AF' },
      { label: '50% Engagement', values: readJsonScript('pe-trend-engage'), color: '#2F3E9E' },
      { label: 'Completion', values: readJsonScript('pe-trend-complete'), color: '#E45757' },
    ];

    if (!labels.length) {
      return;
    }

    const ctx = chartCanvas.getContext('2d');
    const cssWidth = chartCanvas.clientWidth || 760;
    const cssHeight = chartCanvas.clientHeight || 240;
    const dpr = Math.max(1, window.devicePixelRatio || 1);
    chartCanvas.width = Math.round(cssWidth * dpr);
    chartCanvas.height = Math.round(cssHeight * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    const width = cssWidth;
    const height = cssHeight;
    const padding = { top: 16, right: 18, bottom: 44, left: 42 };
    const plotWidth = width - padding.left - padding.right;
    const plotHeight = height - padding.top - padding.bottom;
    const maxValue = Math.max(100, ...series.flatMap((item) => item.values.map((value) => Number(value) || 0)));

    ctx.clearRect(0, 0, width, height);

    ctx.strokeStyle = '#E6EEF1';
    ctx.lineWidth = 1;
    ctx.fillStyle = '#6B7C93';
    ctx.font = '11px Inter, system-ui, sans-serif';

    for (let tick = 0; tick <= 5; tick += 1) {
      const ratio = tick / 5;
      const y = padding.top + plotHeight - plotHeight * ratio;
      ctx.beginPath();
      ctx.moveTo(padding.left, y);
      ctx.lineTo(width - padding.right, y);
      ctx.stroke();
      ctx.fillText(String(Math.round(maxValue * ratio)), 8, y + 4);
    }

    const groupWidth = plotWidth / labels.length;
    const innerGroupWidth = groupWidth * 0.66;
    const barGap = Math.max(3, innerGroupWidth * 0.035);
    const barWidth = Math.min(20, (innerGroupWidth - barGap * (series.length - 1)) / series.length);
    const groupOffset = (groupWidth - (barWidth * series.length + barGap * (series.length - 1))) / 2;

    labels.forEach((label, labelIndex) => {
      const shortLabel = String(label).replace('Week ', 'W');
      const labelX = padding.left + groupWidth * labelIndex + groupWidth / 2;
      ctx.fillStyle = '#6B7C93';
      ctx.textAlign = 'center';
      ctx.fillText(shortLabel, labelX, height - 14);
    });

    ctx.textAlign = 'left';
    series.forEach((item, seriesIndex) => {
      item.values.forEach((rawValue, valueIndex) => {
        const value = Number(rawValue) || 0;
        const barHeight = maxValue ? (value / maxValue) * plotHeight : 0;
        const x = padding.left + groupWidth * valueIndex + groupOffset + seriesIndex * (barWidth + barGap);
        const y = padding.top + plotHeight - barHeight;

        ctx.fillStyle = item.color;
        roundedRectPath(ctx, x, y, barWidth, Math.max(barHeight, 2), 5);
        ctx.fill();
      });
    });
  }

  async function exportDashboardPdf() {
    if (!downloadButton || !reportRoot) {
      return;
    }
    const html2canvasLib = window.html2canvas;
    if (!html2canvasLib) {
      window.print();
      return;
    }
    const canvas = await html2canvasLib(reportRoot, {
      backgroundColor: '#F6FAFB',
      scale: Math.max(2, window.devicePixelRatio || 1),
      useCORS: true,
      logging: false,
      width: Math.ceil(reportRoot.scrollWidth),
      height: Math.ceil(reportRoot.scrollHeight),
      windowWidth: Math.ceil(reportRoot.scrollWidth),
      windowHeight: Math.ceil(reportRoot.scrollHeight),
      scrollX: 0,
      scrollY: -window.scrollY,
    });
    const pngBlob = await new Promise((resolve) => canvas.toBlob(resolve, 'image/png', 1));
    if (!pngBlob) {
      throw new Error('Unable to create dashboard snapshot');
    }
    const formData = new FormData();
    formData.append('snapshot', pngBlob, 'pe-dashboard.png');
    const response = await fetch(downloadButton.dataset.exportUrl, {
      method: 'POST',
      body: formData,
      credentials: 'same-origin',
      headers: csrfInput && csrfInput.value ? { 'X-CSRFToken': csrfInput.value } : {},
    });
    if (!response.ok) {
      throw new Error(`PDF export failed with status ${response.status}`);
    }
    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = reportRoot.dataset.exportFilename || 'patient-education-dashboard.pdf';
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.setTimeout(() => window.URL.revokeObjectURL(url), 1000);
  }

  if (downloadButton) {
    downloadButton.addEventListener('click', async () => {
      downloadButton.disabled = true;
      try {
        await exportDashboardPdf();
      } catch (error) {
        console.error(error);
        window.alert('Dashboard PDF download failed. Please reload the page and try again.');
      } finally {
        downloadButton.disabled = false;
      }
    });
  }

  drawTrendChart();
  window.addEventListener('resize', drawTrendChart);
})();
