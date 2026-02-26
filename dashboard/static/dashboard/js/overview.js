(function () {
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

    const maxVal = Math.max(100, ...series.flatMap((s) => s.values), 1);
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
      const pct = (maxVal - (maxVal * i / 5)).toFixed(0);
      ctx.fillText(`${pct}`, 10, y + 4);
    }

    labels.forEach((label, idx) => {
      const xBase = margin.left + (idx * groupWidth) + groupWidth * 0.1;

      series.forEach((s, sIdx) => {
        const val = s.values[idx] || 0;
        const h = (val / maxVal) * chartH;
        const x = xBase + sIdx * (barWidth + 4);
        const y = margin.top + chartH - h;
        ctx.fillStyle = s.color;
        ctx.fillRect(x, y, barWidth, h);
      });

      ctx.fillStyle = '#374151';
      ctx.font = '12px Arial';
      ctx.fillText(label, xBase + barWidth, margin.top + chartH + 18);
    });

    const legendY = height - 18;
    let legendX = margin.left;
    series.forEach((s) => {
      ctx.fillStyle = s.color;
      ctx.fillRect(legendX, legendY - 9, 12, 12);
      legendX += 16;
      ctx.fillStyle = '#4b5563';
      ctx.font = '11px Arial';
      ctx.fillText(s.name, legendX, legendY);
      legendX += ctx.measureText(s.name).width + 16;
    });
  }

  const weekForm = document.getElementById('week-filter-form');
  const weekSelect = document.getElementById('week-select');
  if (weekForm && weekSelect) {
    weekSelect.addEventListener('change', () => weekForm.submit());
  }

  const downloadBtn = document.getElementById('download-image-btn');
  const reportRoot = document.getElementById('report-root');
  if (!downloadBtn || !reportRoot) return;

  const elementToPng = async (element) => {
    const width = element.scrollWidth;
    const height = element.scrollHeight;
    const clonedNode = element.cloneNode(true);
    const clonedWrapper = document.createElement('div');
    clonedWrapper.setAttribute('xmlns', 'http://www.w3.org/1999/xhtml');
    clonedWrapper.style.width = `${width}px`;
    clonedWrapper.style.height = `${height}px`;
    clonedWrapper.appendChild(clonedNode);

    const cssText = Array.from(document.styleSheets)
      .map((sheet) => {
        try {
          return Array.from(sheet.cssRules || []).map((rule) => rule.cssText).join('\n');
        } catch (err) {
          return '';
        }
      })
      .join('\n');

    const style = document.createElement('style');
    style.textContent = cssText;
    clonedWrapper.insertBefore(style, clonedWrapper.firstChild);

    const svg = `
      <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">
        <foreignObject width="100%" height="100%">${new XMLSerializer().serializeToString(clonedWrapper)}</foreignObject>
      </svg>
    `;

    const svgBlob = new Blob([svg], { type: 'image/svg+xml;charset=utf-8' });
    const svgUrl = URL.createObjectURL(svgBlob);

    const img = new Image();
    await new Promise((resolve, reject) => {
      img.onload = resolve;
      img.onerror = reject;
      img.src = svgUrl;
    });
    URL.revokeObjectURL(svgUrl);

    const canvasOut = document.createElement('canvas');
    canvasOut.width = width;
    canvasOut.height = height;
    const outCtx = canvasOut.getContext('2d');
    outCtx.fillStyle = '#f2f4f8';
    outCtx.fillRect(0, 0, width, height);
    outCtx.drawImage(img, 0, 0);
    return canvasOut;
  };

  downloadBtn.addEventListener('click', async () => {
    const originalText = downloadBtn.textContent;
    downloadBtn.disabled = true;
    downloadBtn.textContent = 'Preparing...';

    try {
      const imageCanvas = await elementToPng(reportRoot);
      const link = document.createElement('a');
      const safeCampaign = (window.location.pathname.split('/')[2] || 'campaign').replace(/[^a-zA-Z0-9-_]/g, '_');
      const params = new URLSearchParams(window.location.search);
      const week = params.get('week');
      const suffix = week ? `_week_${week}` : '_all_weeks';
      link.download = `in_clinic_report_${safeCampaign}${suffix}.png`;
      link.href = imageCanvas.toDataURL('image/png');
      link.click();
    } catch (err) {
      console.error('Failed to download report image', err);
      try {
        const fallbackCanvas = document.createElement('canvas');
        fallbackCanvas.width = window.innerWidth;
        fallbackCanvas.height = window.innerHeight;
        const ctx = fallbackCanvas.getContext('2d');
        ctx.fillStyle = '#f2f4f8';
        ctx.fillRect(0, 0, fallbackCanvas.width, fallbackCanvas.height);
        ctx.fillStyle = '#111827';
        ctx.font = '16px Arial';
        ctx.fillText('Unable to capture full report in this browser.', 24, 40);
        const link = document.createElement('a');
        link.download = 'in_clinic_report_fallback.png';
        link.href = fallbackCanvas.toDataURL('image/png');
        link.click();
      } catch (_) {
        alert('Could not generate image download in this browser.');
      }
    } finally {
      downloadBtn.disabled = false;
      downloadBtn.textContent = originalText;
    }
  });
})();
