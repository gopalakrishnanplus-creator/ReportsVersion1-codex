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

    let legendX = margin.left;
    let legendY = margin.top + chartH + 36;
    const legendMaxX = margin.left + chartW;
    series.forEach((s) => {
      const labelWidth = ctx.measureText(s.name).width;
      const itemWidth = 12 + 4 + labelWidth + 18;
      if (legendX + itemWidth > legendMaxX) {
        legendX = margin.left;
        legendY += 16;
      }
      ctx.fillStyle = s.color;
      ctx.fillRect(legendX, legendY - 9, 12, 12);
      legendX += 16;
      ctx.fillStyle = '#4b5563';
      ctx.font = '11px Arial';
      ctx.fillText(s.name, legendX, legendY);
      legendX += labelWidth + 18;
    });
  }

  const weekForm = document.getElementById('week-filter-form');
  const weekSelect = document.getElementById('week-select');
  if (weekForm && weekSelect) {
    weekSelect.addEventListener('change', () => weekForm.submit());
  }

  const downloadBtn = document.getElementById('download-pdf-btn');
  const reportRoot = document.getElementById('report-root');
  if (!downloadBtn || !reportRoot) return;

  downloadBtn.addEventListener('click', async () => {
    const originalText = downloadBtn.textContent;
    downloadBtn.disabled = true;
    downloadBtn.textContent = 'Preparing PDF...';

    try {
      if (typeof window.html2canvas !== 'function' || !window.jspdf?.jsPDF) {
        throw new Error('Required PDF libraries are unavailable');
      }

      const captureCanvas = await window.html2canvas(reportRoot, {
        backgroundColor: '#f2f4f8',
        useCORS: true,
        scale: Math.min(2, window.devicePixelRatio || 1.5),
        windowWidth: document.documentElement.scrollWidth,
        windowHeight: document.documentElement.scrollHeight,
      });

      const imgData = captureCanvas.toDataURL('image/png');
      const { jsPDF } = window.jspdf;
      const pdfDoc = new jsPDF('p', 'mm', 'a4');

      const pageWidth = pdfDoc.internal.pageSize.getWidth();
      const pageHeight = pdfDoc.internal.pageSize.getHeight();
      const imgWidth = pageWidth;
      const imgHeight = (captureCanvas.height * imgWidth) / captureCanvas.width;

      let remaining = imgHeight;
      let position = 0;

      pdfDoc.addImage(imgData, 'PNG', 0, position, imgWidth, imgHeight);
      remaining -= pageHeight;

      while (remaining > 0) {
        position = remaining - imgHeight;
        pdfDoc.addPage();
        pdfDoc.addImage(imgData, 'PNG', 0, position, imgWidth, imgHeight);
        remaining -= pageHeight;
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
      downloadBtn.disabled = false;
      downloadBtn.textContent = originalText;
    }
  });
})();
