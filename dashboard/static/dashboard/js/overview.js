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
    weekSelect.addEventListener('change', () => weekForm.submit());
  }

  const fieldRepTile = document.getElementById('field_rep_tile');
  const fieldRepPanel = document.getElementById('field_rep_insights_panel');
  const fieldRepToggle = document.getElementById('field-rep-toggle');
  const fieldRepClose = document.getElementById('field-rep-close');

  function setFieldRepPanel(open) {
    if (!fieldRepTile || !fieldRepPanel) return;
    fieldRepPanel.classList.toggle('hidden', !open);
    fieldRepTile.setAttribute('aria-expanded', open ? 'true' : 'false');
    if (fieldRepToggle) {
      fieldRepToggle.textContent = open ? 'Hide insights' : 'View all reps';
    }
    if (open) {
      fieldRepPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
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
  if (fieldRepClose) {
    fieldRepClose.addEventListener('click', (event) => {
      event.stopPropagation();
      setFieldRepPanel(false);
    });
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

      const { jsPDF } = window.jspdf;
      const pdfDoc = new jsPDF('p', 'mm', 'a4');
      const pageWidth = pdfDoc.internal.pageSize.getWidth();
      const pageHeight = pdfDoc.internal.pageSize.getHeight();
      const margin = 9;
      const sectionGap = 5;
      const contentWidth = pageWidth - margin * 2;
      const contentHeight = pageHeight - margin * 2;
      let y = margin;
      let hasContent = false;

      document.body.classList.add('pdf-exporting');
      await document.fonts?.ready;
      await new Promise((resolve) => requestAnimationFrame(resolve));

      const sections = [
        document.getElementById('header'),
        document.querySelector('.controls-card'),
        document.querySelector('.top-grid'),
        document.getElementById('kpi_tiles'),
        !fieldRepPanel?.classList.contains('hidden') ? fieldRepPanel : null,
        document.querySelector('.bottom-grid'),
      ].filter(Boolean);

      for (const section of sections) {
        const captureCanvas = await window.html2canvas(section, {
          backgroundColor: '#ffffff',
          useCORS: true,
          scale: Math.min(2, window.devicePixelRatio || 1.5),
          windowWidth: Math.max(1240, document.documentElement.scrollWidth),
        });
        if (!captureCanvas.width || !captureCanvas.height) continue;

        const imgData = captureCanvas.toDataURL('image/png');
        let imgWidth = contentWidth;
        let imgHeight = (captureCanvas.height * imgWidth) / captureCanvas.width;
        let x = margin;

        if (imgHeight > contentHeight) {
          const scale = contentHeight / imgHeight;
          imgWidth *= scale;
          imgHeight = contentHeight;
          x = margin + (contentWidth - imgWidth) / 2;
        }

        if (hasContent && y + imgHeight > pageHeight - margin) {
          pdfDoc.addPage();
          y = margin;
        }

        pdfDoc.addImage(imgData, 'PNG', x, y, imgWidth, imgHeight);
        y += imgHeight + sectionGap;
        hasContent = true;
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
