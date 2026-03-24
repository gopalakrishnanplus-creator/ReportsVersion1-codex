(function () {
  const downloadBtn = document.getElementById('download-pdf-btn');
  const reportRoot = document.getElementById('report-root');
  const globalFilters = document.getElementById('global-filters');
  const csrfInput = document.querySelector('#pdf-export-csrf input[name=csrfmiddlewaretoken]');
  let isExporting = false;

  if (globalFilters) {
    globalFilters.querySelectorAll('select').forEach((select) => {
      select.addEventListener('change', () => globalFilters.submit());
    });
  }

  if (!downloadBtn || !reportRoot) {
    return;
  }

  const buildFilename = () => {
    const explicit = reportRoot.dataset.exportFilename;
    if (explicit) {
      return explicit;
    }
    return 'sapa-growth-dashboard.pdf';
  };

  const buildPdf = async () => {
    const html2canvasLib = window.html2canvas;
    if (!html2canvasLib) {
      window.print();
      return;
    }

    const exportWidth = Math.ceil(reportRoot.scrollWidth);
    const exportHeight = Math.ceil(reportRoot.scrollHeight);
    const previousScrollX = window.scrollX;
    const previousScrollY = window.scrollY;
    try {
      const canvas = await html2canvasLib(reportRoot, {
        backgroundColor: '#ffffff',
        scale: Math.max(2, window.devicePixelRatio || 1),
        useCORS: true,
        logging: false,
        width: exportWidth,
        height: exportHeight,
        windowWidth: exportWidth,
        windowHeight: exportHeight,
        scrollX: 0,
        scrollY: -previousScrollY,
      });
      const pngBlob = await new Promise((resolve) => canvas.toBlob(resolve, 'image/png', 1));
      if (!pngBlob) {
        throw new Error('Unable to create dashboard snapshot');
      }

      const formData = new FormData();
      formData.append('snapshot', pngBlob, buildFilename().replace(/\.pdf$/i, '.png'));

      const response = await fetch(downloadBtn.dataset.exportUrl || '/sapa-growth/export/dashboard.pdf', {
        method: 'POST',
        body: formData,
        credentials: 'same-origin',
        headers: csrfInput && csrfInput.value ? { 'X-CSRFToken': csrfInput.value } : {},
      });
      if (!response.ok) {
        throw new Error(`PDF export failed with status ${response.status}`);
      }

      const pdfBlob = await response.blob();
      const downloadUrl = window.URL.createObjectURL(pdfBlob);
      const link = document.createElement('a');
      link.href = downloadUrl;
      link.download = buildFilename();
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.setTimeout(() => window.URL.revokeObjectURL(downloadUrl), 1000);
    } finally {
      window.scrollTo(previousScrollX, previousScrollY);
    }
  };

  downloadBtn.addEventListener('click', async () => {
    if (isExporting) {
      return;
    }
    isExporting = true;
    downloadBtn.style.pointerEvents = 'none';
    try {
      await buildPdf();
    } catch (error) {
      console.error(error);
      window.alert('Dashboard PDF download failed. Please reload the page and try again.');
    } finally {
      downloadBtn.style.pointerEvents = '';
      isExporting = false;
    }
  });
})();
