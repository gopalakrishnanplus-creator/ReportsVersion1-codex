(function () {
  const labels = JSON.parse(document.getElementById('trend-labels')?.textContent || '[]');
  const reached = JSON.parse(document.getElementById('trend-reached')?.textContent || '[]');
  const opened = JSON.parse(document.getElementById('trend-opened')?.textContent || '[]');
  const consumed = JSON.parse(document.getElementById('trend-consumed')?.textContent || '[]');
  const health = JSON.parse(document.getElementById('trend-health')?.textContent || '[]');

  const canvas = document.getElementById('trend-chart');
  if (!canvas || !labels.length) return;

  const ctx = canvas.getContext('2d');
  const w = canvas.width = canvas.clientWidth * window.devicePixelRatio;
  const h = canvas.height = 180 * window.devicePixelRatio;
  ctx.scale(window.devicePixelRatio, window.devicePixelRatio);

  const padding = { top: 16, right: 24, bottom: 28, left: 36 };
  const width = canvas.clientWidth;
  const height = 180;
  const all = [...reached, ...opened, ...consumed, ...health, 1];
  const maxV = Math.max(...all);

  const drawLine = (series, color) => {
    if (!series.length) return;
    ctx.beginPath();
    series.forEach((v, i) => {
      const x = padding.left + (i * (width - padding.left - padding.right) / Math.max(labels.length - 1, 1));
      const y = padding.top + ((maxV - v) * (height - padding.top - padding.bottom) / maxV);
      i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.stroke();
  };

  ctx.clearRect(0, 0, width, height);
  ctx.strokeStyle = '#d0d8e4';
  ctx.beginPath();
  ctx.moveTo(padding.left, height - padding.bottom);
  ctx.lineTo(width - padding.right, height - padding.bottom);
  ctx.stroke();

  drawLine(reached, '#2d6cdf');
  drawLine(opened, '#19a974');
  drawLine(consumed, '#f59e0b');
  drawLine(health, '#d9363e');

  ctx.fillStyle = '#5b677a';
  ctx.font = '12px Arial';
  labels.forEach((l, i) => {
    const x = padding.left + (i * (width - padding.left - padding.right) / Math.max(labels.length - 1, 1));
    ctx.fillText(l, x - 10, height - 8);
  });
})();
