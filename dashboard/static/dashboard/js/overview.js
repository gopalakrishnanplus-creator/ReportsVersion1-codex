(function () {
  const labels = JSON.parse(document.getElementById('trend-labels')?.textContent || '[]');
  const opened = JSON.parse(document.getElementById('trend-opened')?.textContent || '[]');
  const reached = JSON.parse(document.getElementById('trend-reached')?.textContent || '[]');
  const pdf = JSON.parse(document.getElementById('trend-pdf')?.textContent || '[]');
  const video = JSON.parse(document.getElementById('trend-video')?.textContent || '[]');

  const canvas = document.getElementById('trend-chart');
  if (!canvas || !labels.length) return;

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

  const maxVal = Math.max(100, ...series.flatMap(s => s.values), 1);
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
})();
