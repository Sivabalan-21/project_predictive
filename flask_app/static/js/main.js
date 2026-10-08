// Auto-refresh dashboard every 30 seconds
if (window.location.pathname === '/') {
  setTimeout(() => location.reload(), 30000);
}

// Active nav highlight
document.querySelectorAll('.nav-item').forEach(item => {
  if (item.href === window.location.href) item.classList.add('active');
});