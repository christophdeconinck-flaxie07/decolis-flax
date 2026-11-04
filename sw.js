// Minimale service worker: maakt de site installeerbaar als app.
// Er wordt NIETS gecachet, zodat je altijd de actuele regendata ziet.
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));
self.addEventListener('fetch', (event) => {
  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request).catch(() =>
        new Response('Geen internetverbinding. Probeer het opnieuw zodra je online bent.', {
          headers: { 'Content-Type': 'text/plain; charset=utf-8' }
        })
      )
    );
  }
});
