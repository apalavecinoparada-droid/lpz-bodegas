/* Service worker de la App (27-10-2026): solo para que la App ABRA SIN SEÑAL en el celular del chofer.
   Reglas:
   - Con internet manda siempre la red: la App se actualiza igual que sin service worker (red primero).
   - La copia guardada se usa solo si no hay internet.
   - Solo se guardan la pantalla, las librerías y las lecturas que necesita Transporte (no se guardan datos de otros módulos).
   - Las escrituras (POST/PUT/DELETE) nunca pasan por aquí: sin señal las guarda la propia App en su cola. */
var CACHE = 'lpz-sin-senal-v2';
var API_GUARDAR = [/^\/api\/trans\//, /^\/api\/equipos$/, /^\/api\/faenas$/, /^\/api\/empresas$/, /^\/api\/comb\/estanques$/];

self.addEventListener('install', function () { self.skipWaiting(); });
self.addEventListener('activate', function (e) {
  e.waitUntil(caches.keys().then(function (ks) {
    return Promise.all(ks.filter(function (k) { return k !== CACHE; }).map(function (k) { return caches.delete(k); }));
  }).then(function () { return self.clients.claim(); }));
});

function sinSenal() {
  return new Response(JSON.stringify({ error: 'Sin señal' }), { status: 503, headers: { 'Content-Type': 'application/json' } });
}
// red primero; si responde bien se guarda una copia; sin red, la copia
function redPrimero(req, clave) {
  return fetch(req).then(function (r) {
    // las librerías del CDN llegan "opacas" (status 0): igual se guardan
    if (r && (r.ok || r.type === 'opaque')) { var c = r.clone(); caches.open(CACHE).then(function (ca) { ca.put(clave || req, c); }); }
    return r;
  }).catch(function () {
    return caches.open(CACHE).then(function (ca) { return ca.match(clave || req); });
  });
}

self.addEventListener('fetch', function (e) {
  var req = e.request;
  if (req.method !== 'GET') return;
  var u = new URL(req.url);
  // la pantalla: una sola copia (sin la parte ?...) para abrirla sin señal
  if (req.mode === 'navigate') {
    e.respondWith(redPrimero(req, u.origin + u.pathname).then(function (r) {
      return r || caches.open(CACHE).then(function (ca) { return ca.match(u.origin + '/'); }).then(function (r2) { return r2 || sinSenal(); });
    }));
    return;
  }
  // librerías de React, etc. (CDN): red primero, copia sin señal
  if (u.origin !== self.location.origin) {
    if (/unpkg\.com|cdnjs\.cloudflare\.com|cdn\.jsdelivr\.net/.test(u.host)) {
      e.respondWith(redPrimero(req).then(function (r) { return r || Response.error(); }));
    }
    return;
  }
  if (u.pathname.indexOf('/api/') === 0) {
    var guardar = API_GUARDAR.some(function (re) { return re.test(u.pathname); });
    e.respondWith(guardar
      ? redPrimero(req, u.href).then(function (r) { return r || sinSenal(); })
      : fetch(req).catch(sinSenal));
  }
});
