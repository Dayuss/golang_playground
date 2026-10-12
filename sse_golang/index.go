package main

// indexHTML is the browser demo page. It uses the EventSource API to consume
// /events and renders every topic into its own panel. Kept dependency-free.
const indexHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Go SSE Demo</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; background: #0b0e14; color: #c9d1d9; margin: 0; padding: 24px; }
  h1 { font-size: 18px; margin: 0 0 4px; }
  .sub { color: #8b949e; font-size: 12px; margin-bottom: 16px; }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #f85149; margin-right: 6px; }
  .dot.on { background: #3fb950; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; }
  .card { background: #11151c; border: 1px solid #21262d; border-radius: 8px; padding: 12px; min-height: 140px; }
  .card h2 { font-size: 12px; margin: 0 0 8px; color: #58a6ff; text-transform: uppercase; letter-spacing: 1px; }
  ul { list-style: none; margin: 0; padding: 0; font-size: 12px; }
  li { padding: 4px 0; border-bottom: 1px dashed #21262d; word-break: break-all; }
  .time { color: #8b949e; margin-right: 8px; }
  .pub { margin-top: 16px; display: flex; gap: 8px; }
  input { flex: 1; background: #0b0e14; border: 1px solid #30363d; color: #c9d1d9; padding: 8px; border-radius: 6px; font-family: inherit; }
  button { background: #238636; color: #fff; border: 0; padding: 8px 14px; border-radius: 6px; cursor: pointer; font-family: inherit; }
  button:hover { background: #2ea043; }
</style>
</head>
<body>
<h1>Server-Sent Events &mdash; Go simulation</h1>
<div class="sub"><span class="dot" id="dot"></span><span id="status">connecting&hellip;</span> &middot; <span id="count">0</span> events received</div>
<div class="grid">
  <div class="card"><h2>stocks</h2><ul id="stocks"></ul></div>
  <div class="card"><h2>metrics</h2><ul id="metrics"></ul></div>
  <div class="card"><h2>notifications</h2><ul id="notifications"></ul></div>
  <div class="card"><h2>custom</h2><ul id="custom"></ul></div>
</div>
<div class="pub">
  <input id="msg" placeholder="type a message and publish it into the stream" />
  <button onclick="publish()">Publish</button>
</div>
<script>
let count = 0, es;

function put(topic, text) {
  const ul = document.getElementById(topic);
  const li = document.createElement('li');
  const t = document.createElement('span');
  t.className = 'time';
  t.textContent = new Date().toLocaleTimeString();
  li.appendChild(t);
  li.appendChild(document.createTextNode(text));
  ul.prepend(li);
  while (ul.children.length > 8) ul.removeChild(ul.lastChild);
  document.getElementById('count').textContent = ++count;
}

function setStatus(on, text) {
  document.getElementById('dot').className = on ? 'dot on' : 'dot';
  document.getElementById('status').textContent = text;
}

// The data: line carries the full Event JSON (id/topic/type/payload/at),
// so the interesting fields live under .payload — not at the top level.
function payloadOf(e) {
  try { return JSON.parse(e.data).payload ?? {}; } catch (err) { return {}; }
}

function connect() {
  es = new EventSource('/events');
  es.onopen = () => setStatus(true, 'connected');
  es.onerror = () => setStatus(false, 'reconnecting\u2026');
  es.addEventListener('hello', (e) => {
    const p = payloadOf(e);
    put('custom', 'hello \u00b7 topics: ' + (p.topics || []).join(', '));
  });
  es.addEventListener('price', (e) => {
    const p = payloadOf(e);
    put('stocks', p.symbol + ' $' + p.price);
  });
  es.addEventListener('metrics', (e) => {
    const p = payloadOf(e);
    put('metrics', 'cpu ' + p.cpu_percent + '% \u00b7 mem ' + p.mem_percent + '%');
  });
  es.addEventListener('notification', (e) => {
    const p = payloadOf(e);
    put('notifications', '[' + p.level + '] ' + p.message);
  });
  es.addEventListener('message', (e) => {
    const p = payloadOf(e);
    put('custom', typeof p.text === 'string' ? p.text : JSON.stringify(p));
  });
}

async function publish() {
  const input = document.getElementById('msg');
  const text = input.value.trim();
  if (!text) return;
  await fetch('/publish', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ topic: 'custom', type: 'message', payload: { text: text, from: 'browser' } })
  });
  input.value = '';
}

connect();
</script>
</body>
</html>`
