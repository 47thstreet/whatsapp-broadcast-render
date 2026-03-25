#!/usr/bin/env node
'use strict';

const http = require('http');
const QRCode = require('qrcode');
const fs = require('fs');
const path = require('path');
const pino = require('pino');

const PORT = process.env.PORT || 3050;
const BASE_SESSION_DIR = path.resolve('./wa-session');
const logger = pino({ level: 'silent' });

// ── Multi-Account WhatsApp (Baileys) ────────────────────────────────────────
const accounts = new Map(); // id -> { id, name, phone, sock, status, qrDataUrl, groups }
let idCounter = 0;

function makeId() {
  return 'wa' + (++idCounter) + '-' + Date.now().toString(36);
}

async function initAccount(id) {
  const acc = accounts.get(id);
  if (!acc) return;

  const {
    default: makeWASocket, useMultiFileAuthState, DisconnectReason,
    fetchLatestBaileysVersion, makeCacheableSignalKeyStore,
  } = await import('@whiskeysockets/baileys');

  const dir = path.join(BASE_SESSION_DIR, id);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const { state, saveCreds } = await useMultiFileAuthState(dir);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
    logger, printQRInTerminal: false, generateHighQualityLinkPreview: false,
  });
  acc.sock = sock;
  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;
    if (qr) {
      acc.status = 'qr';
      try { acc.qrDataUrl = await QRCode.toDataURL(qr, { width: 256, margin: 2 }); }
      catch { acc.qrDataUrl = null; }
      console.log(`[${acc.name}] QR received`);
    }
    if (connection === 'open') {
      acc.status = 'ready';
      acc.qrDataUrl = null;
      if (sock.user) {
        acc.phone = (sock.user.id || '').split(':')[0].split('@')[0];
        if (sock.user.name) acc.name = sock.user.name;
      }
      console.log(`[${acc.name}] Connected`);
      refreshGroups(id);
    }
    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode;
      const retry = code !== DisconnectReason.loggedOut;
      console.log(`[${acc.name}] Disconnected (${code})${retry ? ' — reconnecting' : ' — logged out'}`);
      acc.status = 'disconnected';
      if (retry) { setTimeout(() => initAccount(id), 3000); }
      else {
        fs.rmSync(path.join(BASE_SESSION_DIR, id), { recursive: true, force: true });
        fs.mkdirSync(path.join(BASE_SESSION_DIR, id), { recursive: true });
        acc.status = 'logged_out';
        setTimeout(() => initAccount(id), 1000);
      }
    }
  });
}

async function refreshGroups(id) {
  const acc = accounts.get(id);
  if (!acc || acc.status !== 'ready' || !acc.sock) return;
  try {
    const raw = await acc.sock.groupFetchAllParticipating();
    acc.groups = Object.values(raw).map((g) => ({
      id: g.id, name: g.subject,
      participantCount: g.participants ? g.participants.length : 0,
      accountId: id, accountName: acc.name,
    }));
    console.log(`[${acc.name}] ${acc.groups.length} groups`);
  } catch (e) { console.error(`[${acc.name}] groups error:`, e.message); }
}

// Restore existing sessions on startup
if (!fs.existsSync(BASE_SESSION_DIR)) fs.mkdirSync(BASE_SESSION_DIR, { recursive: true });
fs.readdirSync(BASE_SESSION_DIR, { withFileTypes: true })
  .filter(d => d.isDirectory()).forEach(d => {
    const id = d.name;
    idCounter++;
    accounts.set(id, { id, name: 'Account ' + idCounter, phone: '', sock: null, status: 'initializing', qrDataUrl: null, groups: [] });
    initAccount(id).catch(e => {
      console.error(`Init error [${id}]:`, e.message);
      const a = accounts.get(id); if (a) a.status = 'error';
    });
  });

// ── API Handlers ────────────────────────────────────────────────────────────
function handleAccounts() {
  const list = [];
  for (const [, a] of accounts) {
    list.push({ id: a.id, name: a.name, phone: a.phone || '', status: a.status, groupCount: a.groups.length });
  }
  return { accounts: list };
}

async function handleCreateAccount(body) {
  const id = makeId();
  const name = (body && body.name) || ('Account ' + (accounts.size + 1));
  accounts.set(id, { id, name, phone: '', sock: null, status: 'initializing', qrDataUrl: null, groups: [] });
  await initAccount(id);
  return { id, name };
}

function handleDeleteAccount(id) {
  const acc = accounts.get(id);
  if (!acc) return { error: 'Not found' };
  try { if (acc.sock) acc.sock.end(); } catch {}
  fs.rmSync(path.join(BASE_SESSION_DIR, id), { recursive: true, force: true });
  accounts.delete(id);
  return { ok: true };
}

function handleQR(accountId) {
  const acc = accounts.get(accountId);
  if (!acc) return { status: 'not_found' };
  if (acc.status === 'ready') return { status: 'ready', name: acc.name, phone: acc.phone };
  if (acc.qrDataUrl) return { status: 'qr', qrDataUrl: acc.qrDataUrl };
  return { status: acc.status };
}

function handleGroups() {
  const all = [];
  const seen = new Set();
  for (const [, acc] of accounts) {
    if (acc.status !== 'ready') continue;
    for (const g of acc.groups) {
      if (!seen.has(g.id)) { seen.add(g.id); all.push(g); }
    }
  }
  return { groups: all };
}

async function handleBroadcast(body) {
  const { chatIds, message } = body;
  if (!chatIds || !Array.isArray(chatIds) || !message) return { sent: 0, failed: 0, error: 'Missing chatIds or message' };
  if (chatIds.length > 200) return { sent: 0, failed: 0, error: 'Max 200 recipients per broadcast' };

  // Build chatId -> socket lookup
  const lookup = new Map();
  for (const [, acc] of accounts) {
    if (acc.status === 'ready' && acc.sock) {
      for (const g of acc.groups) lookup.set(g.id, acc.sock);
    }
  }

  let sent = 0, failed = 0;
  const results = [];
  for (const chatId of chatIds) {
    const sock = lookup.get(chatId);
    if (!sock) { failed++; results.push({ chatId, ok: false, error: 'No account' }); continue; }
    try {
      await sock.sendMessage(chatId, { text: message });
      sent++;
      results.push({ chatId, ok: true });
    } catch (e) { failed++; results.push({ chatId, ok: false, error: e.message }); }
    await new Promise(r => setTimeout(r, 300));
  }
  return { sent, failed, total: chatIds.length, results };
}

// ── HTML UI — WhatsApp Native v2 + Multi-Account ────────────────────────────
const HTML = `<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <meta name="theme-color" content="#1f2c34" />
  <title>Broadcast Pro</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Segoe UI', Helvetica, Arial, sans-serif; background: #0b141a; color: #e9edef; min-height: 100vh; min-height: 100dvh; }
    .app { max-width: 500px; margin: 0 auto; display: flex; flex-direction: column; min-height: 100vh; min-height: 100dvh; }

    .topbar { background: #1f2c34; padding: 10px 16px; display: flex; align-items: center; gap: 12px; position: sticky; top: 0; z-index: 10; }
    .topbar-avatar { width: 40px; height: 40px; background: #00a884; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 20px; flex-shrink: 0; }
    .topbar-info { flex: 1; }
    .topbar-title { font-size: 17px; font-weight: 500; color: #e9edef; }
    .topbar-sub { font-size: 12px; color: #8696a0; margin-top: 1px; }
    .topbar-actions { display: flex; gap: 4px; }
    .topbar-btn { background: none; border: none; color: #aebac1; font-size: 13px; padding: 8px; border-radius: 50%; cursor: pointer; transition: background 0.15s; }
    .topbar-btn:hover { background: #ffffff10; }

    /* Account chips bar */
    .accounts-bar { display: flex; gap: 6px; padding: 8px 16px; background: #111b21; overflow-x: auto; }
    .accounts-bar::-webkit-scrollbar { display: none; }
    .acc-chip { display: flex; align-items: center; gap: 6px; padding: 6px 10px; background: #2a3942; border-radius: 20px; font-size: 12px; cursor: pointer; flex-shrink: 0; transition: all 0.15s; color: #e9edef; border: none; }
    .acc-chip:hover { background: #374045; }
    .acc-chip .cd { width: 7px; height: 7px; border-radius: 50%; background: #8696a0; flex-shrink: 0; }
    .acc-chip .cd.on { background: #00a884; }
    .acc-chip .cd.qr { background: #f0b429; }
    .acc-chip .cx { color: #8696a0; font-size: 10px; margin-left: 2px; padding: 2px; line-height: 1; }
    .acc-chip .cx:hover { color: #f15c6d; }
    .add-chip { background: #005c4b; font-weight: 500; }
    .add-chip:hover { background: #06cf9c; }

    .chat-area { flex: 1; padding: 12px 12px 8px; background: #0b141a; background-image: url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.02'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E"); overflow-y: auto; }

    .bubble { background: #1f2c34; border-radius: 8px; padding: 14px; margin-bottom: 8px; position: relative; }
    .bubble::before { content: ''; position: absolute; top: 0; left: -6px; width: 0; height: 0; border-top: 6px solid #1f2c34; border-left: 6px solid transparent; }
    .bubble-label { font-size: 12px; font-weight: 600; color: #00a884; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.04em; }

    textarea { width: 100%; background: #2a3942; border: none; border-radius: 8px; padding: 10px 12px; color: #e9edef; font-size: 15px; resize: none; min-height: 80px; font-family: inherit; outline: none; line-height: 1.4; }
    textarea::placeholder { color: #8696a0; }
    .char-count { font-size: 11px; color: #8696a0; margin-top: 4px; text-align: right; }

    .groups-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
    .groups-list { max-height: 260px; overflow-y: auto; }
    .groups-list::-webkit-scrollbar { width: 3px; }
    .groups-list::-webkit-scrollbar-thumb { background: #374045; border-radius: 3px; }
    .group-item { display: flex; align-items: center; gap: 10px; padding: 10px 8px; border-bottom: 1px solid #222e35; cursor: pointer; transition: background 0.1s; }
    .group-item:last-child { border-bottom: none; }
    .group-item:hover { background: #222e35; }
    .group-item.selected { background: #0c332c; }
    .group-item input[type=checkbox] { accent-color: #00a884; width: 18px; height: 18px; cursor: pointer; flex-shrink: 0; }
    .group-avatar { width: 40px; height: 40px; background: #2a3942; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 16px; flex-shrink: 0; }
    .group-info { flex: 1; min-width: 0; }
    .group-name { font-size: 15px; color: #e9edef; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .group-count { font-size: 12px; color: #8696a0; margin-top: 1px; }
    .group-acct { font-size: 10px; color: #00a884; }
    .empty { text-align: center; padding: 24px; color: #8696a0; font-size: 14px; }

    .wa-link { color: #00a884; background: none; border: none; font-size: 13px; cursor: pointer; font-weight: 500; padding: 0; }
    .wa-link:hover { text-decoration: underline; }
    .wa-icon-btn { background: #2a3942; border: none; color: #8696a0; font-size: 12px; padding: 6px 10px; border-radius: 20px; cursor: pointer; transition: all 0.15s; }
    .wa-icon-btn:hover { background: #374045; color: #e9edef; }

    .bottom-bar { background: #1f2c34; padding: 8px 12px; display: flex; align-items: center; gap: 8px; position: sticky; bottom: 0; }
    .send-btn { width: 48px; height: 48px; background: #00a884; border: none; border-radius: 50%; color: #fff; font-size: 20px; cursor: pointer; display: flex; align-items: center; justify-content: center; transition: background 0.15s; flex-shrink: 0; }
    .send-btn:hover { background: #06cf9c; }
    .send-btn:disabled { background: #2a3942; color: #8696a0; cursor: not-allowed; }
    .send-info { flex: 1; font-size: 13px; color: #8696a0; }
    .send-info strong { color: #e9edef; }

    .toast { padding: 10px 14px; border-radius: 8px; font-size: 14px; margin: 8px 0; }
    .toast.success { background: #005c4b; color: #e9edef; }
    .toast.error { background: #3b1c1e; color: #f15c6d; }

    .progress { margin: 8px 0; }
    .progress-bar-wrap { background: #2a3942; border-radius: 4px; height: 4px; overflow: hidden; margin-top: 4px; }
    .progress-bar { height: 100%; background: #00a884; border-radius: 4px; transition: width 0.3s; }
    .progress-text { font-size: 12px; color: #8696a0; }

    .dot { width: 8px; height: 8px; border-radius: 50%; background: #8696a0; display: inline-block; }
    .dot.online { background: #00a884; }

    .spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid #ffffff30; border-top-color: #fff; border-radius: 50%; animation: spin 0.7s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }

    .modal-overlay { position: fixed; inset: 0; background: #000000cc; display: flex; align-items: center; justify-content: center; z-index: 100; }
    .modal { background: #1f2c34; border-radius: 12px; padding: 24px; max-width: 320px; width: 90%; text-align: center; }
    .modal h3 { font-size: 17px; font-weight: 500; color: #e9edef; margin-bottom: 6px; }
    .modal p { font-size: 13px; color: #8696a0; margin-bottom: 16px; line-height: 1.4; }
    .qr-wrap { background: #fff; border-radius: 8px; padding: 12px; display: inline-block; margin-bottom: 16px; }
    .qr-wrap img { display: block; width: 200px; height: 200px; }
    .qr-status { font-size: 13px; color: #8696a0; margin-bottom: 12px; min-height: 18px; }
    .modal-close { background: #2a3942; color: #8696a0; border: none; border-radius: 20px; padding: 8px 20px; cursor: pointer; font-size: 14px; }
    .modal-close:hover { background: #374045; color: #e9edef; }

    .mod-badge { display: inline-block; background: #00a884; color: #fff; font-size: 9px; font-weight: 700; padding: 2px 5px; border-radius: 3px; letter-spacing: 0.5px; vertical-align: middle; margin-left: 6px; }
  </style>
</head>
<body>
  <div class="app">
    <div class="topbar">
      <div class="topbar-avatar">&#x1F4E2;</div>
      <div class="topbar-info">
        <div class="topbar-title" id="subtitle">Broadcast<span class="mod-badge">PRO</span></div>
        <div class="topbar-sub"><span class="dot" id="statusDot"></span> <span id="statusText">connecting...</span></div>
      </div>
      <div class="topbar-actions">
        <button class="topbar-btn" id="langBtn" onclick="toggleLang()" title="Language">&#x1F310;</button>
      </div>
    </div>

    <div class="accounts-bar" id="accountsBar">
      <button class="acc-chip add-chip" onclick="addAccount()" id="addBtn">+ Add</button>
    </div>

    <div class="modal-overlay" id="qrModal" style="display:none">
      <div class="modal">
        <h3 id="qrTitle">Link Device</h3>
        <p id="qrInstructions">Open WhatsApp > Linked Devices > Link a Device</p>
        <div class="qr-wrap" id="qrWrap" style="display:none"><img id="qrImg" src="" alt="QR" /></div>
        <div class="qr-status" id="qrStatusMsg">Loading...</div>
        <button class="modal-close" onclick="closeQR()" id="qrCloseBtn">Close</button>
      </div>
    </div>

    <div class="chat-area">
      <div class="bubble">
        <div class="bubble-label" id="msgLabel">Compose message</div>
        <textarea id="msgInput" oninput="updateCount()" rows="3"></textarea>
        <div class="char-count"><span id="charCount">0</span> <span id="charLabel">chars</span></div>
      </div>

      <div class="bubble">
        <div class="groups-header">
          <div class="bubble-label" style="margin:0" id="groupsLabel">Groups (0)</div>
          <div style="display:flex;gap:6px">
            <button class="wa-link" onclick="selectAll()" id="selectAllBtn">Select all</button>
            <button class="wa-icon-btn" onclick="loadGroups()" id="refreshBtn">&#x21BB;</button>
          </div>
        </div>
        <div class="groups-list" id="groupsList">
          <div class="empty">Loading...</div>
        </div>
      </div>

      <div id="progressWrap" class="progress" style="display:none">
        <div class="progress-text" id="progressText">Sending...</div>
        <div class="progress-bar-wrap"><div class="progress-bar" id="progressBar" style="width:0%"></div></div>
      </div>

      <div id="result" style="display:none" class="toast"></div>
    </div>

    <div class="bottom-bar">
      <div class="send-info" id="sendLabel">Select groups & write a message</div>
      <button class="send-btn" id="sendBtn" onclick="send()" disabled>&#x27A4;</button>
    </div>
  </div>

  <script>
    let groups = [];
    let selected = new Set();
    let accountsList = [];
    let activeQrAccount = null;
    let lang = localStorage.getItem('wa-lang') || 'en';

    const i18n = {
      en: {
        statusOnline: function(n, t) { return n + '/' + t + ' online'; },
        statusNone: 'no accounts',
        statusServerDown: 'server down',
        qrTitle: 'Link Device',
        qrInstructions: 'Open WhatsApp > Linked Devices > Link a Device',
        qrLoading: 'Loading...',
        qrConnected: 'Connected! Close this.',
        qrScan: 'Scan with WhatsApp',
        qrWaiting: 'Waiting...',
        qrClose: 'Close',
        msgLabel: 'Compose message',
        placeholder: 'Type your broadcast message...',
        charLabel: 'chars',
        groupsLabel: function(n) { return 'Groups (' + n + ')'; },
        selectAll: 'Select all',
        groupsLoading: 'Loading...',
        groupsEmpty: 'No groups',
        groupsError: 'Failed to load',
        members: 'members',
        sending: 'Sending...',
        sendingProgress: function(done, total) { return 'Sending ' + done + '/' + total + '...'; },
        done: 'Done!',
        successMsg: function(n) { return 'Sent to ' + n + ' groups'; },
        partialMsg: function(sent, failed) { return 'Sent: ' + sent + ' | Failed: ' + failed; },
        errorMsg: function(msg) { return 'Error: ' + msg; },
        sendInfo: 'Select groups & write a message',
        sendReady: function(n) { return '<strong>' + n + '</strong> groups ready'; },
        addAccount: '+ Add',
      },
      he: {
        statusOnline: function(n, t) { return n + '/' + t + ' ' + String.fromCharCode(1502, 1495, 1493, 1489, 1512, 1497, 1501); },
        statusNone: String.fromCharCode(1488, 1497, 1503, 32, 1495, 1513, 1489, 1493, 1504, 1493, 1514),
        statusServerDown: String.fromCharCode(1513, 1512, 1514, 32, 1500, 1488, 32, 1494, 1502, 1497, 1503),
        qrTitle: String.fromCharCode(1511, 1497, 1513, 1493, 1512, 32, 1502, 1499, 1513, 1497, 1512),
        qrInstructions: String.fromCharCode(1508, 1514, 1495, 32) + 'WhatsApp > ' + String.fromCharCode(1492, 1514, 1511, 1504, 1497, 1501, 32, 1502, 1511, 1493, 1513, 1512, 1497, 1501) + ' > ' + String.fromCharCode(1511, 1513, 1512, 32, 1492, 1514, 1511, 1503),
        qrLoading: String.fromCharCode(1496, 1493, 1506, 1503) + '...',
        qrConnected: String.fromCharCode(1502, 1495, 1493, 1489, 1512, 33),
        qrScan: String.fromCharCode(1505, 1512, 1493, 1511, 32, 1506, 1501, 32) + 'WhatsApp',
        qrWaiting: String.fromCharCode(1502, 1502, 1514, 1497, 1503) + '...',
        qrClose: String.fromCharCode(1505, 1490, 1493, 1512),
        msgLabel: String.fromCharCode(1499, 1514, 1493, 1489, 32, 1492, 1493, 1491, 1506, 1492),
        placeholder: String.fromCharCode(1499, 1514, 1489, 1493, 32, 1488, 1514, 32, 1492, 1492, 1493, 1491, 1506, 1492) + '...',
        charLabel: String.fromCharCode(1514, 1493, 1493, 1497, 1501),
        groupsLabel: function(n) { return String.fromCharCode(1511, 1489, 1493, 1510, 1493, 1514) + ' (' + n + ')'; },
        selectAll: String.fromCharCode(1489, 1495, 1512, 32, 1492, 1499, 1500),
        groupsLoading: String.fromCharCode(1496, 1493, 1506, 1503) + '...',
        groupsEmpty: String.fromCharCode(1488, 1497, 1503, 32, 1511, 1489, 1493, 1510, 1493, 1514),
        groupsError: String.fromCharCode(1513, 1490, 1497, 1488, 1492),
        members: String.fromCharCode(1495, 1489, 1512, 1497, 1501),
        sending: String.fromCharCode(1513, 1493, 1500, 1495) + '...',
        sendingProgress: function(done, total) { return String.fromCharCode(1513, 1493, 1500, 1495) + ' ' + done + '/' + total + '...'; },
        done: String.fromCharCode(1492, 1493, 1513, 1500, 1501) + '!',
        successMsg: function(n) { return String.fromCharCode(1504, 1513, 1500, 1495, 32, 1500) + '-' + n + ' ' + String.fromCharCode(1511, 1489, 1493, 1510, 1493, 1514); },
        partialMsg: function(sent, failed) { return String.fromCharCode(1504, 1513, 1500, 1495) + ': ' + sent + ' | ' + String.fromCharCode(1504, 1499, 1513, 1500) + ': ' + failed; },
        errorMsg: function(msg) { return String.fromCharCode(1513, 1490, 1497, 1488, 1492) + ': ' + msg; },
        sendInfo: String.fromCharCode(1489, 1495, 1512, 32, 1511, 1489, 1493, 1510, 1493, 1514, 32, 1493, 1499, 1514, 1489, 32, 1492, 1493, 1491, 1506, 1492),
        sendReady: function(n) { return '<strong>' + n + '</strong> ' + String.fromCharCode(1511, 1489, 1493, 1510, 1493, 1514, 32, 1502, 1493, 1499, 1504, 1493, 1514); },
        addAccount: '+ ' + String.fromCharCode(1492, 1493, 1505, 1507),
      }
    };

    function esc(s) { var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

    function t(key) {
      var val = i18n[lang][key];
      if (typeof val === 'function') return val.apply(null, Array.prototype.slice.call(arguments, 1));
      return val;
    }

    function applyLang() {
      var isRTL = lang === 'he';
      document.documentElement.lang = lang;
      document.documentElement.dir = isRTL ? 'rtl' : 'ltr';
      document.getElementById('qrTitle').textContent = t('qrTitle');
      document.getElementById('qrInstructions').textContent = t('qrInstructions');
      document.getElementById('qrCloseBtn').textContent = t('qrClose');
      document.getElementById('msgLabel').textContent = t('msgLabel');
      document.getElementById('msgInput').placeholder = t('placeholder');
      document.getElementById('charLabel').textContent = t('charLabel');
      document.getElementById('groupsLabel').innerHTML = t('groupsLabel', selected.size);
      document.getElementById('selectAllBtn').textContent = t('selectAll');
      updateSendBtn();
      renderAccounts();
      renderGroups();
    }

    function toggleLang() {
      lang = lang === 'en' ? 'he' : 'en';
      localStorage.setItem('wa-lang', lang);
      applyLang();
    }

    // ── Accounts ──
    function renderAccounts() {
      var bar = document.getElementById('accountsBar');
      var chips = accountsList.map(function(a) {
        var dotCls = a.status === 'ready' ? 'on' : (a.status === 'qr' ? 'qr' : '');
        var label = a.phone ? (esc(a.name) + ' (' + esc(a.phone.slice(-4)) + ')') : esc(a.name);
        return '<button class="acc-chip" onclick="connectAccount(\\'' + a.id + '\\')">'
          + '<span class="cd ' + dotCls + '"></span>'
          + '<span>' + label + '</span>'
          + '<span class="cx" onclick="event.stopPropagation();removeAccount(\\'' + a.id + '\\')">&times;</span>'
          + '</button>';
      }).join('');
      bar.innerHTML = chips + '<button class="acc-chip add-chip" onclick="addAccount()" id="addBtn">' + t('addAccount') + '</button>';
    }

    async function addAccount() {
      try {
        var r = await fetch('/api/accounts', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
        var d = await r.json();
        if (d.id) {
          await refreshAccounts();
          openQR(d.id);
        }
      } catch (e) { console.error('Add account error:', e); }
    }

    async function removeAccount(id) {
      var a = accountsList.find(function(x) { return x.id === id; });
      var label = a ? a.name : id;
      if (!confirm('Remove ' + label + '? This disconnects the WhatsApp session.')) return;
      try {
        await fetch('/api/accounts?id=' + encodeURIComponent(id), { method: 'DELETE' });
        await refreshAccounts();
        loadGroups();
      } catch (e) { console.error('Remove error:', e); }
    }

    function connectAccount(id) {
      var a = accountsList.find(function(x) { return x.id === id; });
      if (a && a.status !== 'ready') openQR(id);
    }

    async function refreshAccounts() {
      try {
        var r = await fetch('/api/accounts');
        var d = await r.json();
        accountsList = d.accounts || [];
        renderAccounts();
        updateTopStatus();
      } catch (e) {}
    }

    function updateTopStatus() {
      var online = accountsList.filter(function(a) { return a.status === 'ready'; }).length;
      var dot = document.getElementById('statusDot');
      var txt = document.getElementById('statusText');
      if (accountsList.length === 0) {
        dot.className = 'dot';
        txt.textContent = t('statusNone');
      } else {
        dot.className = online > 0 ? 'dot online' : 'dot';
        txt.textContent = t('statusOnline', online, accountsList.length);
      }
    }

    // ── Groups ──
    async function loadGroups() {
      document.getElementById('groupsList').innerHTML = '<div class="empty">' + t('groupsLoading') + '</div>';
      try {
        var r = await fetch('/api/groups');
        var d = await r.json();
        groups = d.groups || [];
        renderGroups();
      } catch (e) {
        document.getElementById('groupsList').innerHTML = '<div class="empty">' + t('groupsError') + '</div>';
      }
    }

    function renderGroups() {
      var el = document.getElementById('groupsList');
      // Clean stale selections
      var validIds = new Set(groups.map(function(g) { return g.id; }));
      selected.forEach(function(id) { if (!validIds.has(id)) selected.delete(id); });
      if (!groups.length) { el.innerHTML = '<div class="empty">' + t('groupsEmpty') + '</div>'; return; }
      el.innerHTML = groups.map(function(g, i) {
        var acctTag = accountsList.length > 1 ? '<span class="group-acct">' + esc(g.accountName || '') + '</span>' : '';
        return '<div class="group-item ' + (selected.has(g.id) ? 'selected' : '') + '" onclick="toggle(\\'' + g.id + '\\')">'
          + '<input type="checkbox" ' + (selected.has(g.id) ? 'checked' : '') + ' onclick="event.stopPropagation();toggle(\\'' + g.id + '\\')">'
          + '<div class="group-avatar">' + ['&#x1F389;','&#x1F38A;','&#x1F973;','&#x1F3B6;','&#x1FA69;'][i % 5] + '</div>'
          + '<div class="group-info"><div class="group-name">' + esc(g.name) + '</div>'
          + '<div class="group-count">' + (g.participantCount || '?') + ' ' + t('members') + ' ' + acctTag + '</div></div>'
          + '</div>';
      }).join('');
      updateSendBtn();
    }

    function toggle(id) {
      if (selected.has(id)) selected.delete(id); else selected.add(id);
      renderGroups();
      document.getElementById('groupsLabel').innerHTML = t('groupsLabel', selected.size);
    }

    function selectAll() {
      if (selected.size === groups.length) { selected.clear(); } else { groups.forEach(function(g) { selected.add(g.id); }); }
      renderGroups();
      document.getElementById('groupsLabel').innerHTML = t('groupsLabel', selected.size);
    }

    function updateCount() {
      document.getElementById('charCount').textContent = document.getElementById('msgInput').value.length;
      updateSendBtn();
    }

    function updateSendBtn() {
      var msg = document.getElementById('msgInput').value.trim();
      var ready = msg && selected.size > 0;
      document.getElementById('sendBtn').disabled = !ready;
      document.getElementById('sendLabel').innerHTML = ready ? t('sendReady', selected.size) : t('sendInfo');
    }

    // ── Send ──
    async function send() {
      var msg = document.getElementById('msgInput').value.trim();
      if (!msg || selected.size === 0) return;
      var ids = Array.from(selected);

      document.getElementById('sendBtn').disabled = true;
      document.getElementById('sendLabel').innerHTML = '<span class="spinner"></span> ' + t('sending');
      document.getElementById('result').style.display = 'none';
      document.getElementById('progressWrap').style.display = 'block';
      document.getElementById('progressBar').style.width = '0%';
      document.getElementById('progressText').textContent = t('sendingProgress', 0, ids.length);

      try {
        var r = await fetch('/api/broadcast', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chatIds: ids, message: msg })
        });
        var d = await r.json();
        document.getElementById('progressBar').style.width = '100%';
        document.getElementById('progressText').textContent = t('done');

        var res = document.getElementById('result');
        res.style.display = 'block';
        if (d.failed === 0) {
          res.className = 'toast success';
          res.textContent = t('successMsg', d.sent);
        } else {
          res.className = 'toast error';
          res.textContent = t('partialMsg', d.sent, d.failed);
        }
      } catch (e) {
        var res2 = document.getElementById('result');
        res2.style.display = 'block';
        res2.className = 'toast error';
        res2.textContent = t('errorMsg', e.message);
      }

      document.getElementById('progressWrap').style.display = 'none';
      document.getElementById('sendBtn').disabled = false;
      document.getElementById('sendLabel').textContent = t('sendInfo');
    }

    // ── QR Modal ──
    var qrPollTimer = null;

    function openQR(accountId) {
      activeQrAccount = accountId;
      document.getElementById('qrModal').style.display = 'flex';
      document.getElementById('qrWrap').style.display = 'none';
      document.getElementById('qrStatusMsg').textContent = t('qrLoading');
      pollQR();
      qrPollTimer = setInterval(pollQR, 3000);
    }

    function closeQR() {
      document.getElementById('qrModal').style.display = 'none';
      if (qrPollTimer) { clearInterval(qrPollTimer); qrPollTimer = null; }
      activeQrAccount = null;
    }

    async function pollQR() {
      if (!activeQrAccount) return;
      try {
        var r = await fetch('/api/qr?account=' + encodeURIComponent(activeQrAccount));
        var d = await r.json();
        if (d.status === 'ready') {
          document.getElementById('qrWrap').style.display = 'none';
          document.getElementById('qrStatusMsg').textContent = t('qrConnected');
          if (qrPollTimer) { clearInterval(qrPollTimer); qrPollTimer = null; }
          refreshAccounts();
          loadGroups();
        } else if (d.qrDataUrl) {
          document.getElementById('qrImg').src = d.qrDataUrl;
          document.getElementById('qrWrap').style.display = 'inline-block';
          document.getElementById('qrStatusMsg').textContent = t('qrScan');
        } else {
          document.getElementById('qrStatusMsg').textContent = d.status || t('qrWaiting');
        }
      } catch (e) {
        document.getElementById('qrStatusMsg').textContent = t('errorMsg', e.message);
      }
    }

    // Init
    applyLang();
    refreshAccounts();
    loadGroups();
    setInterval(function() { refreshAccounts(); }, 15000);
  </script>
</body>
</html>`;

// ── HTTP Server ─────────────────────────────────────────────────────────────
function readBody(req, limit = 1048576) {
  return new Promise((resolve, reject) => {
    let d = '';
    req.on('data', (c) => {
      d += c;
      if (d.length > limit) { req.destroy(); reject(new Error('Body too large')); }
    });
    req.on('end', () => resolve(d));
  });
}

function json(res, data, code = 200) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

const server = http.createServer(async (req, res) => {
  const parsed = new URL(req.url || '/', 'http://localhost');
  const p = parsed.pathname;
  const params = parsed.searchParams;

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if (p === '/' || p === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(HTML); return;
  }

  if (p === '/api/accounts' && req.method === 'GET') {
    json(res, handleAccounts()); return;
  }

  if (p === '/api/accounts' && req.method === 'POST') {
    try {
      const body = await readBody(req);
      const d = await handleCreateAccount(body ? JSON.parse(body) : {});
      json(res, d);
    } catch (e) { json(res, { error: e.message }, 500); }
    return;
  }

  if (p === '/api/accounts' && req.method === 'DELETE') {
    const id = params.get('id');
    if (!id) { json(res, { error: 'Missing id' }, 400); return; }
    json(res, handleDeleteAccount(id)); return;
  }

  if (p === '/api/status') {
    const online = [...accounts.values()].filter(a => a.status === 'ready').length;
    json(res, { status: online > 0 ? 'ready' : 'disconnected', accounts: accounts.size, online }); return;
  }

  if (p === '/api/qr') {
    const id = params.get('account');
    if (!id) { json(res, { status: 'missing account param' }, 400); return; }
    json(res, handleQR(id)); return;
  }

  if (p === '/api/groups') {
    json(res, handleGroups()); return;
  }

  if (p === '/api/broadcast' && req.method === 'POST') {
    try {
      const body = await readBody(req);
      const d = await handleBroadcast(JSON.parse(body));
      json(res, d);
    } catch (e) { json(res, { error: e.message }, 500); }
    return;
  }

  res.writeHead(404); res.end('Not found');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('WhatsApp Broadcast Pro running on port ' + PORT);
  console.log('Accounts loaded: ' + accounts.size);
});