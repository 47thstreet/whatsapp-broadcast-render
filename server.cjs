#!/usr/bin/env node
'use strict';

const http = require('http');
const QRCode = require('qrcode');
const fs = require('fs');
const path = require('path');
const pino = require('pino');

const Database = require('better-sqlite3');

const PORT = process.env.PORT || 3050;
const BASE_SESSION_DIR = path.resolve('./wa-session');
const DATA_DIR = path.resolve('./data');
const logger = pino({ level: 'silent' });

// ── SQLite Database ─────────────────────────────────────────────────────────
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
const db = new Database(path.join(DATA_DIR, 'contacts.db'));
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS contacts (
    jid TEXT PRIMARY KEY,
    phone TEXT NOT NULL,
    name TEXT DEFAULT '',
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    message_count INTEGER DEFAULT 1,
    interests TEXT DEFAULT '[]',
    notes TEXT DEFAULT ''
  );
  CREATE TABLE IF NOT EXISTS contact_groups (
    jid TEXT NOT NULL,
    group_id TEXT NOT NULL,
    group_name TEXT DEFAULT '',
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    message_count INTEGER DEFAULT 1,
    PRIMARY KEY (jid, group_id),
    FOREIGN KEY (jid) REFERENCES contacts(jid) ON DELETE CASCADE
  );
  CREATE TABLE IF NOT EXISTS contact_tags (
    jid TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (jid, tag),
    FOREIGN KEY (jid) REFERENCES contacts(jid) ON DELETE CASCADE
  );
  CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone);
  CREATE INDEX IF NOT EXISTS idx_contacts_last_seen ON contacts(last_seen);
  CREATE INDEX IF NOT EXISTS idx_contact_groups_group ON contact_groups(group_id);
`);

// Prepared statements for performance
const stmts = {
  upsertContact: db.prepare(`
    INSERT INTO contacts (jid, phone, name, first_seen, last_seen, message_count, interests)
    VALUES (@jid, @phone, @name, @now, @now, 1, '[]')
    ON CONFLICT(jid) DO UPDATE SET
      name = CASE WHEN @name != '' THEN @name ELSE contacts.name END,
      last_seen = @now,
      message_count = contacts.message_count + 1
  `),
  upsertContactGroup: db.prepare(`
    INSERT INTO contact_groups (jid, group_id, group_name, first_seen, last_seen, message_count)
    VALUES (@jid, @groupId, @groupName, @now, @now, 1)
    ON CONFLICT(jid, group_id) DO UPDATE SET
      group_name = @groupName,
      last_seen = @now,
      message_count = contact_groups.message_count + 1
  `),
  addTag: db.prepare(`INSERT OR IGNORE INTO contact_tags (jid, tag) VALUES (@jid, @tag)`),
  removeTag: db.prepare(`DELETE FROM contact_tags WHERE jid = @jid AND tag = @tag`),
  getContact: db.prepare(`SELECT * FROM contacts WHERE jid = ?`),
  getContactGroups: db.prepare(`SELECT * FROM contact_groups WHERE jid = ? ORDER BY last_seen DESC`),
  getContactTags: db.prepare(`SELECT tag FROM contact_tags WHERE jid = ?`),
  updateInterests: db.prepare(`UPDATE contacts SET interests = @interests WHERE jid = @jid`),
  updateNotes: db.prepare(`UPDATE contacts SET notes = @notes WHERE jid = @jid`),
  deleteContact: db.prepare(`DELETE FROM contacts WHERE jid = ?`),
  totalContacts: db.prepare(`SELECT COUNT(*) as count FROM contacts`),
  contactsByGroup: db.prepare(`
    SELECT c.*, GROUP_CONCAT(ct.tag) as tags
    FROM contacts c
    LEFT JOIN contact_tags ct ON c.jid = ct.jid
    INNER JOIN contact_groups cg ON c.jid = cg.jid
    WHERE cg.group_id = ?
    GROUP BY c.jid
    ORDER BY c.last_seen DESC
  `),
  allContacts: db.prepare(`
    SELECT c.*, GROUP_CONCAT(DISTINCT ct.tag) as tags
    FROM contacts c
    LEFT JOIN contact_tags ct ON c.jid = ct.jid
    GROUP BY c.jid
    ORDER BY c.last_seen DESC
    LIMIT ? OFFSET ?
  `),
  searchContacts: db.prepare(`
    SELECT c.*, GROUP_CONCAT(DISTINCT ct.tag) as tags
    FROM contacts c
    LEFT JOIN contact_tags ct ON c.jid = ct.jid
    WHERE c.name LIKE @q OR c.phone LIKE @q
    GROUP BY c.jid
    ORDER BY c.last_seen DESC
    LIMIT 100
  `),
  topGroups: db.prepare(`
    SELECT group_id, group_name, COUNT(*) as contact_count, SUM(message_count) as total_messages
    FROM contact_groups
    GROUP BY group_id
    ORDER BY contact_count DESC
    LIMIT 20
  `),
  recentContacts: db.prepare(`
    SELECT c.*, GROUP_CONCAT(DISTINCT ct.tag) as tags
    FROM contacts c
    LEFT JOIN contact_tags ct ON c.jid = ct.jid
    GROUP BY c.jid
    ORDER BY c.last_seen DESC
    LIMIT 20
  `),
  contactStats: db.prepare(`
    SELECT
      COUNT(*) as total,
      COUNT(CASE WHEN last_seen >= date('now') THEN 1 END) as today,
      COUNT(CASE WHEN last_seen >= date('now', '-7 days') THEN 1 END) as week
    FROM contacts
  `),
};

// ── Contact Scraper ─────────────────────────────────────────────────────────
function scrapeContact(senderJid, senderName, groupId, groupName, matchedKeywords) {
  const phone = senderJid.split('@')[0].split(':')[0];
  const now = new Date().toISOString();

  const txn = db.transaction(() => {
    stmts.upsertContact.run({ jid: senderJid, phone, name: senderName || '', now });
    stmts.upsertContactGroup.run({ jid: senderJid, groupId, groupName, now });

    // Auto-tag with group name
    stmts.addTag.run({ jid: senderJid, tag: 'group:' + groupName });

    // Add interest tags from matched keywords
    if (matchedKeywords && matchedKeywords.length > 0) {
      stmts.addTag.run({ jid: senderJid, tag: 'lead' });
      const existing = stmts.getContact.get(senderJid);
      if (existing) {
        const interests = JSON.parse(existing.interests || '[]');
        for (const kw of matchedKeywords) {
          if (!interests.includes(kw)) interests.push(kw);
        }
        stmts.updateInterests.run({ jid: senderJid, interests: JSON.stringify(interests) });
      }
    }
  });
  txn();
}

// ── Multi-Account WhatsApp (Baileys) ────────────────────────────────────────
const accounts = new Map();
let idCounter = 0;

function makeId() {
  return 'wa' + (++idCounter) + '-' + Date.now().toString(36);
}

// ── Lead Scanner ────────────────────────────────────────────────────────────
const MAX_LEADS = 500;
const leads = []; // { id, groupId, groupName, senderJid, senderName, message, matchedKeywords, accountId, timestamp }
let leadIdCounter = 0;

let keywords = {
  en: ['party', 'event', 'club', 'dj', 'bottle service', 'vip', 'guestlist', 'guest list', 'tickets', 'nightlife', 'tonight', 'this weekend', 'where to go', 'going out', 'celebration', 'birthday party', 'new years', 'nye', 'rooftop', 'venue', 'afterparty', 'after party', 'rave', 'festival', 'who is coming', 'whos coming', 'any events', 'any parties', 'looking for a party', 'whats happening'],
  he: ['\u05DE\u05E1\u05D9\u05D1\u05D4', '\u05D0\u05D9\u05E8\u05D5\u05E2', '\u05DE\u05D5\u05E2\u05D3\u05D5\u05DF', '\u05D3\u05D9\u05D2\u05F3\u05D9\u05D9', '\u05E9\u05D5\u05DC\u05D7\u05DF vip', '\u05E8\u05E9\u05D9\u05DE\u05EA \u05D0\u05D5\u05E8\u05D7\u05D9\u05DD', '\u05DB\u05E8\u05D8\u05D9\u05E1\u05D9\u05DD', '\u05D4\u05D9\u05D5\u05DD \u05D1\u05DC\u05D9\u05DC\u05D4', '\u05D4\u05DC\u05D9\u05DC\u05D4', '\u05E1\u05D5\u05E3 \u05E9\u05D1\u05D5\u05E2', '\u05DC\u05E6\u05D0\u05EA', '\u05D7\u05D2\u05D9\u05D2\u05D4', '\u05D9\u05D5\u05DD \u05D4\u05D5\u05DC\u05D3\u05EA', '\u05D2\u05D2', '\u05DE\u05E7\u05D5\u05DD', '\u05D0\u05E4\u05D8\u05E8', '\u05E8\u05D9\u05D9\u05D1', '\u05E4\u05E1\u05D8\u05D9\u05D1\u05DC', '\u05D0\u05D9\u05E4\u05D4 \u05D9\u05D5\u05E6\u05D0\u05D9\u05DD', '\u05DE\u05D9 \u05D1\u05D0', '\u05D9\u05E9 \u05DE\u05E1\u05D9\u05D1\u05D4', '\u05D9\u05E9 \u05D0\u05D9\u05E8\u05D5\u05E2', '\u05DE\u05D4 \u05E7\u05D5\u05E8\u05D4', '\u05DE\u05D4 \u05D9\u05E9 \u05D4\u05DC\u05D9\u05DC\u05D4']
};

let scannerEnabled = true;

function scanMessage(text, groupId, groupName, senderJid, senderName, accountId) {
  if (!scannerEnabled || !text) return null;
  const lower = text.toLowerCase();
  const matched = [];

  for (const kw of keywords.en) {
    if (lower.includes(kw.toLowerCase())) matched.push(kw);
  }
  for (const kw of keywords.he) {
    if (text.includes(kw)) matched.push(kw);
  }

  if (matched.length === 0) return null;

  const lead = {
    id: 'lead-' + (++leadIdCounter),
    groupId,
    groupName: groupName || groupId,
    senderJid,
    senderName: senderName || senderJid.split('@')[0],
    message: text.slice(0, 500),
    matchedKeywords: matched,
    accountId,
    timestamp: new Date().toISOString(),
    dismissed: false,
  };

  leads.unshift(lead);
  if (leads.length > MAX_LEADS) leads.length = MAX_LEADS;
  console.log(`[LEAD] "${matched.join(', ')}" from ${lead.senderName} in ${lead.groupName}`);
  return matched;
}

// ── Init Account ────────────────────────────────────────────────────────────
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

  // ── Message listener for lead scanning ──
  sock.ev.on('messages.upsert', async ({ messages }) => {
    for (const msg of messages) {
      if (!msg.message || msg.key.fromMe) continue;
      const chatId = msg.key.remoteJid;
      if (!chatId || !chatId.endsWith('@g.us')) continue; // groups only

      const text = msg.message.conversation
        || msg.message.extendedTextMessage?.text
        || '';
      if (!text) continue;

      const senderJid = msg.key.participant || msg.key.remoteJid;
      const senderName = msg.pushName || '';
      const group = acc.groups.find(g => g.id === chatId);
      const groupName = group ? group.name : chatId;

      // Scrape contact into DB (always, regardless of keyword match)
      try { scrapeContact(senderJid, senderName, chatId, groupName, null); } catch (e) { console.error('Scrape error:', e.message); }

      // Scan for leads
      const matched = scanMessage(text, chatId, groupName, senderJid, senderName, id);

      // Update contact with matched keywords if lead detected
      if (matched && matched.length > 0) {
        try { scrapeContact(senderJid, senderName, chatId, groupName, matched); } catch (e) {}
      }
    }
  });

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

// ── Lead Handlers ───────────────────────────────────────────────────────────
function handleGetLeads(since) {
  const active = leads.filter(l => !l.dismissed);
  if (since) {
    return { leads: active.filter(l => l.timestamp > since) };
  }
  return { leads: active.slice(0, 100) };
}

function handleDismissLead(id) {
  const lead = leads.find(l => l.id === id);
  if (!lead) return { error: 'Not found' };
  lead.dismissed = true;
  return { ok: true };
}

function handleDismissAll() {
  leads.forEach(l => l.dismissed = true);
  return { ok: true };
}

async function handleReplyToLead(body) {
  const { leadId, message } = body;
  if (!leadId || !message) return { error: 'Missing leadId or message' };
  const lead = leads.find(l => l.id === leadId);
  if (!lead) return { error: 'Lead not found' };

  // Find a socket that has this group
  let sock = null;
  for (const [, acc] of accounts) {
    if (acc.status === 'ready' && acc.sock && acc.groups.some(g => g.id === lead.groupId)) {
      sock = acc.sock;
      break;
    }
  }
  if (!sock) return { error: 'No connected account for this group' };

  try {
    // DM the sender
    const dmJid = lead.senderJid.includes('@') ? lead.senderJid.replace(/@g\.us|@s\.whatsapp\.net/, '') + '@s.whatsapp.net' : lead.senderJid + '@s.whatsapp.net';
    await sock.sendMessage(dmJid, { text: message });
    return { ok: true, sentTo: dmJid };
  } catch (e) {
    return { error: e.message };
  }
}

function handleLeadStats() {
  const now = new Date();
  const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate()).toISOString();
  const weekStart = new Date(now - 7 * 86400000).toISOString();

  const active = leads.filter(l => !l.dismissed);
  const today = active.filter(l => l.timestamp >= todayStart);
  const week = active.filter(l => l.timestamp >= weekStart);

  // Top groups
  const groupCounts = {};
  active.forEach(l => { groupCounts[l.groupName] = (groupCounts[l.groupName] || 0) + 1; });
  const topGroups = Object.entries(groupCounts).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([name, count]) => ({ name, count }));

  // Top keywords
  const kwCounts = {};
  active.forEach(l => l.matchedKeywords.forEach(kw => { kwCounts[kw] = (kwCounts[kw] || 0) + 1; }));
  const topKeywords = Object.entries(kwCounts).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([keyword, count]) => ({ keyword, count }));

  return { total: active.length, today: today.length, week: week.length, topGroups, topKeywords };
}

function handleGetKeywords() {
  return { keywords, scannerEnabled };
}

function handleUpdateKeywords(body) {
  if (body.keywords) {
    if (body.keywords.en) keywords.en = body.keywords.en;
    if (body.keywords.he) keywords.he = body.keywords.he;
  }
  if (typeof body.scannerEnabled === 'boolean') scannerEnabled = body.scannerEnabled;
  return { ok: true, keywords, scannerEnabled };
}

function handleExportLeads() {
  const active = leads.filter(l => !l.dismissed);
  const header = 'Timestamp,Group,Sender,Phone,Message,Keywords';
  const rows = active.map(l => {
    const phone = l.senderJid.split('@')[0];
    const msg = l.message.replace(/"/g, '""').replace(/\n/g, ' ');
    const kws = l.matchedKeywords.join('; ');
    return `"${l.timestamp}","${l.groupName}","${l.senderName}","${phone}","${msg}","${kws}"`;
  });
  return header + '\n' + rows.join('\n');
}

// ── Contact Handlers ────────────────────────────────────────────────────────
function handleGetContacts(query, groupId, tag, limit, offset) {
  limit = Math.min(parseInt(limit) || 50, 200);
  offset = parseInt(offset) || 0;

  if (query) {
    const q = '%' + query + '%';
    return { contacts: enrichContacts(stmts.searchContacts.all({ q })) };
  }
  if (groupId) {
    return { contacts: enrichContacts(stmts.contactsByGroup.all(groupId)) };
  }
  if (tag) {
    const rows = db.prepare(`
      SELECT c.*, GROUP_CONCAT(DISTINCT ct2.tag) as tags
      FROM contacts c
      INNER JOIN contact_tags ct ON c.jid = ct.jid AND ct.tag = ?
      LEFT JOIN contact_tags ct2 ON c.jid = ct2.jid
      GROUP BY c.jid ORDER BY c.last_seen DESC LIMIT ?
    `).all(tag, limit);
    return { contacts: enrichContacts(rows) };
  }
  return { contacts: enrichContacts(stmts.allContacts.all(limit, offset)) };
}

function enrichContacts(rows) {
  return rows.map(c => {
    const groups = stmts.getContactGroups.all(c.jid);
    const tags = c.tags ? c.tags.split(',') : stmts.getContactTags.all(c.jid).map(r => r.tag);
    const interests = JSON.parse(c.interests || '[]');

    // Activity score: based on message count and recency
    const daysSinceLast = (Date.now() - new Date(c.last_seen).getTime()) / 86400000;
    const recencyScore = Math.max(0, 100 - daysSinceLast * 5);
    const volumeScore = Math.min(100, c.message_count * 2);
    const activityScore = Math.round((recencyScore + volumeScore) / 2);

    let activityLevel = 'cold';
    if (activityScore > 70) activityLevel = 'hot';
    else if (activityScore > 40) activityLevel = 'warm';

    return {
      jid: c.jid,
      phone: c.phone,
      name: c.name,
      firstSeen: c.first_seen,
      lastSeen: c.last_seen,
      messageCount: c.message_count,
      groups,
      tags,
      interests,
      notes: c.notes || '',
      activityScore,
      activityLevel,
    };
  });
}

function handleGetContactDetail(jid) {
  const c = stmts.getContact.get(jid);
  if (!c) return { error: 'Not found' };
  return { contact: enrichContacts([c])[0] };
}

function handleContactStats() {
  const stats = stmts.contactStats.get();
  const topGroups = stmts.topGroups.all();
  const tagCounts = db.prepare(`SELECT tag, COUNT(*) as count FROM contact_tags GROUP BY tag ORDER BY count DESC LIMIT 20`).all();
  return { ...stats, topGroups, topTags: tagCounts };
}

function handleAddContactTag(body) {
  const { jid, tag } = body;
  if (!jid || !tag) return { error: 'Missing jid or tag' };
  stmts.addTag.run({ jid, tag });
  return { ok: true };
}

function handleRemoveContactTag(body) {
  const { jid, tag } = body;
  if (!jid || !tag) return { error: 'Missing jid or tag' };
  stmts.removeTag.run({ jid, tag });
  return { ok: true };
}

function handleUpdateContactNotes(body) {
  const { jid, notes } = body;
  if (!jid) return { error: 'Missing jid' };
  stmts.updateNotes.run({ jid, notes: notes || '' });
  return { ok: true };
}

function handleDeleteContact(jid) {
  stmts.deleteContact.run(jid);
  return { ok: true };
}

function handleExportContacts(groupId) {
  let rows;
  if (groupId) {
    rows = stmts.contactsByGroup.all(groupId);
  } else {
    rows = stmts.allContacts.all(10000, 0);
  }
  const enriched = enrichContacts(rows);
  const header = 'Phone,Name,Groups,Tags,Interests,Messages,First Seen,Last Seen,Activity';
  const csvRows = enriched.map(c => {
    const groups = c.groups.map(g => g.group_name).join('; ');
    const tags = c.tags.filter(t => !t.startsWith('group:')).join('; ');
    const interests = c.interests.join('; ');
    return `"${c.phone}","${(c.name || '').replace(/"/g, '""')}","${groups}","${tags}","${interests}",${c.messageCount},"${c.firstSeen}","${c.lastSeen}","${c.activityLevel}"`;
  });
  return header + '\n' + csvRows.join('\n');
}

// ── HTML UI v3 ──────────────────────────────────────────────────────────────
const HTML = `<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <meta name="theme-color" content="#1f2c34" />
  <title>Broadcast Pro v3</title>
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

    /* Tabs */
    .tab-bar { display: flex; background: #111b21; border-bottom: 1px solid #222e35; }
    .tab-btn { flex: 1; padding: 12px 8px; background: none; border: none; color: #8696a0; font-size: 13px; font-weight: 600; cursor: pointer; text-transform: uppercase; letter-spacing: 0.05em; position: relative; transition: color 0.15s; }
    .tab-btn.active { color: #00a884; }
    .tab-btn.active::after { content: ''; position: absolute; bottom: 0; left: 16px; right: 16px; height: 3px; background: #00a884; border-radius: 3px 3px 0 0; }
    .tab-btn .badge { display: inline-block; background: #f15c6d; color: #fff; font-size: 10px; font-weight: 700; padding: 1px 5px; border-radius: 8px; margin-left: 4px; min-width: 16px; text-align: center; }

    .tab-content { display: none; }
    .tab-content.active { display: block; }

    .chat-area { flex: 1; padding: 12px 12px 8px; background: #0b141a; background-image: url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.02'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E"); overflow-y: auto; }

    .bubble { background: #1f2c34; border-radius: 8px; padding: 14px; margin-bottom: 8px; position: relative; }
    .bubble::before { content: ''; position: absolute; top: 0; left: -6px; width: 0; height: 0; border-top: 6px solid #1f2c34; border-left: 6px solid transparent; }
    .bubble-label { font-size: 12px; font-weight: 600; color: #00a884; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.04em; }

    textarea, input[type=text] { width: 100%; background: #2a3942; border: none; border-radius: 8px; padding: 10px 12px; color: #e9edef; font-size: 15px; font-family: inherit; outline: none; }
    textarea { resize: none; min-height: 80px; line-height: 1.4; }
    textarea::placeholder, input::placeholder { color: #8696a0; }
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
    .modal { background: #1f2c34; border-radius: 12px; padding: 24px; max-width: 360px; width: 90%; text-align: center; }
    .modal h3 { font-size: 17px; font-weight: 500; color: #e9edef; margin-bottom: 6px; }
    .modal p { font-size: 13px; color: #8696a0; margin-bottom: 16px; line-height: 1.4; }
    .qr-wrap { background: #fff; border-radius: 8px; padding: 12px; display: inline-block; margin-bottom: 16px; }
    .qr-wrap img { display: block; width: 200px; height: 200px; }
    .qr-status { font-size: 13px; color: #8696a0; margin-bottom: 12px; min-height: 18px; }
    .modal-close { background: #2a3942; color: #8696a0; border: none; border-radius: 20px; padding: 8px 20px; cursor: pointer; font-size: 14px; }
    .modal-close:hover { background: #374045; color: #e9edef; }

    .mod-badge { display: inline-block; background: #00a884; color: #fff; font-size: 9px; font-weight: 700; padding: 2px 5px; border-radius: 3px; letter-spacing: 0.5px; vertical-align: middle; margin-left: 6px; }

    /* Lead cards */
    .lead-card { background: #1f2c34; border-radius: 8px; padding: 12px; margin-bottom: 8px; border-left: 3px solid #f0b429; }
    .lead-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 6px; }
    .lead-sender { font-size: 14px; font-weight: 600; color: #e9edef; }
    .lead-group { font-size: 11px; color: #00a884; }
    .lead-time { font-size: 11px; color: #8696a0; flex-shrink: 0; }
    .lead-msg { font-size: 14px; color: #d1d7db; line-height: 1.4; margin-bottom: 8px; word-break: break-word; }
    .lead-msg .kw-match { background: #f0b42930; color: #f0b429; padding: 1px 3px; border-radius: 3px; font-weight: 600; }
    .lead-keywords { display: flex; gap: 4px; flex-wrap: wrap; margin-bottom: 8px; }
    .lead-kw-tag { font-size: 10px; background: #f0b42920; color: #f0b429; padding: 2px 6px; border-radius: 4px; }
    .lead-actions { display: flex; gap: 6px; }
    .lead-btn { font-size: 11px; padding: 4px 10px; border-radius: 6px; border: none; cursor: pointer; font-weight: 500; transition: all 0.15s; }
    .lead-btn.reply { background: #005c4b; color: #e9edef; }
    .lead-btn.reply:hover { background: #00a884; }
    .lead-btn.dismiss { background: #2a3942; color: #8696a0; }
    .lead-btn.dismiss:hover { background: #374045; color: #e9edef; }

    /* Stats cards */
    .stats-row { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-bottom: 12px; }
    .stat-card { background: #1f2c34; border-radius: 8px; padding: 12px; text-align: center; }
    .stat-num { font-size: 24px; font-weight: 800; color: #00a884; }
    .stat-label { font-size: 10px; color: #8696a0; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 2px; }

    /* Keywords UI */
    .kw-section { margin-bottom: 16px; }
    .kw-section-title { font-size: 13px; font-weight: 600; color: #00a884; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.04em; }
    .kw-tags { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 8px; }
    .kw-tag { display: flex; align-items: center; gap: 4px; background: #2a3942; color: #e9edef; padding: 4px 10px; border-radius: 16px; font-size: 13px; }
    .kw-tag .kw-x { background: none; border: none; color: #8696a0; cursor: pointer; font-size: 12px; padding: 0 2px; line-height: 1; }
    .kw-tag .kw-x:hover { color: #f15c6d; }
    .kw-add-row { display: flex; gap: 6px; }
    .kw-add-row input { flex: 1; padding: 8px 12px; font-size: 13px; border-radius: 20px; }
    .kw-add-btn { background: #005c4b; color: #fff; border: none; border-radius: 20px; padding: 8px 14px; font-size: 13px; cursor: pointer; font-weight: 500; white-space: nowrap; }
    .kw-add-btn:hover { background: #00a884; }

    .toggle-row { display: flex; align-items: center; justify-content: space-between; background: #1f2c34; border-radius: 8px; padding: 12px; margin-bottom: 12px; }
    .toggle-label { font-size: 14px; color: #e9edef; }
    .toggle-sub { font-size: 11px; color: #8696a0; }
    .toggle-switch { width: 44px; height: 24px; background: #2a3942; border-radius: 12px; position: relative; cursor: pointer; transition: background 0.2s; border: none; flex-shrink: 0; }
    .toggle-switch.on { background: #00a884; }
    .toggle-switch::after { content: ''; position: absolute; top: 3px; left: 3px; width: 18px; height: 18px; background: #fff; border-radius: 50%; transition: transform 0.2s; }
    .toggle-switch.on::after { transform: translateX(20px); }

    /* Reply modal */
    .reply-modal textarea { min-height: 60px; margin: 12px 0; }
    .reply-modal .reply-to { font-size: 12px; color: #8696a0; margin-bottom: 8px; }
    .reply-modal .modal-actions { display: flex; gap: 8px; justify-content: center; }
    .reply-send { background: #00a884; color: #fff; border: none; border-radius: 20px; padding: 8px 20px; cursor: pointer; font-size: 14px; font-weight: 500; }
    .reply-send:hover { background: #06cf9c; }

    /* Contact cards */
    .contact-card { display: flex; align-items: center; gap: 10px; background: #1f2c34; border-radius: 8px; padding: 10px 12px; margin-bottom: 6px; cursor: pointer; transition: background 0.1s; }
    .contact-card:hover { background: #253540; }
    .contact-avatar { width: 42px; height: 42px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 18px; flex-shrink: 0; font-weight: 700; color: #fff; }
    .contact-avatar.hot { background: #dc2626; }
    .contact-avatar.warm { background: #f59e0b; }
    .contact-avatar.cold { background: #2a3942; color: #8696a0; }
    .contact-info { flex: 1; min-width: 0; }
    .contact-name { font-size: 15px; color: #e9edef; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .contact-meta { font-size: 11px; color: #8696a0; margin-top: 2px; display: flex; gap: 8px; flex-wrap: wrap; }
    .contact-tags-mini { display: flex; gap: 3px; flex-wrap: wrap; margin-top: 3px; }
    .contact-tag-mini { font-size: 9px; background: #2a3942; color: #8696a0; padding: 1px 5px; border-radius: 3px; }
    .contact-tag-mini.lead { background: #f0b42920; color: #f0b429; }
    .contact-score { font-size: 11px; font-weight: 700; flex-shrink: 0; padding: 3px 8px; border-radius: 10px; }
    .contact-score.hot { background: #dc262620; color: #dc2626; }
    .contact-score.warm { background: #f59e0b20; color: #f59e0b; }
    .contact-score.cold { background: #2a3942; color: #8696a0; }
  </style>
</head>
<body>
  <div class="app">
    <div class="topbar">
      <div class="topbar-avatar">&#x1F4E2;</div>
      <div class="topbar-info">
        <div class="topbar-title" id="subtitle">Broadcast<span class="mod-badge">PRO v3</span></div>
        <div class="topbar-sub"><span class="dot" id="statusDot"></span> <span id="statusText">connecting...</span></div>
      </div>
      <div class="topbar-actions">
        <button class="topbar-btn" id="langBtn" onclick="toggleLang()" title="Language">&#x1F310;</button>
      </div>
    </div>

    <div class="accounts-bar" id="accountsBar">
      <button class="acc-chip add-chip" onclick="addAccount()" id="addBtn">+ Add</button>
    </div>

    <div class="tab-bar">
      <button class="tab-btn active" onclick="switchTab('broadcast')" id="tabBroadcast">&#x1F4E2; Broadcast</button>
      <button class="tab-btn" onclick="switchTab('leads')" id="tabLeads">&#x1F50D; Leads <span class="badge" id="leadBadge" style="display:none">0</span></button>
      <button class="tab-btn" onclick="switchTab('keywords')" id="tabKeywords">&#x2699; Keywords</button>
      <button class="tab-btn" onclick="switchTab('contacts')" id="tabContacts">&#x1F464; Contacts <span class="badge" id="contactBadge" style="display:none">0</span></button>
    </div>

    <!-- QR Modal -->
    <div class="modal-overlay" id="qrModal" style="display:none">
      <div class="modal">
        <h3 id="qrTitle">Link Device</h3>
        <p id="qrInstructions">Open WhatsApp > Linked Devices > Link a Device</p>
        <div class="qr-wrap" id="qrWrap" style="display:none"><img id="qrImg" src="" alt="QR" /></div>
        <div class="qr-status" id="qrStatusMsg">Loading...</div>
        <button class="modal-close" onclick="closeQR()" id="qrCloseBtn">Close</button>
      </div>
    </div>

    <!-- Reply Modal -->
    <div class="modal-overlay" id="replyModal" style="display:none">
      <div class="modal reply-modal">
        <h3>Reply via DM</h3>
        <div class="reply-to" id="replyTo"></div>
        <textarea id="replyMsg" rows="3" placeholder="Type your DM reply..."></textarea>
        <div class="modal-actions">
          <button class="modal-close" onclick="closeReply()">Cancel</button>
          <button class="reply-send" onclick="sendReply()">Send DM</button>
        </div>
      </div>
    </div>

    <div class="chat-area">
      <!-- ═══ BROADCAST TAB ═══ -->
      <div class="tab-content active" id="panelBroadcast">
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

      <!-- ═══ LEADS TAB ═══ -->
      <div class="tab-content" id="panelLeads">
        <div class="stats-row" id="leadStats">
          <div class="stat-card"><div class="stat-num" id="statTotal">0</div><div class="stat-label">Total</div></div>
          <div class="stat-card"><div class="stat-num" id="statToday">0</div><div class="stat-label">Today</div></div>
          <div class="stat-card"><div class="stat-num" id="statWeek">0</div><div class="stat-label">This Week</div></div>
        </div>

        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
          <div class="bubble-label" style="margin:0">Live Leads</div>
          <div style="display:flex;gap:6px">
            <button class="wa-icon-btn" onclick="exportLeads()" title="Export CSV">&#x1F4E5; CSV</button>
            <button class="wa-icon-btn" onclick="dismissAllLeads()">Clear all</button>
            <button class="wa-icon-btn" onclick="loadLeads()" id="refreshLeadsBtn">&#x21BB;</button>
          </div>
        </div>

        <div id="leadsList"></div>
      </div>

      <!-- ═══ KEYWORDS TAB ═══ -->
      <div class="tab-content" id="panelKeywords">
        <div class="toggle-row">
          <div>
            <div class="toggle-label">Lead Scanner</div>
            <div class="toggle-sub">Scan group messages for event keywords</div>
          </div>
          <button class="toggle-switch on" id="scannerToggle" onclick="toggleScanner()"></button>
        </div>

        <div class="bubble">
          <div class="kw-section">
            <div class="kw-section-title">English Keywords</div>
            <div class="kw-tags" id="kwTagsEn"></div>
            <div class="kw-add-row">
              <input type="text" id="kwInputEn" placeholder="Add keyword..." onkeydown="if(event.key==='Enter')addKeyword('en')" />
              <button class="kw-add-btn" onclick="addKeyword('en')">+ Add</button>
            </div>
          </div>

          <div class="kw-section" style="margin-top:16px">
            <div class="kw-section-title">Hebrew Keywords</div>
            <div class="kw-tags" id="kwTagsHe"></div>
            <div class="kw-add-row">
              <input type="text" id="kwInputHe" placeholder="...\u05D4\u05D5\u05E1\u05E3 \u05DE\u05D9\u05DC\u05D4" dir="rtl" onkeydown="if(event.key==='Enter')addKeyword('he')" />
              <button class="kw-add-btn" onclick="addKeyword('he')">+ Add</button>
            </div>
          </div>
        </div>

        <div class="bubble" style="margin-top:8px">
          <div class="bubble-label">Top Keywords</div>
          <div id="topKeywords" class="empty">No data yet</div>
        </div>
      </div>
    </div>

      <!-- ═══ CONTACTS TAB ═══ -->
      <div class="tab-content" id="panelContacts">
        <div class="stats-row" id="contactStats">
          <div class="stat-card"><div class="stat-num" id="cStatTotal">0</div><div class="stat-label">Total</div></div>
          <div class="stat-card"><div class="stat-num" id="cStatToday">0</div><div class="stat-label">Today</div></div>
          <div class="stat-card"><div class="stat-num" id="cStatWeek">0</div><div class="stat-label">This Week</div></div>
        </div>

        <div class="bubble" style="padding:10px 12px">
          <div style="display:flex;gap:6px">
            <input type="text" id="contactSearch" placeholder="Search by name or phone..." oninput="searchContacts()" style="flex:1;border-radius:20px;padding:8px 14px;font-size:13px" />
            <select id="contactGroupFilter" onchange="filterByGroup()" style="background:#2a3942;border:none;color:#e9edef;border-radius:20px;padding:8px 10px;font-size:12px;max-width:140px">
              <option value="">All groups</option>
            </select>
          </div>
        </div>

        <div style="display:flex;justify-content:space-between;align-items:center;margin:8px 0">
          <div class="bubble-label" style="margin:0" id="contactsCount">Contacts (0)</div>
          <div style="display:flex;gap:6px">
            <button class="wa-icon-btn" onclick="exportContacts()">&#x1F4E5; CSV</button>
            <button class="wa-icon-btn" onclick="loadContacts()">&#x21BB;</button>
          </div>
        </div>

        <div id="contactsList"></div>
      </div>
    </div>

    <!-- Contact Detail Modal -->
    <div class="modal-overlay" id="contactModal" style="display:none">
      <div class="modal" style="max-width:400px;text-align:left">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
          <h3 id="cmName" style="font-size:17px"></h3>
          <button class="modal-close" onclick="closeContactModal()" style="padding:4px 12px">&#x2715;</button>
        </div>
        <div style="font-size:13px;color:#8696a0;margin-bottom:12px">
          <div>&#x1F4F1; <span id="cmPhone"></span></div>
          <div>&#x1F4C5; First seen: <span id="cmFirstSeen"></span></div>
          <div>&#x1F552; Last active: <span id="cmLastSeen"></span></div>
          <div>&#x1F4AC; Messages: <span id="cmMsgCount"></span></div>
          <div>&#x1F525; Activity: <span id="cmActivity"></span></div>
        </div>

        <div style="margin-bottom:12px">
          <div class="kw-section-title">Groups</div>
          <div id="cmGroups" style="font-size:12px;color:#8696a0"></div>
        </div>

        <div style="margin-bottom:12px">
          <div class="kw-section-title">Tags</div>
          <div id="cmTags" class="kw-tags" style="margin-bottom:6px"></div>
          <div class="kw-add-row">
            <input type="text" id="cmTagInput" placeholder="Add tag..." style="font-size:12px;border-radius:20px;padding:6px 10px" onkeydown="if(event.key==='Enter')addContactTag()" />
            <button class="kw-add-btn" onclick="addContactTag()" style="font-size:12px;padding:6px 10px">+ Tag</button>
          </div>
        </div>

        <div style="margin-bottom:12px">
          <div class="kw-section-title">Interests</div>
          <div id="cmInterests" class="kw-tags"></div>
        </div>

        <div style="margin-bottom:12px">
          <div class="kw-section-title">Notes</div>
          <textarea id="cmNotes" rows="2" style="min-height:40px;font-size:13px" placeholder="Add notes about this contact..."></textarea>
          <button class="wa-icon-btn" onclick="saveContactNotes()" style="margin-top:4px;font-size:11px">Save notes</button>
        </div>

        <div style="display:flex;gap:6px;justify-content:flex-end;margin-top:12px;border-top:1px solid #222e35;padding-top:12px">
          <button class="lead-btn reply" onclick="dmContact()">&#x1F4AC; Send DM</button>
          <button class="lead-btn dismiss" onclick="deleteContact()" style="color:#f15c6d">Delete</button>
        </div>
      </div>
    </div>

    <!-- DM Modal -->
    <div class="modal-overlay" id="dmModal" style="display:none">
      <div class="modal reply-modal">
        <h3>Send DM</h3>
        <div class="reply-to" id="dmTo"></div>
        <textarea id="dmMsg" rows="3" placeholder="Type your message..."></textarea>
        <div class="modal-actions">
          <button class="modal-close" onclick="closeDM()">Cancel</button>
          <button class="reply-send" onclick="sendDM()">Send</button>
        </div>
      </div>
    </div>

    <!-- Bottom bar (broadcast tab only) -->
    <div class="bottom-bar" id="bottomBar">
      <div class="send-info" id="sendLabel">Select groups & write a message</div>
      <button class="send-btn" id="sendBtn" onclick="send()" disabled>&#x27A4;</button>
    </div>
  </div>

  <script>
    let groups = [];
    let selected = new Set();
    let accountsList = [];
    let activeQrAccount = null;
    let currentTab = 'broadcast';
    let leadCount = 0;
    let kwData = { en: [], he: [] };
    let replyLeadId = null;
    let leadPollTimer = null;
    let lang = localStorage.getItem('wa-lang') || 'en';

    const i18n = {
      en: {
        statusOnline: function(n, t) { return n + '/' + t + ' online'; },
        statusNone: 'no accounts',
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
        qrTitle: 'Link Device',
        qrInstructions: 'Open WhatsApp > Linked Devices > Link a Device',
        qrLoading: 'Loading...',
        qrConnected: 'Connected! Close this.',
        qrScan: 'Scan with WhatsApp',
        qrWaiting: 'Waiting...',
        qrClose: 'Close',
        noLeads: 'No leads detected yet. Scanner is listening...',
      },
      he: {
        statusOnline: function(n, t) { return n + '/' + t + ' \\u05DE\\u05D7\\u05D5\\u05D1\\u05E8\\u05D9\\u05DD'; },
        statusNone: '\\u05D0\\u05D9\\u05DF \\u05D7\\u05E9\\u05D1\\u05D5\\u05E0\\u05D5\\u05EA',
        msgLabel: '\\u05DB\\u05EA\\u05D5\\u05D1 \\u05D4\\u05D5\\u05D3\\u05E2\\u05D4',
        placeholder: '\\u05DB\\u05EA\\u05D1\\u05D5 \\u05D0\\u05EA \\u05D4\\u05D4\\u05D5\\u05D3\\u05E2\\u05D4...',
        charLabel: '\\u05EA\\u05D5\\u05D5\\u05D9\\u05DD',
        groupsLabel: function(n) { return '\\u05E7\\u05D1\\u05D5\\u05E6\\u05D5\\u05EA (' + n + ')'; },
        selectAll: '\\u05D1\\u05D7\\u05E8 \\u05D4\\u05DB\\u05DC',
        groupsLoading: '\\u05D8\\u05D5\\u05E2\\u05DF...',
        groupsEmpty: '\\u05D0\\u05D9\\u05DF \\u05E7\\u05D1\\u05D5\\u05E6\\u05D5\\u05EA',
        groupsError: '\\u05E9\\u05D2\\u05D9\\u05D0\\u05D4',
        members: '\\u05D7\\u05D1\\u05E8\\u05D9\\u05DD',
        sending: '\\u05E9\\u05D5\\u05DC\\u05D7...',
        sendingProgress: function(done, total) { return '\\u05E9\\u05D5\\u05DC\\u05D7 ' + done + '/' + total + '...'; },
        done: '\\u05D4\\u05D5\\u05E9\\u05DC\\u05DD!',
        successMsg: function(n) { return '\\u05E0\\u05E9\\u05DC\\u05D7 \\u05DC-' + n + ' \\u05E7\\u05D1\\u05D5\\u05E6\\u05D5\\u05EA'; },
        partialMsg: function(sent, failed) { return '\\u05E0\\u05E9\\u05DC\\u05D7: ' + sent + ' | \\u05E0\\u05DB\\u05E9\\u05DC: ' + failed; },
        errorMsg: function(msg) { return '\\u05E9\\u05D2\\u05D9\\u05D0\\u05D4: ' + msg; },
        sendInfo: '\\u05D1\\u05D7\\u05E8 \\u05E7\\u05D1\\u05D5\\u05E6\\u05D5\\u05EA \\u05D5\\u05DB\\u05EA\\u05D1 \\u05D4\\u05D5\\u05D3\\u05E2\\u05D4',
        sendReady: function(n) { return '<strong>' + n + '</strong> \\u05E7\\u05D1\\u05D5\\u05E6\\u05D5\\u05EA \\u05DE\\u05D5\\u05DB\\u05E0\\u05D5\\u05EA'; },
        addAccount: '+ \\u05D4\\u05D5\\u05E1\\u05E3',
        qrTitle: '\\u05E7\\u05D9\\u05E9\\u05D5\\u05E8 \\u05DE\\u05DB\\u05E9\\u05D9\\u05E8',
        qrInstructions: '\\u05E4\\u05EA\\u05D7 WhatsApp > \\u05D4\\u05EA\\u05E7\\u05E0\\u05D9\\u05DD \\u05DE\\u05E7\\u05D5\\u05E9\\u05E8\\u05D9\\u05DD > \\u05E7\\u05E9\\u05E8 \\u05D4\\u05EA\\u05E7\\u05DF',
        qrLoading: '\\u05D8\\u05D5\\u05E2\\u05DF...',
        qrConnected: '\\u05DE\\u05D7\\u05D5\\u05D1\\u05E8!',
        qrScan: '\\u05E1\\u05E8\\u05D5\\u05E7 \\u05E2\\u05DD WhatsApp',
        qrWaiting: '\\u05DE\\u05DE\\u05EA\\u05D9\\u05DF...',
        qrClose: '\\u05E1\\u05D2\\u05D5\\u05E8',
        noLeads: '\\u05E2\\u05D3\\u05D9\\u05D9\\u05DF \\u05D0\\u05D9\\u05DF \\u05DC\\u05D9\\u05D3\\u05D9\\u05DD. \\u05D4\\u05E1\\u05D5\\u05E8\\u05E7 \\u05DE\\u05D0\\u05D6\\u05D9\\u05DF...',
      }
    };

    function esc(s) { var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
    function t(key) { var val = i18n[lang][key]; if (typeof val === 'function') return val.apply(null, Array.prototype.slice.call(arguments, 1)); return val || key; }

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

    function toggleLang() { lang = lang === 'en' ? 'he' : 'en'; localStorage.setItem('wa-lang', lang); applyLang(); }

    // ── Tabs ──
    function switchTab(tab) {
      currentTab = tab;
      document.querySelectorAll('.tab-btn').forEach(function(b) { b.classList.remove('active'); });
      document.querySelectorAll('.tab-content').forEach(function(p) { p.classList.remove('active'); });
      document.getElementById('tab' + tab.charAt(0).toUpperCase() + tab.slice(1)).classList.add('active');
      document.getElementById('panel' + tab.charAt(0).toUpperCase() + tab.slice(1)).classList.add('active');
      document.getElementById('bottomBar').style.display = tab === 'broadcast' ? 'flex' : 'none';
      if (tab === 'leads') { loadLeads(); loadLeadStats(); }
      if (tab === 'keywords') { loadKeywords(); }
      if (tab === 'contacts') { loadContacts(); }
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
        if (d.id) { await refreshAccounts(); openQR(d.id); }
      } catch (e) { console.error('Add account error:', e); }
    }

    async function removeAccount(id) {
      var a = accountsList.find(function(x) { return x.id === id; });
      if (!confirm('Remove ' + (a ? a.name : id) + '?')) return;
      try { await fetch('/api/accounts?id=' + encodeURIComponent(id), { method: 'DELETE' }); await refreshAccounts(); loadGroups(); } catch (e) {}
    }

    function connectAccount(id) { var a = accountsList.find(function(x) { return x.id === id; }); if (a && a.status !== 'ready') openQR(id); }

    async function refreshAccounts() {
      try { var r = await fetch('/api/accounts'); var d = await r.json(); accountsList = d.accounts || []; renderAccounts(); updateTopStatus(); } catch (e) {}
    }

    function updateTopStatus() {
      var online = accountsList.filter(function(a) { return a.status === 'ready'; }).length;
      document.getElementById('statusDot').className = online > 0 ? 'dot online' : 'dot';
      document.getElementById('statusText').textContent = accountsList.length === 0 ? t('statusNone') : t('statusOnline', online, accountsList.length);
    }

    // ── Groups ──
    async function loadGroups() {
      document.getElementById('groupsList').innerHTML = '<div class="empty">' + t('groupsLoading') + '</div>';
      try { var r = await fetch('/api/groups'); var d = await r.json(); groups = d.groups || []; renderGroups(); }
      catch (e) { document.getElementById('groupsList').innerHTML = '<div class="empty">' + t('groupsError') + '</div>'; }
    }

    function renderGroups() {
      var el = document.getElementById('groupsList');
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

    function toggle(id) { if (selected.has(id)) selected.delete(id); else selected.add(id); renderGroups(); document.getElementById('groupsLabel').innerHTML = t('groupsLabel', selected.size); }
    function selectAll() { if (selected.size === groups.length) selected.clear(); else groups.forEach(function(g) { selected.add(g.id); }); renderGroups(); document.getElementById('groupsLabel').innerHTML = t('groupsLabel', selected.size); }
    function updateCount() { document.getElementById('charCount').textContent = document.getElementById('msgInput').value.length; updateSendBtn(); }
    function updateSendBtn() { var msg = document.getElementById('msgInput').value.trim(); var ready = msg && selected.size > 0; document.getElementById('sendBtn').disabled = !ready; document.getElementById('sendLabel').innerHTML = ready ? t('sendReady', selected.size) : t('sendInfo'); }

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
        var r = await fetch('/api/broadcast', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chatIds: ids, message: msg }) });
        var d = await r.json();
        document.getElementById('progressBar').style.width = '100%';
        document.getElementById('progressText').textContent = t('done');
        var res = document.getElementById('result');
        res.style.display = 'block';
        if (d.failed === 0) { res.className = 'toast success'; res.textContent = t('successMsg', d.sent); }
        else { res.className = 'toast error'; res.textContent = t('partialMsg', d.sent, d.failed); }
      } catch (e) { var res2 = document.getElementById('result'); res2.style.display = 'block'; res2.className = 'toast error'; res2.textContent = t('errorMsg', e.message); }
      document.getElementById('progressWrap').style.display = 'none';
      document.getElementById('sendBtn').disabled = false;
      document.getElementById('sendLabel').textContent = t('sendInfo');
    }

    // ── QR Modal ──
    var qrPollTimer = null;
    function openQR(accountId) { activeQrAccount = accountId; document.getElementById('qrModal').style.display = 'flex'; document.getElementById('qrWrap').style.display = 'none'; document.getElementById('qrStatusMsg').textContent = t('qrLoading'); pollQR(); qrPollTimer = setInterval(pollQR, 3000); }
    function closeQR() { document.getElementById('qrModal').style.display = 'none'; if (qrPollTimer) { clearInterval(qrPollTimer); qrPollTimer = null; } activeQrAccount = null; }
    async function pollQR() {
      if (!activeQrAccount) return;
      try {
        var r = await fetch('/api/qr?account=' + encodeURIComponent(activeQrAccount));
        var d = await r.json();
        if (d.status === 'ready') { document.getElementById('qrWrap').style.display = 'none'; document.getElementById('qrStatusMsg').textContent = t('qrConnected'); if (qrPollTimer) { clearInterval(qrPollTimer); qrPollTimer = null; } refreshAccounts(); loadGroups(); }
        else if (d.qrDataUrl) { document.getElementById('qrImg').src = d.qrDataUrl; document.getElementById('qrWrap').style.display = 'inline-block'; document.getElementById('qrStatusMsg').textContent = t('qrScan'); }
        else { document.getElementById('qrStatusMsg').textContent = d.status || t('qrWaiting'); }
      } catch (e) { document.getElementById('qrStatusMsg').textContent = t('errorMsg', e.message); }
    }

    // ── Leads ──
    function highlightMsg(msg, keywords) {
      var escaped = esc(msg);
      keywords.forEach(function(kw) {
        var re = new RegExp('(' + kw.replace(/[.*+?^\${}()|[\\]\\\\]/g, '\\\\$&') + ')', 'gi');
        escaped = escaped.replace(re, '<span class="kw-match">$1</span>');
      });
      return escaped;
    }

    function timeAgo(ts) {
      var diff = (Date.now() - new Date(ts).getTime()) / 1000;
      if (diff < 60) return 'just now';
      if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
      if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
      return Math.floor(diff / 86400) + 'd ago';
    }

    async function loadLeads() {
      try {
        var r = await fetch('/api/leads');
        var d = await r.json();
        var list = d.leads || [];
        leadCount = list.length;
        updateLeadBadge();
        var el = document.getElementById('leadsList');
        if (!list.length) { el.innerHTML = '<div class="empty">&#x1F50D; ' + t('noLeads') + '</div>'; return; }
        el.innerHTML = list.map(function(l) {
          return '<div class="lead-card" id="lead-' + l.id + '">'
            + '<div class="lead-header"><div><div class="lead-sender">' + esc(l.senderName) + '</div><div class="lead-group">&#x1F4AC; ' + esc(l.groupName) + '</div></div>'
            + '<div class="lead-time">' + timeAgo(l.timestamp) + '</div></div>'
            + '<div class="lead-msg">' + highlightMsg(l.message, l.matchedKeywords) + '</div>'
            + '<div class="lead-keywords">' + l.matchedKeywords.map(function(kw) { return '<span class="lead-kw-tag">' + esc(kw) + '</span>'; }).join('') + '</div>'
            + '<div class="lead-actions">'
            + '<button class="lead-btn reply" onclick="openReply(\\'' + l.id + '\\',\\'' + esc(l.senderName).replace(/'/g, "\\\\'") + '\\')">&#x1F4AC; Reply DM</button>'
            + '<button class="lead-btn dismiss" onclick="dismissLead(\\'' + l.id + '\\')">Dismiss</button>'
            + '</div></div>';
        }).join('');
      } catch (e) { console.error('Load leads error:', e); }
    }

    async function loadLeadStats() {
      try {
        var r = await fetch('/api/leads/stats');
        var d = await r.json();
        document.getElementById('statTotal').textContent = d.total || 0;
        document.getElementById('statToday').textContent = d.today || 0;
        document.getElementById('statWeek').textContent = d.week || 0;
        // Top keywords
        var tkEl = document.getElementById('topKeywords');
        if (d.topKeywords && d.topKeywords.length) {
          tkEl.innerHTML = d.topKeywords.map(function(k) { return '<span class="lead-kw-tag" style="margin:2px">' + esc(k.keyword) + ' (' + k.count + ')</span>'; }).join('');
          tkEl.className = '';
        }
      } catch (e) {}
    }

    function updateLeadBadge() {
      var badge = document.getElementById('leadBadge');
      if (leadCount > 0) { badge.style.display = 'inline-block'; badge.textContent = leadCount > 99 ? '99+' : leadCount; }
      else { badge.style.display = 'none'; }
    }

    async function dismissLead(id) {
      try {
        await fetch('/api/leads/dismiss', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: id }) });
        var el = document.getElementById('lead-' + id);
        if (el) el.remove();
        leadCount = Math.max(0, leadCount - 1);
        updateLeadBadge();
      } catch (e) {}
    }

    async function dismissAllLeads() {
      if (!confirm('Dismiss all leads?')) return;
      try { await fetch('/api/leads/dismiss-all', { method: 'POST' }); leadCount = 0; updateLeadBadge(); loadLeads(); loadLeadStats(); } catch (e) {}
    }

    function openReply(leadId, senderName) {
      replyLeadId = leadId;
      document.getElementById('replyTo').textContent = 'To: ' + senderName;
      document.getElementById('replyMsg').value = '';
      document.getElementById('replyModal').style.display = 'flex';
      document.getElementById('replyMsg').focus();
    }

    function closeReply() { document.getElementById('replyModal').style.display = 'none'; replyLeadId = null; }

    async function sendReply() {
      var msg = document.getElementById('replyMsg').value.trim();
      if (!msg || !replyLeadId) return;
      try {
        var r = await fetch('/api/leads/reply', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ leadId: replyLeadId, message: msg }) });
        var d = await r.json();
        if (d.ok) { closeReply(); alert('DM sent!'); }
        else { alert('Error: ' + (d.error || 'Unknown')); }
      } catch (e) { alert('Error: ' + e.message); }
    }

    async function exportLeads() {
      try {
        var r = await fetch('/api/leads/export');
        var text = await r.text();
        var blob = new Blob([text], { type: 'text/csv' });
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url; a.download = 'leads-' + new Date().toISOString().slice(0, 10) + '.csv';
        a.click(); URL.revokeObjectURL(url);
      } catch (e) { alert('Export error: ' + e.message); }
    }

    // ── Keywords ──
    async function loadKeywords() {
      try {
        var r = await fetch('/api/keywords');
        var d = await r.json();
        kwData = d.keywords || { en: [], he: [] };
        var toggle = document.getElementById('scannerToggle');
        toggle.className = d.scannerEnabled ? 'toggle-switch on' : 'toggle-switch';
        renderKeywords();
      } catch (e) {}
    }

    function renderKeywords() {
      ['en', 'he'].forEach(function(lang) {
        var el = document.getElementById('kwTags' + lang.charAt(0).toUpperCase() + lang.slice(1));
        el.innerHTML = (kwData[lang] || []).map(function(kw) {
          return '<span class="kw-tag">' + esc(kw) + '<button class="kw-x" onclick="removeKeyword(\\'' + lang + '\\',\\'' + esc(kw).replace(/'/g, "\\\\'") + '\\')">&times;</button></span>';
        }).join('');
      });
    }

    async function addKeyword(lang) {
      var input = document.getElementById('kwInput' + lang.charAt(0).toUpperCase() + lang.slice(1));
      var val = input.value.trim();
      if (!val) return;
      kwData[lang] = kwData[lang] || [];
      if (kwData[lang].indexOf(val) === -1) kwData[lang].push(val);
      input.value = '';
      renderKeywords();
      await saveKeywords();
    }

    async function removeKeyword(lang, kw) {
      kwData[lang] = (kwData[lang] || []).filter(function(k) { return k !== kw; });
      renderKeywords();
      await saveKeywords();
    }

    async function toggleScanner() {
      var toggle = document.getElementById('scannerToggle');
      var isOn = toggle.classList.contains('on');
      toggle.className = isOn ? 'toggle-switch' : 'toggle-switch on';
      try { await fetch('/api/keywords', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ scannerEnabled: !isOn }) }); } catch (e) {}
    }

    async function saveKeywords() {
      try { await fetch('/api/keywords', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ keywords: kwData }) }); } catch (e) {}
    }

    // ── Poll leads in background ──
    async function pollLeads() {
      try {
        var r = await fetch('/api/leads');
        var d = await r.json();
        var newCount = (d.leads || []).length;
        if (newCount !== leadCount) {
          leadCount = newCount;
          updateLeadBadge();
          if (currentTab === 'leads') { loadLeads(); loadLeadStats(); }
        }
      } catch (e) {}
    }

    // Init
    applyLang();
    refreshAccounts();
    loadGroups();
    setInterval(refreshAccounts, 15000);
    setInterval(pollLeads, 5000);

    // ── Contacts ──
    let contactsData = [];
    let activeContactJid = null;
    let contactGroupsList = [];

    async function loadContacts() {
      try {
        var r = await fetch('/api/contacts?limit=100');
        var d = await r.json();
        contactsData = d.contacts || [];
        renderContacts(contactsData);
        loadContactStats();
        loadContactGroups();
      } catch (e) { console.error('Load contacts error:', e); }
    }

    async function loadContactStats() {
      try {
        var r = await fetch('/api/contacts/stats');
        var d = await r.json();
        document.getElementById('cStatTotal').textContent = d.total || 0;
        document.getElementById('cStatToday').textContent = d.today || 0;
        document.getElementById('cStatWeek').textContent = d.week || 0;
        var badge = document.getElementById('contactBadge');
        if (d.total > 0) { badge.style.display = 'inline-block'; badge.textContent = d.total > 999 ? '999+' : d.total; }
      } catch (e) {}
    }

    async function loadContactGroups() {
      try {
        var r = await fetch('/api/contacts/stats');
        var d = await r.json();
        contactGroupsList = d.topGroups || [];
        var sel = document.getElementById('contactGroupFilter');
        var current = sel.value;
        sel.innerHTML = '<option value="">All groups</option>' + contactGroupsList.map(function(g) {
          return '<option value="' + g.group_id + '">' + esc(g.group_name) + ' (' + g.contact_count + ')</option>';
        }).join('');
        sel.value = current;
      } catch (e) {}
    }

    function renderContacts(list) {
      var el = document.getElementById('contactsList');
      document.getElementById('contactsCount').textContent = 'Contacts (' + list.length + ')';
      if (!list.length) { el.innerHTML = '<div class="empty">&#x1F464; No contacts scraped yet. They\\'ll appear as people chat in your groups.</div>'; return; }
      el.innerHTML = list.map(function(c) {
        var initial = (c.name || c.phone || '?').charAt(0).toUpperCase();
        var tags = (c.tags || []).filter(function(t) { return !t.startsWith('group:'); }).slice(0, 3);
        var groupCount = (c.groups || []).length;
        return '<div class="contact-card" onclick="openContact(\\'' + c.jid.replace(/'/g, "\\\\'") + '\\')">'
          + '<div class="contact-avatar ' + c.activityLevel + '">' + initial + '</div>'
          + '<div class="contact-info">'
          + '<div class="contact-name">' + esc(c.name || c.phone) + '</div>'
          + '<div class="contact-meta"><span>&#x1F4F1; ' + esc(c.phone) + '</span><span>&#x1F4AC; ' + c.messageCount + '</span><span>&#x1F465; ' + groupCount + ' groups</span></div>'
          + (tags.length ? '<div class="contact-tags-mini">' + tags.map(function(t) { return '<span class="contact-tag-mini' + (t === 'lead' ? ' lead' : '') + '">' + esc(t) + '</span>'; }).join('') + '</div>' : '')
          + '</div>'
          + '<span class="contact-score ' + c.activityLevel + '">' + c.activityScore + '</span>'
          + '</div>';
      }).join('');
    }

    var searchTimeout = null;
    function searchContacts() {
      clearTimeout(searchTimeout);
      searchTimeout = setTimeout(async function() {
        var q = document.getElementById('contactSearch').value.trim();
        if (!q) { renderContacts(contactsData); return; }
        try {
          var r = await fetch('/api/contacts?q=' + encodeURIComponent(q));
          var d = await r.json();
          renderContacts(d.contacts || []);
        } catch (e) {}
      }, 300);
    }

    async function filterByGroup() {
      var groupId = document.getElementById('contactGroupFilter').value;
      if (!groupId) { renderContacts(contactsData); return; }
      try {
        var r = await fetch('/api/contacts?group=' + encodeURIComponent(groupId));
        var d = await r.json();
        renderContacts(d.contacts || []);
      } catch (e) {}
    }

    async function openContact(jid) {
      activeContactJid = jid;
      try {
        var r = await fetch('/api/contacts/detail?jid=' + encodeURIComponent(jid));
        var d = await r.json();
        if (d.error) return;
        var c = d.contact;
        document.getElementById('cmName').textContent = c.name || c.phone;
        document.getElementById('cmPhone').textContent = c.phone;
        document.getElementById('cmFirstSeen').textContent = new Date(c.firstSeen).toLocaleDateString();
        document.getElementById('cmLastSeen').textContent = timeAgo(c.lastSeen);
        document.getElementById('cmMsgCount').textContent = c.messageCount;
        var actColors = { hot: '#dc2626', warm: '#f59e0b', cold: '#8696a0' };
        document.getElementById('cmActivity').innerHTML = '<span style="color:' + (actColors[c.activityLevel] || '#8696a0') + ';font-weight:700">' + c.activityScore + '/100 (' + c.activityLevel + ')</span>';

        document.getElementById('cmGroups').innerHTML = (c.groups || []).map(function(g) {
          return '<div style="padding:3px 0">&#x1F4AC; ' + esc(g.group_name) + ' <span style="color:#64748b">(' + g.message_count + ' msgs)</span></div>';
        }).join('') || '<span style="color:#64748b">None</span>';

        var userTags = (c.tags || []).filter(function(t) { return !t.startsWith('group:'); });
        document.getElementById('cmTags').innerHTML = userTags.map(function(t) {
          return '<span class="kw-tag">' + esc(t) + '<button class="kw-x" onclick="removeContactTag(\\'' + esc(t).replace(/'/g, "\\\\'") + '\\')">&times;</button></span>';
        }).join('') || '<span style="color:#64748b;font-size:12px">No tags</span>';

        document.getElementById('cmInterests').innerHTML = (c.interests || []).map(function(i) {
          return '<span class="lead-kw-tag">' + esc(i) + '</span>';
        }).join('') || '<span style="color:#64748b;font-size:12px">No interests detected</span>';

        document.getElementById('cmNotes').value = c.notes || '';
        document.getElementById('contactModal').style.display = 'flex';
      } catch (e) { console.error('Open contact error:', e); }
    }

    function closeContactModal() { document.getElementById('contactModal').style.display = 'none'; activeContactJid = null; }

    async function addContactTag() {
      var input = document.getElementById('cmTagInput');
      var tag = input.value.trim();
      if (!tag || !activeContactJid) return;
      try {
        await fetch('/api/contacts/tag', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jid: activeContactJid, tag: tag }) });
        input.value = '';
        openContact(activeContactJid);
      } catch (e) {}
    }

    async function removeContactTag(tag) {
      if (!activeContactJid) return;
      try {
        await fetch('/api/contacts/tag', { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jid: activeContactJid, tag: tag }) });
        openContact(activeContactJid);
      } catch (e) {}
    }

    async function saveContactNotes() {
      if (!activeContactJid) return;
      var notes = document.getElementById('cmNotes').value;
      try {
        await fetch('/api/contacts/notes', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jid: activeContactJid, notes: notes }) });
        alert('Notes saved!');
      } catch (e) {}
    }

    async function deleteContact() {
      if (!activeContactJid || !confirm('Delete this contact?')) return;
      try {
        await fetch('/api/contacts/delete', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jid: activeContactJid }) });
        closeContactModal();
        loadContacts();
      } catch (e) {}
    }

    function dmContact() {
      if (!activeContactJid) return;
      var c = contactsData.find(function(x) { return x.jid === activeContactJid; });
      document.getElementById('dmTo').textContent = 'To: ' + (c ? (c.name || c.phone) : activeContactJid);
      document.getElementById('dmMsg').value = '';
      document.getElementById('dmModal').style.display = 'flex';
      document.getElementById('dmMsg').focus();
    }

    function closeDM() { document.getElementById('dmModal').style.display = 'none'; }

    async function sendDM() {
      var msg = document.getElementById('dmMsg').value.trim();
      if (!msg || !activeContactJid) return;
      // Find a connected socket
      try {
        var phone = activeContactJid.split('@')[0];
        var dmJid = phone + '@s.whatsapp.net';
        // Use the lead reply endpoint with a fake lead
        var r = await fetch('/api/broadcast', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chatIds: [dmJid], message: msg }) });
        var d = await r.json();
        if (d.sent > 0) { closeDM(); alert('DM sent!'); }
        else { alert('Failed to send: ' + (d.error || 'No connected account')); }
      } catch (e) { alert('Error: ' + e.message); }
    }

    async function exportContacts() {
      try {
        var groupId = document.getElementById('contactGroupFilter').value;
        var url = '/api/contacts/export' + (groupId ? '?group=' + encodeURIComponent(groupId) : '');
        var r = await fetch(url);
        var text = await r.text();
        var blob = new Blob([text], { type: 'text/csv' });
        var a = document.createElement('a'); a.href = URL.createObjectURL(blob);
        a.download = 'contacts-' + new Date().toISOString().slice(0, 10) + '.csv';
        a.click(); URL.revokeObjectURL(a.href);
      } catch (e) { alert('Export error: ' + e.message); }
    }
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
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if (p === '/' || p === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(HTML); return;
  }

  // Existing routes
  if (p === '/api/accounts' && req.method === 'GET') { json(res, handleAccounts()); return; }
  if (p === '/api/accounts' && req.method === 'POST') {
    try { const body = await readBody(req); json(res, await handleCreateAccount(body ? JSON.parse(body) : {})); }
    catch (e) { json(res, { error: e.message }, 500); } return;
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
  if (p === '/api/groups') { json(res, handleGroups()); return; }
  if (p === '/api/broadcast' && req.method === 'POST') {
    try { const body = await readBody(req); json(res, await handleBroadcast(JSON.parse(body))); }
    catch (e) { json(res, { error: e.message }, 500); } return;
  }

  // Lead routes
  if (p === '/api/leads' && req.method === 'GET') {
    json(res, handleGetLeads(params.get('since'))); return;
  }
  if (p === '/api/leads/stats') { json(res, handleLeadStats()); return; }
  if (p === '/api/leads/dismiss' && req.method === 'POST') {
    try { const body = await readBody(req); json(res, handleDismissLead(JSON.parse(body).id)); }
    catch (e) { json(res, { error: e.message }, 500); } return;
  }
  if (p === '/api/leads/dismiss-all' && req.method === 'POST') { json(res, handleDismissAll()); return; }
  if (p === '/api/leads/reply' && req.method === 'POST') {
    try { const body = await readBody(req); json(res, await handleReplyToLead(JSON.parse(body))); }
    catch (e) { json(res, { error: e.message }, 500); } return;
  }
  if (p === '/api/leads/export') {
    const csv = handleExportLeads();
    res.writeHead(200, { 'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename="leads.csv"' });
    res.end(csv); return;
  }

  // Keyword routes
  if (p === '/api/keywords' && req.method === 'GET') { json(res, handleGetKeywords()); return; }
  if (p === '/api/keywords' && req.method === 'PUT') {
    try { const body = await readBody(req); json(res, handleUpdateKeywords(JSON.parse(body))); }
    catch (e) { json(res, { error: e.message }, 500); } return;
  }

  // Contact routes
  if (p === '/api/contacts' && req.method === 'GET') {
    json(res, handleGetContacts(params.get('q'), params.get('group'), params.get('tag'), params.get('limit'), params.get('offset'))); return;
  }
  if (p === '/api/contacts/stats') { json(res, handleContactStats()); return; }
  if (p === '/api/contacts/detail' && req.method === 'GET') {
    const jid = params.get('jid');
    if (!jid) { json(res, { error: 'Missing jid' }, 400); return; }
    json(res, handleGetContactDetail(jid)); return;
  }
  if (p === '/api/contacts/tag' && req.method === 'POST') {
    try { const body = await readBody(req); json(res, handleAddContactTag(JSON.parse(body))); }
    catch (e) { json(res, { error: e.message }, 500); } return;
  }
  if (p === '/api/contacts/tag' && req.method === 'DELETE') {
    try { const body = await readBody(req); json(res, handleRemoveContactTag(JSON.parse(body))); }
    catch (e) { json(res, { error: e.message }, 500); } return;
  }
  if (p === '/api/contacts/notes' && req.method === 'PUT') {
    try { const body = await readBody(req); json(res, handleUpdateContactNotes(JSON.parse(body))); }
    catch (e) { json(res, { error: e.message }, 500); } return;
  }
  if (p === '/api/contacts/delete' && req.method === 'POST') {
    try { const body = await readBody(req); json(res, handleDeleteContact(JSON.parse(body).jid)); }
    catch (e) { json(res, { error: e.message }, 500); } return;
  }
  if (p === '/api/contacts/export') {
    const csv = handleExportContacts(params.get('group'));
    res.writeHead(200, { 'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename="contacts.csv"' });
    res.end(csv); return;
  }

  res.writeHead(404); res.end('Not found');
});

server.listen(PORT, '0.0.0.0', () => {
  const contactCount = stmts.totalContacts.get().count;
  console.log('WhatsApp Broadcast Pro v3 running on port ' + PORT);
  console.log('Accounts loaded: ' + accounts.size);
  console.log('Contacts in DB: ' + contactCount);
  console.log('Scanner: ' + (scannerEnabled ? 'ON' : 'OFF') + ' | Keywords: ' + keywords.en.length + ' EN, ' + keywords.he.length + ' HE');
});