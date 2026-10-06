/**
 * =====================================================================
 * SNAPGEN AI WRAPPER - ADMIN DASHBOARD & DUAL HYBRID AUTOMATION ENGINE
 * =====================================================================
 * Mode 1: Direct API (Super Cepat, Ultra Ringan untuk Skala 100+ User)
 * Mode 2: Browser Puppeteer (Full CCTV Streaming + Auto-Click Cloudflare)
 * =====================================================================
 */

require('dotenv').config();

const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs-extra');
const cors = require('cors');
const axios = require('axios');
const multer = require('multer');
const session = require('express-session');
const cron = require('node-cron');
const { v4: uuidv4 } = require('uuid');
const { Server } = require('socket.io');
const PQueue = require('p-queue').default;

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

/* ===================================================================
   KONFIGURASI ENVIRONMENT
=================================================================== */
const ENV = {
  PORT: process.env.PORT || 3000,
  NODE_ENV: process.env.NODE_ENV || 'development',
  ADMIN_USERNAME: process.env.ADMIN_USERNAME || 'admin',
  ADMIN_PASSWORD: process.env.ADMIN_PASSWORD || 'admin123',
  SESSION_SECRET: process.env.SESSION_SECRET || 'change_this_secret_key',
  PUBLIC_API_KEY: process.env.PUBLIC_API_KEY || 'sk_default_change_me',
  CAPTCHA_PROVIDER: process.env.CAPTCHA_PROVIDER || 'none',
  CAPTCHA_API_KEY: process.env.CAPTCHA_API_KEY || '',
  CAPTCHA_AUTO_SOLVE: process.env.CAPTCHA_AUTO_SOLVE === 'true',
  HEADLESS: process.env.HEADLESS !== 'false',
  PUPPETEER_EXECUTABLE_PATH: process.env.PUPPETEER_EXECUTABLE_PATH || null,
  BASE_URL: process.env.BASE_URL || `http://localhost:${process.env.PORT || 3000}`,
  QUEUE_CONCURRENCY: parseInt(process.env.QUEUE_CONCURRENCY || '3'),
  DOCKER_MODE: process.env.DOCKER_MODE === 'true'
};

/* ===================================================================
   DIREKTORI DATA
=================================================================== */
const DATA_DIR = path.join(__dirname, 'data');
const SESSIONS_DIR = path.join(__dirname, 'sessions');
const DOWNLOADS_DIR = path.join(__dirname, 'downloads');
const UPLOADS_DIR = path.join(__dirname, 'uploads');
const DEBUG_DIR = path.join(__dirname, 'debug');

fs.ensureDirSync(DATA_DIR);
fs.ensureDirSync(SESSIONS_DIR);
fs.ensureDirSync(DOWNLOADS_DIR);
fs.ensureDirSync(UPLOADS_DIR);
fs.ensureDirSync(DEBUG_DIR);

function ts() { return new Date().toISOString(); }
const logger = {
  info: (...a) => console.log(`[INFO ${ts()}]`, ...a),
  warn: (...a) => console.warn(`[WARN ${ts()}]`, ...a),
  error: (...a) => console.error(`[ERROR ${ts()}]`, ...a),
  success: (...a) => console.log(`[OK ${ts()}]`, ...a)
};

/* ===================================================================
   JSON DATABASE (FILE-BASED DENGAN LOCK)
=================================================================== */
const _locks = {};

async function dbRead(name, defaultValue) {
  const fp = path.join(DATA_DIR, `${name}.json`);
  if (!(await fs.pathExists(fp))) {
    await fs.writeJson(fp, defaultValue, { spaces: 2 });
    return JSON.parse(JSON.stringify(defaultValue));
  }
  try {
    return await fs.readJson(fp);
  } catch (e) {
    logger.error(`Gagal baca ${name}.json, reset ke default`, e.message);
    await fs.writeJson(fp, defaultValue, { spaces: 2 });
    return JSON.parse(JSON.stringify(defaultValue));
  }
}

async function dbWrite(name, data) {
  while (_locks[name]) await new Promise(r => setTimeout(r, 15));
  _locks[name] = true;
  try {
    const fp = path.join(DATA_DIR, `${name}.json`);
    await fs.writeJson(fp, data, { spaces: 2 });
  } finally {
    _locks[name] = false;
  }
}

(async () => {
  await dbRead('accounts', []);
  await dbRead('settings', {
    captchaProvider: 'none',
    captchaApiKey: '',
    captchaAutoSolve: false,
    defaultProxy: { host: '', port: '', username: '', password: '', type: 'http' }
  });
  await dbRead('stats', { videoFromDashboard: 0, videoFromApi: 0, imageFromDashboard: 0, imageFromApi: 0 });
  await dbRead('tasks', []);
})();

function genTaskId(prefix = 'TASK') {
  return `${prefix}_${Date.now()}_${uuidv4().slice(0, 8)}`;
}

function buildProxyUrl(proxy) {
  if (!proxy || !proxy.host) return null;
  const protocol = proxy.type === 'socks5' ? 'socks5' : (proxy.type || 'http');
  return `${protocol}://${proxy.host}:${proxy.port}`;
}

async function getEffectiveProxy(account) {
  if (account && account.proxy && account.proxy.host) {
    return account.proxy;
  }
  try {
    const settings = await dbRead('settings', {});
    if (settings && settings.defaultProxy && settings.defaultProxy.host) {
      return settings.defaultProxy;
    }
  } catch (e) {}
  return null;
}

/* ===================================================================
   ACCOUNT MANAGER
=================================================================== */
const AccountManager = {
  async getAll() { return dbRead('accounts', []); },
  async getById(id) {
    const all = await this.getAll();
    return all.find(a => a.id === id) || null;
  },
  async create({ email, password, proxy }) {
    const all = await this.getAll();
    const id = `acc_${Date.now()}`;
    const account = {
      id, email, password,
      proxy: proxy && proxy.host ? proxy : null,
      statusProxy: 'unknown',
      statusCookie: 'expired',
      creditsLeft: 0,
      isUnlimited: false,
      bearerToken: null,
      sessionFile: `./sessions/${id}.json`,
      lastLogin: null,
      lastCheck: null,
      createdAt: new Date().toISOString()
    };
    all.push(account);
    await dbWrite('accounts', all);
    logger.info('Akun baru ditambahkan:', email);
    return account;
  },
  async update(id, patch) {
    const all = await this.getAll();
    const idx = all.findIndex(a => a.id === id);
    if (idx === -1) throw new Error('Akun tidak ditemukan');
    all[idx] = { ...all[idx], ...patch };
    await dbWrite('accounts', all);
    return all[idx];
  },
  async remove(id) {
    const all = await this.getAll();
    const filtered = all.filter(a => a.id !== id);
    await dbWrite('accounts', filtered);
    const sp = path.join(SESSIONS_DIR, `${id}.json`);
    await fs.remove(sp);
    return true;
  },
  async saveSession(id, sessionData) {
    const sp = path.join(SESSIONS_DIR, `${id}.json`);
    await fs.writeJson(sp, sessionData, { spaces: 2 });
    return this.update(id, { 
      statusCookie: 'active', 
      bearerToken: sessionData.bearerToken || null,
      lastLogin: new Date().toISOString() 
    });
  },
  async loadSession(id) {
    const sp = path.join(SESSIONS_DIR, `${id}.json`);
    if (!(await fs.pathExists(sp))) return null;
    return fs.readJson(sp);
  },
  _rrIndex: 0,
  async getOptimalAccount(costRequired = 1) {
    const all = await this.getAll();
    const eligible = all.filter(a =>
      a.statusCookie === 'active' &&
      (a.isUnlimited || a.creditsLeft >= costRequired)
    );
    if (eligible.length === 0) return null;
    this._rrIndex = (this._rrIndex + 1) % eligible.length;
    return eligible[this._rrIndex];
  }
};

/* ===================================================================
   SOCKET.IO SYSTEM
=================================================================== */
let ioInstance = null;
const pendingOtpResolvers = new Map();

function initSocket(io) {
  ioInstance = io;
  io.on('connection', (socket) => {
    logger.info('Dashboard terhubung:', socket.id);
    socket.on('submit-otp', ({ accountId, otp }) => {
      if (pendingOtpResolvers.has(accountId)) {
        pendingOtpResolvers.get(accountId)(otp);
        pendingOtpResolvers.delete(accountId);
      }
    });
  });
}

function requestOtpFromAdmin(accountId, email) {
  if (!ioInstance) return Promise.reject(new Error('Socket belum siap'));
  ioInstance.emit('otp-required', { accountId, email });
  return new Promise((resolve, reject) => {
    pendingOtpResolvers.set(accountId, resolve);
    setTimeout(() => {
      if (pendingOtpResolvers.has(accountId)) {
        pendingOtpResolvers.delete(accountId);
        reject(new Error('Timeout menunggu input OTP dari Admin'));
      }
    }, 5 * 60 * 1000);
  });
}

function emitAccountUpdate(account) { if (ioInstance) ioInstance.emit('account-updated', account); }
function emitStatsUpdate(stats) { if (ioInstance) ioInstance.emit('stats-updated', stats); }
function emitHealthStatus(status) { if (ioInstance) ioInstance.emit('health-status', status); }
function emitProgress(taskId, data) { if (ioInstance) ioInstance.emit('generation-progress', { taskId, ...data }); }
function emitLog(message) { if (ioInstance) ioInstance.emit('system-log', { message, time: new Date().toISOString() }); }

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/* ===================================================================
   CAPTCHA SOLVER & DETECTION
=================================================================== */
const CAPTCHA_ENDPOINTS = {
  '2captcha': 'https://api.2captcha.com',
  'capsolver': 'https://api.capsolver.com',
  'anticaptcha': 'https://api.anti-captcha.com',
  'nextcaptcha': 'https://api.nextcaptcha.com'
};

async function solveCaptcha(provider, apiKey, { type, siteKey, pageUrl }) {
  const base = CAPTCHA_ENDPOINTS[provider] || CAPTCHA_ENDPOINTS['2captcha'];
  const taskType = type === 'turnstile' ? 'TurnstileTaskProxyless' : 'HCaptchaTaskProxyless';

  const createRes = await axios.post(`${base}/createTask`, {
    clientKey: apiKey,
    task: { type: taskType, websiteURL: pageUrl, websiteKey: siteKey }
  });

  const taskId = createRes.data.taskId;
  if (!taskId) throw new Error('Gagal membuat task captcha: ' + JSON.stringify(createRes.data));

  for (let i = 0; i < 30; i++) {
    await sleep(5000);
    const resultRes = await axios.post(`${base}/getTaskResult`, { clientKey: apiKey, taskId });
    if (resultRes.data.status === 'ready') {
      return resultRes.data.solution.token || resultRes.data.solution.gRecaptchaResponse;
    }
  }
  throw new Error('Timeout menyelesaikan captcha');
}

async function detectAndSolveCaptcha(page, provider, apiKey) {
  if (!provider || provider === 'none' || !apiKey) return false;
  const siteInfo = await page.evaluate(() => {
    const turnstileEl = document.querySelector('[data-sitekey]');
    const iframeTurnstile = document.querySelector('iframe[src*="challenges.cloudflare.com"]');
    if (turnstileEl) return { type: 'turnstile', siteKey: turnstileEl.getAttribute('data-sitekey') };
    if (iframeTurnstile) {
      try {
        const url = new URL(iframeTurnstile.src);
        return { type: 'turnstile', siteKey: url.searchParams.get('sitekey') };
      } catch (e) { return null; }
    }
    return null;
  });

  if (!siteInfo || !siteInfo.siteKey) return false;
  const token = await solveCaptcha(provider, apiKey, { type: siteInfo.type, siteKey: siteInfo.siteKey, pageUrl: page.url() });

  await page.evaluate((tok, type) => {
    const fieldName = type === 'turnstile' ? 'cf-turnstile-response' : 'h-captcha-response';
    let el = document.querySelector(`[name="${fieldName}"]`);
    if (!el) {
      el = document.createElement('textarea');
      el.name = fieldName;
      el.style.display = 'none';
      document.body.appendChild(el);
    }
    el.value = tok;
    if (window.turnstileCallback) window.turnstileCallback(tok);
  }, token, siteInfo.type);

  return true;
}

/* ===================================================================
   RADAR AUTO-KLIK CLOUDFLARE TURNSTILE (PRESISI TINGGI)
=================================================================== */
async function solveTurnstileWidget(page, email) {
  try {
    for (const f of page.frames()) {
      const u = (f.url() || '').toLowerCase();
      if (u.includes('challenges.cloudflare.com') || u.includes('turnstile')) {
        const frameEl = await f.frameElement();
        if (frameEl) {
          const box = await frameEl.boundingBox();
          if (box && box.width > 0 && box.height > 0) {
            const clickX = box.x + 30;
            const clickY = box.y + (box.height / 2);
            emitLog(`[${email}] 🎯 Menembak mouse fisik ke kotak Cloudflare (${Math.round(clickX)}, ${Math.round(clickY)})...`);
            await page.mouse.move(clickX, clickY, { steps: 5 });
            await sleep(100);
            await page.mouse.down();
            await sleep(150);
            await page.mouse.up();
            await sleep(2000);
            return;
          }
        }
      }
    }
  } catch (e) {}
}

/* ===================================================================
   BROWSER MANAGER (PUPPETEER-EXTRA + STEALTH)
=================================================================== */
const activeBrowsers = new Map();

async function launchBrowserForAccount(account) {
  if (activeBrowsers.has(account.id)) {
    const existing = activeBrowsers.get(account.id);
    if (existing.isConnected()) return existing;
    activeBrowsers.delete(account.id);
  }

  const args = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    '--disable-gpu',
    '--window-size=1366,768',
    '--disable-software-rasterizer',
    '--disable-accelerated-2d-canvas',
    '--disable-background-networking',
    '--disable-extensions',
    '--ignore-certificate-errors',
    '--ignore-certificate-errors-spki-list',
    '--js-flags="--max-old-space-size=256"'
  ];

  const effectiveProxy = await getEffectiveProxy(account);
  const proxyUrl = buildProxyUrl(effectiveProxy);
  if (proxyUrl) args.push(`--proxy-server=${proxyUrl}`);

  const browser = await puppeteer.launch({
    headless: ENV.HEADLESS ? 'new' : false,
    args,
    executablePath: ENV.PUPPETEER_EXECUTABLE_PATH || undefined,
    defaultViewport: { width: 1366, height: 768 }
  });

  activeBrowsers.set(account.id, browser);
  browser.on('disconnected', () => activeBrowsers.delete(account.id));
  return browser;
}

async function newPageWithProxyAuth(browser, account) {
  const page = await browser.newPage();
  const effectiveProxy = await getEffectiveProxy(account);
  if (effectiveProxy && effectiveProxy.username) {
    await page.authenticate({ username: effectiveProxy.username, password: effectiveProxy.password });
  }
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36');

  await page.setRequestInterception(true);
  page.on('request', (req) => {
    const type = req.resourceType();
    if (['media', 'font'].includes(type) || req.url().includes('analytics') || req.url().includes('tracking')) {
      req.abort();
    } else {
      req.continue();
    }
  });
  return page;
}

async function checkProxyAlive(page) {
  try {
    const resp = await page.goto('https://api.ipify.org?format=json', { timeout: 15000, waitUntil: 'domcontentloaded' });
    return resp && resp.ok();
  } catch (e) {
    return false;
  }
}

async function captureDebugSnapshot(page, account, label) {
  try {
    const screenshotPath = path.join(DEBUG_DIR, `${account.id}_${label}.png`);
    await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
    emitLog(`[${account.email}] 📸 Snapshot: /debug/${account.id}_${label}.png`);
  } catch (e) {}
}

/* ===================================================================
   AUTO-LOGIN FLOW
=================================================================== */
const LOGIN_URL = 'https://snapgen.ai/auth/login';

async function autoLogin(account) {
  emitLog(`[${account.email}] Memulai Auto-Login...`);
  const browser = await launchBrowserForAccount(account);
  const page = await newPageWithProxyAuth(browser, account);

  const proxyAlive = await checkProxyAlive(page);
  await AccountManager.update(account.id, { statusProxy: proxyAlive ? 'online' : 'offline' });
  emitAccountUpdate(await AccountManager.getById(account.id));

  await page.goto(LOGIN_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  await sleep(3000);

  const EMAIL_SELECTOR = 'input[type="email"], input[name="email"], input[placeholder*="email" i]';
  const PASSWORD_SELECTOR = 'input[type="password"], input[name="password"]';
  const OTP_SELECTOR = 'input[name="otp"], input[autocomplete="one-time-code"], input[placeholder*="code" i]';

  await page.waitForSelector(EMAIL_SELECTOR, { timeout: 45000, visible: true });
  await page.click(EMAIL_SELECTOR);
  await page.type(EMAIL_SELECTOR, account.email, { delay: 60 });
  await page.click(PASSWORD_SELECTOR);
  await page.type(PASSWORD_SELECTOR, account.password, { delay: 60 });

  await solveTurnstileWidget(page, account.email);

  await page.evaluate(() => {
    const buttons = Array.from(document.querySelectorAll('button'));
    const target = buttons.find(b => /continue|sign in|log ?in|masuk/i.test(b.textContent));
    if (target) target.click();
    else document.querySelector('button[type="submit"]')?.click();
  });

  await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 30000 }).catch(() => {});
  await sleep(2000);

  const otpField = await page.$(OTP_SELECTOR);
  if (otpField) {
    await AccountManager.update(account.id, { statusCookie: 'need_otp' });
    emitAccountUpdate(await AccountManager.getById(account.id));
    emitLog(`[${account.email}] Menunggu input OTP dari Admin...`);

    const otp = await requestOtpFromAdmin(account.id, account.email);
    await page.type(OTP_SELECTOR, otp, { delay: 80 });
    await page.evaluate(() => {
      const btn = Array.from(document.querySelectorAll('button')).find(b => /continue|verify|submit/i.test(b.textContent));
      if (btn) btn.click();
    });
    await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 30000 }).catch(() => {});
  }

  const cookies = await page.cookies();
  const localStorageData = await page.evaluate(() => {
    const data = {};
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i);
      data[k] = localStorage.getItem(k);
    }
    return data;
  });

  let bearerToken = localStorageData['access_token'] || localStorageData['token'] || null;
  let parsedCredit = null;

  try {
    const authStoreRaw = localStorageData['authStore'];
    if (authStoreRaw) {
      const parsed = JSON.parse(authStoreRaw);
      if (parsed.access_token) bearerToken = parsed.access_token;
      if (parsed.user && parsed.user.user_credit) parsedCredit = parsed.user.user_credit;
    }
  } catch (e) {}

  const sessionData = { cookies, localStorage: localStorageData, bearerToken, savedAt: new Date().toISOString() };
  await AccountManager.saveSession(account.id, sessionData);

  if (parsedCredit) {
    await AccountManager.update(account.id, {
      creditsLeft: parsedCredit.available_credit || 0,
      isUnlimited: false
    });
  }

  emitLog(`[${account.email}] ✅ Login berhasil! Kredit: ${parsedCredit ? parsedCredit.available_credit : 'unknown'}`);
  emitAccountUpdate(await AccountManager.getById(account.id));

  await page.close().catch(() => {});
  return sessionData;
}

async function restoreSessionToPage(page, account) {
  const session = await AccountManager.loadSession(account.id);
  if (!session) return false;

  await page.goto('https://snapgen.ai', { waitUntil: 'domcontentloaded', timeout: 30000 });
  if (session.cookies && session.cookies.length) await page.setCookie(...session.cookies);
  if (session.localStorage) {
    await page.evaluate((data) => {
      for (const k in data) localStorage.setItem(k, data[k]);
    }, session.localStorage);
  }
  return true;
}

/* ===================================================================
   HELPER AUTO-NUKE POP-UP & POLICY
=================================================================== */
async function clearOverlaysAndCheckboxes(page, email) {
  await page.evaluate(() => {
    const clickables = document.querySelectorAll('button, a, [role="button"]');
    clickables.forEach(btn => {
      const txt = (btn.innerText || '').toLowerCase().trim();
      const aria = (btn.getAttribute('aria-label') || '').toLowerCase();
      if (txt === "don't show again" || txt === "ok" || txt === "got it" || txt === "dismiss" || aria.includes('close')) {
        if (!btn.disabled) btn.click();
      }
    });

    const elements = Array.from(document.querySelectorAll('label, p, span, div'));
    const policyEl = elements.find(el => el.innerText && el.innerText.toLowerCase().includes('i understand that intentionally'));
    if (policyEl) {
      const container = policyEl.closest('label') || policyEl.parentElement;
      if (container) {
        const customCb = container.querySelector('[role="checkbox"]');
        if (customCb && customCb.getAttribute('aria-checked') !== 'true') customCb.click();
        const nativeCb = container.querySelector('input[type="checkbox"]');
        if (nativeCb && !nativeCb.checked) {
          nativeCb.click();
          nativeCb.dispatchEvent(new Event('change', { bubbles: true }));
        }
      }
    }
  }).catch(() => {});
  await sleep(1000);
}

async function downloadRemoteFile(page, url, destPath) {
  const dataUrl = await page.evaluate(async (fileUrl) => {
    const res = await fetch(fileUrl);
    const blob = await res.blob();
    return new Promise((resolve) => {
      const reader = new FileReader();
      reader.onloadend = () => resolve(reader.result);
      reader.readAsDataURL(blob);
    });
  }, url);
  const base64Data = dataUrl.split(',')[1];
  await fs.writeFile(destPath, Buffer.from(base64Data, 'base64'));
}

/* ===================================================================
   MODE 1: DIRECT API ENGINE (ULTRA RINGAN UNTUK 100+ USER)
=================================================================== */
async function generateViaDirectApi(type, account, params, taskId) {
  emitLog(`[${account.email}] ⚡ Menjalankan Direct API (${type.toUpperCase()})...`);
  emitProgress(taskId, { status: 'processing', progress: 25 });

  const session = await AccountManager.loadSession(account.id);
  const token = (session && session.bearerToken) || account.bearerToken;

  if (!token) {
    emitLog(`[${account.email}] ⚠️ Bearer token tidak ditemukan, beralih ke Mode Browser...`);
    return type === 'video' ? generateVideoOnPage(account, params, taskId) : generateImageOnPage(account, params, taskId);
  }

  const effectiveProxy = await getEffectiveProxy(account);
  const axiosConfig = {
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    },
    timeout: 30000
  };

  try {
    const endpoint = type === 'video' ? 'https://snapgen.ai/api/v1/video/create' : 'https://snapgen.ai/api/v1/image/create';
    const payload = { ...params };
    const res = await axios.post(endpoint, payload, axiosConfig);

    if (res.data && (res.data.mediaUrl || res.data.url)) {
      const remoteUrl = res.data.mediaUrl || res.data.url;
      const ext = type === 'video' ? 'mp4' : 'png';
      const fileName = `${type}_${taskId}.${ext}`;
      const localPath = path.join(DOWNLOADS_DIR, fileName);

      const dl = await axios.get(remoteUrl, { responseType: 'arraybuffer' });
      await fs.writeFile(localPath, dl.data);

      emitLog(`[${account.email}] 🎉 Direct API Sukses!`);
      return { mediaUrl: `/downloads/${fileName}`, previewUrl: remoteUrl };
    }
  } catch (apiErr) {
    emitLog(`[${account.email}] ℹ️ Direct API butuh otentikasi browser. Mengalihkan ke Browser CCTV...`);
  }

  // Fallback otomatis ke browser jika API butuh sinkronisasi DOM
  return type === 'video' ? generateVideoOnPage(account, params, taskId) : generateImageOnPage(account, params, taskId);
}

/* ===================================================================
   MODE 2: PUPPETEER BROWSER ENGINE (ADA CCTV LIVE VIEW)
=================================================================== */
const IMAGE_GEN_URL = 'https://snapgen.ai/app/imagen';
const VIDEO_GEN_URL = 'https://snapgen.ai/app/video-gen/veo';

async function generateImageOnPage(account, params, taskId) {
  const browser = await launchBrowserForAccount(account);
  const page = await newPageWithProxyAuth(browser, account);

  const streamLive = async () => {
    while (!page.isClosed()) {
      try {
        const b64 = await page.screenshot({ encoding: 'base64', type: 'jpeg', quality: 20 });
        if (ioInstance) ioInstance.emit('live-view', { taskId, frame: `data:image/jpeg;base64,${b64}` });
        await sleep(1500);
      } catch (e) { break; }
    }
  };

  await restoreSessionToPage(page, account);
  await page.goto(IMAGE_GEN_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  streamLive();
  emitProgress(taskId, { status: 'queued', progress: 5 });

  emitLog(`[${account.email}] Memilih Provider & Model...`);
  const clickText = async (txt) => {
    if (!txt) return;
    await page.evaluate((textToFind) => {
      const els = Array.from(document.querySelectorAll('button, [role="combobox"], [role="option"], [role="tab"]'));
      const target = els.find(e => e.innerText && e.innerText.trim().toLowerCase().includes(textToFind.toLowerCase().split(' ')[0]));
      if (target) target.click();
    }, txt);
  };

  await clickText(params.provider);
  await sleep(500);
  await clickText(params.model);
  await sleep(1000);

  await clearOverlaysAndCheckboxes(page, account.email);

  emitLog(`[${account.email}] Mengetik prompt...`);
  const promptSelector = 'textarea[placeholder*="image" i]';
  await page.waitForSelector(promptSelector, { timeout: 30000 });
  await page.click(promptSelector, { clickCount: 3 });
  await page.keyboard.press('Backspace');
  await page.type(promptSelector, params.prompt, { delay: 40 });
  await page.keyboard.press('Space');
  await sleep(1000);

  const clickAria = async (lbl) => {
    if (!lbl) return;
    const btn = await page.$(`button[aria-label="${lbl}"]`);
    if (btn) await btn.click().catch(() => {});
  };

  await clickAria(params.aspect_ratio);
  await clickAria(params.resolution);

  if (params.imageReference && params.imageReference.localPath) {
    const fileInputs = await page.$$('input[type="file"]');
    if (fileInputs.length > 0) await fileInputs[0].uploadFile(params.imageReference.localPath).catch(() => {});
  }

  await clearOverlaysAndCheckboxes(page, account.email);
  emitProgress(taskId, { status: 'processing', progress: 20 });

  const existingImages = await page.evaluate(() => Array.from(document.querySelectorAll('img')).map(i => i.src));

  emitLog(`[${account.email}] MENGKLIK TOMBOL GENERATE!`);
  const btnBox = await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    const target = btns.find(b => {
      const txt = (b.innerText || '').toLowerCase();
      return txt.includes('generate') && !b.disabled && b.getAttribute('aria-disabled') !== 'true';
    });
    if (!target) return null;
    target.scrollIntoView({ block: 'center' });
    const rect = target.getBoundingClientRect();
    return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2 };
  });

  if (btnBox) {
    await page.mouse.click(btnBox.x, btnBox.y);
  } else {
    await page.evaluate(() => {
      const b = Array.from(document.querySelectorAll('button')).find(x => (x.innerText || '').toLowerCase().includes('generate'));
      if (b) b.click();
    }).catch(() => {});
  }

  let progress = 20, completed = false, resultUrl = null;

  for (let i = 0; i < 60; i++) {
    await sleep(3000);

    // RADAR AKTIF: Tembak kotak Cloudflare jika muncul di tengah antrean
    await solveTurnstileWidget(page, account.email);

    const pct = await page.evaluate(() => {
      const match = document.body.innerText.match(/(\d+)%/);
      return match ? parseInt(match[1]) : null;
    });
    if (pct && pct > progress) progress = pct;

    const currentImages = await page.evaluate(() => Array.from(document.querySelectorAll('img')).map(i => i.src));
    const newImage = currentImages.find(src =>
      src &&
      !existingImages.includes(src) &&
      !src.startsWith('data:') &&
      !src.includes('avatar') &&
      !src.includes('logo') &&
      src.includes('blob')
    );

    if (newImage) {
      resultUrl = newImage;
      completed = true;
      progress = 100;
      emitLog(`[${account.email}] 🎉 HASIL AI DITEMUKAN!`);
    }

    emitProgress(taskId, { status: completed ? 'completed' : 'processing', progress });
    if (completed) break;
  }

  if (!completed || !resultUrl) {
    await captureDebugSnapshot(page, account, `FAILED-IMG`);
    await page.close().catch(() => {});
    throw new Error(`Gagal mendapatkan gambar. Cek foto debug di dashboard.`);
  }

  const fileName = `image_${taskId}.png`;
  const localPath = path.join(DOWNLOADS_DIR, fileName);
  await downloadRemoteFile(page, resultUrl, localPath);
  await page.close().catch(() => {});

  return { mediaUrl: `/downloads/${fileName}`, previewUrl: resultUrl };
}

async function generateVideoOnPage(account, params, taskId) {
  const browser = await launchBrowserForAccount(account);
  const page = await newPageWithProxyAuth(browser, account);

  const streamLive = async () => {
    while (!page.isClosed()) {
      try {
        const b64 = await page.screenshot({ encoding: 'base64', type: 'jpeg', quality: 20 });
        if (ioInstance) ioInstance.emit('live-view', { taskId, frame: `data:image/jpeg;base64,${b64}` });
        await sleep(1500);
      } catch (e) { break; }
    }
  };

  await restoreSessionToPage(page, account);
  await page.goto(VIDEO_GEN_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  streamLive();
  emitProgress(taskId, { status: 'queued', progress: 5 });

  const clickText = async (txt) => {
    if (!txt) return;
    await page.evaluate((textToFind) => {
      const els = Array.from(document.querySelectorAll('button, [role="combobox"], [role="option"], [role="tab"]'));
      const target = els.find(e => e.innerText && e.innerText.trim().toLowerCase().includes(textToFind.toLowerCase().split(' ')[0]));
      if (target) target.click();
    }, txt);
  };

  await clickText(params.provider);
  await sleep(500);
  await clickText(params.model);
  await sleep(1000);

  await clearOverlaysAndCheckboxes(page, account.email);

  const promptSelector = 'textarea[placeholder*="video" i]';
  await page.waitForSelector(promptSelector, { timeout: 30000 });
  await page.click(promptSelector, { clickCount: 3 });
  await page.keyboard.press('Backspace');
  await page.type(promptSelector, params.prompt, { delay: 40 });
  await page.keyboard.press('Space');
  await sleep(1000);

  const clickAria = async (lbl) => {
    if (!lbl) return;
    const btn = await page.$(`button[aria-label="${lbl}"]`);
    if (btn) await btn.click().catch(() => {});
  };

  await clickAria(params.orientation);
  await clickAria(params.resolution);
  await clickAria(String(params.duration));

  await clearOverlaysAndCheckboxes(page, account.email);
  emitProgress(taskId, { status: 'processing', progress: 15 });

  const existingVideos = await page.evaluate(() => Array.from(document.querySelectorAll('video')).map(v => v.src));

  emitLog(`[${account.email}] MENGKLIK TOMBOL GENERATE VIDEO!`);
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    const target = btns.find(b => (b.innerText || '').toLowerCase().includes('generate') && !b.disabled);
    if (target) target.click();
  }).catch(() => {});

  let progress = 15, completed = false, resultUrl = null;

  for (let i = 0; i < 120; i++) {
    await sleep(5000);
    await solveTurnstileWidget(page, account.email);

    const pct = await page.evaluate(() => {
      const match = document.body.innerText.match(/(\d+)%/);
      return match ? parseInt(match[1]) : null;
    });
    if (pct && pct > progress) progress = pct;

    const currentVideos = await page.evaluate(() => Array.from(document.querySelectorAll('video')).map(v => v.src));
    const newVideo = currentVideos.find(src => src && !existingVideos.includes(src) && src.includes('blob'));

    if (newVideo) {
      resultUrl = newVideo;
      completed = true;
      progress = 100;
      emitLog(`[${account.email}] 🎉 HASIL VIDEO DITEMUKAN!`);
    }

    emitProgress(taskId, { status: completed ? 'completed' : 'processing', progress });
    if (completed) break;
  }

  if (!completed || !resultUrl) {
    await captureDebugSnapshot(page, account, `FAILED-VID`);
    await page.close().catch(() => {});
    throw new Error(`Gagal mendapatkan video.`);
  }

  const fileName = `video_${taskId}.mp4`;
  const localPath = path.join(DOWNLOADS_DIR, fileName);
  await downloadRemoteFile(page, resultUrl, localPath);
  await page.close().catch(() => {});

  return { mediaUrl: `/downloads/${fileName}`, previewUrl: resultUrl };
}

/* ===================================================================
   HEALTH CHECK
=================================================================== */
async function checkAccountHealth(account) {
  try {
    const browser = await launchBrowserForAccount(account);
    const page = await newPageWithProxyAuth(browser, account);

    const proxyAlive = await checkProxyAlive(page);
    await AccountManager.update(account.id, { statusProxy: proxyAlive ? 'online' : 'offline' });

    const restored = await restoreSessionToPage(page, account);
    if (!restored) {
      await page.close().catch(() => {});
      return autoLogin(account);
    }

    await page.goto('https://snapgen.ai/app', { waitUntil: 'networkidle2', timeout: 30000 });
    const isLoggedIn = !page.url().includes('/login');

    if (!isLoggedIn) {
      await page.close().catch(() => {});
      return autoLogin(account);
    }

    await AccountManager.update(account.id, { statusCookie: 'active', lastCheck: new Date().toISOString() });
    emitAccountUpdate(await AccountManager.getById(account.id));
    await page.close().catch(() => {});
  } catch (err) {}
}

async function runHealthCheckAll() {
  const accounts = await AccountManager.getAll();
  for (const acc of accounts) await checkAccountHealth(acc);
  const refreshed = await AccountManager.getAll();
  const anyActive = refreshed.some(a => a.statusCookie === 'active');
  emitHealthStatus({ videoGen: anyActive, imageGen: anyActive });
}

/* ===================================================================
   QUEUE MANAGER
=================================================================== */
const genQueue = new PQueue({ concurrency: ENV.QUEUE_CONCURRENCY });

function estimateCost(type, params) {
  if (type === 'video') {
    let base = 8;
    if (params.resolution === '1080p') base += 4;
    if (parseInt(params.duration) >= 15) base += 4;
    return base;
  }
  return params.resolution === '4K' ? 4 : 2;
}

async function upsertTask(taskId, patch) {
  const tasks = await dbRead('tasks', []);
  const idx = tasks.findIndex(t => t.taskId === taskId);
  if (idx === -1) tasks.push({ taskId, ...patch });
  else tasks[idx] = { ...tasks[idx], ...patch };
  await dbWrite('tasks', tasks);
}

async function incrementStat(key) {
  const stats = await dbRead('stats', {});
  stats[key] = (stats[key] || 0) + 1;
  await dbWrite('stats', stats);
  emitStatsUpdate(stats);
}

async function enqueueGenerationJob(type, params, source = 'api') {
  const cost = estimateCost(type, params);
  const taskId = genTaskId(type.toUpperCase());
  const engineMode = params.engineMode || 'browser';

  await upsertTask(taskId, { taskId, type, status: 'queued', cost, source, engineMode, createdAt: new Date().toISOString() });

  genQueue.add(async () => {
    try {
      const account = await AccountManager.getOptimalAccount(cost);
      if (!account) {
        const errMsg = `Tidak ada akun aktif yang mencukupi untuk biaya ${cost} kredit.`;
        await upsertTask(taskId, { status: 'failed', error: errMsg });
        emitProgress(taskId, { status: 'failed', progress: 0, error: errMsg });
        return;
      }

      await upsertTask(taskId, { status: 'processing', accountId: account.id });

      let result;
      if (engineMode === 'direct_api') {
        result = await generateViaDirectApi(type, account, params, taskId);
      } else {
        result = type === 'video' ? await generateVideoOnPage(account, params, taskId) : await generateImageOnPage(account, params, taskId);
      }

      await incrementStat(type === 'video' ? (source === 'dashboard' ? 'videoFromDashboard' : 'videoFromApi') : (source === 'dashboard' ? 'imageFromDashboard' : 'imageFromApi'));

      if (!account.isUnlimited) {
        await AccountManager.update(account.id, { creditsLeft: Math.max(0, account.creditsLeft - cost) });
      }
      emitAccountUpdate(await AccountManager.getById(account.id));

      await upsertTask(taskId, {
        status: 'completed',
        mediaUrl: result.mediaUrl,
        previewUrl: result.previewUrl,
        completedAt: new Date().toISOString()
      });

      emitProgress(taskId, { status: 'completed', progress: 100, mediaUrl: result.mediaUrl });
      if (params.webhookUrl) {
        axios.post(params.webhookUrl, { taskId, status: 'completed', mediaUrl: `${ENV.BASE_URL}${result.mediaUrl}` }).catch(() => {});
      }
    } catch (err) {
      await upsertTask(taskId, { status: 'failed', error: err.message });
      emitProgress(taskId, { status: 'failed', progress: 0, error: err.message });
    }
  });

  return { taskId, cost };
}

async function getTaskStatus(taskId) {
  const tasks = await dbRead('tasks', []);
  return tasks.find(t => t.taskId === taskId) || null;
}

/* ===================================================================
   EXPRESS SERVER SETUP & ROUTES
=================================================================== */
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
initSocket(io);

app.set('trust proxy', 1);
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true }));

app.use(session({
  name: 'snapgen_admin_sid',
  secret: ENV.SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 24 * 60 * 60 * 1000, secure: ENV.NODE_ENV === 'production', sameSite: 'lax' }
}));

app.use(express.static(path.join(__dirname, 'public')));
app.use('/downloads', express.static(DOWNLOADS_DIR));
app.use('/uploads', express.static(UPLOADS_DIR));
app.use('/debug', express.static(DEBUG_DIR));

const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, UPLOADS_DIR),
    filename: (req, file, cb) => cb(null, `${Date.now()}_${file.originalname}`)
  }),
  limits: { fileSize: 25 * 1024 * 1024 }
});

function requireAdminAuth(req, res, next) {
  if (req.session && req.session.isAdmin) return next();
  return res.status(401).json({ success: false, message: 'Unauthorized' });
}

function requirePublicApiKey(req, res, next) {
  const key = req.headers['x-api-key'];
  if (key && key === ENV.PUBLIC_API_KEY) return next();
  return res.status(401).json({ success: false, message: 'Invalid API Key' });
}

/* AUTH ROUTES */
app.post('/api/admin/login', (req, res) => {
  const { username, password } = req.body || {};
  if (username === ENV.ADMIN_USERNAME && password === ENV.ADMIN_PASSWORD) {
    req.session.isAdmin = true;
    return res.json({ success: true });
  }
  res.status(401).json({ success: false, message: 'Password salah' });
});

app.post('/api/admin/logout', (req, res) => {
  req.session.destroy(() => {});
  res.json({ success: true });
});

app.get('/api/admin/check-session', (req, res) => {
  res.json({ loggedIn: !!(req.session && req.session.isAdmin) });
});

/* ADMIN ROUTER */
const adminRouter = express.Router();
adminRouter.use(requireAdminAuth);

adminRouter.get('/accounts', async (req, res) => {
  const accounts = await AccountManager.getAll();
  res.json({ success: true, data: accounts.map(({ password, ...r }) => r) });
});

adminRouter.post('/accounts', async (req, res) => {
  const { email, password, proxy } = req.body || {};
  if (!email || !password) return res.status(400).json({ success: false, message: 'Wajib diisi' });
  const account = await AccountManager.create({ email, password, proxy });
  emitAccountUpdate(account);
  autoLogin(account).catch(() => {});
  res.json({ success: true, data: account });
});

adminRouter.delete('/accounts/:id', async (req, res) => {
  await AccountManager.remove(req.params.id);
  res.json({ success: true });
});

adminRouter.post('/accounts/:id/relogin', async (req, res) => {
  const account = await AccountManager.getById(req.params.id);
  if (!account) return res.status(404).json({ success: false });
  autoLogin(account).catch(() => {});
  res.json({ success: true, message: 'Re-login berjalan di background' });
});

adminRouter.post('/accounts/:id/check-credits', async (req, res) => {
  const account = await AccountManager.getById(req.params.id);
  if (!account) return res.status(404).json({ success: false });
  checkAccountHealth(account).catch(() => {});
  res.json({ success: true, message: 'Mengecek kredit...' });
});

adminRouter.get('/accounts/:id/cookie', async (req, res) => {
  const session = await AccountManager.loadSession(req.params.id);
  res.json({ success: true, data: session || {} });
});

adminRouter.get('/settings', async (req, res) => {
  res.json({ success: true, data: await dbRead('settings', {}) });
});

adminRouter.post('/settings', async (req, res) => {
  const current = await dbRead('settings', {});
  const updated = { ...current, ...req.body };
  await dbWrite('settings', updated);
  res.json({ success: true, data: updated });
});

adminRouter.get('/stats', async (req, res) => {
  res.json({ success: true, data: await dbRead('stats', {}) });
});

adminRouter.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ success: false });
  res.json({ success: true, url: `/uploads/${req.file.filename}`, localPath: path.join(UPLOADS_DIR, req.file.filename) });
});

adminRouter.post('/debug/test-connection', async (req, res) => {
  try {
    const { captchaProvider, captchaApiKey, proxy } = req.body;
    let resultMsg = [];

    if (captchaProvider !== 'none' && captchaApiKey) {
      try {
        if (captchaProvider === '2captcha' || captchaProvider === 'rucaptcha') {
          const r = await axios.get(`https://2captcha.com/res.php?key=${captchaApiKey}&action=getbalance&json=1`);
          if (r.data.status === 1) resultMsg.push(`✅ [${captchaProvider}] Saldo: $${r.data.request}`);
        } else if (captchaProvider === 'capsolver') {
          const r = await axios.post('https://api.capsolver.com/getBalance', { clientKey: captchaApiKey });
          if (r.data.errorId === 0) resultMsg.push(`✅ [CapSolver] Saldo: $${r.data.balance}`);
        }
      } catch (e) {
        resultMsg.push(`❌ [Captcha] Gagal cek saldo.`);
      }
    } else {
      resultMsg.push(`ℹ️ [Captcha] Mode Tanpa API / Web Unlocker diaktifkan.`);
    }

    if (proxy && proxy.host && proxy.port) {
      try {
        const browser = await puppeteer.launch({
          headless: 'new',
          args: ['--no-sandbox', `--proxy-server=${proxy.type || 'http'}://${proxy.host}:${proxy.port}`, '--ignore-certificate-errors']
        });
        const page = await browser.newPage();
        if (proxy.username) await page.authenticate({ username: proxy.username, password: proxy.password });
        const resp = await page.goto('https://api.ipify.org?format=json', { timeout: 15000 });
        const ipData = await resp.json();
        await browser.close().catch(() => {});
        resultMsg.push(`✅ [Proxy] SUPER SUKSES! IP Masking: ${ipData.ip}`);
      } catch (e) {
        resultMsg.push(`❌ [Proxy] Gagal terhubung.`);
      }
    }

    res.json({ success: true, message: resultMsg.join('<br><br>') });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

adminRouter.post('/debug/dump', async (req, res) => {
  try {
    const type = req.body.type || 'image';
    const account = await AccountManager.getOptimalAccount(1);
    if (!account) return res.status(400).json({ success: false, message: 'Tidak ada akun aktif' });

    const browser = await launchBrowserForAccount(account);
    const page = await newPageWithProxyAuth(browser, account);
    await restoreSessionToPage(page, account);

    await page.goto(type === 'video' ? VIDEO_GEN_URL : IMAGE_GEN_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    const dumpData = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('button, input, textarea, select')).map(el => ({
        tag: el.tagName, type: el.type || '', name: el.name || '', text: (el.innerText || '').slice(0, 80)
      }));
    });

    const fileName = `${account.id}_${type}-dump.json`;
    await fs.writeJson(path.join(DEBUG_DIR, fileName), dumpData, { spaces: 2 });
    await page.close().catch(() => {});
    res.json({ success: true, url: `/debug/${fileName}` });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

adminRouter.post('/generate/video', async (req, res) => {
  try {
    const body = req.body || {};
    const result = await enqueueGenerationJob('video', {
      provider: body.provider, model: body.model, prompt: body.prompt,
      imageReference: body.image_reference ? { localPath: body.image_reference_local, url: body.image_reference } : null,
      orientation: body.orientation, resolution: body.resolution,
      duration: body.duration, audio: body.audio, engineMode: body.engineMode || 'browser'
    }, 'dashboard');
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

adminRouter.post('/generate/image', async (req, res) => {
  try {
    const body = req.body || {};
    const result = await enqueueGenerationJob('image', {
      provider: body.provider, model: body.model, prompt: body.prompt,
      imageReference: body.image_reference ? { localPath: body.image_reference_local, url: body.image_reference } : null,
      aspect_ratio: body.aspect_ratio, resolution: body.resolution, engineMode: body.engineMode || 'browser'
    }, 'dashboard');
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

adminRouter.get('/task/:taskId', async (req, res) => {
  const task = await getTaskStatus(req.params.taskId);
  if (!task) return res.status(404).json({ success: false });
  res.json({ success: true, data: task });
});

adminRouter.get('/gallery', async (req, res) => {
  const tasks = await dbRead('tasks', []);
  res.json({ success: true, data: tasks.filter(t => t.status === 'completed').reverse() });
});

app.use('/api/admin', adminRouter);

/* PUBLIC API */
const publicRouter = express.Router();
publicRouter.use(requirePublicApiKey);

publicRouter.post('/video/generate', async (req, res) => {
  try {
    const b = req.body || {};
    const r = await enqueueGenerationJob('video', { ...b, engineMode: b.engineMode || 'direct_api' }, 'api');
    res.json({ success: true, ...r });
  } catch (e) { res.status(500).json({ success: false, message: e.message }); }
});

publicRouter.post('/image/generate', async (req, res) => {
  try {
    const b = req.body || {};
    const r = await enqueueGenerationJob('image', { ...b, engineMode: b.engineMode || 'direct_api' }, 'api');
    res.json({ success: true, ...r });
  } catch (e) { res.status(500).json({ success: false, message: e.message }); }
});

publicRouter.get('/task/status/:taskId', async (req, res) => {
  const task = await getTaskStatus(req.params.taskId);
  if (!task) return res.status(404).json({ success: false });
  res.json({ success: true, ...task });
});

app.use('/api/v1', publicRouter);

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

cron.schedule('*/10 * * * *', () => { runHealthCheckAll().catch(() => {}); });
setTimeout(() => { runHealthCheckAll().catch(() => {}); }, 8000);

process.on('SIGTERM', async () => {
  for (const [, b] of activeBrowsers.entries()) {
    try { await b.close(); } catch (e) {}
  }
  process.exit(0);
});

server.listen(ENV.PORT, () => {
  logger.success(`🚀 SnapGen Dual Hybrid Engine aktif di port ${ENV.PORT}`);
});