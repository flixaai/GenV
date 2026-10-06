/**
 * =====================================================================
 * SNAPGEN AI WRAPPER - ADMIN DASHBOARD & AUTOMATION ENGINE
 * =====================================================================
 * Single-file backend: Express + Socket.io + Puppeteer-Extra (Stealth)
 * + Queue System + JSON Database + Captcha Solver + REST API
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
  CAPTCHA_PROVIDER: process.env.CAPTCHA_PROVIDER || '2captcha',
  CAPTCHA_API_KEY: process.env.CAPTCHA_API_KEY || '',
  CAPTCHA_AUTO_SOLVE: process.env.CAPTCHA_AUTO_SOLVE !== 'false',
  HEADLESS: process.env.HEADLESS !== 'false',
  PUPPETEER_EXECUTABLE_PATH: process.env.PUPPETEER_EXECUTABLE_PATH || null,
  BASE_URL: process.env.BASE_URL || `http://localhost:${process.env.PORT || 3000}`,
  QUEUE_CONCURRENCY: parseInt(process.env.QUEUE_CONCURRENCY || '2'),
  DOCKER_MODE: process.env.DOCKER_MODE === 'true'
};

/* ===================================================================
   DIREKTORI DATA
=================================================================== */
const DATA_DIR = path.join(__dirname, 'data');
const SESSIONS_DIR = path.join(__dirname, 'sessions');
const DOWNLOADS_DIR = path.join(__dirname, 'downloads');
const UPLOADS_DIR = path.join(__dirname, 'uploads');

fs.ensureDirSync(DATA_DIR);
fs.ensureDirSync(SESSIONS_DIR);
fs.ensureDirSync(DOWNLOADS_DIR);
fs.ensureDirSync(UPLOADS_DIR);

/* ===================================================================
   LOGGER SEDERHANA
=================================================================== */
function ts() { return new Date().toISOString(); }
const logger = {
  info: (...a) => console.log(`[INFO ${ts()}]`, ...a),
  warn: (...a) => console.warn(`[WARN ${ts()}]`, ...a),
  error: (...a) => console.error(`[ERROR ${ts()}]`, ...a),
  success: (...a) => console.log(`[OK ${ts()}]`, ...a)
};

/* ===================================================================
   JSON DATABASE (FILE-BASED, DENGAN SIMPLE LOCK)
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

// Inisialisasi file database awal
(async () => {
  await dbRead('accounts', []);
  await dbRead('settings', {
    captchaProvider: ENV.CAPTCHA_PROVIDER,
    captchaApiKey: ENV.CAPTCHA_API_KEY,
    captchaAutoSolve: ENV.CAPTCHA_AUTO_SOLVE,
    defaultProxy: { host: '', port: '', username: '', password: '', type: 'http' }
  });
  await dbRead('stats', {
    videoFromDashboard: 0, videoFromApi: 0,
    imageFromDashboard: 0, imageFromApi: 0
  });
  await dbRead('tasks', []);
})();

/* ===================================================================
   HELPER FUNCTIONS
=================================================================== */
function genTaskId(prefix = 'TASK') {
  return `${prefix}_${Date.now()}_${uuidv4().slice(0, 8)}`;
}

function buildProxyUrl(proxy) {
  if (!proxy || !proxy.host) return null;
  const protocol = proxy.type === 'socks5' ? 'socks5' : (proxy.type || 'http');
  return `${protocol}://${proxy.host}:${proxy.port}`;
}

/* ===================================================================
   ACCOUNT MANAGER
=================================================================== */
const AccountManager = {
  async getAll() {
    return dbRead('accounts', []);
  },
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
    return this.update(id, { statusCookie: 'active', lastLogin: new Date().toISOString() });
  },
  async loadSession(id) {
    const sp = path.join(SESSIONS_DIR, `${id}.json`);
    if (!(await fs.pathExists(sp))) return null;
    return fs.readJson(sp);
  },
  // Round-robin + filter status aktif & kredit cukup
  _rrIndex: 0,
  async getOptimalAccount(costRequired = 1) {
    const all = await this.getAll();
    const eligible = all.filter(a =>
      a.statusCookie === 'active' &&
      a.statusProxy === 'online' &&
      (a.isUnlimited || a.creditsLeft >= costRequired)
    );
    if (eligible.length === 0) return null;
    this._rrIndex = (this._rrIndex + 1) % eligible.length;
    return eligible[this._rrIndex];
  }
};

/* ===================================================================
   SOCKET.IO HANDLER (didefinisikan di sini, di-attach setelah io dibuat)
=================================================================== */
let ioInstance = null;
const pendingOtpResolvers = new Map();

function initSocket(io) {
  ioInstance = io;
  io.on('connection', (socket) => {
    logger.info('Dashboard terhubung via socket:', socket.id);

    socket.on('submit-otp', ({ accountId, otp }) => {
      logger.info(`OTP diterima untuk ${accountId}: ${otp}`);
      if (pendingOtpResolvers.has(accountId)) {
        pendingOtpResolvers.get(accountId)(otp);
        pendingOtpResolvers.delete(accountId);
      }
    });

    socket.on('disconnect', () => {
      logger.info('Socket terputus:', socket.id);
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

/* ===================================================================
   CAPTCHA SOLVER (2Captcha, CapSolver, Anti-Captcha, NextCaptcha)
=================================================================== */
const CAPTCHA_ENDPOINTS = {
  '2captcha': 'https://api.2captcha.com',
  'capsolver': 'https://api.capsolver.com',
  'anticaptcha': 'https://api.anti-captcha.com',
  'nextcaptcha': 'https://api.nextcaptcha.com'
};

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function solveCaptcha(provider, apiKey, { type, siteKey, pageUrl }) {
  const base = CAPTCHA_ENDPOINTS[provider] || CAPTCHA_ENDPOINTS['2captcha'];
  const taskType = type === 'turnstile' ? 'TurnstileTaskProxyless' : 'HCaptchaTaskProxyless';

  const createRes = await axios.post(`${base}/createTask`, {
    clientKey: apiKey,
    task: { type: taskType, websiteURL: pageUrl, websiteKey: siteKey }
  });

  const taskId = createRes.data.taskId;
  if (!taskId) throw new Error('Gagal membuat task captcha: ' + JSON.stringify(createRes.data));
  logger.info(`[Captcha:${provider}] Task dibuat:`, taskId);

  for (let i = 0; i < 30; i++) {
    await sleep(5000);
    const resultRes = await axios.post(`${base}/getTaskResult`, { clientKey: apiKey, taskId });
    if (resultRes.data.status === 'ready') {
      logger.success('[Captcha] Berhasil diselesaikan');
      return resultRes.data.solution.token || resultRes.data.solution.gRecaptchaResponse;
    }
  }
  throw new Error('Timeout menyelesaikan captcha');
}

async function detectAndSolveCaptcha(page, provider, apiKey) {
  const siteInfo = await page.evaluate(() => {
    const turnstileEl = document.querySelector('[data-sitekey]');
    const iframeTurnstile = document.querySelector('iframe[src*="challenges.cloudflare.com"]');
    const iframeHcaptcha = document.querySelector('iframe[src*="hcaptcha.com"]');

    if (turnstileEl) return { type: 'turnstile', siteKey: turnstileEl.getAttribute('data-sitekey') };
    if (iframeTurnstile) {
      try {
        const url = new URL(iframeTurnstile.src);
        return { type: 'turnstile', siteKey: url.searchParams.get('sitekey') };
      } catch (e) { return null; }
    }
    if (iframeHcaptcha) {
      try {
        const url = new URL(iframeHcaptcha.src);
        return { type: 'hcaptcha', siteKey: url.searchParams.get('sitekey') };
      } catch (e) { return null; }
    }
    return null;
  });

  if (!siteInfo || !siteInfo.siteKey) return false;

  const pageUrl = page.url();
  const token = await solveCaptcha(provider, apiKey, { type: siteInfo.type, siteKey: siteInfo.siteKey, pageUrl });

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
    if (window.hcaptchaCallback) window.hcaptchaCallback(tok);
  }, token, siteInfo.type);

  return true;
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

  // OPTIMASI RAM SUPER EKSTREM UNTUK RAILWAY (Mencegah OOM Crash)
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
    '--js-flags="--max-old-space-size=256"' // Batasi RAM V8 Engine agar tidak bocor
  ];

  const proxyUrl = buildProxyUrl(account.proxy);
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
  if (account.proxy && account.proxy.username) {
    await page.authenticate({ username: account.proxy.username, password: account.proxy.password });
  }
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36');
  
  // BLOKIR RESOURCE BERAT (Mencegah Railway Crash saat Generate Animasi)
  await page.setRequestInterception(true);
  page.on('request', (req) => {
    const type = req.resourceType();
    // Blokir Video/Media, Font, dan Script tracking yang memakan banyak RAM
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
    logger.warn('Cek proxy gagal:', e.message);
    return false;
  }
}

/* ===================================================================
   LOGIN FLOW / AUTO-LOGIN / RESTORE SESSION
=================================================================== */
const LOGIN_URL = 'https://snapgen.ai/auth/login'; // URL terverifikasi dari screenshot user

async function getCaptchaSettings() {
  const settings = await dbRead('settings', {});
  return {
    provider: settings.captchaProvider || '2captcha',
    apiKey: settings.captchaApiKey || '',
    autoSolve: settings.captchaAutoSolve !== false
  };
}

const DEBUG_DIR = path.join(__dirname, 'debug');
fs.ensureDirSync(DEBUG_DIR);

async function captureDebugSnapshot(page, account, label) {
  try {
    const screenshotPath = path.join(DEBUG_DIR, `${account.id}_${label}.png`);
    const htmlPath = path.join(DEBUG_DIR, `${account.id}_${label}.html`);
    await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
    const html = await page.content().catch(() => '');
    await fs.writeFile(htmlPath, html).catch(() => {});
    emitLog(`[${account.email}] 📸 Snapshot: /debug/${account.id}_${label}.png`);
  } catch (e) {
    logger.error('Gagal capture debug snapshot:', e.message);
  }
}

async function autoLogin(account) {
  emitLog(`[${account.email}] Memulai Auto-Login...`);
  const browser = await launchBrowserForAccount(account);
  const page = await newPageWithProxyAuth(browser, account);

  const proxyAlive = await checkProxyAlive(page);
  await AccountManager.update(account.id, { statusProxy: proxyAlive ? 'online' : 'offline' });
  emitAccountUpdate(await AccountManager.getById(account.id));

  if (!proxyAlive) {
    await page.close().catch(() => {});
    throw new Error('Proxy tidak merespon / mati');
  }

  await page.goto(LOGIN_URL, { waitUntil: 'networkidle2', timeout: 60000 }).catch(async (e) => {
    await captureDebugSnapshot(page, account, 'goto-failed');
    throw new Error(`Gagal membuka halaman login: ${e.message}`);
  });

  await sleep(5000);
  await captureDebugSnapshot(page, account, 'step1-page-loaded');

  const EMAIL_SELECTOR = 'input[type="email"], input[name="email"], input[id*="email" i], input[placeholder*="email" i]';
  const PASSWORD_SELECTOR = 'input[type="password"], input[name="password"], input[id*="password" i]';
  const OTP_SELECTOR = 'input[name="otp"], input[autocomplete="one-time-code"], input[placeholder*="code" i]';

  try {
    await page.waitForSelector(EMAIL_SELECTOR, { timeout: 45000, visible: true });
  } catch (err) {
    const allInputsInfo = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('input')).map(el => ({
        type: el.type, name: el.name, id: el.id, placeholder: el.placeholder, className: el.className
      }));
    }).catch(() => []);

    const dumpPath = path.join(DEBUG_DIR, `${account.id}_input-dump.json`);
    await fs.writeJson(dumpPath, allInputsInfo, { spaces: 2 }).catch(() => {});

    await captureDebugSnapshot(page, account, 'selector-not-found');
    const currentUrl = page.url();
    const pageTitle = await page.title().catch(() => 'unknown');
    await page.close().catch(() => {});
    throw new Error(`Form email tidak ditemukan. URL: ${currentUrl} | Title: "${pageTitle}" | Total input ditemukan: ${allInputsInfo.length} | Cek: /debug/${account.id}_input-dump.json`);
  }

  await page.click(EMAIL_SELECTOR);
  await page.type(EMAIL_SELECTOR, account.email, { delay: 60 });
  await page.click(PASSWORD_SELECTOR);
  await page.type(PASSWORD_SELECTOR, account.password, { delay: 60 });

  await captureDebugSnapshot(page, account, 'step2-form-filled');

  const { provider, apiKey, autoSolve } = await getCaptchaSettings();
  if (autoSolve && apiKey) {
    try {
      const solved = await detectAndSolveCaptcha(page, provider, apiKey);
      if (solved) emitLog(`[${account.email}] Captcha berhasil diselesaikan otomatis.`);
    } catch (e) {
      logger.warn('Captcha solve gagal/tidak ditemukan:', e.message);
    }
  }

  const clicked = await page.evaluate(() => {
    const buttons = Array.from(document.querySelectorAll('button'));
    const target = buttons.find(b => /continue|sign in|log ?in|masuk/i.test(b.textContent));
    if (target) { target.click(); return true; }
    return false;
  });
  if (!clicked) await page.click('button[type="submit"]').catch(() => {});

  await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 30000 }).catch(() => {});
  await sleep(1500);
  await captureDebugSnapshot(page, account, 'step3-after-submit');

  const otpField = await page.$(OTP_SELECTOR);
  if (otpField) {
    await AccountManager.update(account.id, { statusCookie: 'need_otp' });
    emitAccountUpdate(await AccountManager.getById(account.id));
    emitLog(`[${account.email}] Menunggu input OTP dari Admin Dashboard...`);

    const otp = await requestOtpFromAdmin(account.id, account.email);
    await page.type(OTP_SELECTOR, otp, { delay: 80 });

    const otpClicked = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const target = buttons.find(b => /continue|verify|submit|confirm/i.test(b.textContent));
      if (target) { target.click(); return true; }
      return false;
    });
    if (!otpClicked) await page.click('button[type="submit"]').catch(() => {});

    await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 30000 }).catch(() => {});
  }

  const currentUrl = page.url();
  if (currentUrl.includes('/auth/') || currentUrl.includes('/login')) {
    await captureDebugSnapshot(page, account, 'login-failed-still-on-auth-page');
    const errorText = await page.evaluate(() => {
      const el = document.querySelector('[role="alert"], .error, .text-red-500, .text-danger');
      return el ? el.textContent.trim() : null;
    }).catch(() => null);

    await AccountManager.update(account.id, { statusCookie: 'expired' });
    emitAccountUpdate(await AccountManager.getById(account.id));
    await page.close().catch(() => {});
    throw new Error(`Login gagal di ${currentUrl}. ${errorText ? 'Pesan: ' + errorText : ''} Cek: /debug/${account.id}_login-failed-still-on-auth-page.png`);
  }

  const cookies = await page.cookies();
  const localStorageData = await page.evaluate(() => {
    const data = {};
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      data[key] = localStorage.getItem(key);
    }
    return data;
  });

  let bearerToken = localStorageData['access_token'] || localStorageData['token'] || null;
  let parsedCredit = null;

  try {
    const authStoreRaw = localStorageData['authStore'];
    if (authStoreRaw) {
      const authParsed = JSON.parse(authStoreRaw);
      if (authParsed.access_token) bearerToken = authParsed.access_token;
      if (authParsed.user && authParsed.user.user_credit) {
        parsedCredit = authParsed.user.user_credit;
      }
    }
  } catch (e) {
    logger.warn('Gagal parsing authStore:', e.message);
  }

  const sessionData = { cookies, localStorage: localStorageData, bearerToken, savedAt: new Date().toISOString() };
  await AccountManager.saveSession(account.id, sessionData);

  if (parsedCredit) {
    await AccountManager.update(account.id, {
      creditsLeft: parsedCredit.available_credit || 0,
      isUnlimited: false
    });
  }

  emitLog(`[${account.email}] ✅ Login berhasil, sesi tersimpan. Kredit: ${parsedCredit ? parsedCredit.available_credit : 'unknown'}`);
  emitAccountUpdate(await AccountManager.getById(account.id));

  await page.close().catch(() => {});
  return sessionData;
}

async function restoreSessionToPage(page, account) {
  const session = await AccountManager.loadSession(account.id);
  if (!session) return false;

  await page.goto('https://snapgen.ai', { waitUntil: 'domcontentloaded', timeout: 30000 });
  if (session.cookies && session.cookies.length) {
    await page.setCookie(...session.cookies);
  }
  if (session.localStorage) {
    await page.evaluate((data) => {
      for (const k in data) localStorage.setItem(k, data[k]);
    }, session.localStorage);
  }
  return true;
}

/* ===================================================================
   HELPER PENGHANCUR POP-UP & PENCENTANG POLICY (AGRESIF)
=================================================================== */
async function clearOverlaysAndCheckboxes(page, email) {
  emitLog(`[${email}] Membersihkan pop-up dan mencentang policy...`);
  await page.evaluate(() => {
    const buttons = Array.from(document.querySelectorAll('button'));
    buttons.forEach(btn => {
      const txt = (btn.innerText || '').trim().toLowerCase();
      if (txt === 'got it!' || txt === 'got it' || txt === 'close' || txt === 'ok' || txt === 'i understand') {
        btn.click();
      }
      const ariaLabel = (btn.getAttribute('aria-label') || '').toLowerCase();
      if (ariaLabel.includes('close')) {
        btn.click();
      }
    });

    const labels = Array.from(document.querySelectorAll('label, div, span'));
    labels.forEach(el => {
      const text = (el.innerText || '').toLowerCase();
      if (text.includes('i understand that intentionally') || text.includes('policy') || text.includes('guidelines')) {
        const checkbox = el.querySelector('input[type="checkbox"]') || el.closest('label')?.querySelector('input[type="checkbox"]');
        if (checkbox && !checkbox.checked) {
          el.click();
        } else if (!checkbox) {
          el.click();
        }
      }
    });

    const checkboxes = document.querySelectorAll('input[type="checkbox"]');
    checkboxes.forEach(cb => {
      if (!cb.checked) {
         cb.click();
         if (cb.parentElement) {
            cb.parentElement.click();
         }
      }
    });
  }).catch(() => {});
  
  await new Promise(r => setTimeout(r, 1500)); 
}

/* ===================================================================
   VIDEO GENERATION FLOW
=================================================================== */
const VIDEO_GEN_URL = 'https://snapgen.ai/app/video-gen/veo';

async function selectDropdownByLabel(page, selector, label) {
  if (!label) return;
  try {
    await page.click(selector);
    await sleep(300);
    const optionSelector = '[role="option"]';
    await page.waitForSelector(optionSelector, { timeout: 5000 });
    const options = await page.$$(optionSelector);
    for (const opt of options) {
      const text = await page.evaluate(el => el.textContent.trim(), opt);
      if (text.toLowerCase().includes(String(label).toLowerCase())) {
        await opt.click();
        return;
      }
    }
  } catch (e) {
    logger.warn(`Gagal pilih dropdown ${selector} -> ${label}:`, e.message);
  }
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

async function generateVideoOnPage(account, params, taskId) {
  const browser = await launchBrowserForAccount(account);
  const page = await newPageWithProxyAuth(browser, account);

  // === FITUR BARU: LIVE VIEW STREAMING ===
  const streamLive = async () => {
    while(!page.isClosed()) {
      try {
        const b64 = await page.screenshot({ encoding: 'base64', type: 'jpeg', quality: 20 });
        if(ioInstance) ioInstance.emit('live-view', { taskId, frame: `data:image/jpeg;base64,${b64}` });
        await sleep(1500);
      } catch(e) { break; }
    }
  };

  await restoreSessionToPage(page, account);
  await page.goto(VIDEO_GEN_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  
  streamLive(); // Mulai siaran langsung!
  emitProgress(taskId, { status: 'queued', progress: 5 });

  emitLog(`[${account.email}] Menutup pop-up (jika ada)...`);
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    const closeBtns = btns.filter(b => {
      const t = b.innerText ? b.innerText.trim().toLowerCase() : '';
      return t.includes('got it') || t.includes('close') || t.includes('ok');
    });
    closeBtns.forEach(b => b.click());
  }).catch(() => {});
  await sleep(1000);

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
  await sleep(1500);

  await clearOverlaysAndCheckboxes(page, account.email);

  emitLog(`[${account.email}] Mengetik prompt (Mouse Mode)...`);
  const promptSelector = 'textarea[placeholder*="video" i]';
  await page.waitForSelector(promptSelector, { timeout: 30000 });

  await page.click(promptSelector, { clickCount: 3 });
  await sleep(300);
  await page.keyboard.press('Backspace');
  await sleep(300);
  await page.type(promptSelector, params.prompt, { delay: 40 });
  await page.keyboard.press('Space');
  await sleep(1000);

  emitLog(`[${account.email}] Memilih orientasi, resolusi & durasi...`);
  const clickAria = async (lbl) => {
    if (!lbl) return;
    const btn = await page.$(`button[aria-label="${lbl}"]`);
    if (btn) await btn.click().catch(() => {});
  };

  await clickAria(params.orientation); 
  await clickAria(params.resolution);
  await clickAria(String(params.duration));

  if (params.imageReference && params.imageReference.localPath) {
    const fileInputs = await page.$$('input[type="file"]');
    if (fileInputs.length > 0) {
      await fileInputs[0].uploadFile(params.imageReference.localPath).catch(() => {});
    }
  }

  await clearOverlaysAndCheckboxes(page, account.email);

  const { provider: capProvider, apiKey: capKey, autoSolve } = await getCaptchaSettings();
  if (autoSolve && capKey) {
    await detectAndSolveCaptcha(page, capProvider, capKey).catch(() => {});
  }

  emitProgress(taskId, { status: 'processing', progress: 15 });
  
  const existingVideos = await page.evaluate(() => Array.from(document.querySelectorAll('video')).map(v => v.src));

  await clearOverlaysAndCheckboxes(page, account.email);

  emitLog(`[${account.email}] MENGKLIK TOMBOL GENERATE VIDEO (Pakai Mouse Asli)!`);
  
  const btnBox = await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    const target = btns.find(b => {
      const txt = b.innerText ? b.innerText.trim().toLowerCase() : '';
      const isGen = txt.includes('generate');
      const isLocked = b.disabled || b.getAttribute('aria-disabled') === 'true';
      return isGen && !isLocked;
    });
    if (!target) return null;
    target.scrollIntoView({ block: 'center' });
    const rect = target.getBoundingClientRect();
    return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2 };
  });

  if (!btnBox) {
    emitLog(`[ERROR] Tombol Generate tetap terkunci / tidak ketemu!`);
    await captureDebugSnapshot(page, account, `BTN-LOCKED-VID`);
    throw new Error(`Tombol Generate masih terkunci. Cek: ${ENV.BASE_URL}/debug/BTN-LOCKED-VID.png`);
  }

  await page.mouse.move(btnBox.x, btnBox.y);
  await sleep(300);
  await page.mouse.down();
  await sleep(100);
  await page.mouse.up();
  
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    const target = btns.find(b => {
      const txt = b.innerText ? b.innerText.trim().toLowerCase() : '';
      return txt.includes('generate') && !b.disabled;
    });
    if (target) target.click();
  }).catch(() => {});

  let progress = 15, completed = false, resultUrl = null;

  for (let i = 0; i < 120; i++) {
    await sleep(5000);

    if (i === 10) {
       emitLog(`[${account.email}] Cek CCTV Layar...`);
       await captureDebugSnapshot(page, account, `STUCK-AT-15`);
       emitLog(`[📸 CCTV] Cek layar di sini: ${ENV.BASE_URL}/debug/${account.id}_STUCK-AT-15.png`);
    }

    const pct = await page.evaluate(() => {
       const match = document.body.innerText.match(/(\d+)%/);
       return match ? parseInt(match[1]) : null;
    });
    if (pct && pct > progress) progress = pct;

    const currentVideos = await page.evaluate(() => Array.from(document.querySelectorAll('video')).map(v => v.src));
    const newVideo = currentVideos.find(src => 
      src && 
      !existingVideos.includes(src) && 
      !src.startsWith('data:') &&
      src.includes('blob')
    );

    if (newVideo) {
      resultUrl = newVideo;
      completed = true;
      progress = 100;
      emitLog(`[${account.email}] 🎉 HASIL VIDEO ASLI DITEMUKAN!`);
    }

    emitProgress(taskId, { status: completed ? 'completed' : 'processing', progress });
    if (completed) break;
  }

  if (!completed || !resultUrl) {
    await captureDebugSnapshot(page, account, `FAILED-GENERATE-VID`);
    await page.close().catch(() => {});
    throw new Error(`Gagal dapat hasil asli. Cek foto: ${ENV.BASE_URL}/debug/${account.id}_FAILED-GENERATE-VID.png`);
  }

  const fileName = `video_${taskId}.mp4`;
  const localPath = path.join(DOWNLOADS_DIR, fileName);
  await downloadRemoteFile(page, resultUrl, localPath);
  await page.close().catch(() => {});

  return { mediaUrl: `/downloads/${fileName}`, previewUrl: resultUrl };
}

/* ===================================================================
   IMAGE GENERATION FLOW
=================================================================== */
const IMAGE_GEN_URL = 'https://snapgen.ai/app/imagen';

async function generateImageOnPage(account, params, taskId) {
  const browser = await launchBrowserForAccount(account);
  const page = await newPageWithProxyAuth(browser, account);

  // === FITUR BARU: LIVE VIEW STREAMING ===
  const streamLive = async () => {
    while(!page.isClosed()) {
      try {
        const b64 = await page.screenshot({ encoding: 'base64', type: 'jpeg', quality: 20 });
        if(ioInstance) ioInstance.emit('live-view', { taskId, frame: `data:image/jpeg;base64,${b64}` });
        await sleep(1500); // Kirim foto setiap 1.5 detik (Hemat RAM)
      } catch(e) { break; }
    }
  };

  await restoreSessionToPage(page, account);
  await page.goto(IMAGE_GEN_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  
  streamLive(); // Mulai siaran langsung!
  emitProgress(taskId, { status: 'queued', progress: 5 });

  emitLog(`[${account.email}] Menutup pop-up (jika ada)...`);
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    const closeBtns = btns.filter(b => {
      const t = b.innerText ? b.innerText.trim().toLowerCase() : '';
      return t.includes('got it') || t.includes('close') || t.includes('ok');
    });
    closeBtns.forEach(b => b.click());
  }).catch(() => {});
  await sleep(1000);

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
  await sleep(1500);

  await clearOverlaysAndCheckboxes(page, account.email);

  emitLog(`[${account.email}] Mengetik prompt (Metode Ketik Manual)...`);
  const promptSelector = 'textarea[placeholder*="image" i]';
  await page.waitForSelector(promptSelector, { timeout: 30000 });

  // KLIK 3X LALU KETIK (Trik Paling Ampuh Tembus Keamanan React)
  await page.click(promptSelector, { clickCount: 3 });
  await sleep(300);
  await page.keyboard.press('Backspace');
  await sleep(300);
  await page.type(promptSelector, params.prompt, { delay: 40 });
  await page.keyboard.press('Space');
  await sleep(1000);

  emitLog(`[${account.email}] Memilih rasio & resolusi...`);
  const clickAria = async (lbl) => {
    if (!lbl) return;
    const btn = await page.$(`button[aria-label="${lbl}"]`);
    if (btn) await btn.click().catch(() => {});
  };

  await clickAria(params.aspect_ratio); 
  await clickAria(params.resolution);   

  if (params.imageReference && params.imageReference.localPath) {
    const fileInputs = await page.$$('input[type="file"]');
    if (fileInputs.length > 0) {
      await fileInputs[0].uploadFile(params.imageReference.localPath).catch(() => {});
    }
  }

  await clearOverlaysAndCheckboxes(page, account.email);

  const { provider: capProvider, apiKey: capKey, autoSolve } = await getCaptchaSettings();
  if (autoSolve && capKey) {
    await detectAndSolveCaptcha(page, capProvider, capKey).catch(() => {});
  }

  emitProgress(taskId, { status: 'processing', progress: 20 });
  
  const existingImages = await page.evaluate(() => Array.from(document.querySelectorAll('img')).map(i => i.src));

  await clearOverlaysAndCheckboxes(page, account.email);

  emitLog(`[${account.email}] MENGKLIK TOMBOL GENERATE!`);
  
  const btnBox = await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    const target = btns.find(b => {
      const txt = b.innerText ? b.innerText.trim().toLowerCase() : '';
      const isGen = txt.includes('generate');
      const isLocked = b.disabled || b.getAttribute('aria-disabled') === 'true';
      return isGen && !isLocked;
    });
    if (!target) return null;
    target.scrollIntoView({ block: 'center' });
    const rect = target.getBoundingClientRect();
    return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2 };
  });

  if (!btnBox) {
    emitLog(`[ERROR] Tombol Generate tetap terkunci / tidak ketemu!`);
    await captureDebugSnapshot(page, account, `BTN-LOCKED`);
    throw new Error(`Tombol Generate masih terkunci. Cek: ${ENV.BASE_URL}/debug/BTN-LOCKED.png`);
  }

  await page.mouse.move(btnBox.x, btnBox.y);
  await sleep(300);
  await page.mouse.down();
  await sleep(100);
  await page.mouse.up();
  
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    const target = btns.find(b => {
      const txt = b.innerText ? b.innerText.trim().toLowerCase() : '';
      return txt.includes('generate') && !b.disabled;
    });
    if (target) target.click();
  }).catch(() => {});

  let progress = 20, completed = false, resultUrl = null;

  for (let i = 0; i < 60; i++) {
    await sleep(3000);

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
      emitLog(`[${account.email}] 🎉 HASIL AI ASLI DITEMUKAN!`);
    }

    emitProgress(taskId, { status: completed ? 'completed' : 'processing', progress });
    if (completed) break;
  }

  if (!completed || !resultUrl) {
    await captureDebugSnapshot(page, account, `FAILED-GENERATE-IMG`);
    await page.close().catch(() => {});
    throw new Error(`Gagal dapat hasil asli. Cek foto: ${ENV.BASE_URL}/debug/${account.id}_FAILED-GENERATE-IMG.png`);
  }

  const ext = resultUrl.includes('.png') ? 'png' : 'jpg';
  const fileName = `image_${taskId}.${ext}`;
  const localPath = path.join(DOWNLOADS_DIR, fileName);
  await downloadRemoteFile(page, resultUrl, localPath);
  await page.close().catch(() => {});

  return { mediaUrl: `/downloads/${fileName}`, previewUrl: resultUrl };
}

/* ===================================================================
   HEALTH CHECK / KEEP-ALIVE
=================================================================== */
const DASHBOARD_HOME_URL = 'https://snapgen.ai/app';

async function checkAccountHealth(account) {
  try {
    const browser = await launchBrowserForAccount(account);
    const page = await newPageWithProxyAuth(browser, account);

    const proxyAlive = await checkProxyAlive(page);
    await AccountManager.update(account.id, { statusProxy: proxyAlive ? 'online' : 'offline' });

    if (!proxyAlive) {
      await page.close().catch(() => {});
      emitAccountUpdate(await AccountManager.getById(account.id));
      return;
    }

    const restored = await restoreSessionToPage(page, account);
    if (!restored) {
      await AccountManager.update(account.id, { statusCookie: 'expired' });
      emitAccountUpdate(await AccountManager.getById(account.id));
      await page.close().catch(() => {});
      emitLog(`[${account.email}] Sesi tidak ditemukan, menjalankan auto-login...`);
      return autoLogin(account).catch(err => emitLog(`[${account.email}] Auto-login gagal: ${err.message}`));
    }

    await page.goto(DASHBOARD_HOME_URL, { waitUntil: 'networkidle2', timeout: 30000 });
    const isLoggedIn = !page.url().includes('/login');

    if (!isLoggedIn) {
      await AccountManager.update(account.id, { statusCookie: 'expired' });
      emitAccountUpdate(await AccountManager.getById(account.id));
      await page.close().catch(() => {});
      emitLog(`[${account.email}] Sesi expired, menjalankan auto-login ulang...`);
      return autoLogin(account).catch(err => emitLog(`[${account.email}] Auto-login gagal: ${err.message}`));
    }

    await AccountManager.update(account.id, { statusCookie: 'active', lastCheck: new Date().toISOString() });

    // Ambil kredit langsung dari localStorage authStore (data asli snapgen.ai, lebih akurat dari DOM scraping)
    const creditInfo = await page.evaluate(() => {
      try {
        const raw = localStorage.getItem('authStore');
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        return (parsed.user && parsed.user.user_credit) ? parsed.user.user_credit : null;
      } catch (e) {
        return null;
      }
    }).catch(() => null);

    if (creditInfo) {
      await AccountManager.update(account.id, {
        creditsLeft: creditInfo.available_credit || 0,
        isUnlimited: false
      });
      emitLog(`[${account.email}] Kredit terbaru: ${creditInfo.available_credit}`);
    }

    emitAccountUpdate(await AccountManager.getById(account.id));
    await page.close().catch(() => {});
  } catch (err) {
    logger.error(`Health check gagal untuk ${account.email}:`, err.message);
  }
}

async function runHealthCheckAll() {
  const accounts = await AccountManager.getAll();
  logger.info(`Menjalankan health check untuk ${accounts.length} akun...`);
  for (const acc of accounts) {
    await checkAccountHealth(acc);
  }
  const refreshed = await AccountManager.getAll();
  const anyActive = refreshed.some(a => a.statusCookie === 'active');
  emitHealthStatus({ videoGen: anyActive, imageGen: anyActive });
}

/* ===================================================================
   QUEUE MANAGER (p-queue) + ESTIMASI BIAYA + ROTASI AKUN
=================================================================== */
const genQueue = new PQueue({ concurrency: ENV.QUEUE_CONCURRENCY });

function estimateCost(type, params) {
  if (type === 'video') {
    let base = 8;
    if (params.resolution === '1080p') base += 4;
    if (parseInt(params.duration) >= 15) base += 4;
    return base;
  }
  if (type === 'image') {
    let base = 2;
    if (params.resolution === '4K') base += 2;
    return base;
  }
  return 1;
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

  await upsertTask(taskId, { taskId, type, status: 'queued', cost, source, createdAt: new Date().toISOString() });

  genQueue.add(async () => {
    try {
      const account = await AccountManager.getOptimalAccount(cost);
      if (!account) {
        const allAcc = await AccountManager.getAll();
        const debugInfo = allAcc.map(a => `${a.email}(cookie:${a.statusCookie},proxy:${a.statusProxy},credit:${a.creditsLeft})`).join(' | ');
        const errMsg = `Tidak ada akun tersedia untuk cost ${cost}. Detail: ${debugInfo}`;
        await upsertTask(taskId, { status: 'failed', error: errMsg });
        emitProgress(taskId, { status: 'failed', progress: 0, error: errMsg });
        return;
      }

      await upsertTask(taskId, { status: 'processing', accountId: account.id });

      let result;
      if (type === 'video') {
        result = await generateVideoOnPage(account, params, taskId);
        await incrementStat(source === 'dashboard' ? 'videoFromDashboard' : 'videoFromApi');
      } else {
        result = await generateImageOnPage(account, params, taskId);
        await incrementStat(source === 'dashboard' ? 'imageFromDashboard' : 'imageFromApi');
      }

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
      logger.error(`Job ${taskId} gagal:`, err.message);
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
   EXPRESS APP SETUP
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
  cookie: {
    maxAge: 24 * 60 * 60 * 1000,
    secure: ENV.NODE_ENV === 'production',
    sameSite: 'lax'
  }
}));

app.use(express.static(path.join(__dirname, 'public')));
app.use('/downloads', express.static(DOWNLOADS_DIR));
app.use('/uploads', express.static(UPLOADS_DIR));
app.use('/debug', express.static(DEBUG_DIR));

// Multer untuk upload image reference
const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, UPLOADS_DIR),
    filename: (req, file, cb) => cb(null, `${Date.now()}_${file.originalname}`)
  }),
  limits: { fileSize: 25 * 1024 * 1024 }
});

/* ===================================================================
   MIDDLEWARE AUTH
=================================================================== */
function requireAdminAuth(req, res, next) {
  if (req.session && req.session.isAdmin) return next();
  return res.status(401).json({ success: false, message: 'Unauthorized, silakan login kembali' });
}

function requirePublicApiKey(req, res, next) {
  const key = req.headers['x-api-key'];
  if (key && key === ENV.PUBLIC_API_KEY) return next();
  return res.status(401).json({ success: false, message: 'Invalid API Key' });
}

/* ===================================================================
   ADMIN AUTH ROUTES (TIDAK BUTUH SESSION)
=================================================================== */
app.post('/api/admin/login', (req, res) => {
  const { username, password } = req.body || {};
  if (username === ENV.ADMIN_USERNAME && password === ENV.ADMIN_PASSWORD) {
    req.session.isAdmin = true;
    return res.json({ success: true });
  }
  res.status(401).json({ success: false, message: 'Username atau password salah' });
});

app.post('/api/admin/logout', (req, res) => {
  req.session.destroy(() => {});
  res.json({ success: true });
});

app.get('/api/admin/check-session', (req, res) => {
  res.json({ loggedIn: !!(req.session && req.session.isAdmin) });
});

/* ===================================================================
   ADMIN API ROUTES (BUTUH SESSION)
=================================================================== */
const adminRouter = express.Router();
adminRouter.use(requireAdminAuth);

// ---- ACCOUNTS ----
adminRouter.get('/accounts', async (req, res) => {
  const accounts = await AccountManager.getAll();
  const sanitized = accounts.map(({ password, ...rest }) => rest);
  res.json({ success: true, data: sanitized });
});

adminRouter.post('/accounts', async (req, res) => {
  const { email, password, proxy } = req.body || {};
  if (!email || !password) return res.status(400).json({ success: false, message: 'Email & password wajib diisi' });

  const account = await AccountManager.create({ email, password, proxy });
  emitAccountUpdate(account);

  autoLogin(account).catch(err => emitLog(`[${email}] Auto-login gagal: ${err.message}`));

  res.json({ success: true, data: account });
});

adminRouter.delete('/accounts/:id', async (req, res) => {
  await AccountManager.remove(req.params.id);
  res.json({ success: true });
});

adminRouter.post('/accounts/:id/relogin', async (req, res) => {
  const account = await AccountManager.getById(req.params.id);
  if (!account) return res.status(404).json({ success: false, message: 'Akun tidak ditemukan' });

  autoLogin(account).catch(err => emitLog(`[${account.email}] Re-login gagal: ${err.message}`));
  res.json({ success: true, message: 'Re-login dijalankan di background' });
});

adminRouter.post('/accounts/:id/check-credits', async (req, res) => {
  const account = await AccountManager.getById(req.params.id);
  if (!account) return res.status(404).json({ success: false, message: 'Akun tidak ditemukan' });

  checkAccountHealth(account).catch(err => emitLog(`Check credits error: ${err.message}`));
  res.json({ success: true, message: 'Mengecek kredit di background' });
});

adminRouter.get('/accounts/:id/cookie', async (req, res) => {
  const session = await AccountManager.loadSession(req.params.id);
  if (!session) return res.status(404).json({ success: false, message: 'Sesi/cookie tidak ditemukan' });
  res.json({ success: true, data: session });
});

// ---- SETTINGS ----
adminRouter.get('/settings', async (req, res) => {
  const settings = await dbRead('settings', {});
  res.json({ success: true, data: settings });
});

adminRouter.post('/settings', async (req, res) => {
  const current = await dbRead('settings', {});
  const updated = { ...current, ...req.body };
  await dbWrite('settings', updated);
  res.json({ success: true, data: updated });
});

// ---- STATS ----
adminRouter.get('/stats', async (req, res) => {
  const stats = await dbRead('stats', {});
  res.json({ success: true, data: stats });
});

// ---- UPLOAD IMAGE REFERENCE ----
adminRouter.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ success: false, message: 'File tidak ditemukan' });
  res.json({
    success: true,
    url: `/uploads/${req.file.filename}`,
    localPath: path.join(UPLOADS_DIR, req.file.filename)
  });
});

// ---- DEBUGGER / UI EXTRACTOR (Untuk Update Web SnapGen) ----
adminRouter.post('/debug/dump', async (req, res) => {
  try {
    const type = req.body.type || 'image';
    const account = await AccountManager.getOptimalAccount(1);
    if (!account) return res.status(400).json({ success: false, message: 'Tidak ada akun aktif/berkredit' });

    const browser = await launchBrowserForAccount(account);
    const page = await newPageWithProxyAuth(browser, account);
    await restoreSessionToPage(page, account);
    
    const targetUrl = type === 'video' ? VIDEO_GEN_URL : IMAGE_GEN_URL;
    await page.goto(targetUrl, { waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(5000);

    const dumpData = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('button, input, textarea, select, [role="combobox"], [role="option"]')).map(el => ({
        tag: el.tagName, type: el.type || '', name: el.name || '',
        id: el.id || '', className: el.className || '', placeholder: el.placeholder || '',
        text: el.innerText ? el.innerText.substring(0, 100) : '', ariaLabel: el.getAttribute('aria-label') || ''
      }));
    });

    const fileName = `${account.id}_${type}gen-dump.json`;
    const dumpPath = path.join(DEBUG_DIR, fileName);
    await fs.writeJson(dumpPath, dumpData, { spaces: 2 });
    await page.close().catch(() => {});
    
    res.json({ success: true, url: `/debug/${fileName}` });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// ---- DIRECT GENERATE DARI DASHBOARD ----
adminRouter.post('/generate/video', async (req, res) => {
  emitLog('Menerima perintah Generate Video dari dashboard...');
  try {
    const body = req.body || {};
    const params = {
      provider: body.provider, model: body.model, prompt: body.prompt,
      imageReference: body.image_reference ? { localPath: body.image_reference_local, url: body.image_reference } : null,
      orientation: body.orientation, resolution: body.resolution,
      duration: body.duration, audio: body.audio
    };
    const result = await enqueueGenerationJob('video', params, 'dashboard');
    res.json({ success: true, ...result });
  } catch (err) {
    emitLog(`Gagal memproses request Video: ${err.message}`);
    res.status(500).json({ success: false, message: err.message });
  }
});

adminRouter.post('/generate/image', async (req, res) => {
  emitLog('Menerima perintah Generate Image dari dashboard...');
  try {
    const body = req.body || {};
    const params = {
      provider: body.provider, model: body.model, prompt: body.prompt,
      imageReference: body.image_reference ? { localPath: body.image_reference_local, url: body.image_reference } : null,
      aspect_ratio: body.aspect_ratio, resolution: body.resolution
    };
    const result = await enqueueGenerationJob('image', params, 'dashboard');
    res.json({ success: true, ...result });
  } catch (err) {
    emitLog(`Gagal memproses request Image: ${err.message}`);
    res.status(500).json({ success: false, message: err.message });
  }
});

adminRouter.get('/task/:taskId', async (req, res) => {
  const task = await getTaskStatus(req.params.taskId);
  if (!task) return res.status(404).json({ success: false, message: 'Task tidak ditemukan' });
  res.json({ success: true, data: task });
});

// ---- GALLERY ----
adminRouter.get('/gallery', async (req, res) => {
  const tasks = await dbRead('tasks', []);
  const completed = tasks.filter(t => t.status === 'completed').reverse();
  res.json({ success: true, data: completed });
});

app.use('/api/admin', adminRouter);

/* ===================================================================
   PUBLIC API V1 (UNTUK INTEGRASI WEBSITE UTAMA)
=================================================================== */
const publicRouter = express.Router();
publicRouter.use(requirePublicApiKey);

publicRouter.post('/video/generate', async (req, res) => {
  try {
    const { provider, model, prompt, image_reference, orientation, resolution, duration, audio, webhook_url } = req.body || {};
    if (!provider || !model || !prompt) {
      return res.status(400).json({ success: false, message: 'provider, model, prompt wajib diisi' });
    }

    const { taskId, cost } = await enqueueGenerationJob('video', {
      provider, model, prompt,
      imageReference: image_reference ? { url: image_reference } : null,
      orientation, resolution, duration, audio,
      webhookUrl: webhook_url
    }, 'api');

    res.json({ success: true, taskId, cost });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

publicRouter.post('/image/generate', async (req, res) => {
  try {
    const { provider, model, prompt, aspect_ratio, resolution, image_reference, webhook_url } = req.body || {};
    if (!provider || !model || !prompt) {
      return res.status(400).json({ success: false, message: 'provider, model, prompt wajib diisi' });
    }

    const { taskId, cost } = await enqueueGenerationJob('image', {
      provider, model, prompt, aspect_ratio, resolution,
      imageReference: image_reference ? { url: image_reference } : null,
      webhookUrl: webhook_url
    }, 'api');

    res.json({ success: true, taskId, cost });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

publicRouter.get('/task/status/:taskId', async (req, res) => {
  const task = await getTaskStatus(req.params.taskId);
  if (!task) return res.status(404).json({ success: false, message: 'Task tidak ditemukan' });

  if (task.status === 'completed') {
    const accounts = await AccountManager.getAll();
    const acc = accounts.find(a => a.id === task.accountId);
    return res.json({
      success: true,
      status: 'completed',
      type: task.type,
      mediaUrl: `${ENV.BASE_URL}${task.mediaUrl}`,
      previewUrl: task.previewUrl,
      creditsLeft: acc ? (acc.isUnlimited ? 'Unlimited' : acc.creditsLeft) : null
    });
  }

  res.json({ success: true, status: task.status, cost: task.cost, error: task.error || null });
});

app.use('/api/v1', publicRouter);

/* ===================================================================
   FALLBACK ROUTE -> SPA
=================================================================== */
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

/* ===================================================================
   CRON HEALTH CHECK (SETIAP 10 MENIT)
=================================================================== */
cron.schedule('*/10 * * * *', () => {
  logger.info('[CRON] Menjalankan health check berkala...');
  runHealthCheckAll().catch(err => logger.error('Health check cron error:', err.message));
});

setTimeout(() => {
  runHealthCheckAll().catch(err => logger.error('Initial health check error:', err.message));
}, 8000);

/* ===================================================================
   GRACEFUL SHUTDOWN
=================================================================== */
process.on('SIGTERM', async () => {
  logger.warn('SIGTERM diterima, menutup browser aktif...');
  for (const [id, browser] of activeBrowsers.entries()) {
    try { await browser.close(); } catch (e) {}
  }
  process.exit(0);
});

/* ===================================================================
   START SERVER
=================================================================== */
server.listen(ENV.PORT, () => {
  logger.success(`🚀 SnapGen AI Wrapper berjalan di port ${ENV.PORT}`);
  logger.info(`📊 Dashboard: ${ENV.BASE_URL}/`);
  logger.info(`🔑 Admin: ${ENV.ADMIN_USERNAME} / (password dari ENV)`);
});