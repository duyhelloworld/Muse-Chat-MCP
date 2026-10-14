/**
 * muse-driver.mjs
 * ------------------------------------------------------------------
 * Browser driver for Meta Muse (https://muse.ai) — the "Hatch" agent VM.
 *
 * WHY THIS EXISTS
 *   Muse chat is NOT a REST/SSE API. It is an RPC gateway over an
 *   *encrypted* WebSocket ("noise" transport: X25519 + HKDF + AES-GCM),
 *   with Ed25519-signed auth_token/notary_token in the WS URL. See
 *   https://hatch.metaaivm.com/v1/noise and methods like chat.stream.
 *
 *   Rather than reimplementing the Noise handshake (phase 2), this driver
 *   drives the REAL client in a real Chrome so the app performs all crypto
 *   itself. It then reads the rendered assistant reply from the DOM.
 *
 * SELECTORS (harvested from the captured client JS / HAR)
 *   composer root : [data-hatch-composer-root]
 *   editor        : [data-lexical-editor="true"]   (Lexical contenteditable)
 *   send          : Enter key (analytics event "composer_enter_key")
 *   streaming     : [data-testid="hatch-composer-stop-button"]
 *   messages      : [data-message-item] (data-message-role = user|assistant)
 *   error         : [data-testid="assistant-response-error-notice"]
 *   auth check    : POST /api/auth/check  -> { ok, viewer_id, access_token }
 * ------------------------------------------------------------------
 */
import fs from 'node:fs'
import path from 'node:path'
import { spawn } from 'node:child_process'
import { AsyncLocalStorage } from 'node:async_hooks'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

export const SELECTORS = {
  composerRoot: '[data-hatch-composer-root]',
  editor: '[data-lexical-editor="true"], [data-hatch-composer-root] textarea, [data-hatch-composer-prehydration-input], [data-hatch-composer-root] [contenteditable="true"]',
  stopButton: '[data-testid="hatch-composer-stop-button"]',
  actionSlot: '[data-hatch-composer-action-slot]',
  fileInput: '[data-hatch-composer-root] input[type="file"]',
  attachButton: '[data-hatch-composer-root] button[aria-label="Attach file"]',
  message: '[data-message-item]',
  threadRow: '[data-testid="hatch-thread-row"]',
  assistant: '[data-message-item][data-message-role="assistant"]',
  user: '[data-message-item][data-message-role="user"]',
  errorNotice: '[data-testid="assistant-response-error-notice"]',
  approvalStack: '[data-hatch-composer-approval-stack]',
}

const APP_URL = process.env.MUSE_URL || 'https://muse.ai/'
const PROFILE_DIR =
  process.env.MUSE_PROFILE_DIR || path.join(__dirname, '.muse-profile')
const HEADLESS = process.env.MUSE_HEADLESS === '1'
const CHANNEL = process.env.MUSE_CHANNEL || 'chrome'
const LAUNCH_TIMEOUT = Number(process.env.MUSE_LAUNCH_TIMEOUT_MS || 60000)
// Streaming: only surface text that has been stable this long, so we never emit
// a mid-generation "skeleton" that Muse later rewrites (which would duplicate in
// an append-only SSE stream).
const STREAM_QUIET_MS = Number(process.env.MUSE_STREAM_QUIET_MS || 600)

// Multi-agent: every MCP client (each Claude session, each Orca tab) spawns its own muse-server, and a
// Chrome profile can only be opened by one browser. So nobody owns the browser: the first server starts
// Chrome DETACHED on the profile with a debugging port, and every server (that one included) attaches to
// it over CDP. A server exiting no longer closes the window under the others.
const CDP_PORT = Number(process.env.MUSE_CDP_PORT || 9333)
const CHROME_PATH = process.env.MUSE_CHROME_PATH ||
  (process.platform === 'darwin' ? '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
    : process.platform === 'win32' ? 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe'
      : 'google-chrome')
// Each call runs in its own tab, so parallel agents never type into or navigate each other's thread.
const MAX_TABS = Number(process.env.MUSE_MAX_TABS || 4)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

class MuseDriver {
  constructor() {
    this.ctx = null
    this.browser = null
    this.lastError = null
    this._tab = new AsyncLocalStorage() // the tab of the call in flight
    this._lastPage = null               // tab of the latest call: target of follow-ups with no chat given
    this._tabs = new Set()              // tabs this process opened
    this._idle = []
    this._waiters = []
    this._launching = null
  }

  /** The tab of the current call (inside _serial), else the one the latest call used. */
  get page() {
    const t = this._tab.getStore()
    return t || this._lastPage
  }

  /**
   * Run fn in a tab of its own (was: one shared tab, one call at a time). `prefer` (a thread URL or id)
   * picks the idle tab already showing that thread, then the latest call's tab, then any idle one; with
   * none idle a new tab opens, up to MAX_TABS per process, after which calls wait their turn.
   */
  async _serial(fn, prefer) {
    await this.launch()
    const page = await this._acquire(prefer)
    try {
      return await this._tab.run(page, fn)
    } finally {
      this._lastPage = page
      this._release(page)
    }
  }

  async _acquire(prefer) {
    for (;;) {
      this._idle = this._idle.filter((p) => !p.isClosed())
      for (const p of [...this._tabs]) if (p.isClosed()) this._tabs.delete(p)
      if (this._idle.length) {
        const want = prefer ? String(prefer) : null
        let i = want ? this._idle.findIndex((p) => p.url().includes(want)) : -1
        if (i < 0) i = this._idle.indexOf(this._lastPage)
        if (i < 0) i = 0
        return this._idle.splice(i, 1)[0]
      }
      if (this._tabs.size < MAX_TABS) {
        const p = await this.ctx.newPage()
        p.setDefaultTimeout(LAUNCH_TIMEOUT)
        this._tabs.add(p)
        return p
      }
      await new Promise((r) => this._waiters.push(r))
    }
  }

  _release(page) {
    if (!page.isClosed()) {
      if (this._idle.length >= 1) {
        this._tabs.delete(page)
        if (this._lastPage === page) this._lastPage = this._idle[this._idle.length - 1]
        page.close().catch(() => {})
      } else this._idle.push(page)
    }
    const w = this._waiters.shift()
    if (w) w()
  }

  isRunning() {
    return !!(this.ctx && this.browser && this.browser.isConnected())
  }

  async launch() {
    if (this.isRunning()) return
    if (!this._launching) this._launching = this._launch().finally(() => { this._launching = null })
    return this._launching
  }

  async _launch() {
    const { chromium } = await import('playwright-core')
    const attach = async (url) => {
      const b = await chromium.connectOverCDP(url, { timeout: 5000 })
      this.browser = b
      this.ctx = b.contexts()[0] || (await b.newContext())
      b.on('disconnected', () => {
        this.browser = null
        this.ctx = null
        this._lastPage = null
        this._tabs.clear()
        this._idle = []
      })
    }

    if (process.env.MUSE_CDP) {
      // Escape hatch: attach to a Chrome you started yourself with --remote-debugging-port.
      await attach(process.env.MUSE_CDP)
      return
    }
    const url = `http://127.0.0.1:${CDP_PORT}`
    try {
      await attach(url) // another muse-server already started it
      return
    } catch { /* nobody has: start it */ }

    const args = [
      `--user-data-dir=${PROFILE_DIR}`,
      `--remote-debugging-port=${CDP_PORT}`,
      '--no-first-run',
      '--no-default-browser-check',
      // Same cookie encryption as the playwright-launched Chrome that did the Meta login (it always passes
      // these): without them Chrome uses the real macOS keychain, cannot decrypt the stored cookies, and
      // muse.ai comes up logged out.
      '--use-mock-keychain',
      '--password-store=basic',
      '--disable-blink-features=AutomationControlled',
      '--disable-features=Translate,OptimizationGuideModelDownloading',
      // calls run in background tabs: keep them at full speed
      '--disable-background-timer-throttling',
      '--disable-backgrounding-occluded-windows',
      '--disable-renderer-backgrounding',
      '--window-size=1366,900',
      ...(HEADLESS ? ['--headless=new'] : []),
      APP_URL,
    ]
    const child = spawn(CHROME_PATH, args, { detached: true, stdio: 'ignore' })
    child.on('error', () => {})
    child.unref()
    const deadline = Date.now() + 20000
    let last = null
    while (Date.now() < deadline) {
      await sleep(500)
      try { await attach(url); return } catch (e) { last = e }
    }
    throw new Error(
      `Started Chrome on ${PROFILE_DIR} but nothing answered on ${url}: ${last && last.message}. ` +
        `A Chrome opened on this profile WITHOUT --remote-debugging-port (an older muse-server) is probably ` +
        `holding it - quit that Muse window, then call again.`,
    )
  }

  async gotoApp() {
    const p = await this.requirePage()
    if (!p.url().startsWith('https://muse.ai')) {
      await p.goto(APP_URL, { waitUntil: 'domcontentloaded', timeout: LAUNCH_TIMEOUT })
    }
    return p
  }

  async requirePage() {
    if (!this.isRunning()) await this.launch()
    if (!this.page) throw new Error('no tab: call through _serial')
    return this.page
  }

  /** Cookie-based auth probe performed from inside the page origin. */
  async checkAuth() {
    const p = await this.requirePage()
    try {
      const r = await p.evaluate(async () => {
        try {
          const res = await fetch('/api/auth/check', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: '{}',
            credentials: 'same-origin',
          })
          const j = await res.json().catch(() => null)
          return { status: res.status, ok: !!(j && j.ok), viewerId: (j && j.viewer_id) || null }
        } catch (e) {
          return { status: 0, ok: false, error: String(e) }
        }
      })
      return r
    } catch (e) {
      return { status: 0, ok: false, error: String(e) }
    }
  }

  async hasComposer() {
    const p = await this.requirePage()
    try {
      await p.locator(SELECTORS.editor).first().waitFor({ state: 'visible', timeout: 8000 })
      return true
    } catch {
      return false
    }
  }

  async status() {
    if (!this.isRunning()) {
      try { await this.launch() } catch (e) {
        return { browserRunning: false, url: null, loggedIn: false, composerReady: false, profileDir: PROFILE_DIR, headless: HEADLESS, error: e.message }
      }
    }
    return this._serial(async () => {
      await this.gotoApp()
      const auth = await this.checkAuth()
      const composerReady = await this.hasComposer()
      return {
        browserRunning: true,
        url: this.page.url(),
        loggedIn: auth.ok,
        viewerId: auth.viewerId || null,
        composerReady,
        profileDir: PROFILE_DIR,
        headless: HEADLESS,
        cdp: `127.0.0.1:${CDP_PORT}`,
        tabs: this._tabs.size,
      }
    })
  }

  /** Open the app and, if needed, wait for the user to complete Meta login. */
  async login(timeoutMs = 300000) {
    return this._serial(() => this._login(timeoutMs))
  }

  async _login(timeoutMs) {
    await this.gotoApp()
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      const auth = await this.checkAuth()
      if (auth.ok) return { loggedIn: true, viewerId: auth.viewerId || null }
      await sleep(2000)
    }
    return { loggedIn: false, hint: 'Complete the Meta sign-in in the opened Chrome window, then call muse_login again or muse_status.' }
  }

  async _newChat() {
    const p = await this.requirePage()
    await this.gotoApp()
    // "New side chat" now sits behind Chats (hatch-chat-switcher-trigger) -> Side chat options. The old
    // fallback - reload the home composer - silently dropped every "new thread" into the ONE main chat,
    // where parallel callers then read each other's replies. So no fallback: a new thread or an error.
    await p.keyboard.press('Escape').catch(() => {})
    const compose = p.locator('[data-testid="hatch-chat-compose"]').first()
    const menuItem = p.getByRole('menuitem', { name: /new side chat/i }).first()
    const visible = (l) => l.isVisible().catch(() => false)
    // Wide window: the sidebar's "+" by Side chats IS hatch-chat-compose. Narrow: it hides behind the
    // Chats switcher. Wait for whichever renders - checking once raced the app still hydrating.
    const trigger = p.locator('[data-testid="hatch-chat-switcher-trigger"]').first()
    await compose.or(trigger).first().waitFor({ state: 'visible', timeout: 30000 })
    if (!(await visible(compose))) {
      await trigger.click({ timeout: 5000 })
      await sleep(700)
    }
    if (await visible(compose)) {
      await compose.click({ timeout: 10000 })
    } else {
      await p.locator('[data-testid="hatch-side-chats-options"]').first().click({ timeout: 10000 })
      await sleep(700)
      await menuItem.click({ timeout: 10000 })
    }
    await sleep(1200)
    await this.waitForComposer()
    return { ok: true, url: p.url() }
  }

  async newChat() {
    return this._serial(() => this._newChat())
  }

  /**
   * Normalize attachment entries into Playwright file payloads.
   * Accepts: "C:\\path\\img.png" | { path } | { url } (http/https/data:) |
   *          { name, mimeType, buffer } (Buffer or base64 string).
   */
  async _toFilePayloads(files) {
    const out = []
    for (const f of files || []) {
      if (!f) continue
      let e = f
      if (typeof e === 'string') e = /^(https?:|data:)/i.test(e) ? { url: e } : { path: e }

      if (e.buffer) {
        const buf = Buffer.isBuffer(e.buffer) ? e.buffer : Buffer.from(e.buffer, 'base64')
        out.push({ name: e.name || 'file', mimeType: e.mimeType || 'application/octet-stream', buffer: buf })
        continue
      }
      if (e.url && /^data:/i.test(e.url)) {
        const m = e.url.match(/^data:([^;,]*)(;base64)?,(.*)$/s)
        if (m) {
          const buf = Buffer.from(m[3], m[2] ? 'base64' : 'utf8')
          out.push({ name: e.name || `attachment`, mimeType: m[1] || e.mimeType || 'application/octet-stream', buffer: buf })
          continue
        }
      }
      if (e.url && /^file:\/\//i.test(e.url)) { out.push(fileURLToPath(e.url)); continue }
      if (e.url && /^https?:\/\//i.test(e.url)) {
        const res = await fetch(e.url)
        if (!res.ok) throw new Error(`failed to fetch attachment ${e.url}: ${res.status}`)
        const buf = Buffer.from(await res.arrayBuffer())
        let name = e.name
        if (!name) { try { name = decodeURIComponent(new URL(e.url).pathname.split('/').pop()) || 'attachment' } catch { name = 'attachment' } }
        out.push({ name, mimeType: res.headers.get('content-type') || e.mimeType || 'application/octet-stream', buffer: buf })
        continue
      }
      if (e.url) { out.push(e.url); continue } // bare local path passed via `url`
      if (e.path) out.push(e.path)
    }
    return out
  }

  /** Attach files/images/videos to the composer (no send). */
  async attachFiles(files) {
    return this._serial(() => this._attachFiles(files))
  }

  async _attachFiles(files) {
    const payloads = await this._toFilePayloads(files)
    if (!payloads.length) return { ok: true, count: 0 }
    const p = await this.requirePage()
    await this.gotoApp()
    await this.waitForComposer()
    await p.locator(SELECTORS.fileInput).first().setInputFiles(payloads)
    await sleep(600) // let the app register the attachment(s)
    return { ok: true, count: payloads.length }
  }

  async waitForComposer(timeoutMs = 90000) {
    const p = await this.requirePage()
    await p.locator(SELECTORS.editor).first().waitFor({ state: 'visible', timeout: timeoutMs })
    // wait until not busy (aria-busy on composer root)
    const root = p.locator(SELECTORS.composerRoot).first()
    try {
      await root.waitFor({ state: 'visible', timeout: 10000 })
      const start = Date.now()
      while (Date.now() - start < 15000) {
        const busy = await root.getAttribute('aria-busy')
        if (busy !== 'true') break
        await sleep(250)
      }
    } catch {
      /* composer root optional */
    }
  }

  async composerText() {
    const p = await this.requirePage()
    try {
      const el = p.locator(SELECTORS.editor).first()
      const tag = await el.evaluate((e) => e.tagName)
      if (tag === 'TEXTAREA' || tag === 'INPUT') return await el.inputValue()
      return await el.innerText()
    } catch {
      return ''
    }
  }

  /**
   * Send a prompt and wait for the assistant reply to finish streaming.
   * @returns {Promise<{reply:string, messages:string[], threadUrl:string, elapsedMs:number, error?:string, timedOut?:boolean, needsApproval?:boolean}>}
   */
  async _run(prompt, { timeoutMs = 240000, newThread = false, onDelta, files, chat } = {}) {
    const hasFiles = Array.isArray(files) && files.length > 0
    if ((!prompt || !prompt.trim()) && !hasFiles) throw new Error('prompt is empty')
    const p = await this.requirePage()
    if (newThread) await this._newChat()
    else if (chat !== undefined && chat !== null && chat !== '') await this._openChat(chat)
    else await this.gotoApp()
    await this.waitForComposer()

    // Clear any transient error notice.
    this.lastError = null

    // Attach images/videos/files (if any) before typing the prompt.
    if (hasFiles) {
      const payloads = await this._toFilePayloads(files)
      if (payloads.length) {
        await p.locator(SELECTORS.fileInput).first().setInputFiles(payloads)
        await sleep(800) // let the app ingest + preview the attachment(s)
      }
    }

    const before = await p.locator(SELECTORS.assistant).count()

    // Focus + fill the composer. insertText is O(1) for big prompts;
    // fall back to per-key typing if the editor ignores it.
    const editor = p.locator(SELECTORS.editor).first()
    await editor.click()
    await p.keyboard.press('Control+A').catch(() => {})
    await p.keyboard.press('Delete').catch(() => {})
    try { if (prompt) await p.keyboard.insertText(prompt) } catch { /* fall back below */ }
    await sleep(120)
    if (prompt && !(await this.composerText()).trim()) {
      await p.keyboard.type(prompt, { delay: 1 })
    }
    await sleep(150)

    const t0 = Date.now()
    await p.keyboard.press('Enter')

    // Confirm the message actually left the composer; else click the send action.
    let sent = false
    for (let i = 0; i < 10; i++) {
      await sleep(300)
      const txt = (await this.composerText()).trim()
      const stop = await p.locator(SELECTORS.stopButton).count()
      if (txt.length === 0 || stop > 0) { sent = true; break }
    }
    if (!sent) {
      const slot = p.locator(`${SELECTORS.actionSlot} button`).last()
      if (await slot.count()) { await slot.click({ timeout: 5000 }).catch(() => {}) }
      await p.keyboard.press('Control+Enter').catch(() => {})
    }

    // Wait for the reply to complete.
    const deadline = t0 + timeoutMs
    let started = false
    let lastText = ''
    let committed = ''
    let stableSince = Date.now()
    let timedOut = false

    while (Date.now() < deadline) {
      const stop = await p.locator(SELECTORS.stopButton).count()
      if (stop > 0) started = true
      const n = await p.locator(SELECTORS.assistant).count()
      if (n > before) {
        const txt = await p.locator(SELECTORS.assistant).last().innerText().catch(() => '')
        if (txt !== lastText) {
          lastText = txt
          stableSince = Date.now()
        }
        const idle = Date.now() - stableSince
        // Commit only stable + monotonic growth (never a re-written draft).
        if (onDelta && txt && txt !== committed && txt.startsWith(committed) && idle >= STREAM_QUIET_MS) {
          committed = txt
          try { onDelta(committed) } catch { /* ignore */ }
        }
        if (stop === 0 && ((started && idle > 1200) || (!started && idle > 2500))) break
      }
      if (await p.locator(SELECTORS.errorNotice).count()) {
        this.lastError = await p.locator(SELECTORS.errorNotice).first().innerText().catch(() => 'assistant error')
      }
      await sleep(300)
    }
    if (Date.now() >= deadline) timedOut = true

    const items = await p.locator(SELECTORS.assistant).evaluateAll(
      (els, start) => els.slice(start).map((e) => (e.innerText || '').trim()).filter(Boolean),
      before,
    )
    const reply = (items.length ? items[items.length - 1] : lastText).trim()
    const needsApproval = (await p.locator(SELECTORS.approvalStack).count()) > 0

    // Flush any remainder that arrived after the last stable commit.
    if (onDelta && reply && reply !== committed && reply.startsWith(committed)) {
      try { onDelta(reply) } catch { /* ignore */ }
    }

    return {
      reply,
      messages: items,
      threadUrl: p.url(),
      elapsedMs: Date.now() - t0,
      ...(this.lastError ? { error: this.lastError } : {}),
      ...(timedOut ? { timedOut: true } : {}),
      ...(needsApproval ? { needsApproval: true } : {}),
    }
  }

  /** Send a prompt and wait for the full assistant reply. */
  async chat(prompt, opts = {}) {
    return this._serial(() => this._run(prompt, opts), opts.chat)
  }

  /**
   * Same as chat(), but streams incremental text.
   * @param {(fullText:string)=>void} opts.onDelta called with the cumulative reply text
   */
  async chatStream(prompt, opts = {}) {
    return this._serial(() => this._run(prompt, opts), opts.chat)
  }

  async readLast() {
    return this._serial(() => this._readLast())
  }

  async _readLast() {
    const p = await this.requirePage()
    await this.gotoApp()
    const n = await p.locator(SELECTORS.assistant).count()
    if (!n) return { reply: '', messages: [], threadUrl: p.url() }
    const reply = await p.locator(SELECTORS.assistant).last().innerText().catch(() => '')
    return { reply: reply.trim(), messages: [reply.trim()], threadUrl: p.url() }
  }

  /** List the chats shown in the Muse sidebar (Main chat, Channels, Side chats). */
  async listChats(query) {
    return this._serial(() => this._listChats(query))
  }

  async _listChats(query) {
    const p = await this.requirePage()
    await this.gotoApp()
    await sleep(400)
    let chats = await p.locator(SELECTORS.threadRow).evaluateAll((els) =>
      els.map((el) => ({
        title: (el.innerText || '').replace(/\s+/g, ' ').replace(/\s*More thread actions\s*$/i, '').trim(),
        active: el.getAttribute('aria-current') === 'page' || el.getAttribute('aria-selected') === 'true',
      })).filter((c) => c.title),
    )
    if (query) {
      const q = String(query).toLowerCase()
      chats = chats.filter((c) => c.title.toLowerCase().includes(q))
    }
    return { chats, threadUrl: p.url() }
  }

  /**
   * Open a chat. `target` can be:
   *   - a number         -> chat index from listChats()
   *   - a title/name     -> matched (substring, case-insensitive) against the sidebar
   *   - a thread URL or bare thread id (uuid) -> navigated to directly
   *   - null/undefined   -> the home / main chat
   */
  async openChat(target) {
    return this._serial(() => this._openChat(target), typeof target === 'string' ? target : undefined)
  }

  async _openChat(target) {
    const p = await this.requirePage()
    if (target === null || target === undefined || target === '') {
      await this.gotoApp()
    } else if (typeof target === 'number') {
      await this.gotoApp()
      const row = p.locator(SELECTORS.threadRow).nth(target)
      if (!(await row.count())) throw new Error(`chat index ${target} out of range`)
      await row.click()
    } else {
      const s = String(target).trim()
      if (/^https?:\/\//i.test(s)) {
        await p.goto(s, { waitUntil: 'domcontentloaded', timeout: LAUNCH_TIMEOUT })
      } else if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s)) {
        await p.goto(`https://muse.ai/thread/${s}`, { waitUntil: 'domcontentloaded', timeout: LAUNCH_TIMEOUT })
      } else {
        await this.gotoApp()
        const row = p.locator(SELECTORS.threadRow, { hasText: s }).first()
        if (!(await row.count())) throw new Error(`no chat matching "${s}"`)
        await row.click()
      }
    }
    await sleep(1200)
    await this.waitForComposer()
    return { ok: true, url: p.url() }
  }

  /** Read messages of the currently open chat. */
  async readChat(max = 100) {
    return this._serial(() => this._readChat(max))
  }

  async _readChat(max = 100) {
    const p = await this.requirePage()
    const all = await p.locator(SELECTORS.message).evaluateAll((els) =>
      els
        .map((el) => {
          const text = (el.innerText || '').replace(/^(You:|User message:|Assistant message:)\s*/i, '').trim()
          const media = [...new Set(
            [...el.querySelectorAll('a[href], img[src], video[src], source[src]')]
              .map((n) => n.href || n.currentSrc || n.src || (n.getAttribute && n.getAttribute('src')))
              .filter((u) => u && /^(https?:|blob:)/i.test(u)),
          )]
          return { role: el.getAttribute('data-message-role') || '', text, ...(media.length ? { media } : {}) }
        })
        .filter((m) => m.text || (m.media && m.media.length)),
    )
    return { messages: all.slice(-max), count: all.length, threadUrl: p.url() }
  }

  /** Extract media links (images/videos/attachments) from a chat's messages, optionally downloading. */
  async chatMedia(chat, opts = {}) {
    return this._serial(() => this._chatMedia(chat, opts), typeof chat === 'string' ? chat : undefined)
  }

  async _chatMedia(chat, { download = false, dir } = {}) {
    const p = await this.requirePage()
    if (chat !== undefined && chat !== null && chat !== '') await this._openChat(chat)
    else await this.gotoApp()
    // A thread opened in a fresh tab (any tab, now that calls get their own) renders its messages and
    // images a few seconds after the composer: wait for an assistant image/link before scanning.
    const hasMedia = () => p.locator(`${SELECTORS.assistant} img[src], ${SELECTORS.assistant} video[src], ${SELECTORS.assistant} a[href]`).count()
    for (let t = 0; t < 40 && !(await hasMedia().catch(() => 0)); t++) await sleep(500)
    const items = await p.locator(SELECTORS.message).evaluateAll((els) =>
      els
        .map((el, index) => {
          const role = el.getAttribute('data-message-role') || ''
          const urls = [...new Set(
            [...el.querySelectorAll('a[href], img[src], video[src], source[src]')]
              .map((n) => n.href || n.currentSrc || n.src || (n.getAttribute && n.getAttribute('src')))
              .filter((u) => u && /^(https?:|blob:)/i.test(u)),
          )]
          const text = (el.innerText || '').replace(/^(You:|User message:|Assistant message:)\s*/i, '').replace(/\s+/g, ' ').trim()
          return { index, role, text: text.slice(0, 120), urls }
        })
        .filter((m) => m.urls.length),
    )
    const all = [...new Set(items.flatMap((m) => m.urls))]
    const result = { items, urls: all, count: all.length, threadUrl: p.url() }
    if (download && all.length) result.downloads = await this._downloadMedia(all, dir)
    return result
  }

  async downloadMedia(urls, dir) {
    return this._serial(() => this._downloadMedia(urls, dir))
  }

  async _downloadMedia(urls, dir) {
    const outDir = dir || path.join(__dirname, 'downloads')
    fs.mkdirSync(outDir, { recursive: true })
    const p = this.page
    const saved = []
    for (const u of urls) {
      try {
        let buf = null
        let mime = ''
        // Prefer fetching inside the page: handles blob: URLs and cookie-authed http.
        if (p && !p.isClosed()) {
          const r = await p.evaluate(async (url) => {
            try {
              const res = await fetch(url)
              const ab = await res.arrayBuffer()
              const bytes = new Uint8Array(ab)
              let s = ''
              const CH = 0x8000
              for (let i = 0; i < bytes.length; i += CH) s += String.fromCharCode.apply(null, bytes.subarray(i, i + CH))
              return { ok: res.ok, status: res.status, mime: res.headers.get('content-type') || '', b64: btoa(s), len: bytes.length }
            } catch (e) { return { ok: false, error: String(e) } }
          }, u)
          if (r && r.ok && r.b64) { buf = Buffer.from(r.b64, 'base64'); mime = r.mime }
          else if (r && r.error) throw new Error(r.error)
        }
        if (!buf) {
          const res = await fetch(u)
          if (!res.ok) throw new Error(`HTTP ${res.status}`)
          buf = Buffer.from(await res.arrayBuffer())
          mime = res.headers.get('content-type') || ''
        }
        const ext = mime.includes('mp4') ? '.mp4' : mime.includes('webm') ? '.webm' : mime.includes('webp') ? '.webp'
          : mime.includes('png') ? '.png' : (mime.includes('jpeg') || mime.includes('jpg')) ? '.jpg' : mime.includes('gif') ? '.gif' : ''
        let name = 'media'
        try { name = decodeURIComponent(new URL(u).pathname.split('/').pop() || 'media') } catch { /* blob: -> uuid */ }
        if (!path.extname(name)) name += ext || '.bin'
        const dest = path.join(outDir, `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}-${name}`)
        fs.writeFileSync(dest, buf)
        saved.push({ url: u.startsWith('blob:') ? 'blob' : u, file: dest, bytes: buf.length, mime })
      } catch (e) {
        saved.push({ url: u.startsWith('blob:') ? 'blob' : u, error: String((e && e.message) || e) })
      }
    }
    return { dir: outDir, saved }
  }

  /** Diagnostic: dump transcript HTML so selectors can be re-verified. */
  async dumpDom(maxChars = 20000) {
    return this._serial(() => this._dumpDom(maxChars))
  }

  async _dumpDom(maxChars = 20000) {
    const p = await this.requirePage()
    await this.gotoApp()
    const info = await p.evaluate((sels) => {
      const pick = (s) => document.querySelector(s)
      const log = pick('[role="log"]')
      const first = pick('[data-message-item]')
      const container = log || (first && first.parentElement) || document.body
      return {
        url: location.href,
        title: document.title,
        counts: {
          messages: document.querySelectorAll(sels.message).length,
          assistant: document.querySelectorAll(sels.assistant).length,
          user: document.querySelectorAll(sels.user).length,
          editor: document.querySelectorAll(sels.editor).length,
        },
        hasApproval: !!document.querySelector(sels.approvalStack),
        html: container.outerHTML,
      }
    }, SELECTORS)
    return { ...info, html: (info.html || '').slice(0, maxChars) }
  }

  /** Close this process's tabs and detach. The shared Chrome stays up for the other servers. */
  async close() {
    for (const p of this._tabs) await p.close().catch(() => {})
    this._tabs.clear()
    this._idle = []
    this._lastPage = null
    if (this.browser) await this.browser.close().catch(() => {}) // CDP-attached: disconnects only
    this.browser = null
    this.ctx = null
    return { ok: true, disconnected: true }
  }
}

export const driver = new MuseDriver()
export default driver
