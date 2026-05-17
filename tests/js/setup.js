// Vitest global setup for the scheming-ai-suggestions JS tests.
//
// The JS file under test runs in the browser via CKAN's webassets
// pipeline (jQuery + the ``ckan`` global). jsdom gives us a DOM but
// not those globals — we install them here so the script can be
// loaded with its registration / document-ready bindings intact.
//
// Each `describe` block in a test file gets a fresh module
// registration via ``loadSchemingAiSuggestions`` (exposed below) so
// state from one test can't bleed into another. Polling timers are
// always fake — every test that triggers polling MUST use
// ``vi.useFakeTimers()`` in its setup.

import fs from 'node:fs';
import path from 'node:path';
import { beforeEach, vi } from 'vitest';
// jquery's ESM default-export shape varies under different bundlers
// (sometimes the factory function, sometimes a wrapper). Use
// `createRequire` to get the CJS module — guaranteed to be the
// factory function — then call it with our jsdom window to get a
// real jQuery instance.
import { createRequire } from 'node:module';
const requireCjs = createRequire(import.meta.url);

// Path to the JS file under test. Kept as a constant so the test
// file can reference it without re-deriving.
export const SUT_PATH = path.resolve(
  __dirname,
  '../../ckanext/datapusher_plus/assets/js/scheming-ai-suggestions.js',
);

// Captured by the ckan.module stub. Tests grab the registered
// callback (the per-instance factory) via ``getModuleFactory()``.
let registeredFactory = null;

// jQuery binding: load the jquery source straight into the jsdom
// realm via fs + `new Function` so it attaches to `window.jQuery` /
// `window.$` as the proper callable selector. Importing jquery as
// an ESM/CJS module and calling `jquery(window)` returns the
// jQuery namespace (an object), not the callable form — different
// shape than the in-browser global.
const jqueryPath = requireCjs.resolve('jquery');
const jquerySrc = fs.readFileSync(jqueryPath, 'utf8');
new Function('window', 'document', jquerySrc)(window, document);
// After loading the script into the jsdom realm, the callable $
// is at window.$ / window.jQuery — same as in a real browser.
const $ = window.$;

beforeEach(() => {
  // Reset DOM + globals between tests so module state doesn't bleed.
  document.body.innerHTML = '';
  document.documentElement.innerHTML = '<head></head><body></body>';

  window.$ = $;
  window.jQuery = $;
  global.$ = $;
  global.jQuery = $;

  // CKAN module registrar stub. The real ckan.module(name, callback)
  // wires the returned object up for DOM auto-discovery via
  // ``[data-module=name]``. For tests we just capture the callback
  // so we can instantiate the returned object manually with the
  // ``this`` binding (``this.el``, ``this.options``) we choose.
  window.ckan = {
    SITE_ROOT: '',
    module: (name, factory) => {
      registeredFactory = { name, factory };
    },
  };
  global.ckan = window.ckan;

  // Wipe any leaked global state from prior tests.
  delete window._schemingAiSuggestionsGlobalState;

  // Wipe any previously-registered factory.
  registeredFactory = null;
});

/**
 * Load scheming-ai-suggestions.js fresh into the current jsdom
 * realm and return the registered module factory + a helper that
 * instantiates an "instance" the way ckan.module would.
 *
 * The JS file is loaded via raw fs + ``new Function(...)`` rather
 * than ``import`` because it isn't an ES module — it's a side-effect
 * script that calls ``$``, ``ckan``, and ``window`` at the top
 * level. Using ``new Function`` lets us inject those as named
 * parameters explicitly so they're in lexical scope when the script
 * body executes (indirect ``eval`` in jsdom doesn't reliably see
 * the globals vitest installs on the realm).
 */
export function loadSchemingAiSuggestions() {
  const src = fs.readFileSync(SUT_PATH, 'utf8');
  // Each of these names is referenced unqualified in the script
  // (e.g. ``$(document).ready(...)``, ``ckan.module(...)``,
  // ``window._schemingAiSuggestionsGlobalState``). Pass them in so
  // the function-scope evaluation sees them.
  const runner = new Function('$', 'jQuery', 'ckan', 'window', 'document', src);
  runner(window.$, window.jQuery, window.ckan, window, document);
  if (!registeredFactory) {
    throw new Error(
      'scheming-ai-suggestions.js did not register a ckan module — ' +
      'something is wrong with the setup',
    );
  }
  return registeredFactory;
}

/**
 * Build an "instance" of the module the way ckan.module would, with
 * `this.el`, `this.options`, and the methods returned by the factory.
 *
 * Pass an element (or HTML string), and optionally an options
 * override; returns the bound instance. Doesn't call `initialize()` —
 * tests do that themselves so they can control timing.
 */
export function buildInstance(elOrHtml, optionsOverride = {}) {
  const { factory } = loadSchemingAiSuggestions();
  const moduleObj = factory(window.$);

  let el;
  if (typeof elOrHtml === 'string') {
    const wrap = document.createElement('div');
    wrap.innerHTML = elOrHtml.trim();
    el = wrap.firstChild;
    document.body.appendChild(el);
  } else {
    el = elOrHtml;
  }

  // Spread moduleObj first so `el` + the merged `options` win over
  // its defaults — otherwise `optionsOverride` is silently discarded.
  const instance = {
    ...moduleObj,
    el,
    options: { ...(moduleObj.options || {}), ...optionsOverride },
  };
  return instance;
}

/**
 * Stub `$.ajax` for the duration of one test, capturing every call
 * for inspection and returning the queued response. Pass an array of
 * responses; each subsequent ajax() call dequeues one.
 *
 *   const ajax = stubAjax([
 *     { success: { result: { dpp_suggestions: { ai_suggestions: {...} } } } },
 *     { success: { result: { dpp_suggestions: { ai_suggestions: {...}, STATUS: 'DONE' } } } },
 *   ]);
 *   // ... trigger polling ...
 *   expect(ajax.calls.length).toBe(2);
 *
 * The stub returns synchronously — callers don't need to await
 * anything. The real jQuery $.ajax is async; in tests we don't need
 * the async behavior since vitest fake-timers drive polling.
 */
export function stubAjax(responses) {
  const queue = [...responses];
  const calls = [];
  const stub = vi.fn((opts) => {
    calls.push(opts);
    const next = queue.shift();
    if (next === undefined) {
      // Quiet pass-through for any unexpected extra calls — the
      // assertion is "we called ajax N times", which calls.length
      // tells you, rather than "the (N+1)th call exploded".
      return;
    }
    if (next.success !== undefined && typeof opts.success === 'function') {
      opts.success(next.success);
    }
    if (next.error !== undefined && typeof opts.error === 'function') {
      opts.error(next.error, next.errorStatus || 'error', next.errorThrown || '');
    }
  });
  window.$.ajax = stub;
  return { stub, calls };
}
