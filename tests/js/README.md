# JS unit tests

Pins the contract between [`scheming-ai-suggestions.js`](../../ckanext/datapusher_plus/assets/js/scheming-ai-suggestions.js)
and the Python `AISuggestionsStage` that writes
`package["dpp_suggestions"]["ai_suggestions"]`. The JS polls
`package_show` until `STATUS` is terminal; the Python stage produces the
payload the JS reads. These tests lock that handshake from the JS side
so a future stage refactor that changes the shape gets caught here.

## Run

```bash
npm install      # one-time: installs vitest + jsdom + jquery
npm test         # one-shot run
npm run test:watch
```

Node 22+ recommended (`createRequire` + ESM interop). The toolchain
lives in `package.json` + `vitest.config.js` at the repo root.

## What's covered

* Module registration as `ckan.module('scheming-ai-suggestions', ...)`.
* The early-return guard added in PR #302 follow-up: `initialize()`
  bails when `data-field-name` is missing (a dataset-level CTA that
  grabs this module by accident shouldn't be hidden + start polling).
* `_pollForAiSuggestions` state machine: termination on `STATUS=DONE`,
  cap at `maxPollAttempts`, exponential backoff on ajax error.
* `_showAiSuggestionButtons` DOM mutations.

## What's NOT covered (deferred)

* Popover creation / positioning / dismissal (lots of jQuery
  scaffolding, low signal-per-line for unit tests).
* Real CKAN-on-page integration (covered by manual UI checks against
  the integration stack — see [`tests/integration/README.md`](../integration/README.md)).

## How the harness works

`setup.js`:

1. Loads jQuery directly into the jsdom realm (via `fs` + `new Function`)
   so `window.$` is the proper callable selector, not the namespace
   object you get from `require('jquery')(window)` in this context.
2. Stubs `window.ckan` with a `module(name, factory)` registrar that
   captures the registration so tests can build instances manually.
3. Provides `buildInstance(elOrHtml, optionsOverride)` to construct a
   module instance the way `ckan.module` would — with `this.el`,
   `this.options`, and the lifecycle methods.
4. Provides `stubAjax(responses)` for queue-based ajax mocking; tests
   drive polling with `vi.useFakeTimers()` + `vi.advanceTimersByTime`.

## Why this layout (and not Jest / Karma)

* **Vitest** — minimal config, native ESM, fast. Single
  `vitest.config.js`. ~24 npm packages total (vs. ~500 for Jest+jsdom
  via Babel).
* **jsdom** — standard browser-like env for unit testing DOM mutation
  without a real browser.
* **jQuery loaded via fs+Function** rather than `import jquery from
  'jquery'` because the latter returns the jQuery namespace
  (non-callable object) rather than the `$` selector function under
  vitest's ESM-CJS interop.
