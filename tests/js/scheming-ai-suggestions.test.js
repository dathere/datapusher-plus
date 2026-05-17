// Unit coverage for assets/js/scheming-ai-suggestions.js.
//
// The JS pairs with AISuggestionsStage (Python) — the stage writes
// per-field suggestions to `package["dpp_suggestions"]["ai_suggestions"]`
// and this JS reads them back via package_show, polling until
// `STATUS=DONE`. These tests pin the JS half of that contract.
//
// Scope is intentionally narrow: module registration, the
// early-return guard added in PR #302/#303 follow-up, and the
// polling state machine. DOM-heavy interactive paths (popover
// creation, click-outside-to-close) are skipped — they have lots
// of jQuery scaffolding and low signal-per-line for unit tests.
// They're best covered by manual UI checks on a real CKAN form.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  buildInstance,
  loadSchemingAiSuggestions,
  stubAjax,
} from './setup.js';

describe('module registration', () => {
  it('registers itself as the ckan module "scheming-ai-suggestions"', () => {
    const { name, factory } = loadSchemingAiSuggestions();
    expect(name).toBe('scheming-ai-suggestions');
    expect(typeof factory).toBe('function');
  });

  it('factory returns an object with the documented option defaults', () => {
    const { factory } = loadSchemingAiSuggestions();
    const moduleObj = factory(window.$);
    expect(moduleObj.options.pollingInterval).toBe(2500);
    expect(moduleObj.options.maxPollAttempts).toBe(40);
    expect(moduleObj.options.terminalStatuses).toEqual([
      'DONE',
      'ERROR',
      'FAILED',
    ]);
  });

  it('factory exposes initialize + _pollForAiSuggestions + _showAiSuggestionButtons', () => {
    const { factory } = loadSchemingAiSuggestions();
    const moduleObj = factory(window.$);
    expect(typeof moduleObj.initialize).toBe('function');
    expect(typeof moduleObj._pollForAiSuggestions).toBe('function');
    expect(typeof moduleObj._showAiSuggestionButtons).toBe('function');
  });
});

describe('initialize() — early-return guard', () => {
  it('returns early for elements without data-field-name (the guard added on PR #302 / Copilot review)', () => {
    // A CTA button without data-field-name: pre-guard, initialize
    // would hide() it AND register polling. With the guard it just
    // returns — element stays visible, no further global-state
    // mutation. (The global state DOES get initialized when the
    // script loads at top level; what the guard prevents is
    // ``initialize`` going on to populate ``datasetId`` and flip
    // ``globalInitDone`` for an element that has no business
    // driving the polling loop.)
    const cta = buildInstance(
      '<button class="ai-suggestions-button" data-module="scheming-ai-suggestions">Get AI Suggestions</button>',
    );

    cta.initialize();

    // Element still visible.
    expect(cta.el.style.display).not.toBe('none');
    // initialize() did not mutate the (script-load-initialized)
    // global state — datasetId stayed null, globalInitDone stayed
    // false.
    expect(window._schemingAiSuggestionsGlobalState).toBeDefined();
    expect(window._schemingAiSuggestionsGlobalState.globalInitDone).toBe(false);
    expect(window._schemingAiSuggestionsGlobalState.datasetId).toBeNull();
  });

  it('hides element AND starts polling when data-field-name is present', () => {
    // Use a path that contains the dataset segment so initialize
    // can extract datasetId via the URL fallback. ``pushState``
    // mutates ``window.location.pathname`` in-place; redefining
    // ``window.location`` via ``Object.defineProperty`` works in
    // most jsdom versions but is fragile (some configs mark it
    // non-configurable / unforgeable) and pushState is the proper
    // browser-equivalent idiom.
    window.history.pushState({}, '', '/dataset/edit/widgets-dataset-id-1234');

    const btn = buildInstance(
      '<button class="ai-suggestion-btn" data-field-name="description">AI</button>',
    );

    // initialize() kicks off polling, so queue ONE sentinel response
    // (`{}` — no success/error callback triggered, but the call is
    // still counted) to satisfy strict stubAjax. The test only cares
    // that the first call fires; the polling behavior itself is
    // covered by the _pollForAiSuggestions describe block below.
    const { calls } = stubAjax([{}]);
    btn.initialize();

    // The opted-in button is hidden until suggestions arrive.
    expect(btn.el.style.display).toBe('none');
    // datasetId picked up from URL.
    expect(window._schemingAiSuggestionsGlobalState.datasetId).toBe(
      'widgets-dataset-id-1234',
    );
    expect(window._schemingAiSuggestionsGlobalState.globalInitDone).toBe(true);
    // Polling kicked off → exactly one ajax call so far.
    expect(calls.length).toBe(1);
    expect(calls[0].url).toContain('/api/3/action/package_show');
    expect(calls[0].data.id).toBe('widgets-dataset-id-1234');
  });
});

describe('_pollForAiSuggestions() — state machine', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('stops polling when dpp_suggestions.STATUS is in terminalStatuses', () => {
    // Seed a button + datasetId; one ajax response with STATUS=DONE.
    const btn = buildInstance(
      '<button class="ai-suggestion-btn" data-field-name="description">AI</button>',
    );
    window._schemingAiSuggestionsGlobalState = {
      datasetId: 'pkg-1',
      globalInitDone: true,
      pollAttempts: 0,
      isPolling: false,
      aiSuggestions: {},
    };
    const { calls } = stubAjax([
      {
        success: {
          success: true,
          result: {
            dpp_suggestions: {
              ai_suggestions: {
                description: { value: 'Hi.', source: 'qsv describegpt' },
                STATUS: 'DONE',
              },
            },
          },
        },
      },
    ]);

    btn._pollForAiSuggestions();

    // First call fired, suggestions captured in global state.
    expect(calls.length).toBe(1);
    const state = window._schemingAiSuggestionsGlobalState;
    expect(state.aiSuggestions.description.value).toBe('Hi.');
    expect(state.isPolling).toBe(false);

    // Advance timers past the polling interval. No new ajax call
    // should fire — STATUS=DONE terminated the loop.
    vi.advanceTimersByTime(10_000);
    expect(calls.length).toBe(1);
  });

  it('keeps polling when ai_suggestions is absent', () => {
    const btn = buildInstance(
      '<button class="ai-suggestion-btn" data-field-name="description">AI</button>',
    );
    window._schemingAiSuggestionsGlobalState = {
      datasetId: 'pkg-1',
      globalInitDone: true,
      pollAttempts: 0,
      isPolling: false,
      aiSuggestions: {},
    };
    // Two responses: first has no ai_suggestions, second has STATUS=DONE.
    const { calls } = stubAjax([
      {
        success: {
          success: true,
          result: { dpp_suggestions: {} },
        },
      },
      {
        success: {
          success: true,
          result: {
            dpp_suggestions: {
              ai_suggestions: {
                description: { value: 'Hi.', source: 'qsv' },
                STATUS: 'DONE',
              },
            },
          },
        },
      },
    ]);

    btn._pollForAiSuggestions();
    expect(calls.length).toBe(1);

    // Tick through the polling interval (default 2500ms).
    vi.advanceTimersByTime(2500);
    expect(calls.length).toBe(2);

    // The second response carried STATUS=DONE — polling must stop.
    // Without this assertion a regression that ignored terminal
    // status would still pass the test (the first response would
    // re-trigger polling and the count would just hit 2 anyway).
    vi.advanceTimersByTime(2500);
    expect(calls.length).toBe(2);
  });

  it('stops polling after maxPollAttempts', () => {
    const btn = buildInstance(
      '<button class="ai-suggestion-btn" data-field-name="description">AI</button>',
      { maxPollAttempts: 3, pollingInterval: 100 },
    );
    window._schemingAiSuggestionsGlobalState = {
      datasetId: 'pkg-1',
      globalInitDone: true,
      pollAttempts: 0,
      isPolling: false,
      aiSuggestions: {},
    };
    // Responses with no terminal STATUS so polling would otherwise
    // continue forever.
    const { calls } = stubAjax(
      Array.from({ length: 10 }, () => ({
        success: { success: true, result: { dpp_suggestions: {} } },
      })),
    );

    btn._pollForAiSuggestions();
    // First call kicked off immediately.
    expect(calls.length).toBe(1);

    // Advance enough timer ticks to exceed maxPollAttempts.
    for (let i = 0; i < 10; i += 1) {
      vi.advanceTimersByTime(100);
    }

    // Capped at maxPollAttempts.
    expect(calls.length).toBe(3);
    expect(window._schemingAiSuggestionsGlobalState.isPolling).toBe(false);
  });

  it('retries with backoff on ajax error', () => {
    const btn = buildInstance(
      '<button class="ai-suggestion-btn" data-field-name="description">AI</button>',
      { pollingInterval: 100, maxPollAttempts: 5 },
    );
    window._schemingAiSuggestionsGlobalState = {
      datasetId: 'pkg-1',
      globalInitDone: true,
      pollAttempts: 0,
      isPolling: false,
      aiSuggestions: {},
    };
    // First call errors, then immediately ramps backoff. JS computes
    // nextDelay = interval * 1.2^min(attempts, 7). For our setup
    // (interval=100, attempts=1 after first call) → 120ms.
    const { calls } = stubAjax([
      { error: {}, errorStatus: 'timeout', errorThrown: '' },
      {
        success: {
          success: true,
          result: {
            dpp_suggestions: {
              ai_suggestions: { STATUS: 'DONE' },
            },
          },
        },
      },
    ]);

    btn._pollForAiSuggestions();
    expect(calls.length).toBe(1);

    // Backoff after 1 attempt = 100 * 1.2 = 120ms. Tick a bit more.
    vi.advanceTimersByTime(150);
    expect(calls.length).toBe(2);
  });
});

describe('_showAiSuggestionButtons() — DOM updates', () => {
  it('reveals + updates data attributes for fields with matching suggestions', () => {
    const btn = buildInstance(
      '<button class="ai-suggestion-btn" data-field-name="description" style="display:none">AI</button>',
    );

    // Suggestions arriving from the polling response. The
    // ``confidence`` slot is optional; without it the JS appends
    // ``(Confidence: N/A)`` to the source — the test just checks
    // for the value bits.
    btn._showAiSuggestionButtons({
      description: { value: 'A widget dataset.', source: 'qsv describegpt' },
    });

    expect(btn.el.getAttribute('data-suggestion-value')).toBe(
      'A widget dataset.',
    );
    expect(btn.el.getAttribute('data-suggestion-source')).toContain(
      'qsv describegpt',
    );
    // Element revealed (jQuery .show() clears inline display:none).
    expect(btn.el.style.display).not.toBe('none');
  });

  it('leaves unrelated buttons hidden', () => {
    // Two buttons in the DOM — one we drive (description), one
    // unrelated (tags). The reveal call only carries a description
    // suggestion, so the tags button stays hidden.
    const desc = buildInstance(
      '<button class="ai-suggestion-btn" data-field-name="description" style="display:none">AI</button>',
    );
    const otherBtn = document.createElement('button');
    otherBtn.className = 'ai-suggestion-btn';
    otherBtn.setAttribute('data-field-name', 'tags');
    otherBtn.style.display = 'none';
    document.body.appendChild(otherBtn);

    desc._showAiSuggestionButtons({
      description: { value: 'A widget dataset.', source: 'qsv' },
    });

    expect(otherBtn.style.display).toBe('none');
  });

  it('does not break when a suggestion is present but its value is empty', () => {
    const btn = buildInstance(
      '<button class="ai-suggestion-btn" data-field-name="description" style="display:none">AI</button>',
    );

    // A suggestion with no value — the JS guards on ``suggestion.value``
    // so this should leave the button hidden.
    btn._showAiSuggestionButtons({
      description: { value: '', source: 'qsv' },
    });

    expect(btn.el.style.display).toBe('none');
  });
});
