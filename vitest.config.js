import { defineConfig } from 'vitest/config';

// Vitest config for the scheming-ai-suggestions JS module.
//
// The JS file under test is a side-effect script (not an ES module) that
// registers a CKAN module via `ckan.module('scheming-ai-suggestions', cb)`
// and binds document-ready click handlers. The test setup file installs
// the `ckan` / `$` / `window` globals jsdom doesn't supply on its own,
// then loads the script via raw fs read + `eval` inside the jsdom realm
// so the registration is captured before the tests run.
//
// Scope is intentionally narrow: this config covers ONLY the JS tests
// under `tests/js/`. The 171-test Python unit suite continues to run
// via pytest, unrelated to this toolchain.
export default defineConfig({
  test: {
    environment: 'jsdom',
    include: ['tests/js/**/*.test.js'],
    setupFiles: ['./tests/js/setup.js'],
    globals: true,
  },
});
