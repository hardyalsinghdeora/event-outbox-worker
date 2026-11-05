import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    // node:sqlite is still flagged experimental on Node 22; keep worker output clean.
    execArgv: ['--no-warnings=ExperimentalWarning'],
    testTimeout: 15_000,
  },
});
