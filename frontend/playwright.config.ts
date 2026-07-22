import {defineConfig} from '@playwright/test'

export default defineConfig({
  testDir:'./e2e',
  // Keep generated traces outside the source tree.  Test containers may write as a
  // different UID, which previously left frontend/test-results impossible for the
  // host Playwright process to replace on the next run.
  outputDir:process.env.PLAYWRIGHT_OUTPUT_DIR||'../.local/playwright-results',
  timeout:30_000,
  // Four concurrent Firefox processes exhaust the older staging host before navigation starts.
  // Run viewports sequentially; this also makes full-page screenshot output deterministic.
  workers:1,
  use:{baseURL:process.env.PLAYWRIGHT_BASE_URL||'http://127.0.0.1:18000',trace:'retain-on-failure'},
  projects:[
    {name:'mobile',use:{browserName:'firefox',viewport:{width:360,height:800}}},
    {name:'tablet',use:{browserName:'firefox',viewport:{width:768,height:1024}}},
    {name:'desktop',use:{browserName:'firefox',viewport:{width:1440,height:1000}}},
    {name:'wide',use:{browserName:'firefox',viewport:{width:1900,height:1000}}},
  ]
})
