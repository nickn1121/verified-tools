name: Upload Excess to IC Source

on:
  workflow_dispatch:
    inputs:
      dry_run:
        type: boolean
        default: false
        description: "Generate CSV only (no FTP upload)"
  schedule:
    - cron: "0 */6 * * *"   # every 6 hours UTC

jobs:
  upload:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node
        uses: actions/setup-node@v4
        with:
          node-version: 20

      - name: Show Node & npm env
        run: |
          node -v
          npm -v

      # Still install even if there is no lockfile
      - name: Install deps (no lockfile)
        run: |
          npm ci --ignore-scripts || npm i

      - name: Export & (optionally) Upload
        env:
          # --- Supabase (read & write logs) ---
          SUPABASE_URL: https://ijzroisggstqkfhpjndq.supabase.co
          SUPABASE_ANON_KEY: >-
            eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlqenJvaXNnZ3N0cWtmaHBqbmRxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTQ0NzYzMjEsImV4cCI6MjA3MDA1MjMyMX0.ZqI2EiNNROR3Y7MflOeHlAY49N7b89oHA_VcEHVEwjc

          # --- FTP creds for IC Source (keep these as secrets) ---
          ICSOURCE_FTP_USER: ${{ secrets.ICSOURCE_FTP_USER }}
          ICSOURCE_FTP_PASS: ${{ secrets.ICSOURCE_FTP_PASS }}
          ICSOURCE_FTPS:     ${{ secrets.ICSOURCE_FTPS }}  # "true" or "false"

          # --- Optional overrides (table/schema) ---
          SUPABASE_EXCESS_TABLE: ${{ vars.SUPABASE_EXCESS_TABLE }}
          SUPABASE_EXCESS_SCHEMA: ${{ vars.SUPABASE_EXCESS_SCHEMA }}

          # --- Run metadata for logs ---
          GITHUB_REPOSITORY: ${{ github.repository }}
          GITHUB_RUN_ID: ${{ github.run_id }}
        run: |
          mkdir -p out
          npm run send:icsource

      - name: Upload CSV as artifact (so you can download it)
        uses: actions/upload-artifact@v4
        with:
          name: verified_inventory
          path: out/verified_inventory.csv
          if-no-files-found: ignore










