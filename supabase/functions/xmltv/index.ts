# .github/workflows/deploy-xmltv.yml
name: Deploy xmltv Edge Function

on:
  push:
    paths:
      - "supabase/functions/xmltv/**"
      - ".github/workflows/deploy-xmltv.yml"
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Supabase CLI
        uses: supabase/setup-cli@v1
        with:
          version: latest

      # set runtime secrets for the function (no DB password needed)
      - name: Set function secrets
        env:
          SUPABASE_ACCESS_TOKEN: ${{ secrets.SUPABASE_ACCESS_TOKEN }}
        run: |
          supabase secrets set \
            --project-ref "${{ secrets.SUPABASE_PROJECT_REF }}" \
            SUPABASE_URL="https://${{ secrets.SUPABASE_PROJECT_REF }}.supabase.co" \
            SUPABASE_SERVICE_ROLE_KEY="${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}"

      - name: Deploy xmltv
        env:
          SUPABASE_ACCESS_TOKEN: ${{ secrets.SUPABASE_ACCESS_TOKEN }}
        run: |
          supabase functions deploy xmltv \
            --project-ref "${{ secrets.SUPABASE_PROJECT_REF }}"
