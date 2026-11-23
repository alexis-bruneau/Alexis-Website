#!/usr/bin/env bash
# Smoke test helper for deploy_azure.yml.
# Usage: bash .github/scripts/wait_for_200.sh SECONDS URL [URL...]
# Each URL must end in HTTP 200 after following redirects (at most 5): on quartierinsights.ca the root answers 302 to
# /ottawa_map on purpose, so a test that stopped at the 302 could never pass. A redirect loop or a redirect to an error still fails.
# Retries every 10 s; fails when SECONDS have passed in total.
# Prints only the time, status code and URL, never headers or bodies.
set -u

limit="$1"
shift
deadline=$(( $(date +%s) + limit ))

for url in "$@"; do
  while :; do
    code=$(curl -s -L --max-redirs 5 -o /dev/null -w '%{http_code}' --max-time 60 "$url" || true)
    echo "$(date +%T)  HTTP ${code}  ${url}"
    if [ "$code" = "200" ]; then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "::error::${url} did not return 200 within ${limit} seconds (last answer: ${code})"
      exit 1
    fi
    sleep 10
  done
done
echo "All URLs returned 200."
