#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_DOMAIN="${AIRFLOW_DOMAIN:-airflow.thetransporterlabs.de}"
PGADMIN_DOMAIN="${PGADMIN_DOMAIN:-pgadmin.thetransporterlabs.de}"
REVERSE_PORT="${REVERSE_PORT:-8080}"
AIRFLOW_PORT="${AIRFLOW_PORT:-8085}"
PGADMIN_PORT="${PGADMIN_PORT:-8081}"
SCHEME="${SCHEME:-http}"

echo "Checking containers"
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | sed -n '1p;/airflow_webserver/p;/airflow_scheduler/p;/pgadmin/p;/nginx_reverse_proxy/p'

echo "\nTesting direct ports"
AF_CODE=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:${AIRFLOW_PORT}/" || true)
PG_CODE=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:${PGADMIN_PORT}/" || true)
echo "AIRFLOW_PORT ${AIRFLOW_PORT} => ${AF_CODE}"
echo "PGADMIN_PORT ${PGADMIN_PORT} => ${PG_CODE}"

echo "\nTesting reverse proxy by Host header"
AF_PROXY_CODE=$(curl -s -o /dev/null -w '%{http_code}' -H "Host: ${AIRFLOW_DOMAIN}" "${SCHEME}://localhost:${REVERSE_PORT}/" || true)
PG_PROXY_CODE=$(curl -s -o /dev/null -w '%{http_code}' -H "Host: ${PGADMIN_DOMAIN}" "${SCHEME}://localhost:${REVERSE_PORT}/" || true)
echo "AIRFLOW_HOST ${AIRFLOW_DOMAIN} via :${REVERSE_PORT} => ${AF_PROXY_CODE}"
echo "PGADMIN_HOST ${PGADMIN_DOMAIN} via :${REVERSE_PORT} => ${PG_PROXY_CODE}"

echo "\nTesting domain without port (if reverse proxy on 80/443)"
AF_NOPORT_CODE="N/A"
PG_NOPORT_CODE="N/A"
if [[ "${REVERSE_PORT}" == "80" ]]; then
  AF_NOPORT_CODE=$(curl -s -o /dev/null -w '%{http_code}' --resolve "${AIRFLOW_DOMAIN}:80:127.0.0.1" "${SCHEME}://${AIRFLOW_DOMAIN}/" || true)
  PG_NOPORT_CODE=$(curl -s -o /dev/null -w '%{http_code}' --resolve "${PGADMIN_DOMAIN}:80:127.0.0.1" "${SCHEME}://${PGADMIN_DOMAIN}/" || true)
fi
echo "AIRFLOW_NOPORT ${AIRFLOW_DOMAIN} => ${AF_NOPORT_CODE}"
echo "PGADMIN_NOPORT ${PGADMIN_DOMAIN} => ${PG_NOPORT_CODE}"

echo "\nSummary"
status_ok=true
AF_OK_CODES=("200" "302")
PG_OK_CODES=("200" "302" "401")

af_direct_ok=false
for c in "${AF_OK_CODES[@]}"; do [[ "${AF_CODE}" == "$c" ]] && af_direct_ok=true; done
pg_direct_ok=false
for c in "${PG_OK_CODES[@]}"; do [[ "${PG_CODE}" == "$c" ]] && pg_direct_ok=true; done
af_proxy_ok=false
for c in "${AF_OK_CODES[@]}"; do [[ "${AF_PROXY_CODE}" == "$c" ]] && af_proxy_ok=true; done
pg_proxy_ok=false
for c in "${PG_OK_CODES[@]}"; do [[ "${PG_PROXY_CODE}" == "$c" ]] && pg_proxy_ok=true; done

if [[ "${af_direct_ok}" != "true" ]]; then status_ok=false; echo "Airflow direct port unexpected ${AF_CODE}"; fi
if [[ "${pg_direct_ok}" != "true" ]]; then status_ok=false; echo "pgAdmin direct port unexpected ${PG_CODE}"; fi
if [[ "${af_proxy_ok}" != "true" ]]; then status_ok=false; echo "Airflow proxy unexpected ${AF_PROXY_CODE}"; fi
if [[ "${pg_proxy_ok}" != "true" ]]; then status_ok=false; echo "pgAdmin proxy unexpected ${PG_PROXY_CODE}"; fi
if [[ "${REVERSE_PORT}" == "80" ]]; then
  af_noport_ok=false
  for c in "${AF_OK_CODES[@]}"; do [[ "${AF_NOPORT_CODE}" == "$c" ]] && af_noport_ok=true; done
  pg_noport_ok=false
  for c in "${PG_OK_CODES[@]}"; do [[ "${PG_NOPORT_CODE}" == "$c" ]] && pg_noport_ok=true; done
  if [[ "${af_noport_ok}" != "true" ]]; then status_ok=false; echo "Airflow no-port unexpected ${AF_NOPORT_CODE}"; fi
  if [[ "${pg_noport_ok}" != "true" ]]; then status_ok=false; echo "pgAdmin no-port unexpected ${PG_NOPORT_CODE}"; fi
fi

if [[ "${status_ok}" == "true" ]]; then
  echo "All checks passed"
  exit 0
else
  echo "One or more checks failed"
  exit 1
fi
