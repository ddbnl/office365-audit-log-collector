#!/bin/bash
if [[ -z "${TENANT_ID}" ]]; then
  echo "The Tendant ID is undefined, please specify using the Environment Variable TENANT_ID"
  exit 1
fi
if [[ -z "${CLIENT_KEY}" ]]; then
  echo "The Client Key is undefined, please specify using the Environment Variable CLIENT_KEY"
  exit 1
fi
if [[ -z "${SECRET_KEY}" ]]; then
  echo "The Secret Key is undefined, please specify using the Environment Variable SECRET_KEY"
  exit 1
fi
if [[ ! -f /app/config.yaml ]]; then
  echo "No Config File found. Please Mount under /app/config.yaml. Examples: https://github.com/ddbnl/office365-audit-log-collector/tree/master/ConfigExamples"
  exit 1
fi
cd /app
./OfficeAuditLogCollector ${TENANT_ID} ${CLIENT_KEY} ${SECRET_KEY} --config /app/config.yaml