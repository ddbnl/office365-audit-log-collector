FROM debian:stable-slim

COPY Linux/OfficeAuditLogCollector /

RUN apt-get update && apt-get install ca-certificates -y

WORKDIR /app

RUN \
  chmod +x /OfficeAuditLogCollector && \
  chown -R 1001:1001 /app /OfficeAuditLogCollector

USER 1001
CMD "ls /app -la"
ENTRYPOINT ["/OfficeAuditLogCollector"]
