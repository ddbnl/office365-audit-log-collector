FROM debian:stable-slim
COPY Docker/entrypoint.sh /
WORKDIR /app
COPY Linux/* .
RUN \
  mv *OfficeAuditLogCollector* OfficeAuditLogCollector && \
  chmod +x OfficeAuditLogCollector && \
  chmod +x /entrypoint.sh

ENTRYPOINT ["/bin/bash", "-c", "/entrypoint.sh"]