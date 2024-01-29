FROM debian:stable-slim
COPY Docker/entrypoint.sh /
WORKDIR /app
COPY Linux/* .
RUN apt-get update && apt-get install ca-certificates -y
RUN \
  mv *OfficeAuditLogCollector* /usr/local/bin/OfficeAuditLogCollector && \
  chmod +x /usr/local/bin/OfficeAuditLogCollector && \
  chmod +x /entrypoint.sh && \
  chown -R 1001:1001 /app /entrypoint.sh /usr/local/bin/OfficeAuditLogCollector
USER 1001
ENTRYPOINT ["/bin/bash", "-c", "/entrypoint.sh"]