FROM bitnami/kafka:3.9.0

# Switch to root to perform necessary operations
USER root

COPY kafka-setup.sh /
RUN chmod +x /kafka-setup.sh

CMD ["/kafka-setup.sh"]