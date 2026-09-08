# ==============================================================================
# Project Argus: Hardened Spark 4.1.2 Image with Embedded livy-next REST Gateway
# ==============================================================================
FROM apache/spark:4.1.2

# Switch to root to perform installations and write configuration
USER root

# Install required Python packages for PySpark Declarative Pipelines (SDP) and Spark Connect
RUN pip3 install --no-cache-dir \
    "pyyaml" \
    "pandas>=2.2.0" \
    "pyarrow>=15.0.0" \
    "grpcio>=1.48.1" \
    "protobuf<7.0.0" \
    "grpcio-status>=1.48.1" \
    "zstandard>=0.25.0" \
    "redis" \
    "neo4j"

    

# Copy Spark defaults configuration
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Create the event logs directory with correct permissions
RUN mkdir -p /opt/spark/event_logs && \
    chown -R spark:spark /opt/spark/event_logs

# Copy the pre-built Linux livy-next binary from the host
COPY bin/livy-next /usr/local/bin/livy-next
RUN chmod +x /usr/local/bin/livy-next

# Create the entrypoint script
RUN echo '#!/bin/bash\n\
echo "Starting Spark Connect Server on port 15002..."\n\
/opt/spark/sbin/start-connect-server.sh --master "local[*]"\n\
\n\
echo "Waiting for Spark Connect to start..."\n\
sleep 5\n\
\n\
echo "Starting livy-next REST API gateway on port 8998..."\n\
exec /usr/local/bin/livy-next --addr :8998 --spark-remote sc://localhost:15002 --cors-allowed-origins "*"\n\
' > /usr/local/bin/entrypoint.sh && \
    chmod +x /usr/local/bin/entrypoint.sh

# Switch back to the non-root spark user
USER spark

# Expose ports:
# - 8998: Livy-Next REST API Gateway
# - 15002: Spark Connect gRPC endpoint
# - 4040: Spark Web UI (for active application monitoring)
EXPOSE 8998 15002 4040

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
