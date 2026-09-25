FROM golang:1.25-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /build/bin/livy-next cmd/livy-next/main.go

# ==============================================================================
# Stage 2: Apache Spark 4.1.2 Image with Embedded livy-next REST Gateway
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

# Copy the pre-built Linux livy-next binary from builder stage
COPY --from=builder /build/bin/livy-next /usr/local/bin/livy-next
RUN chmod +x /usr/local/bin/livy-next

# Copy and configure the container entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Switch back to the non-root spark user
USER spark

# Expose ports:
# - 8998: Livy-Next REST API Gateway
# - 15002: Spark Connect gRPC endpoint
# - 4040: Spark Web UI (for active application monitoring)
EXPOSE 8998 15002 4040

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
