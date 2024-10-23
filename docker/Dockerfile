# Use the official Go image for building the Go application
# Step 1: Build stage
FROM golang:1.22.7 AS builder

# Set environment variables for cross-compilation
ARG TARGETOS=linux
ARG TARGETARCH
ENV GOOS=$TARGETOS
ENV GOARCH=$TARGETARCH

# Set the working directory inside the container
WORKDIR /app

# Copy Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the remaining source code
COPY . .

# Build the Go application for the target OS and architecture
RUN go build -o /myduckserver

# Step 2: Final stage
FROM mysql:9.1

ARG TARGETOS=linux
ARG TARGETARCH

# Install curl, libstdc++6, and other necessary tools
RUN microdnf update && microdnf install -y \
    curl \
    unzip \
    libstdc++ \
    python3 \
    python3-pip && \
    microdnf clean all

# Install Python dependencies
RUN pip install --no-cache-dir "sqlglot[rs]"

# Dynamic DuckDB CLI download based on architecture
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        ARCH="aarch64"; \
    else \
        ARCH="amd64"; \
    fi && \
    curl -LJO https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-$ARCH.zip \
    && unzip duckdb_cli-linux-$ARCH.zip \
    && chmod +x duckdb \
    && mv duckdb /usr/local/bin \
    && rm duckdb_cli-linux-$ARCH.zip \
    && duckdb -c 'SELECT extension_name, loaded, install_path FROM duckdb_extensions() where installed'

RUN duckdb -version

# Set the working directory inside the container
WORKDIR /app

# Copy the compiled Go binary from the builder stage
COPY --from=builder /myduckserver /usr/local/bin/myduckserver

ENV LC_CTYPE="en_US.UTF-8"
ENV LANG="en_US.UTF-8"

# Expose the port your server will run on (if applicable)
EXPOSE 3306

# Set the default command to run the Go server
CMD ["myduckserver"]