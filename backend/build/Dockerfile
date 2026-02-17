# Stage 1: Build the frontend
FROM oven/bun:1 AS frontend-builder
WORKDIR /app/frontend

# Copy package files first to leverage Docker cache
COPY frontend/package.json frontend/bun.lock ./
RUN bun install --frozen-lockfile

# Copy the rest of the frontend source code
COPY frontend/ ./
# Build the frontend (outputs to /app/frontend/dist)
RUN bun run build

# Stage 2: Build the backend with embedded frontend
FROM golang:1.25 AS backend-builder
WORKDIR /app

# Copy backend module files first
COPY backend/go.mod backend/go.sum ./
RUN go mod download

# Copy backend source code
COPY backend/ ./

# Copy the built frontend artifacts into the backend source tree
# This is crucial for the //go:embed directive to pick them up
COPY --from=frontend-builder /app/frontend/dist ./public

# Build the Go binary
# CGO_ENABLED=0 creates a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -o lakehouse-admin .

# Stage 3: Create the final runtime image
# distroless/static is ideal for static Go binaries (small, secure)
FROM gcr.io/distroless/static-debian12 AS runtime

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=backend-builder /app/lakehouse-admin .

# Copy migration files into the runtime image
COPY --from=backend-builder /app/build/migrations ./build/migrations

# Expose the port configured in config.dist.yml
EXPOSE 8081

# Run the binary
ENTRYPOINT ["./lakehouse-admin"]
