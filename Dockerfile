FROM node:20-slim AS builder

WORKDIR /app

# Install build essentials & pnpm
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g pnpm@9.15.4

# Copy dependency specifications
COPY package.json pnpm-lock.yaml ./

# Install dependencies (reproducible install)
RUN pnpm install --frozen-lockfile

# Copy application sources
COPY . .

# Set build-time environment variables for Next.js build
ENV NEXT_TELEMETRY_DISABLED=1
ENV NODE_ENV=production

# Build Next.js application
RUN pnpm run build

# Production runtime stage
FROM node:20-slim AS runner

WORKDIR /app

RUN npm install -g pnpm@9.15.4

ENV NODE_ENV=production
ENV PORT=3000
ENV HOSTNAME="0.0.0.0"
ENV NEXT_TELEMETRY_DISABLED=1

# Copy full built application and node_modules from builder
COPY --from=builder /app ./

EXPOSE 3000

HEALTHCHECK --interval=20s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:3000/api/health || exit 0

CMD ["pnpm", "start"]
