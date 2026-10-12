# Kurir Message Broker Container
FROM node:22-alpine

WORKDIR /app

# Copy package manifest (zero dependencies, no npm install needed)
COPY package.json ./

# Copy broker application code, dashboard, and client tools
COPY src/ ./src/
COPY public/ ./public/
COPY clients/ ./clients/

ENV NODE_ENV=production
ENV BROKER_PORT=4000
ENV DASHBOARD_PORT=8080
ENV HOST=0.0.0.0

# Expose ports:
#   4000: TCP broker for producers & consumers
#   8080: HTTP dashboard + SSE live stream + API
EXPOSE 4000 8080

# Health check verifies that the HTTP server responds
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/api/state >/dev/null || exit 1

CMD ["node", "src/server.js"]
