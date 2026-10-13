FROM node:20-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY tsconfig.json ./
COPY src ./src
COPY apps ./apps
COPY infra/netem-entrypoint.sh /usr/local/bin/netem-entrypoint.sh
RUN chmod +x /usr/local/bin/netem-entrypoint.sh
ENV NODE_ENV=production
ENTRYPOINT ["netem-entrypoint.sh"]
CMD ["npx", "tsx", "apps/gateway/main.ts"]
