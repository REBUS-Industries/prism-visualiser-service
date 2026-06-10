FROM node:22-alpine AS builder
WORKDIR /build

ARG PACKAGES_READ_TOKEN
RUN echo "@rebus-industries:registry=https://npm.pkg.github.com" >> /root/.npmrc && \
    echo "//npm.pkg.github.com/:_authToken=${PACKAGES_READ_TOKEN}" >> /root/.npmrc

COPY package.json package-lock.json* ./
RUN npm ci --no-audit --no-fund
RUN npm prune --omit=dev && rm /root/.npmrc

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:22-alpine AS runtime
WORKDIR /prism-visualiser

COPY package.json ./
COPY --from=builder /build/node_modules ./node_modules
COPY --from=builder /build/dist ./dist

ENV NODE_ENV=production
ENV PORT=8768

EXPOSE 8768
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -qO- http://localhost:${PORT}/health || exit 1

CMD ["node", "dist/main.js"]
