FROM node:22-alpine AS builder
WORKDIR /build

ARG PACKAGES_READ_TOKEN
RUN echo "@rebus-industries:registry=https://npm.pkg.github.com" >> /root/.npmrc && \
    echo "//npm.pkg.github.com/:_authToken=${PACKAGES_READ_TOKEN}" >> /root/.npmrc

COPY package.json package-lock.json* ./
RUN npm ci --no-audit --no-fund

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# Copy migrations from prism-shared into a staging dir for the runtime stage
RUN cp -r node_modules/@rebus-industries/prism-shared/src/db/migrations ./dist-migrations

FROM node:22-alpine AS runtime
WORKDIR /prism-visualiser

RUN apk add --no-cache wget

ARG PACKAGES_READ_TOKEN
RUN echo "@rebus-industries:registry=https://npm.pkg.github.com" >> /root/.npmrc && \
    echo "//npm.pkg.github.com/:_authToken=${PACKAGES_READ_TOKEN}" >> /root/.npmrc

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force && rm /root/.npmrc

COPY --from=builder /build/dist ./dist
COPY --from=builder /build/dist-migrations ./migrations

ENV NODE_ENV=production
ENV PORT=8768
ENV MIGRATIONS_DIR=/prism-visualiser/migrations

EXPOSE 8768
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -qO- http://localhost:${PORT}/health || exit 1

CMD ["node", "dist/main.js"]
