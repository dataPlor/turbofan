FROM --platform=linux/arm64 node:20-slim

COPY --from=proxy-ca proxy-ca.crt /usr/local/share/ca-certificates/proxy-ca.crt
RUN apt-get update && apt-get install -y ca-certificates && update-ca-certificates && rm -rf /var/lib/apt/lists/*
ENV NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/proxy-ca.crt

WORKDIR /app
COPY package.json .
RUN npm install --omit=dev

COPY . .

CMD ["node", "index.mjs"]
