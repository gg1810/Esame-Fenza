FROM node:20-slim

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

EXPOSE 5173

# Rimuovi --host da qui, lascia che vite.config.ts lo gestisca
CMD ["npm", "run", "dev"]