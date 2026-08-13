FROM node:18-alpine

# Nastavenie pracovného adresára
WORKDIR /app

# Kopírovanie package.json a package-lock.json (ak existuje)
COPY package*.json ./

# Inštalácia závislostí
RUN npm install --omit=dev

# Kopírovanie zvyšku aplikácie
COPY . .

# Vytvorenie adresárov pre dáta a uploady (ak neexistujú)
RUN mkdir -p data public/uploads

# Exponovanie portu, na ktorom beží server
EXPOSE 3001

# Spustenie aplikácie
CMD ["node", "server.js"]
