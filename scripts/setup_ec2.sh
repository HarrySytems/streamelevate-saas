#!/bin/bash
set -e

echo "=========================================================="
echo "  StreamElevate SaaS / RadarStream - AWS EC2 Bootstrap   "
echo "=========================================================="

# 1. Configurar Swap de 2GB (para estabilidad de FFmpeg y Puppeteer)
if [ ! -f /swapfile ]; then
    echo "[1/6] Configurando 2GB de Memoria Swap..."
    sudo fallocate -l 2G /swapfile || sudo dd if=/dev/zero of=/swapfile bs=1M count=2048
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    echo "Swap de 2GB activado con éxito."
else
    echo "[1/6] Swapfile ya existe."
fi

# 2. Actualizar sistema e instalar dependencias básicas
echo "[2/6] Instalando herramientas del sistema y FFmpeg..."
sudo apt-get update -y
sudo apt-get install -y curl git ffmpeg build-essential

# 3. Instalar Node.js 20 LTS
echo "[3/6] Instalando Node.js 20 LTS..."
if ! command -v node &> /dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
    sudo apt-get install -y nodejs
fi
node -v
npm -v

# 4. Instalar librerías nativas requeridas por Puppeteer (Chromium) en Ubuntu
echo "[4/6] Instalando librerías para Chromium/Puppeteer..."
sudo apt-get install -y \
    ca-certificates \
    fonts-liberation \
    libasound2t64 || sudo apt-get install -y libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libgcc1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libstdc++6 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    lsb-release \
    wget \
    xdg-utils

# 5. Instalar PM2 para ejecución 24/7 en segundo plano
echo "[5/6] Instalando PM2..."
sudo npm install -g pm2

# 6. Clonar o actualizar el repositorio
echo "[6/6] Desplegando RadarStream..."
cd ~
if [ -d "streamelevate-saas" ]; then
    echo "Carpeta streamelevate-saas ya existe. Actualizando con git pull..."
    cd streamelevate-saas
    git pull origin main
else
    git clone https://github.com/HarrySytems/streamelevate-saas.git
    cd streamelevate-saas
fi

npm install

echo "=========================================================="
echo "  ¡Instalación completada con éxito!                     "
echo "  Para iniciar el servidor ejecuta:                      "
echo "    pm2 start server.js --name radarstream               "
echo "    pm2 save                                             "
echo "    pm2 startup                                          "
echo "=========================================================="
