#!/usr/bin/env bash
set -euo pipefail

# Usage: ./build.sh


DATE_STR=$(date +%Y%m%d)
OUTPUT_NAME="jump-server-bundle-${DATE_STR}"
mkdir dist
if [ ! -d "builds" ]; then
  mkdir builds
fi

echo "📦 Step 1: Build shaded jump.jar..."
mvn -B clean package -DskipTests
JAR=$(find target -name 'binaryflux-jump-server-*.jar' | head -n1)
cp "$JAR" dist/jump.jar

echo "🔍 Step 2: Calculate Java modules..."
MODULES=$(jdeps --multi-release 21 --ignore-missing-deps \
           --print-module-deps dist/jump.jar)
MODULES="$MODULES,jdk.crypto.ec"

echo "🧬 Step 3: Create custom Java runtime..."
jlink --add-modules "$MODULES" \
      --strip-debug --no-header-files --no-man-pages \
      --compress=2 \
      --output dist/runtime

echo "📁 Step 4: Add config and service file..."
cp config.yml       dist/
cp binaryflux-jumpserver.service     dist/
cp install.sh     dist/
cp uninstall.sh     dist/
cp README.md     dist/README.md
chmod +x            dist/install.sh
chmod +x            dist/uninstall.sh

echo "🗜️ Step 5: Create distributable tar.gz..."
tar -C dist -czf "builds/${OUTPUT_NAME}.tar.gz" .

rm -rf dist
echo "✅ Bundle ready: ${OUTPUT_NAME}.tar.gz"
ls -lh "builds/${OUTPUT_NAME}.tar.gz"