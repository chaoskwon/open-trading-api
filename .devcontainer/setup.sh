#!/bin/bash
set -e

echo "=== KIS Strategy Builder: Codespaces Setup ==="

# 1. uv 설치
if ! command -v uv &> /dev/null; then
  echo "[1/5] Installing uv..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi
echo "[1/5] uv: $(uv --version)"

# 2. Python 의존성 (루트 + strategy_builder)
echo "[2/5] Installing Python dependencies..."
uv sync --quiet
cd strategy_builder && uv sync --quiet && cd ..

# 3. Frontend 의존성
echo "[3/5] Installing frontend dependencies..."
cd strategy_builder/frontend && npm install --silent && cd ../..

# 4. 카테고리 마스터 데이터 생성
echo "[4/5] Generating category master data..."
cd strategy_builder && uv run python backend/scripts/generate_category_data.py 2>&1 | tail -1 && cd ..

# 5. KIS config 설정 (Codespaces Secrets → kis_devlp.yaml)
echo "[5/5] Setting up KIS config..."
KIS_CONFIG_DIR="$HOME/KIS/config"
KIS_CONFIG_FILE="$KIS_CONFIG_DIR/kis_devlp.yaml"
mkdir -p "$KIS_CONFIG_DIR"

if [ -n "$KIS_APP_KEY" ] && [ -n "$KIS_APP_SECRET" ] && [ -n "$KIS_ACCOUNT" ]; then
  cat > "$KIS_CONFIG_FILE" << YAML
# KIS API 설정 (Codespaces Secrets로 자동 생성됨)

# 실전투자 (Codespaces에서는 모의투자만 사용 — 실전 키는 더미)
my_app: "dummy_real_app_key"
my_sec: "dummy_real_secret"

# 모의투자
paper_app: "${KIS_APP_KEY}"
paper_sec: "${KIS_APP_SECRET}"

# HTS ID
my_htsid: "@codespaces"

# 계좌번호 앞 8자리
my_acct_stock: "00000000"
my_paper_stock: "${KIS_ACCOUNT}"

# 계좌번호 뒤 2자리
my_prod: "01"

# domain
prod: "https://openapi.koreainvestment.com:9443"
ops: "ws://ops.koreainvestment.com:21000"
vps: "https://openapivts.koreainvestment.com:29443"
vops: "ws://ops.koreainvestment.com:31000"

my_token: ""
my_agent: "Mozilla/5.0"
YAML
  echo "      KIS config created from Codespaces Secrets"
else
  echo "      [SKIP] KIS_APP_KEY / KIS_APP_SECRET / KIS_ACCOUNT secrets not set."
  echo "      Set them in: GitHub > Settings > Codespaces > Secrets"
  echo "      Then rebuild the container."
fi

echo ""
echo "=== Setup Complete ==="
echo "Run: cd strategy_builder && bash start.sh"
echo ""
