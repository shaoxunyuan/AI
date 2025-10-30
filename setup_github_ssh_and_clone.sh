#!/usr/bin/env bash
set -euo pipefail

EMAIL="ysx0226@gmail.com"
GITHUB_USER="shaoxunyuan"
REPO_NAME="AI"
KEY_PATH="${HOME}/.ssh/id_ed25519"
TITLE="$(hostname)-$(date +%F)"

echo "==> 检查依赖项（git / ssh-keygen / curl）"
for bin in git ssh-keygen curl ssh-agent ssh-add; do
  command -v "$bin" >/dev/null 2>&1 || { echo "缺少依赖：$bin。请先安装（CentOS: sudo yum install -y git curl openssh-clients）"; exit 1; }
done

mkdir -p "${HOME}/.ssh"
chmod 700 "${HOME}/.ssh"

if [[ ! -f "${KEY_PATH}" ]]; then
  echo "==> 生成 SSH 密钥：${KEY_PATH}"
  ssh-keygen -t ed25519 -C "${EMAIL}" -f "${KEY_PATH}" -N ""
else
  echo "==> 已检测到现有密钥：${KEY_PATH}（跳过生成）"
fi

echo "==> 启动 ssh-agent 并添加密钥"
eval "$(ssh-agent -s)" >/dev/null

# 检测 ssh-add 是否支持 -q
if ssh-add -h 2>&1 | grep -q "\-q"; then
  ssh-add -q "${KEY_PATH}"
else
  ssh-add "${KEY_PATH}"
fi

# 添加 github.com 指纹到 known_hosts
echo "==> 写入 github.com 主机指纹到 known_hosts"
ssh-keyscan -t rsa,ecdsa,ed25519 github.com 2>/dev/null | sort -u >> "${HOME}/.ssh/known_hosts" || true
chmod 600 "${HOME}/.ssh/known_hosts" || true

PUB_KEY_CONTENT="$(cat "${KEY_PATH}.pub")"

# 读取/提示 GitHub Token
GITHUB_TOKEN="${GITHUB_TOKEN:-}"
if [[ -z "${GITHUB_TOKEN}" ]]; then
  echo -n "请输入 GitHub Personal Access Token（具备 write:public_key 或 admin:public_key 权限）: "
  stty -echo
  read -r GITHUB_TOKEN
  stty echo
  echo
fi

echo "==> 向 GitHub 上传公钥"
API_URL="https://api.github.com/user/keys"
TMP_RESP="$(mktemp)"
HTTP_CODE=$(
  curl -sS -o "${TMP_RESP}" -w "%{http_code}" \
    -H "Authorization: token ${GITHUB_TOKEN}" \
    -H "Accept: application/vnd.github+json" \
    -d "{\"title\":\"${TITLE}\",\"key\":\"${PUB_KEY_CONTENT}\"}" \
    "${API_URL}" || true
)

if [[ "${HTTP_CODE}" == "201" ]]; then
  echo "==> 公钥上传成功 ✅"
elif [[ "${HTTP_CODE}" == "422" ]]; then
  echo "==> 公钥可能已存在（422）。继续后续步骤。"
else
  echo "GitHub 返回 HTTP ${HTTP_CODE}，响应如下："
  cat "${TMP_RESP}"
  rm -f "${TMP_RESP}"
  echo
  echo "❌ 上传公钥失败。请检查 Token 权限是否至少包含 write:public_key / admin:public_key。"
  exit 1
fi
rm -f "${TMP_RESP}"

echo "==> 测试与 GitHub 的 SSH 认证"
if ssh -o StrictHostKeyChecking=accept-new -T git@github.com 2>&1 | grep -q "successfully authenticated"; then
  echo "==> SSH 认证成功 ✅"
else
  echo "⚠️  SSH 认证有提示信息（可能依然成功），继续克隆尝试..."
fi

REPO_SSH="git@github.com:${GITHUB_USER}/${REPO_NAME}.git"
TARGET_DIR="${REPO_NAME}"

if [[ -d "${TARGET_DIR}/.git" ]]; then
  echo "==> 目标目录 ${TARGET_DIR} 已存在且是 Git 仓库（跳过克隆）"
else
  echo "==> 克隆仓库：${REPO_SSH}"
  git clone "${REPO_SSH}"
  echo "==> 克隆完成：$(pwd)/${TARGET_DIR}"
fi

echo "🎉 全部完成！你现在可以进入目录： cd ${TARGET_DIR}"
