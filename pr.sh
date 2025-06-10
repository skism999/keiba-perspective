# pr.sh — プルリクエスト作成スクリプト
#!/usr/bin/env bash
set -e

# 現在のブランチ名取得
branch=$(git rev-parse --abbrev-ref HEAD)
# feature/プレフィックスがあれば除去
default_value=${branch#feature/}

# Notion の Quest ページリンク
html_link="Quest Done: <a href=\"https://www.notion.so/${default_value}\" target=\"_blank\">[Notionページを見る]</a>"

# プルリク作成
gh pr create \
  --base main \
  --title "[Quest] ${default_value}" \
  --body "${html_link}"
