# push.sh
#!/usr/bin/env bash
set -e

# 引数でコミットメッセージを受け取り、なければ "update"
msg=${1:-"update"}

git add .
git commit -m "$msg"
git push
