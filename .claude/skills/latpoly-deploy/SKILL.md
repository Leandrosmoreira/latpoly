---
name: latpoly-deploy
description: Deploy latpoly to VPS, commit and push code, restart the bot service, or check VPS status. Use when user says "comita", "push", "deploy", "restart", "stop", "start", or mentions the VPS.
disable-model-invocation: true
allowed-tools: Read, Grep, Glob, Bash
argument-hint: "[commit message or action: push/restart/stop/status]"
---

# Latpoly Deploy & VPS Management

Deploy code to the latpoly VPS at `31.97.165.64`.

## VPS DETAILS

- **IP**: 31.97.165.64
- **User**: root
- **Bot path**: ~/latpoly
- **Service**: `latpoly` (systemd)
- **Python**: venv at ~/latpoly/.venv
- **Git remote**: https://github.com/Leandrosmoreira/latpoly

## DEPLOY WORKFLOW

### 1. Commit & Push (local)
```bash
# Check what changed
git status
git diff --stat

# Stage specific files (NEVER git add -A)
git add src/latpoly/strategy/strategy_N.py
git add src/latpoly/strategy/registry.py

# Commit
git commit -m "feat: description

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"

# Push
git push origin main
```

### 2. Pull on VPS
```bash
ssh root@31.97.165.64 'cd ~/latpoly && git pull origin main'
```

### 3. Restart Service
```bash
ssh root@31.97.165.64 'systemctl restart latpoly'
```

### 4. Verify
```bash
ssh root@31.97.165.64 'systemctl status latpoly'
ssh root@31.97.165.64 'journalctl -u latpoly -n 20 --no-pager'
```

## COMMON ACTIONS

### Full deploy (commit + push + pull + restart)
```bash
# Local
git add [files]
git commit -m "message"
git push origin main

# VPS
ssh root@31.97.165.64 'cd ~/latpoly && git pull origin main && systemctl restart latpoly && sleep 2 && journalctl -u latpoly -n 10 --no-pager'
```

### Just restart
```bash
ssh root@31.97.165.64 'systemctl restart latpoly'
```

### Stop bot
```bash
ssh root@31.97.165.64 'systemctl stop latpoly'
```

### Check logs
```bash
ssh root@31.97.165.64 'journalctl -u latpoly -n 50 --no-pager'
```

### Change strategy on VPS
```bash
ssh root@31.97.165.64 'cd ~/latpoly && sed -i "s/LATPOLY_STRATEGY=.*/LATPOLY_STRATEGY=strategy_4/" .env && systemctl restart latpoly'
```

## GIT CONFLICT RESOLUTION

If `git pull` fails on VPS due to conflicts:
```bash
ssh root@31.97.165.64 'cd ~/latpoly && git stash && git pull origin main && git stash pop'
```

If `.env` conflicts (common — VPS has different env):
```bash
ssh root@31.97.165.64 'cd ~/latpoly && git checkout --theirs .env && git add .env'
```

## SAFETY RULES

- NEVER commit `.env` files (contains API keys)
- NEVER `git add -A` (might include secrets)
- NEVER force push to main
- Always check `git status` before committing
- Always verify bot is running after deploy with `journalctl`
- The user must explicitly say "comita" or "push" — don't auto-commit
