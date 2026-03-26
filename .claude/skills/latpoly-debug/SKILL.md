---
name: latpoly-debug
description: Analyze latpoly bot logs, diagnose trading errors, find logic bugs, and troubleshoot live_trader issues. Use when the user pastes bot logs, asks about errors, wants PnL analysis, or says "log" or "erro" or "bug".
allowed-tools: Read, Grep, Glob, Bash
---

# Latpoly Log Analyzer & Debugger

You are analyzing production logs from the latpoly Polymarket trading bot.

## LOG SOURCES

**VPS logs** (user pastes from `journalctl -u latpoly`):
```
Mar 26 14:05:23 vps latpoly[1234]: INFO     [live_trader] ...
```

**Local log files**: `data/live/*.jsonl`

## KEY LOG PATTERNS TO LOOK FOR

### Entry Flow
```
ENTRY SIGNAL: BUY_YES @ $0.44 sz=6        # Strategy generated signal
placing ENTRY GTC … price=$0.44 size=6     # Order sent to CLOB
ENTRY order filled                          # Fill confirmed
POST_ENTRY_SETTLEMENT wait 3s              # Wait for on-chain
POSITION_CONFIRM: balance=5                 # On-chain settled (5 of 6)
```

### Exit Flow
```
EXIT_PLACING: GTC sell @ $0.46             # Sell order placed
EXIT check #5: best_bid=$0.45              # Monitoring bid
EXIT repaint: $0.46 → $0.45               # Following bid down
SELL FILLED @ $0.46 sz=5                   # Maker sell filled
RESIDUAL TAKER SELL: 1 share              # Selling leftover
TRADE COMPLETE: pnl=$0.10                 # Trade done
```

### Error Patterns
```
RESIDUAL TAKER SELL: "not enough balance"  # Balance API stale
ENTRY_REPRICE #3: $0.44 → $0.53           # Reprice too aggressive
EXIT check #50+: stuck                     # Sell floor too high
HTTP 400: "not enough balance/allowance"   # Order rejected
WebSocket disconnected                      # Connection issue
```

## COMMON ISSUES & ROOT CAUSES

### 1. "not enough balance/allowance"
**Cause**: Balance API returns stale data 1-3s after transactions
**Check**: Did the bot try to sell shares it already sold?
**Fix pattern**: Use computed difference (bought - sold), not balance API

### 2. Entry reprice too expensive
**Cause**: `best_ask` jumped far from signal price, no cap on reprice
**Check**: Compare original signal price vs reprice target
**Fix pattern**: Cap at original_price + 3 ticks max (`_entry_original_price` dict)

### 3. Sell stuck 50+ checks
**Cause**: Floor at entry+2 never decays, bid drops far below
**Check**: What was floor vs best_bid? How many checks?
**Fix pattern**: Floor decay schedule (20→+1tick, 40→breakeven, 60→taker)

### 4. PnL negative from expired positions
**Cause**: Held shares through market expiry, settled at $0
**Check**: Was SELL ever filled? What was time_to_expiry?
**Fix pattern**: Emergency taker at 60 checks + expiry cancel at 7s

### 5. Partial fills (buy 6, settle 5)
**Cause**: Polymarket on-chain settlement discrepancy
**Check**: POSITION_CONFIRM balance vs order size
**This is expected** — residual taker sell handles the 1 extra share

## ANALYSIS CHECKLIST

When analyzing logs, always report:

1. **Time range**: Start → End
2. **Markets traded**: Which condition_ids / slugs
3. **Trade count**: How many entries/exits
4. **Fill rate**: How many entries actually filled vs timed out
5. **PnL breakdown**: Per-trade and session total
6. **Errors found**: List with timestamps
7. **Logic issues**: Entry reprices, stuck sells, timing
8. **Recommendations**: What to fix, ordered by impact

## PnL CALCULATION

```
Per trade:
  pnl = (sell_price - buy_price) * shares_sold

Session:
  session_pnl = sum(all trade pnls)

Expired positions (settlement):
  If YES won: pnl = (1.00 - buy_price) * shares
  If NO won:  pnl = (0.00 - buy_price) * shares  ← LOSS
```

## VPS COMMANDS (for reference)

```bash
# Full bot log
ssh root@31.97.165.64 'journalctl -u latpoly --since "11:00" --no-pager'

# Last 100 lines
ssh root@31.97.165.64 'journalctl -u latpoly -n 100 --no-pager'

# Restart bot
ssh root@31.97.165.64 'systemctl restart latpoly'

# Stop bot
ssh root@31.97.165.64 'systemctl stop latpoly'

# Check status
ssh root@31.97.165.64 'systemctl status latpoly'
```

## OUTPUT FORMAT

Structure your analysis as:

### Resumo
- Período: HH:MM — HH:MM
- Mercados: N mercados
- Trades: N entradas, N saídas
- PnL sessão: $X.XX

### Trades
| # | Hora | Lado | Entry | Exit | Shares | PnL | Status |
|---|------|------|-------|------|--------|-----|--------|

### Erros
| Hora | Erro | Causa | Impacto |
|------|------|-------|---------|

### Recomendações
1. [Mais impactante primeiro]
