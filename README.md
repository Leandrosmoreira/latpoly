# latpoly — Fase 1: Observacao Bruta Binance vs Polymarket

Sistema de telemetria de baixa latencia que observa em tempo real o delta entre
Binance spot (BTC/USDT) e mercados de predicao BTC 15 minutos da Polymarket.

**Esta fase e somente observacao — nenhuma ordem e enviada.**

---

## Arquitetura

```
W1 Binance WS ──► SharedState ◄── W2 Polymarket WS
                      │
              W3 Signal Fusion (200ms poll)
                      │
              W4 Writer (JSONL batch)
```

| Worker | Responsabilidade |
|--------|-----------------|
| **W1** | Consome trade + bookTicker da Binance via combined stream |
| **W2** | Discovery de mercado BTC 15m + REST snapshot + WebSocket + rotacao automatica |
| **W3** | Polling do SharedState, calcula todos os indicadores, emite ticks normalizados |
| **W4** | Grava JSONL em lote — arquivos por sessao de mercado e diario |

---

## Instalacao na VPS (Linux / Debian / Ubuntu)

```bash
# 1. Instalar dependencias do sistema
sudo apt update && sudo apt install -y python3-full python3-pip git

# 2. Clonar o repositorio
git clone https://github.com/Leandrosmoreira/latpoly.git
cd latpoly

# 3. Criar e ativar virtualenv
python3 -m venv .venv
source .venv/bin/activate

# 4. Instalar o projeto com uvloop (otimizado para Linux)
pip install -e ".[linux]"

# 5. Copiar configuracao
cp .env.example .env
```

### Instalacao local (Windows / dev)

```bash
git clone https://github.com/Leandrosmoreira/latpoly.git
cd latpoly
python -m venv .venv
.venv\Scripts\activate
pip install -e .
cp .env.example .env
```

---

## Como executar

```bash
# Ativar o virtualenv (se nao estiver ativo)
source .venv/bin/activate   # Linux
# .venv\Scripts\activate    # Windows

# Rodar o sistema
python -m latpoly.main
```

### O que acontece ao iniciar

1. Discovery automatico do mercado BTC 15m ativo na Polymarket
2. REST snapshot do order book (YES e NO) para ter dados imediatos
3. Conexao WebSocket na Binance (trade + bookTicker) e Polymarket (market channel)
4. A cada 200ms, W3 le o estado mais recente e calcula todos os indicadores
5. W4 grava os ticks em arquivos JSONL
6. Health monitor loga metricas a cada 10s no stderr
7. A cada ~15 minutos, rotacao automatica para o proximo mercado

### Parar o sistema

`Ctrl+C` — faz shutdown graceful (flush dos dados pendentes + fecha conexoes)

---

## Indicadores calculados (W3 Signal Worker)

O Signal Worker calcula todos os indicadores a cada tick (200ms) usando funcoes
puras e modulares. Cada funcao retorna `None` quando os dados nao estao disponiveis,
diferenciando "sem dados" de "valor zero".

### Tabela de indicadores

| Indicador | Formula | O que mede |
|-----------|---------|------------|
| **mid_binance** | `(best_bid + best_ask) / 2` | Preco medio BTC na Binance |
| **mid_yes** | `(yes_best_bid + yes_best_ask) / 2` | Preco medio do token YES na Polymarket |
| **mid_no** | `(no_best_bid + no_best_ask) / 2` | Preco medio do token NO na Polymarket |
| **spread_yes** | `yes_best_ask - yes_best_bid` | Spread do lado YES |
| **spread_no** | `no_best_ask - no_best_bid` | Spread do lado NO |
| **prob_implied** | `mid_yes / (mid_yes + mid_no)` | Probabilidade implicita de YES pelo mercado |
| **distance_to_strike** | `mid_binance - strike` | Distancia do preco BTC ao strike. Positivo = acima, negativo = abaixo |
| **time_to_expiry_ms** | `(end_time - now) * 1000` | Tempo restante ate expiracao do mercado (ms). Clamped a 0 |
| **age_binance_ms** | `now - ts_ultimo_evento_binance` | Quanto tempo faz desde o ultimo dado da Binance (freshness) |
| **age_poly_ms** | `now - ts_ultimo_evento_polymarket` | Quanto tempo faz desde o ultimo dado da Polymarket (freshness) |
| **recv_delta_ms** | `ts_poly_recv - ts_binance_recv` | Gap entre recebimento das fontes. Positivo = Poly chegou depois |
| **delta_price** | `prob_implied - theoretical_prob` | Divergencia entre prob observada e fair value teorico. **None na Fase 1** (sera implementado na Fase 2) |

### Como interpretar os indicadores

**prob_implied** — Se o mercado pergunta "BTC acima de $85,000 em 15 min?":
- `prob_implied = 0.75` significa que o mercado da 75% de chance de YES
- Se BTC esta em $85,500 (distance_to_strike = +500), mas prob_implied = 0.55, pode haver oportunidade

**distance_to_strike** — Mostra se o BTC esta acima ou abaixo do strike:
- `+200` = BTC esta $200 acima do strike (favorece YES)
- `-150` = BTC esta $150 abaixo do strike (favorece NO)

**age_binance_ms / age_poly_ms** — Freshness dos dados:
- Binance: tipicamente < 100ms (muitos eventos/segundo)
- Polymarket: pode ser varios segundos (menos liquidez)
- Se `age_poly_ms` e alto e `distance_to_strike` mudou rapido, o preco da Polymarket pode estar defasado

**recv_delta_ms** — Latencia relativa entre fontes:
- Valor positivo grande = Polymarket esta chegando mais devagar que Binance
- Util para medir se ha janela de oportunidade temporal

**spread_yes / spread_no** — Custo de execucao:
- Spreads apertados = mais liquidez, melhor para executar
- Spreads largos perto da expiracao = menos certeza do mercado

### Exemplo de tick normalizado

```json
{
  "ts_ns": 1773616288844000000,
  "ts_wall": 1773616288.844,
  "condition_id": "0xabc123def456...",
  "strike": 85000.0,
  "end_ts_ms": 1773617188000,
  "mid_binance": 85102.4,
  "bn_best_bid": 85100.0,
  "bn_best_ask": 85104.8,
  "bn_last_price": 85101.5,
  "bn_last_qty": 0.015,
  "pm_yes_best_bid": 0.55,
  "pm_yes_best_ask": 0.57,
  "mid_yes": 0.56,
  "pm_no_best_bid": 0.42,
  "pm_no_best_ask": 0.44,
  "mid_no": 0.43,
  "spread_yes": 0.02,
  "spread_no": 0.02,
  "prob_implied": 0.565657,
  "distance_to_strike": 102.4,
  "time_to_expiry_ms": 84500.0,
  "age_binance_ms": 12.34,
  "age_poly_ms": 380.50,
  "recv_delta_ms": 368.16,
  "delta_price": null
}
```

---

## Onde os dados sao gravados

```
data/
  sessions/                                      # Um arquivo por mercado de 15 min
    2026-03-15_0xabc123def4_143000.jsonl
    2026-03-15_0xdef456abc1_144500.jsonl
  daily/                                         # Arquivo unico por dia (todos os mercados)
    2026-03-15_merged.jsonl
  health/                                        # Metricas de saude (opcional)
    health.jsonl
```

- **sessions/**: Unidade natural de analise. Cada arquivo contem todos os ticks de um mercado de 15 minutos
- **daily/**: Agregado do dia inteiro para replay e analise de longo prazo
- Formato: JSONL (uma linha JSON por tick, ~5 ticks/segundo)

---

## Configuracao

Todas as variaveis podem ser definidas no `.env` ou como variaveis de ambiente:

| Variavel | Default | Descricao |
|----------|---------|-----------|
| `LATPOLY_SYMBOL` | BTCUSDT | Par da Binance |
| `LATPOLY_SIGNAL_INTERVAL` | 0.2 | Intervalo de polling do W3 (segundos) |
| `LATPOLY_QUEUE_SIZE` | 500 | Tamanho maximo da fila W3 -> W4 |
| `LATPOLY_WRITER_BATCH` | 50 | Records por batch de escrita |
| `LATPOLY_WRITER_TIMEOUT` | 1.0 | Timeout de flush (segundos) |
| `LATPOLY_DATA_DIR` | ./data | Diretorio de saida dos dados |
| `LATPOLY_HEALTH_INTERVAL` | 10 | Intervalo de health check (segundos) |
| `LATPOLY_DISCOVERY_LEAD` | 60 | Segundos antes da expiracao para buscar proximo mercado |
| `LATPOLY_LOG_LEVEL` | INFO | Nivel de log (DEBUG, INFO, WARNING) |

---

## Validacao (como saber que esta funcionando)

### 1. Logs no stderr

Ao iniciar, voce deve ver algo como:

```
14:30:01 INFO  [latpoly.main] Config: symbol=btcusdt signal_interval=0.20s
14:30:01 INFO  [latpoly.workers.polymarket_ws] Running market discovery...
14:30:02 INFO  [latpoly.utils.discovery] Discovered market: Will BTC be above $85,000... | strike=85000.0
14:30:02 INFO  [latpoly.workers.polymarket_ws] REST snapshot: YES bid=0.5500 ask=0.5700
14:30:02 INFO  [latpoly.workers.binance_ws] Connected (#1) to wss://stream.binance.com...
14:30:02 INFO  [latpoly.workers.polymarket_ws] Polymarket WS connected
14:30:03 INFO  [latpoly.workers.signal] Signal worker ready — both sources have data
14:30:12 INFO  [latpoly.health] HEALTH up=10s bn=847/0.01s pm=12/1.23s ticks=50 q=0 mkt=0xabc123 ttx=588s
```

### 2. Verificar arquivos de dados

```bash
# Ver se arquivos estao sendo criados
ls -la data/sessions/
ls -la data/daily/

# Ver ultimos ticks gravados
tail -1 data/daily/*.jsonl | python3 -m json.tool
```

### 3. Health check rapido

Os health logs mostram:
- `bn=847/0.01s` — 847 eventos Binance, ultimo ha 0.01s (saudavel)
- `pm=12/1.23s` — 12 eventos Polymarket, ultimo ha 1.23s (normal)
- `ticks=50` — 50 ticks normalizados gerados
- `q=0` — fila do writer vazia (bom, sem backlog)
- `ttx=588s` — 588 segundos ate o mercado expirar

### 4. Alertas

- `STALE Binance data: 6.2s` — Binance sem dados ha >5s (problema de conexao)
- `STALE Polymarket data: 35.0s` — Polymarket sem dados ha >30s (pode ser normal em mercado pouco ativo)

---

## Limitacoes (Fase 1)

- Nao envia ordens
- `delta_price` e sempre `null` (theoretical_prob sera implementado na Fase 2)
- Nao tem UI ou dashboard
- Discovery depende do formato atual da Gamma API da Polymarket
- Usa apenas endpoints publicos (sem autenticacao)

## Proximos passos (Fase 2)

- Implementar `theoretical_prob` (fair value) para ativar o `delta_price`
- Logica de sinal para detectar oportunidades de arbitragem
- Envio de ordens na Polymarket
- Gestao de posicao e risco
- Dashboard de monitoramento em tempo real
