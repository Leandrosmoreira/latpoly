# latpoly — Fase 1: Observacao Bruta Binance vs Polymarket

Sistema de telemetria de baixa latencia que observa em tempo real o delta entre
Binance spot (BTC/USDT) e mercados de predicao BTC 15 minutos da Polymarket.

**Esta fase e somente observacao — nenhuma ordem e enviada.**

## Arquitetura

```
W1 Binance WS ──► SharedState ◄── W2 Polymarket WS
                      │
              W3 Signal Fusion (200ms poll)
                      │
              W4 Writer (JSONL batch)
```

- **W1**: Consome trade + bookTicker da Binance via combined stream
- **W2**: Discovery de mercado BTC 15m + REST snapshot + WebSocket + rotacao automatica
- **W3**: Polling do estado compartilhado, calcula metricas derivadas (mids, spreads, deltas)
- **W4**: Grava JSONL em lote — arquivos por sessao de mercado e diario

## Setup

```bash
# Instalar dependencias
pip install -e .

# No Linux (VPS), instalar uvloop
pip install -e ".[linux]"

# Copiar e editar configuracao
cp .env.example .env
```

## Executar

```bash
python -m latpoly.main
```

O sistema vai:
1. Descobrir automaticamente o mercado BTC 15m ativo na Polymarket
2. Conectar na Binance e Polymarket via WebSocket
3. Gerar ticks normalizados a cada 200ms
4. Gravar em `data/sessions/` e `data/daily/`
5. Logar metricas de saude a cada 10 segundos no stderr

## Configuracao

Todas as variaveis podem ser definidas via ambiente ou `.env`:

| Variavel | Default | Descricao |
|----------|---------|-----------|
| `LATPOLY_SYMBOL` | BTCUSDT | Par da Binance |
| `LATPOLY_SIGNAL_INTERVAL` | 0.2 | Intervalo de polling do signal worker (segundos) |
| `LATPOLY_QUEUE_SIZE` | 500 | Tamanho maximo da fila W3->W4 |
| `LATPOLY_WRITER_BATCH` | 50 | Records por batch de escrita |
| `LATPOLY_WRITER_TIMEOUT` | 1.0 | Timeout de flush (segundos) |
| `LATPOLY_DATA_DIR` | ./data | Diretorio de saida |
| `LATPOLY_HEALTH_INTERVAL` | 10 | Intervalo de health check (segundos) |
| `LATPOLY_DISCOVERY_LEAD` | 60 | Segundos antes da expiracao para buscar proximo mercado |

## Dados gravados

```
data/
  sessions/    # Um arquivo por mercado de 15 minutos
    2026-03-15_abc123def456_143000.jsonl
  daily/       # Arquivo unico por dia
    2026-03-15_merged.jsonl
```

Cada linha JSONL contem um tick normalizado com:
- `mid_binance`, `bn_best_bid`, `bn_best_ask`
- `mid_yes`, `mid_no`, spreads
- `distance_to_strike`, `time_to_expiry_ms`
- `age_binance_ms`, `age_poly_ms`, `recv_delta_ms`

## Validacao

1. **Logs no stderr** devem mostrar:
   - Discovery do mercado com strike e horario de expiracao
   - Conexao Binance e Polymarket
   - "Signal worker ready" quando ambas as fontes tem dados
   - Health lines a cada 10s com contadores crescentes

2. **Arquivos em `data/`** devem conter JSONL com valores reais

3. **Ctrl+C** deve fazer shutdown graceful (flush + close)

4. **Rotacao** de mercado a cada ~15 minutos gera novo arquivo de sessao

## Limitacoes (Fase 1)

- Nao envia ordens
- Nao calcula sinais de trading
- Nao tem UI ou dashboard
- Discovery depende do formato atual da Gamma API
- Nao tem autenticacao (usa apenas endpoints publicos)

## Proximos passos (Fase 2)

- Logica de sinal para detectar oportunidades
- Envio de ordens na Polymarket
- Gestao de posicao e risco
- Dashboard de monitoramento
