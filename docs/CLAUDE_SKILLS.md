# Claude Code Skills — Como Funciona

## O que são Skills?

Skills são **instruções especializadas** que o Claude Code carrega automaticamente
quando detecta que o contexto é relevante, ou manualmente via `/nome-do-skill`.

Pense neles como "modos especializados" — quando você pede algo sobre trading,
o Claude já sabe toda a arquitetura do projeto sem você ter que explicar.

## Onde ficam?

```
.claude/skills/
├── latpoly-trading/     # Skill principal de trading
│   └── SKILL.md
├── latpoly-strategy/    # Criar/modificar estratégias
│   └── SKILL.md
├── latpoly-debug/       # Analisar logs e debugar erros
│   └── SKILL.md
└── latpoly-deploy/      # Deploy no VPS, commit, restart
    └── SKILL.md
```

| Escopo | Pasta | Quem vê |
|--------|-------|---------|
| Projeto | `.claude/skills/` (no repo) | Qualquer um que clone o projeto |
| Pessoal | `~/.claude/skills/` | Só você, em todos os projetos |

## Formato do Arquivo

Cada skill é um `SKILL.md` com **frontmatter YAML** + **conteúdo Markdown**:

```yaml
---
name: meu-skill
description: Quando usar este skill. Claude lê isso pra decidir se ativa automaticamente.
allowed-tools: Read, Grep, Glob, Edit, Write
disable-model-invocation: false   # true = só manual (/nome)
argument-hint: "[argumento opcional]"
---

# Instruções pro Claude aqui

Contexto, regras, templates, exemplos...
```

## Campos do Frontmatter

| Campo | Tipo | Pra que serve |
|-------|------|---------------|
| `name` | string | Nome do skill (minúsculas, hífens) |
| `description` | string | **O mais importante** — Claude usa pra decidir quando ativar |
| `allowed-tools` | string | Ferramentas permitidas sem pedir confirmação |
| `disable-model-invocation` | bool | `true` = só ativa com `/nome` (ex: deploy) |
| `user-invocable` | bool | `false` = só Claude ativa (background knowledge) |
| `argument-hint` | string | Dica de argumento no autocomplete |
| `model` | string | Forçar modelo específico (ex: `claude-opus-4-1`) |
| `effort` | string | Nível de esforço: `low`, `medium`, `high`, `max` |
| `context` | string | `fork` = roda em subagent isolado |
| `paths` | string | Glob de arquivos que ativam o skill |

## Como Ativar

### Automático (recomendado)
O Claude lê o `description` e ativa sozinho quando o pedido combina:
- "corrija o bug no strategy_2" → ativa `latpoly-trading`
- "crie a estratégia 5" → ativa `latpoly-strategy`
- "analisa esse log" → ativa `latpoly-debug`

### Manual
Digite `/` + nome:
```
/latpoly-trading
/latpoly-strategy
/latpoly-debug
/latpoly-deploy push
```

## Skills do Projeto Latpoly

### 1. `/latpoly-trading` (Auto-ativa)
**Quando**: Qualquer trabalho com código de trading, lógica de entrada/saída,
live_trader, inventory control, Binance lag.

**O que sabe**:
- Arquitetura completa (5 workers, BaseStrategy, live_trader state machine)
- Constraints críticos (1 posição por slot, sem lock, sem acumulação)
- Todas as 4 estratégias existentes
- Tick dict com 40+ campos
- Padrões de código do projeto

### 2. `/latpoly-strategy` (Auto-ativa)
**Quando**: Criar nova estratégia ou modificar existente.

**O que sabe**:
- Template completo de strategy_N.py
- Interface BaseStrategy + Signal dataclass
- Checklist de registro (registry.py)
- Env vars pattern (`LATPOLY_SN_*`)
- 13-step entry filter chain

### 3. `/latpoly-debug` (Auto-ativa)
**Quando**: Analisar logs, diagnosticar erros, calcular PnL.

**O que sabe**:
- Formatos de log (journalctl, JSONL)
- Padrões de erro conhecidos (balance stale, reprice, stuck sell)
- Cálculo de PnL por trade e sessão
- Formato de relatório estruturado (tabelas)
- Comandos VPS

### 4. `/latpoly-deploy` (Só manual)
**Quando**: Commitar, push, deploy no VPS, restart, stop.

**O que sabe**:
- Workflow git → push → pull VPS → restart
- Detalhes do VPS (IP, paths, systemd)
- Resolução de conflitos git
- Safety rules (nunca commitar .env)

**`disable-model-invocation: true`** — só ativa com `/latpoly-deploy`
porque deploy tem efeitos colaterais sérios.

## Variáveis Dinâmicas

Skills suportam substituição de variáveis:

| Variável | Valor |
|----------|-------|
| `$ARGUMENTS` | Argumentos passados: `/skill arg1 arg2` |
| `$0`, `$1` | Argumento por índice |
| `${CLAUDE_SESSION_ID}` | ID da sessão atual |
| `${CLAUDE_SKILL_DIR}` | Caminho absoluto da pasta do skill |

Também suportam execução de comandos inline:
```markdown
Resultado do git: !`git status --short`
```

## Boas Práticas

1. **Description é tudo** — escreva como o usuário pediria naturalmente
2. **Um skill, um propósito** — não misture debug com deploy
3. **Constraints primeiro** — liste o que NÃO pode antes do que pode
4. **Exemplos concretos** — código real > teoria abstrata
5. **< 500 linhas** por SKILL.md — use arquivos auxiliares se precisar
6. **`disable-model-invocation`** em skills com efeitos colaterais (deploy, commit)
