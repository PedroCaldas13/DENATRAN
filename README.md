# DENATRAN — Microsserviços com MQTT

Atividade 01 de Sistemas Distribuídos (COMP0470 — UFS).

Três microsserviços em Python, cada um com seu próprio banco SQLite, que se comunicam apenas por MQTT 5 através de um broker Mosquitto.

| Serviço | Responsável por |
|---|---|
| Cadastro / Transferência | condutores e dono de cada placa; cadastrar e transferir |
| Emplacamento / IPVA | veículos; emplacar, calcular IPVA (2%), emplacados no ano |
| Multas | multas; lançar, consultas por veículo, condutor e ano, top 5 |

## Como executar

```bash
docker compose up -d --build             # broker + 3 serviços
docker compose run --rm cliente --teste  # testes automáticos
docker compose run --rm cliente          # menu interativo
docker compose down -v                   # derruba e apaga os dados
```

## Decisões de projeto

- **Comunicação entre serviços só por eventos** (`condutor/cadastrado`, `veiculo/emplacado`, `posse/alterada`). Cada serviço mantém uma cópia local do que precisa dos outros e nunca consulta outro serviço diretamente.
- **Request/response do MQTT 5:** o cliente envia *Response Topic* e *Correlation Data*; o serviço responde nesse tópico com `status`, `codigo`, `mensagem` e `dados`. Sem resposta em 5 s, o cliente informa timeout.
- **O CPF é gravado na multa no momento do lançamento.** Após uma transferência, multas novas vão para o novo dono e as antigas permanecem com o anterior.
- **Sessões persistentes com QoS 1:** se um serviço cair, o broker guarda os eventos e entrega quando ele voltar.

## Limitações

- **Consistência eventual:** logo após um comando, os outros serviços podem levar uma fração de segundo para receber o evento.
- Um serviço só conhece os dados criados depois de sua primeira conexão ao broker.