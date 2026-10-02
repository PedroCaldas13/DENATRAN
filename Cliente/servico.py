import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import json
import os
import random
import sys
import threading
import time
import uuid

# Cliente do sistema DENATRAN
#   python cliente.py           → menu interativo
#   python cliente.py --teste   → roteiro automático que exercita todas as funcionalidades



BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT", "1883"))

# cada execução do cliente tem um id próprio → um tópico de resposta só dele
CLIENT_ID = f"cliente-{uuid.uuid4().hex[:8]}"
TOPICO_RESPOSTA = f"denatran/resp/{CLIENT_ID}"

TIMEOUT = float(os.environ.get("TIMEOUT", "5"))   # segundos esperando a resposta
EXPIRACAO_PEDIDO = 30                             # pedido parado no broker por mais que isso é descartado

TOPICOS = {
    "cadastrar":     "denatran/cmd/condutor/cadastrar",
    "transferir":    "denatran/cmd/veiculo/transferir",
    "emplacar":      "denatran/cmd/veiculo/emplacar",
    "ipva":          "denatran/qry/veiculo/ipva",
    "emplacados":    "denatran/qry/veiculo/emplacados",
    "lancar":        "denatran/cmd/multa/lancar",
    "por_veiculo":   "denatran/qry/multa/por-veiculo",
    "por_condutor":  "denatran/qry/multa/por-condutor",
    "por_ano":       "denatran/qry/multa/por-ano",
    "top5":          "denatran/qry/multa/top5",
}



# Pedidos aguardando resposta: correlation-data → {"evento": Event, "resposta": dict}
pendentes = {}
trava = threading.Lock()
conectado = threading.Event()


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print("Erro ao conectar:", reason_code)
        return
    # assina o próprio tópico de resposta ANTES de qualquer pedido
    client.subscribe(TOPICO_RESPOSTA, qos=1)


def on_subscribe(client, userdata, mid, reason_codes, properties):
    conectado.set()


def on_message(client, userdata, message):
    correlacao = getattr(message.properties, "CorrelationData", None)
    if correlacao is None:
        return
    with trava:
        pedido = pendentes.get(correlacao)
    if pedido is None:
        return  # resposta de um pedido que já deu timeout
    try:
        pedido["resposta"] = json.loads(message.payload.decode("utf-8"))
    except json.decoder.JSONDecodeError:
        pedido["resposta"] = {"status": "erro", "codigo": "RESPOSTA_INVALIDA",
                              "mensagem": "Resposta não é JSON", "dados": None}
    pedido["evento"].set()


def pedir(client, operacao, dados):
    """Publica o pedido e espera a resposta. Devolve o envelope, ou None em caso de timeout."""
    correlacao = uuid.uuid4().bytes
    pedido = {"evento": threading.Event(), "resposta": None}
    with trava:
        pendentes[correlacao] = pedido

    props = Properties(PacketTypes.PUBLISH)
    props.ResponseTopic = TOPICO_RESPOSTA
    props.CorrelationData = correlacao
    props.MessageExpiryInterval = EXPIRACAO_PEDIDO

    client.publish(TOPICOS[operacao], json.dumps(dados), qos=1, properties=props)
    chegou = pedido["evento"].wait(TIMEOUT)

    with trava:
        pendentes.pop(correlacao, None)
    return pedido["resposta"] if chegou else None


def conectar():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID, protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT)
    client.loop_start()   # rede em segundo plano; o programa principal fica livre para esperar respostas
    if not conectado.wait(TIMEOUT):
        print(f"Não foi possível conectar ao broker em {BROKER_HOST}:{BROKER_PORT}")
        sys.exit(1)
    return client



def mostrar(operacao, resposta):
    if resposta is None:
        if operacao in ("cadastrar", "transferir", "emplacar", "lancar"):
            print("   Sem confirmação em", TIMEOUT, "s. O comando pode ter sido processado; "
                  "o serviço pode estar fora do ar.")
        else:
            print("   Serviço indisponível: nenhuma resposta em", TIMEOUT, "s.")
        return
    if resposta["status"] == "ok":
        print("  ✔", resposta["mensagem"])
    else:
        print(f"  ✘ [{resposta['codigo']}] {resposta['mensagem']}")
    dados = resposta.get("dados")
    if isinstance(dados, list):
        for item in dados:
            print("    -", item)
    elif dados:
        print("   ", dados)


# =====================================================================
# MENU INTERATIVO
# =====================================================================
OPCOES = [
    ("1", "Cadastrar condutor",              "cadastrar",    [("cpf", str), ("nome", str)]),
    ("2", "Emplacar veículo",                "emplacar",     [("placa", str), ("modelo", str), ("valor", float), ("cpf", str)]),
    ("3", "Calcular IPVA",                   "ipva",         [("placa", str)]),
    ("4", "Transferir proprietário",         "transferir",   [("placa", str), ("cpf_novo", str)]),
    ("5", "Lançar multa",                    "lancar",       [("placa", str), ("ano", int), ("descricao", str), ("pontuacao", int)]),
    ("6", "Veículos emplacados em um ano",   "emplacados",   [("ano", int)]),
    ("7", "Multas de um veículo",            "por_veiculo",  [("placa", str), ("ano (vazio = todos)", int)]),
    ("8", "Multas de um condutor em um ano", "por_condutor", [("cpf", str), ("ano", int)]),
    ("9", "Multas lançadas em um ano",       "por_ano",      [("ano", int)]),
    ("10", "Top 5 condutores por pontuação", "top5",         []),
]


def ler_campos(campos):
    dados = {}
    for nome, tipo in campos:
        chave = nome.split(" ")[0]
        while True:
            texto = input(f"  {nome}: ").strip()
            if texto == "" and "vazio" in nome:
                break
            try:
                dados[chave] = tipo(texto)
                break
            except ValueError:
                print("  valor inválido, tente de novo")
    return dados


def menu(client):
    while True:
        print("\n=== DENATRAN ===")
        for codigo, titulo, _, _ in OPCOES:
            print(f" {codigo:>2}. {titulo}")
        print("  0. Sair")
        escolha = input("> ").strip()
        if escolha == "0":
            return
        opcao = next((o for o in OPCOES if o[0] == escolha), None)
        if opcao is None:
            print("Opção inválida")
            continue
        _, titulo, operacao, campos = opcao
        print(f"\n{titulo}")
        dados = ler_campos(campos)
        mostrar(operacao, pedir(client, operacao, dados))



# Usa dados novos a cada execução (sufixo aleatório) para poder rodar várias vezes.
# Entre um comando que gera eventos e o passo que depende deles há uma pequena pausa:
# os serviços se atualizam por eventos (consistência eventual), não instantaneamente.
PAUSA_EVENTOS = 0.5


def roteiro_teste(client):
    sufixo = f"{random.randint(0, 99999):05d}"
    cpf_a, cpf_b = "1" + sufixo + "00001", "1" + sufixo + "00002"
    placa = "T" + sufixo + "X"
    ano = time.localtime().tm_year
    falhas = 0

    def passo(titulo, operacao, dados, esperado, conferir=None, pausa=0):
        nonlocal falhas
        resposta = pedir(client, operacao, dados)
        if pausa:
            time.sleep(pausa)
        if resposta is None:
            ok = False
            detalhe = "sem resposta (timeout)"
        else:
            obtido = resposta["status"] if resposta["status"] == "ok" else resposta["codigo"]
            ok = obtido == esperado and (conferir is None or conferir(resposta["dados"]))
            detalhe = f"{obtido}: {resposta['mensagem']}"
        falhas += 0 if ok else 1
        print(f"{'✔' if ok else '✘'} {titulo:<52} {detalhe}")
        return resposta

    print(f"Roteiro de teste — condutores {cpf_a} e {cpf_b}, placa {placa}\n")

    print("Cadastro / Transferência")
    passo("cadastrar condutor A", "cadastrar", {"cpf": cpf_a, "nome": "Ana Teste"}, "ok")
    passo("cadastrar condutor B", "cadastrar", {"cpf": cpf_b, "nome": "Bruno Teste"}, "ok", pausa=PAUSA_EVENTOS)
    passo("cadastrar CPF repetido", "cadastrar", {"cpf": cpf_a, "nome": "Outro"}, "CPF_JA_CADASTRADO")
    passo("cadastrar sem nome", "cadastrar", {"cpf": "123"}, "DADOS_INVALIDOS")

    print("\nEmplacamento / IPVA")
    passo("emplacar para CPF inexistente", "emplacar",
          {"placa": placa, "modelo": "Gol", "valor": 50000, "cpf": "000"}, "CPF_NAO_ENCONTRADO")
    passo("emplacar para A", "emplacar",
          {"placa": placa, "modelo": "Gol", "valor": 50000, "cpf": cpf_a}, "ok", pausa=PAUSA_EVENTOS)
    passo("emplacar mesma placa", "emplacar",
          {"placa": placa, "modelo": "Gol", "valor": 50000, "cpf": cpf_a}, "PLACA_JA_EMPLACADA")
    passo("IPVA = 2% de 50000 = 1000", "ipva", {"placa": placa}, "ok",
          conferir=lambda d: d["ipva"] == 1000)
    passo("IPVA de placa inexistente", "ipva", {"placa": "ZZZ0000"}, "PLACA_NAO_ENCONTRADA")
    passo(f"emplacados em {ano} incluem a placa", "emplacados", {"ano": ano}, "ok",
          conferir=lambda d: any(v["placa"] == placa for v in d))

    print("\nMultas")
    passo("multa 1 (A é dona)", "lancar",
          {"placa": placa, "ano": ano, "descricao": "Excesso de velocidade", "pontuacao": 5}, "ok",
          conferir=lambda d: d["cpf"] == cpf_a)
    passo("multa 2, ano anterior (A é dona)", "lancar",
          {"placa": placa, "ano": ano - 1, "descricao": "Estacionamento proibido", "pontuacao": 3}, "ok",
          conferir=lambda d: d["cpf"] == cpf_a)
    passo("transferir placa para B", "transferir", {"placa": placa, "cpf_novo": cpf_b}, "ok",
          pausa=PAUSA_EVENTOS)
    passo("multa 3 vai para B (dono atual)", "lancar",
          {"placa": placa, "ano": ano, "descricao": "Sinal vermelho", "pontuacao": 7}, "ok",
          conferir=lambda d: d["cpf"] == cpf_b)
    passo("multa para placa inexistente", "lancar",
          {"placa": "ZZZ0000", "ano": ano, "descricao": "x", "pontuacao": 1}, "PLACA_NAO_ENCONTRADA")
    passo("multas do veículo: 3, com nomes", "por_veiculo", {"placa": placa}, "ok",
          conferir=lambda d: len(d) == 3 and {m["nome"] for m in d} == {"Ana Teste", "Bruno Teste"})
    passo(f"multas do veículo em {ano}: 2", "por_veiculo", {"placa": placa, "ano": ano}, "ok",
          conferir=lambda d: len(d) == 2)
    passo(f"multas de A em {ano}: 1", "por_condutor", {"cpf": cpf_a, "ano": ano}, "ok",
          conferir=lambda d: len(d) == 1)
    passo(f"multas de {ano} incluem as 2 da placa", "por_ano", {"ano": ano}, "ok",
          conferir=lambda d: sum(1 for m in d if m["placa"] == placa) == 2)
    passo("top 5 em ordem decrescente", "top5", {}, "ok",
          conferir=lambda d: [r["pontos"] for r in d] == sorted((r["pontos"] for r in d), reverse=True))

    print(f"\n{'Todos os testes passaram.' if falhas == 0 else f'{falhas} teste(s) falharam.'}")
    return falhas



if __name__ == "__main__":
    cliente = conectar()
    try:
        if "--teste" in sys.argv:
            sys.exit(1 if roteiro_teste(cliente) else 0)
        menu(cliente)
    finally:
        cliente.loop_stop()
        cliente.disconnect()