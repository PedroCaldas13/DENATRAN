import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import sqlite3
import os
import json

# Serviço de Multas


# =====================================================================
# CONFIGURAÇÃO
# =====================================================================
BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT", "1883"))
CLIENT_ID = os.environ.get("CLIENT_ID", "svc-multas")

# tópicos que ele assina — comandos e consultas do cliente
TOPICO_LANCAR = "denatran/cmd/multa/lancar"
TOPICO_POR_VEICULO = "denatran/qry/multa/por-veiculo"
TOPICO_POR_CONDUTOR = "denatran/qry/multa/por-condutor"
TOPICO_POR_ANO = "denatran/qry/multa/por-ano"
TOPICO_TOP5 = "denatran/qry/multa/top5"
# tópicos que ele assina — eventos dos outros serviços (cópias locais)
TOPICO_CONDUTOR_CADASTRADO = "denatran/evt/condutor/cadastrado"
TOPICO_POSSE_ALTERADA = "denatran/evt/posse/alterada"

CAMINHO_BANCO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "multas.db")


con = sqlite3.connect(CAMINHO_BANCO)
cur = con.cursor()


def criar_tabela():
    # dono: as multas. O CPF é gravado no momento do lançamento.
    cur.execute("CREATE TABLE IF NOT EXISTS multas("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "placa TEXT NOT NULL, "
                "cpf TEXT NOT NULL, "
                "ano INTEGER NOT NULL, "
                "descricao TEXT NOT NULL, "
                "pontuacao INTEGER NOT NULL)")
    # cópias locais, alimentadas por eventos
    cur.execute("CREATE TABLE IF NOT EXISTS posses_local(placa TEXT PRIMARY KEY, cpf TEXT NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS nomes_local(cpf TEXT PRIMARY KEY, nome TEXT NOT NULL)")


def inserir_multa(placa, cpf, ano, descricao, pontuacao):
    cur.execute("INSERT INTO multas (placa, cpf, ano, descricao, pontuacao) VALUES (?,?,?,?,?)",
                (placa, cpf, ano, descricao, pontuacao))
    con.commit()
    return cur.lastrowid  # id gerado pelo AUTOINCREMENT


def gravar_posse_local(placa, cpf):
    cur.execute("INSERT OR REPLACE INTO posses_local (placa, cpf) VALUES (?,?)", (placa, cpf))
    con.commit()


def gravar_nome_local(cpf, nome):
    cur.execute("INSERT OR REPLACE INTO nomes_local (cpf, nome) VALUES (?,?)", (cpf, nome))
    con.commit()


def dono_atual(placa):
    cur.execute("SELECT cpf FROM posses_local WHERE placa = ?", (placa,))
    linha = cur.fetchone()
    return linha[0] if linha else None


# Todas as consultas de multa trazem o nome do condutor que levou a multa.
# LEFT JOIN: se o nome não estiver na cópia local, a multa aparece com nome None.
SELECT_MULTAS = ("SELECT m.id, m.placa, m.cpf, n.nome, m.ano, m.descricao, m.pontuacao "
                 "FROM multas m LEFT JOIN nomes_local n ON n.cpf = m.cpf ")


def multas_por_veiculo(placa, ano=None):
    if ano is None:
        cur.execute(SELECT_MULTAS + "WHERE m.placa = ? ORDER BY m.ano, m.id", (placa,))
    else:
        cur.execute(SELECT_MULTAS + "WHERE m.placa = ? AND m.ano = ? ORDER BY m.id", (placa, ano))
    return cur.fetchall()


def multas_por_condutor(cpf, ano):
    cur.execute(SELECT_MULTAS + "WHERE m.cpf = ? AND m.ano = ? ORDER BY m.id", (cpf, ano))
    return cur.fetchall()


def multas_por_ano(ano):
    cur.execute(SELECT_MULTAS + "WHERE m.ano = ? ORDER BY m.id", (ano,))
    return cur.fetchall()


def top5_condutores():
    cur.execute("SELECT m.cpf, n.nome, SUM(m.pontuacao) AS total "
                "FROM multas m LEFT JOIN nomes_local n ON n.cpf = m.cpf "
                "GROUP BY m.cpf ORDER BY total DESC LIMIT 5")
    return cur.fetchall()


def multa_para_dict(linha):
    # ordem das colunas do SELECT_MULTAS
    return {
        "id": linha[0],
        "placa": linha[1],
        "cpf": linha[2],
        "nome": linha[3],
        "ano": linha[4],
        "descricao": linha[5],
        "pontuacao": linha[6],
    }



def envelope(status, codigo, mensagem, dados):
    return {"status": status, "codigo": codigo, "mensagem": mensagem, "dados": dados}


def responder(client, message, resposta):
    topico_resposta = getattr(message.properties, "ResponseTopic", None)
    if topico_resposta is None:
        return

    props = Properties(PacketTypes.PUBLISH)
    correlacao = getattr(message.properties, "CorrelationData", None)
    if correlacao is not None:
        props.CorrelationData = correlacao

    client.publish(topico_resposta, json.dumps(resposta), qos=1, properties=props)


def ler_inteiro(valor):
    # aceita 2026 ou "2026"; devolve None se não for número inteiro
    try:
        return int(valor)
    except (TypeError, ValueError):
        return None



def tratar_condutor_cadastrado(client, dados):
    cpf = dados.get("cpf")
    nome = dados.get("nome")
    if not cpf or not nome:
        print("Evento de condutor incompleto")
        return None
    gravar_nome_local(cpf, nome)
    print(f"Nome de {cpf} adicionado à cópia local")
    return None


def tratar_posse_alterada(client, dados):
    placa = dados.get("placa")
    cpf_novo = dados.get("cpf_novo")
    if not placa or not cpf_novo:
        print("Evento de posse incompleto")
        return None
    gravar_posse_local(placa, cpf_novo)
    print(f"Cópia local: {placa} -> {cpf_novo}")
    return None


# --- comando: lançar multa
def tratar_lancar(client, dados):
    placa = dados.get("placa")
    ano = ler_inteiro(dados.get("ano"))
    descricao = dados.get("descricao")
    pontuacao = ler_inteiro(dados.get("pontuacao"))

    if not placa or ano is None or not descricao or pontuacao is None:
        return envelope("erro", "DADOS_INVALIDOS",
                        "Placa, ano, descrição e pontuação (números) são obrigatórios", None)

    cpf = dono_atual(placa)
    if cpf is None:
        return envelope("erro", "PLACA_NAO_ENCONTRADA", f"Placa {placa} não encontrada", None)

    id_multa = inserir_multa(placa, cpf, ano, descricao, pontuacao)
    print(f"Multa {id_multa} lançada: {placa} / CPF {cpf}")
    return envelope("ok", None, f"Multa {id_multa} lançada para o CPF {cpf}",
                    {"id": id_multa, "placa": placa, "cpf": cpf, "ano": ano,
                     "descricao": descricao, "pontuacao": pontuacao})


# --- consultas
def tratar_por_veiculo(client, dados):
    placa = dados.get("placa")
    if not placa:
        return envelope("erro", "DADOS_INVALIDOS", "Placa é obrigatória", None)

    ano = None
    if dados.get("ano") is not None:          # ano é opcional nesta consulta
        ano = ler_inteiro(dados.get("ano"))
        if ano is None:
            return envelope("erro", "DADOS_INVALIDOS", "Ano deve ser numérico", None)

    multas = [multa_para_dict(l) for l in multas_por_veiculo(placa, ano)]
    return envelope("ok", None, f"{len(multas)} multa(s) do veículo {placa}", multas)


def tratar_por_condutor(client, dados):
    cpf = dados.get("cpf")
    ano = ler_inteiro(dados.get("ano"))
    if not cpf or ano is None:
        return envelope("erro", "DADOS_INVALIDOS", "CPF e ano são obrigatórios", None)

    multas = [multa_para_dict(l) for l in multas_por_condutor(cpf, ano)]
    return envelope("ok", None, f"{len(multas)} multa(s) do CPF {cpf} em {ano}", multas)


def tratar_por_ano(client, dados):
    ano = ler_inteiro(dados.get("ano"))
    if ano is None:
        return envelope("erro", "DADOS_INVALIDOS", "Ano é obrigatório", None)

    multas = [multa_para_dict(l) for l in multas_por_ano(ano)]
    return envelope("ok", None, f"{len(multas)} multa(s) em {ano}", multas)


def tratar_top5(client, dados):
    ranking = [{"cpf": cpf, "nome": nome, "pontos": total}
               for cpf, nome, total in top5_condutores()]
    return envelope("ok", None, f"Top {len(ranking)} condutores por pontuação", ranking)


ROTAS = {
    TOPICO_LANCAR: tratar_lancar,
    TOPICO_POR_VEICULO: tratar_por_veiculo,
    TOPICO_POR_CONDUTOR: tratar_por_condutor,
    TOPICO_POR_ANO: tratar_por_ano,
    TOPICO_TOP5: tratar_top5,
    TOPICO_CONDUTOR_CADASTRADO: tratar_condutor_cadastrado,
    TOPICO_POSSE_ALTERADA: tratar_posse_alterada,
}


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print("Erro ao conectar:", reason_code)
        return

    print("Conectado. Sessão anterior:", flags.session_present)
    for topico in ROTAS:
        client.subscribe(topico, qos=1)


def on_message(cliente, userdata, message):
    print(message.topic)
    print(message.payload.decode("utf-8"))
    try:
        dados = json.loads(message.payload.decode("utf-8"))
    except json.decoder.JSONDecodeError:
        print(f"JSON inválido em {message.topic}")
        return
    if not isinstance(dados, dict):
        print(f"Formato inválido em {message.topic}")
        return
    funcao = ROTAS.get(message.topic)
    if funcao is None:
        print("Tópico sem tratamento")
        return
    resposta = funcao(cliente, dados)
    if resposta is not None:
        responder(cliente, message, resposta)


if __name__ == "__main__":
    criar_tabela()
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID, protocol=mqtt.MQTTv5)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.connect(BROKER_HOST, BROKER_PORT, clean_start=False)
    mqttc.loop_forever()