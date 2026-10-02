import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import sqlite3
import os
import json
import datetime


# CONFIGURAÇÃO

BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT", "1883"))
CLIENT_ID = os.environ.get("CLIENT_ID", "svc-emplacamento")

# tópicos que ele assina
TOPICO_EMPLACAR = "denatran/cmd/veiculo/emplacar"
TOPICO_IPVA = "denatran/qry/veiculo/ipva"
TOPICO_EMPLACADOS = "denatran/qry/veiculo/emplacados"
TOPICO_CONDUTOR_CADASTRADO = "denatran/evt/condutor/cadastrado"
# tópicos que ele publica
TOPICO_EVT_EMPLACADO = "denatran/evt/veiculo/emplacado"

ALIQUOTA_IPVA = 0.02

# no Docker, DB_DIR aponta para um volume; rodando local, o banco fica na pasta do serviço
DB_DIR = os.environ.get("DB_DIR", os.path.dirname(os.path.abspath(__file__)))
CAMINHO_BANCO = os.path.join(DB_DIR, "emplacamento.db")



# BANCO

con = sqlite3.connect(CAMINHO_BANCO)
cur = con.cursor()


def criar_tabela():
    cur.execute("CREATE TABLE IF NOT EXISTS veiculos("
                "placa TEXT PRIMARY KEY, "
                "modelo TEXT NOT NULL, "
                "valor REAL NOT NULL, "
                "data_emplacamento TEXT NOT NULL)")
    # cópia local, alimentada pelo evento condutor/cadastrado
    cur.execute("CREATE TABLE IF NOT EXISTS cpfs_validos(cpf TEXT PRIMARY KEY)")


def inserir_veiculo(placa, modelo, valor, data):
    # INSERT normal: placa repetida tem que dar IntegrityError
    cur.execute("INSERT INTO veiculos (placa, modelo, valor, data_emplacamento) VALUES (?,?,?,?)",
                (placa, modelo, valor, data))
    con.commit()


def buscar_veiculo(placa):
    cur.execute("SELECT * FROM veiculos WHERE placa = ?", (placa,))
    return cur.fetchone()


def listar_emplacados_no_ano(ano):
    # data guardada como "2026-10-02": os 4 primeiros caracteres são o ano
    cur.execute("SELECT * FROM veiculos WHERE substr(data_emplacamento, 1, 4) = ?", (str(ano),))
    return cur.fetchall()


def gravar_cpf_valido(cpf):
    # cópia vinda de evento → pode chegar repetido → OR REPLACE
    cur.execute("INSERT OR REPLACE INTO cpfs_validos (cpf) VALUES (?)", (cpf,))
    con.commit()


def cpf_existe(cpf):
    cur.execute("SELECT 1 FROM cpfs_validos WHERE cpf = ?", (cpf,))
    return cur.fetchone() is not None


def linha_para_dict(linha):
    # tupla (placa, modelo, valor, data) → dicionário com nomes de campo
    return {
        "placa": linha[0],
        "modelo": linha[1],
        "valor": linha[2],
        "data_emplacamento": linha[3],
    }



# AUXILIARES (iguais ao Cadastro)

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



# TRATAMENTO DAS MENSAGENS


# --- evento: mantém a cópia local de CPFs
def tratar_condutor_cadastrado(client, dados):
    cpf = dados.get("cpf")
    if not cpf:
        print("Evento de condutor sem CPF")
        return None
    gravar_cpf_valido(cpf)
    print(f"CPF {cpf} adicionado à cópia local")
    return None


# --- comando: emplacar
def tratar_emplacar(client, dados):
    placa = dados.get("placa")
    modelo = dados.get("modelo")
    valor = dados.get("valor")
    cpf = dados.get("cpf")

    if not placa or not modelo or valor is None or not cpf:
        return envelope("erro", "DADOS_INVALIDOS", "Placa, modelo, valor e CPF são obrigatórios", None)

    try:
        valor = float(valor)
    except (TypeError, ValueError):
        return envelope("erro", "DADOS_INVALIDOS", "Valor deve ser numérico", None)

    if not cpf_existe(cpf):
        return envelope("erro", "CPF_NAO_ENCONTRADO", f"CPF {cpf} não cadastrado", None)

    data = datetime.date.today().isoformat()

    try:
        inserir_veiculo(placa, modelo, valor, data)
    except sqlite3.IntegrityError:
        return envelope("erro", "PLACA_JA_EMPLACADA", f"Placa {placa} já emplacada", None)

    evento = {"placa": placa, "cpf": cpf, "data": data}
    client.publish(TOPICO_EVT_EMPLACADO, json.dumps(evento), qos=1)
    print(f"Veículo {placa} emplacado para {cpf}")
    return envelope("ok", None, f"Veículo {placa} emplacado", evento)


# --- consulta: IPVA
def tratar_ipva(client, dados):
    placa = dados.get("placa")
    if not placa:
        return envelope("erro", "DADOS_INVALIDOS", "Placa é obrigatória", None)

    linha = buscar_veiculo(placa)
    if linha is None:
        return envelope("erro", "PLACA_NAO_ENCONTRADA", f"Placa {placa} não encontrada", None)

    veiculo = linha_para_dict(linha)
    ipva = round(veiculo["valor"] * ALIQUOTA_IPVA, 2)
    return envelope("ok", None, f"IPVA de {placa}: R$ {ipva:.2f}",
                    {"placa": placa, "valor": veiculo["valor"], "ipva": ipva})


# --- consulta: emplacados no ano
def tratar_emplacados(client, dados):
    ano = dados.get("ano")
    if not ano:
        return envelope("erro", "DADOS_INVALIDOS", "Ano é obrigatório", None)

    linhas = listar_emplacados_no_ano(ano)
    veiculos = [linha_para_dict(l) for l in linhas]     # cada tupla vira dicionário
    return envelope("ok", None, f"{len(veiculos)} veículo(s) emplacado(s) em {ano}", veiculos)



# CALLBACKS MQTT

ROTAS = {
    TOPICO_EMPLACAR: tratar_emplacar,
    TOPICO_IPVA: tratar_ipva,
    TOPICO_EMPLACADOS: tratar_emplacados,
    TOPICO_CONDUTOR_CADASTRADO: tratar_condutor_cadastrado,
}


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print("Erro ao conectar:", reason_code)
        return

    print("Conectado. Sessão anterior:", flags.session_present)
    # assina todos os tópicos do ROTAS de uma vez
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
    # sessão persistente: o broker guarda as mensagens por até 1 h enquanto o serviço estiver fora
    props_conexao = Properties(PacketTypes.CONNECT)
    props_conexao.SessionExpiryInterval = 3600
    mqttc.connect(BROKER_HOST, BROKER_PORT, clean_start=False, properties=props_conexao)
    mqttc.loop_forever()