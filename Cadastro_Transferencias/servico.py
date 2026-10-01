
import paho.mqtt.client as mqtt
import sqlite3
import os


#configuracao
BROKER_HOST = os.environ.get("BROKER_HOST","localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT","1883"))
CLIENT_ID = os.environ.get("CLIENT_ID","svc-cadastro")

TOPICO_CADASTRAR =  "denatran/cmd/condutor/cadastrar"
TOPICO_TRANSFERIR = "denatran/cmd/veiculo/transferir"
TOPICO_EMPLACADO =  "denatran/evt/veiculo/emplacado"

CAMINHO_BANCO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cadastro.db")

#funcoes do banco SQLlite
con = sqlite3.connect(CAMINHO_BANCO)
cur = con.cursor()
# banco
def criar_tabela():
    cur.execute("CREATE TABLE IF NOT EXISTS condutores(cpf TEXT PRIMARY KEY ,nome TEXT NOT NULL)")
        #tambem é dono do vinculo placa -> cpf
    cur.execute("CREATE TABLE IF NOT EXISTS posses(placa TEXT PRIMARY KEY, cpf TEXT NOT NULL)")

def inserir_condutor(cpf,nome):
    cur.execute("INSERT INTO condutores VALUES (?,?)",(cpf,nome))
    con.commit()

def buscar_condutor(cpf):
    cur.execute("SELECT * FROM condutores WHERE cpf= ?",(cpf,))
    return cur.fetchone()

def gravar_posse(placa,cpf): #deve ficar assim?
    cur.execute("INSERT OR REPLACE INTO posses (placa,cpf) VALUES (?,?)",(placa,cpf))
    con.commit()


#TRATAMENTO DAS MENSAGENS: 5.2
#funcao connect, quando e revebe uma connack message from the server
def on_connect(client, userdata, flags, reason_code, properties):

    if reason_code.is_failure:
        print("Erro ? ", reason_code)
        return

    print("Conectado ? ", flags.session_present)
    client.subscribe(TOPICO_CADASTRAR, qos=1)
    client.subscribe(TOPICO_TRANSFERIR, qos=1)
    client.subscribe(TOPICO_EMPLACADO, qos=1)


def on_message(cliente,userdata,message): #é o callback de quando ele recebe uma publish message
    print(message.topic)
    print(message.payload.decode("utf-8"))



if __name__ == '__main__':
    criar_tabela()
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id=CLIENT_ID,protocol=mqtt.MQTTv5)  # main

    mqttc.on_connect = on_connect
    mqttc.on_message = on_message

    mqttc.connect(BROKER_HOST, BROKER_PORT, clean_start=False)
    mqttc.loop_forever()  # mantem o trafico de rede com o broker

    #criar_tabelas
    #criar o cliente com CallbackAPIVersion.VERSION2, o client-id e protocol=MQTTv5
    #associar as funções: client.on_connect = on_connect e client.on_message = on_message
    #client.connect(...) com host, porta 1883 e clean_start=False.
    #  A propriedade Session Expiry Interval pode ficar para quando formos testar falhas.
    #client.loop_forever()