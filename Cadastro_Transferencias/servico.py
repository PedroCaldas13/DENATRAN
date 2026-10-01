from paho import mqtt
import sqlite3
import os

#preciso configura-lo!!

#funcoes do banco SQLlite
con = sqlite3.connect('cadastro.db')
cur = con.cursor()
def criar_tabela():
    cur.execute("CREATE TABLE IF NOT EXISTS condutores(cpf TEXT PRIMARY KEY ,nome TEXT NOT NULL)")
        #tambem é dono do vinculo placa -> cpf
    cur.execute("CREATE TABLE IF NOT EXISTS posses(placa TEXT PRIMARY KEY, cpf TEXT NOT NULL)")

def inserir_condutor(cpf,nome):
    cur.execute("INSERT INTO condutores VALUES (?,?)",(cpf,nome))
    con.commit()

def buscar_condutor(cpf):
    cur.execute("SELECT * FROM condutores WHERE cpf= ?",(cpf,))

def gravar_posse(placa,cpf): #deve ficar assim?
    cur.execute("INSERT OR REPLACE INTO posses (cpf,nome) VALUES (?,?)",(placa,cpf))
    con.commit()

#DEVO FAZER AS TRANSFERENCIAS DAQUI??



#funcao connect, quando e revebe uma connack message from the server
def on_connect(client, userdata, flags, reason_code, properties):
    if flags.session_present:

    if reason_code.is_failure:
        client.subscribe("") #oq colocar aqui? p subscribe recebe mensagens do broker
        # success connect, ACHO QQUE NAO É AQUI O CLIENT.SUBSCRIBE, na descricao é no else
     if reason_code > 0:


def on_message(cliente,userdata,message): #é o callback de quando ele recebe uma publish message
    print(message.topic)
    print(message.payload.decode("utf-8"))

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2) #main
mqttc.on_connect = on_connect
mqttc.on_message = on_message

mqttc.connect("localhost",1883,60) #acredito que esteja certo
mqttc.loop_forever(retry_first_connection=True) #mantem o trafico de rede com o broker
#repete a primeira coneccao, devo usar?
#COLOCO LOOP FOREVER DENTRO DO MAIN



#devo criar o cliente, associar as duas funcoes a ee,conectar e chamar o loopforever
def main():



if __name__ == '__main__':
    #criar_tabelas
    #criar o cliente com CallbackAPIVersion.VERSION2, o client-id e protocol=MQTTv5
    #associar as funções: client.on_connect = on_connect e client.on_message = on_message
    #client.connect(...) com host, porta 1883 e clean_start=False.
    #  A propriedade Session Expiry Interval pode ficar para quando formos testar falhas.
    #client.loop_forever()