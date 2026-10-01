from paho import mqtt
import sqlite3
import os

#configurar aqui

#montar o database
con = sqlite3.connect("multas.db")
cur = con.cursor()

#crinado a tabela
# a multa tem ID automatico e unico
def criar_tabela(): #como pego a placa?
    cur.execute("CREATE TABLE IF NOT EXISTS multas(id,cpf,ano, descricao,pontuacao,placa)")

    cur.execute("CREATE TABLE IF NOT EXISTS posses_local(placa,cpf)")

    cur.execute("CREATE TABLE IF NOT EXISTS nomes_local(cpf,nome)")
