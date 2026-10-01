from paho import mqtt
import sqlite3
import os

#configuracao necessaria

#criando o banco de dados/funcoes
con = sqlite3.connect("emplacamento.db")
cur = con.cursor()

def criar_tabela(): # E O CPF?
    cur.execute("CREATE TABLE IF NOT EXISTS veiculos(placa UNIQUE ID , modelo TEXT NOT NULL,valor NUMERIC NOT NULL,cpf IS NOT NULL) ")

    cur.execute("CREATE TABLE IF NOT EXISTS cpfs_validos(cpf IF IS UNIQUE)")  #POSSO?
    #isso é uma copia que vem de evento, mas como vem, pego do broker? Mando do cadastro para ca?

def inserir_emplacamento(placa, modelo,valor,cpf):
    cur.execute("INSERT INTO veiculos VALUES (?,?,?,?)",(placa,modelo,valor,cpf))
    con.commit()

def inserir_cpf(cpf):# SE O CPF FOR VALIDO, COMO FAREI A CONDICIONAL?
    cur.execute("INSERT OR REPLACE INTO cpfs_validos VALUES (?)",(cpf,))

def buscar_veiculo(placa):# SO PRECISO DA PLACA PARA BUSCAR O VEICULO
    cur.execute("SELECT * FROM veiculos WHERE placa = ?",(placa,))






