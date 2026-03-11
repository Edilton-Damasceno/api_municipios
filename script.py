# Edilton de Jesus Damasceno

import pandas as pd
import requests
from unidecode import unidecode
from rapidfuzz import process

df = pd.read_csv("input.csv")
municipios_ibge = {}
try:
    response = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/municipios", timeout=10)
    if response.status_code != 200:
        raise Exception("Erro na API")
    lista_municipios = response.json()
except:
    lista_municipios = None

resultados = []

if lista_municipios is None:

    for index, row in df.iterrows():

        resultados.append({
            "municipio_input": row["municipio"],
            "populacao_input": row["populacao"],
            "municipio_ibge": "",
            "uf": "",
            "regiao": "",
            "id_ibge": "",
            "status": "ERRO_API"
        })
else:
    for municipio in lista_municipios:
        nome = unidecode(municipio["nome"].lower())

        microrregiao = municipio.get("microrregiao")
        if microrregiao is None:
            continue

        mesorregiao = microrregiao.get("mesorregiao")
        if mesorregiao is None:
            continue

        uf = mesorregiao["UF"]

        municipios_ibge[nome] = {
            "municipio": municipio["nome"],
            "uf": uf["sigla"],
            "regiao": uf["regiao"]["nome"],
            "id": municipio["id"]
        }

def padronizarNomes(nome):
    return unidecode(nome.lower())

def tem_letra_repetida(nome):
    for i in range(len(nome) - 1):
        if nome[i] == nome[i+1]:
            return True
    return False

lista_local_municipios = list(municipios_ibge.keys())

for index, row in df.iterrows():
    municipio_input = row["municipio"]
    populacao = row["populacao"]

    nome_padronizado = padronizarNomes(municipio_input)

    encontrado = process.extractOne(nome_padronizado, lista_local_municipios)

    if encontrado:
        nome_encontrado, score, c = encontrado

        if score >= 90 and not tem_letra_repetida(nome_padronizado):
            info = municipios_ibge[nome_encontrado]

            status = "OK"

            municipio = info["municipio"]
            uf = info["uf"]
            regiao = info["regiao"]
            id = info["id"]

        else:
            status = "NAO_ENCONTRADO"
            municipio = ""
            uf = ""
            regiao = ""
            id = ""
    else:
        status = "NAO_ENCONTRADO"
        municipio = ""
        uf = ""
        regiao = ""
        id = ""

    resultados.append({
        "municipio_input": municipio_input,
        "populacao_input": populacao,
        "municipio_ibge": municipio,
        "uf": uf,
        "regiao": regiao,
        "id_ibge": id,
        "status": status
    })

resultado_df = pd.DataFrame(resultados)
resultado_df.to_csv("resultado.csv", index=False)

total_municipios = len(resultado_df)
total_ok = len(resultado_df[resultado_df["status"] == "OK"])
total_nao = len(resultado_df[resultado_df["status"] == "NAO_ENCONTRADO"])
total_erro_api = len(resultado_df[resultado_df["status"] == "ERRO_API"])
pop_total_ok = resultado_df[resultado_df["status"] == "OK"]["populacao_input"].sum()

ok_df = resultado_df[resultado_df["status"] == "OK"]
medias = ok_df.groupby("regiao")["populacao_input"].mean().to_dict()

stats = {
    "stats": {
        "total_municipios": int(total_municipios),
        "total_ok": int(total_ok),
        "total_nao_encontrado": int(total_nao),
        "total_erro_api": int(total_erro_api),
        "pop_total_ok": int(pop_total_ok),
        "medias_por_regiao": medias
    }
}

ACCESS_TOKEN = ""

url = "https://mynxlubykylncinttggu.functions.supabase.co/ibge-submit"

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

response = requests.post(url, json=stats, headers=headers)

resultado = response.json()
print("Score:", resultado["score"])