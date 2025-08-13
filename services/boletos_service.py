import io
import json

import pandas as pd
import requests
import requests as req
import os
from sqlalchemy import text
from config import SessionLocal
from zeep import Client
import xml.etree.ElementTree as ET

def salvar_df_consulta(df, nome):
    df.to_excel("../data/" + nome + ".xlsx", index=False)
    df = pd.read_excel("../data/" + nome + ".xlsx", sheet_name=0)
    df.to_csv("../data/" + nome + ".csv", sep=";", decimal=",", index=False, encoding="utf-8-sig")

def consultar_boleto_sefaz(nosso_numero):

    wsdl_url = 'https://www4.sefaz.pb.gov.br/atfws/DARDistribuicao?wsdl'
    client = Client(wsdl_url)

    # Força usar a URL pública, ignorando a do WSDL (interno)
    client.service._binding_options['address'] = 'https://www4.sefaz.pb.gov.br/atfws/DARDistribuicao?wsdl'

    param_xml = f"""
    <Pagamento><nrNossoNumero>{nosso_numero}</nrNossoNumero></Pagamento>
    """
    response = client.service.consultarPagamento(elementoEntrada=param_xml)

    # Extrai a string XML que está dentro de CDATA
    xml_str = response  # ou response['retorno']

    # Parseia o XML interno
    root = ET.fromstring(xml_str)
    numero = root.find("nrNossoNumero").text
    data = root.find("dtPagamento").text
    status = root.find("stLancamento").text
    return numero, data, status

def gerarDataBoletos():
    numeros_sefaz = []
    data_sefaz = []
    status_sefaz = []
    session = SessionLocal()
    # data_inicio = datetime.strptime(dataInicio, "%Y/%m/%d").date()
    # data_fim = datetime.strptime(dataFim, "%Y/%m/%d").date()

    query = text("""
        select * from outorga.view_boleto vb where vb.data_pagamento is null and vb.tipo_boleto = 3 
        and vb.data_emissao between '2024/01/01' and '2024/12/31'
        """)

    result = session.execute(query)

    dados = [dict(row) for row in result.mappings()]

    df = pd.DataFrame(dados)

    for index, row in df.iterrows():
        num, datas, stat = consultar_boleto_sefaz(row['nosso_numero'])
        numeros_sefaz.append(num)
        data_sefaz.append(datas)
        status_sefaz.append(stat)

    df['nrNossoNumero'] = numeros_sefaz
    df['dtPagamento'] = data_sefaz
    df['stLancamento'] = status_sefaz

    return df





df  = gerarDataBoletos()

salvar_df_consulta(df, "boletos")
# numero, data, status = consultar_boleto_sefaz("3031064261")
print(df.to_string())