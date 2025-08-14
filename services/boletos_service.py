import time
import logging
import pandas as pd
import requests
from sqlalchemy import text
from config import SessionLocal
from zeep import Client, Transport
import xml.etree.ElementTree as ET

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("sefaz.log"),
        logging.StreamHandler()
    ]
)

def salvar_df_consulta(df, nome):
    df.to_excel("../data/" + nome + ".xlsx", index=False)
    df = pd.read_excel("../data/" + nome + ".xlsx", sheet_name=0)
    df.to_csv("../data/" + nome + ".csv", sep=";", decimal=",", index=False, encoding="utf-8-sig")

def consultar_boleto_sefaz(nosso_numero):
    try:
        wsdl_url = 'https://www4.sefaz.pb.gov.br/atfws/DARDistribuicao?wsdl'

        # Cria uma sessão para capturar o status HTTP
        session = requests.Session()
        transport = Transport(session=session)

        client = Client(wsdl_url, transport=transport)

        # Força usar a URL pública
        client.service._binding_options['address'] = wsdl_url

        param_xml = f"""
        <Pagamento><nrNossoNumero>{nosso_numero}</nrNossoNumero></Pagamento>
        """

        # Faz a requisição SOAP
        response = client.service.consultarPagamento(elementoEntrada=param_xml)

        # Captura o último código de status HTTP
        status_code = session.get_adapter(wsdl_url).max_retries  # este não é o status real, vamos pegar de outra forma
        last_response = transport.session  # sessão da requisição

        logging.info(f"Consulta boleto {nosso_numero} - Status HTTP: {transport.session.adapters['https://'].max_retries}")  # ajustável

        # Parse do XML
        root = ET.fromstring(response)

        # Verifica se houve erro no XML
        erro = root.find("mensagemErro") or root.find("erro") or root.find("mensagem")
        if erro is not None:
            msg_erro = erro.text
            logging.error(f"[ERRO SEFAZ] Nosso Número {nosso_numero}: {msg_erro}")
            return None, None, f"ERRO: {msg_erro}"

        numero = root.find("nrNossoNumero").text
        data = root.find("dtPagamento").text
        status = root.find("stLancamento").text
        # logging.info(f"Consulta boleto {nosso_numero} - Status HTTP: ")  # ajustável
        print(response)
        return numero, data, status

    except Exception as e:
        logging.exception(f"Erro ao consultar boleto {nosso_numero}")
        return None, None, f"EXCEPTION: {str(e)}"

def gerarDataBoletos():
    numeros_sefaz = []
    data_sefaz = []
    status_sefaz = []
    session = SessionLocal()
    # data_inicio = datetime.strptime(dataInicio, "%Y/%m/%d").date()
    # data_fim = datetime.strptime(dataFim, "%Y/%m/%d").date()

    query = text("""
        select * from outorga.view_boleto vb where vb.data_pagamento is null and vb.tipo_boleto = 3 
        and vb.data_emissao between '2025/01/01' and '2025/01/31'
        """)

    result = session.execute(query)

    dados = [dict(row) for row in result.mappings()]

    df = pd.DataFrame(dados)

    for index, row in df.iterrows():
        num, datas, stat = consultar_boleto_sefaz(row['nosso_numero'])
        numeros_sefaz.append(num)
        data_sefaz.append(datas)
        status_sefaz.append(stat)
        print(index, 'nrNossoNumero: ', num, 'dtPagamento: ', datas, 'stLancamento: ', stat)
        time.sleep(0.5)

    df['nrNossoNumero'] = numeros_sefaz
    df['dtPagamento'] = data_sefaz
    df['stLancamento'] = status_sefaz

    return df





df  = gerarDataBoletos()

salvar_df_consulta(df, "boletos")
# numero, data, status = consultar_boleto_sefaz("3031064261")
print(df.to_string())