from classes.scraping_bs4 import WebScrapingBS4
from bs4 import BeautifulSoup
from urllib.request import urlopen
from core.database import Database
from classes.bot_logger import BotHealthManager
from datetime import datetime
from classes.funcoes_apoio import converter_data
from config import Config

bot_manager = BotHealthManager()
logging = bot_manager.logger

config = Config()

data_atualizacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def main():

    coleta = f'{config.CORRIDAS_DF}'
    site = WebScrapingBS4(f'{coleta}')
    link = site.pega_url(elementoPAI='div', tipoPAI='class',
                         descricaoPAI='evo_event_schema', elementoFILHO='a',
                         tipoFILHO='itemprop', descricaoFILHO='url')
    logging.info(f"Coleta iniciada em: {coleta}")

    numero_de_coletas: int = 1
    with Database() as cur:
        for url in link:

            bot_manager.add_registro_analisado()

            logging.info(f'Coleta {numero_de_coletas} inicida.')
            html = urlopen(f'{url}')
            soup = BeautifulSoup(html.read(), 'html.parser')

            nome_do_evento = soup.find(name='span', attrs=({'class': 'evoet_title evcal_desc2 evcal_event_title'})).text
            distancia = soup.find(name='div', attrs=({'class': 'evo_custom_content evo_data_val'})).text
            local = soup.find(name='p', attrs=({'class': 'evo_location_name'})).text
            horario = soup.find(name='span', attrs=({'class': 'evo_eventcard_time_t'})).text
            data_evento = soup.find(name='span', attrs=({'class': 'evo_start'})).text
            data_evento = data_evento[0:2] + data_evento[2:5]
            data_evento = converter_data(data_evento)

            """
                A estrutura desta página conta com duas divs idênticas, por isso foi necessário esse tratamento para pegar o valor da inscrição da corrida.
            """

            parse_valor = soup.find_all(name='div', attrs=({'class': 'evo_custom_content evo_data_val'}))
            if len(parse_valor) == 2:
                valor = parse_valor[1].get_text()
            else:
                valor = None

            link = soup.find(name='a', attrs=({'class': 'evcal_evdata_row evo_clik_row'}))
            if link is not None:
                inscricao = link.get('href')
            else:
                inscricao = url

            if valor:
                cur.execute("""SELECT nome, valor FROM corridas_df WHERE nome = %s AND valor = %s AND data_evento = %s""", (nome_do_evento, valor, data_evento))
            else:
                cur.execute("""SELECT nome, valor FROM corridas_df WHERE nome = %s AND data_evento = %s""",(nome_do_evento, data_evento))
            verifica_cadastros = cur.fetchone()

            if verifica_cadastros:
                logging.info('Coleta já cadastrada anteriormente.')
                numero_de_coletas += 1
                cur.execute("""
                UPDATE
                    corridas_df
                SET
                    data_atualizacao = %s
                WHERE
                    nome = %s
                """, (data_atualizacao, nome_do_evento))
                continue

            cur.execute("""INSERT INTO corridas_df(nome, distancia, local, valor, horario, inscricao, data_evento)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""", (nome_do_evento, distancia, local, valor, horario, inscricao, data_evento))

            logging.info('Coleta persistida com sucesso!')
            numero_de_coletas += 1
            bot_manager.add_registro_persistido()
            
        logging.info('\tDeletando Eventos que já ocorreram...')
        cur.execute("""DELETE FROM corridas_df WHERE data_evento < CURDATE() AND data_evento IS NOT NULL;""")
            
    bot_manager.finalizar_execucao(st_sucesso_execucao=True)


if __name__ == "__main__":
    main()
