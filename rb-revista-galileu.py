from classes.scraping_bs4 import WebScrapingBS4
from core.database import Database
from classes.bot_logger import BotHealthManager
from datetime import datetime
from config import Config

bot_manager = BotHealthManager()
logging = bot_manager.logger

config = Config()

data_atualizacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def main():

    scraping1 = WebScrapingBS4(link=f"{config.REVISTA_GALILEU}")
    noticias = scraping1.perga_texto(elementoFILHO='a', elementoPAI='div', tipoPAI='class', descricaoPAI='feed-post-body')
    links = scraping1.pega_url(elementoFILHO='a', elementoPAI='div', tipoPAI='class', descricaoPAI='feed-post-body')

    numero_de_coletas: int = 1
    with Database() as cur:
        for noticia, link in zip(noticias, links):

            bot_manager.add_registro_analisado()
            logging.info(f'Coleta {numero_de_coletas} inicida.')
            cur.execute("""SELECT noticia FROM noticias_revista_galileu
                        WHERE noticia = %s""", noticia)
            varificao = cur.fetchone()

            cur.execute("""DELETE FROM noticias_revista_galileu WHERE DATEDIFF(CURDATE(), date_cadastro) > 3;
            """)

            if varificao:
                logging.info("Coleta já cadastrada anteriormente.")
                cur.execute("""
                UPDATE 
                    noticias_revista_galileu
                SET
                    data_atualizacao = %s
                WHERE
                    noticia = %s
                """, (data_atualizacao, noticia))
                numero_de_coletas += 1
                continue

            cur.execute("""INSERT INTO noticias_revista_galileu(noticia, link) VALUES (%s, %s)""", (noticia, link))
            logging.info('Coleta persistida com sucesso!')
            numero_de_coletas += 1
            bot_manager.add_registro_persistido()

    bot_manager.finalizar_execucao(st_sucesso_execucao=True)


if __name__ == "__main__":
    main()
