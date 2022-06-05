import sqlite3

import numpy as np
import pandas as pd
from flask import Flask
from flask_restful import Api, Resource
from pandas import Series

app = Flask(__name__)
api = Api(app)


class ReadCsv(Resource):
    def get(self, path=None):
        db = WorstMovie()
        db.create_database()
        db.update_data(path=path)
        response = db.get_data()

        return response

class WorstMovie:
    def __init__(self):
        self.conn = sqlite3.connect('worst_movie.db')
        self.cursor = self.conn.cursor()
        self.response = {}

    def create_database(self):
        try:
            # Cria a tabela caso ainda não exista
            self.conn.execute('CREATE TABLE IF NOT EXISTS awards (year INTEGER , title TEXT, studios TEXT, producers TEXT, winner TEXT)')
            # Apaga todos os dados para que seja sempre atualizado com o arquivo final
            self.conn.execute('DELETE FROM awards')
            self.conn.commit()
        except Exception as e:
            print(e)
            return False

        return True

    def update_data(self, path=None, delimiter=None):
        if not path:
            path = 'movielist.csv'

        if not delimiter:
            delimiter = ';'

        # Busca as informações do arquivo CSV
        df = pd.read_csv(path, delimiter=delimiter)
        df2 = df.replace({np.nan: ''})
        data_ = [tuple(x) for x in df2.to_numpy()]
        for row in data_:
            self.cursor.execute('insert into awards values {}'.format(tuple(row)))
        self.conn.commit()

        # Bulk insert
        # records_list_template = ','.join([''] * len(data_))
        # insert_query = 'insert into awards values {};'.format(records_list_template)
        # self.cursor.execute(insert_query, data_)
        # self.conn.commit()

    def get_data(self):
        # Busca os produtores que ganharam o premio e o respectivo ano
        #   É feito um replace na palavra " and " baseado no padrão do arquivo cvs, isso é usado para separar os nomes dos produtores
        #       vencedore de cada ano. O resultado é atribuido a um Dataframe para simplificação do processo de criar uma linha para cada
        #       produtor vencedor.
        winners = pd.DataFrame(self.conn.execute('SELECT replace(producers, \' and \', \',\'), year FROM awards where winner = \'yes\''))
        winners = pd.concat([Series(row[1], row[0].split(',')) for _, row in winners.iterrows()]).reset_index()
        winners['index'] = winners['index'].apply(lambda x: x.strip())
        winners = winners.rename(columns={'index': 'producers', 0: 'year'})

        # Uma tabela temporária é criada para armazenar a informação do nome do produtor e ano que ganhou o premio
        #   Esta decisão foi tomada pela simplicidade do cálculo de quais produtores ganharam mais de uma vez e qual foi o gap entre as
        #       vitórias em SQL.
        self._create_winers_temp_table(winners)
        self._get_gaps()

        # Apaga a tabela temporária após o cálculo realizado
        self._drop_temp_table()

        return self.response

    def _get_gaps(self):
        # Query para buscar as informações de quem já ganhou mais de uma vez e quais os respectivos anos
        queryset = self.conn.execute('select producer, max(year) as max, min(year) as min, max(year) - min(year) as gap '
                                     'from WINNERS_TEMP '
                                     'group by producer '
                                     'HAVING max(year) - min(year) > 0 '
                                     'order by max(year) - min(year) desc;')
        gaps = queryset.fetchall()

        # Encontra os registros com maior e o menor gap dos produtores que já venceram
        min_gap = min(gaps, key=lambda t: t[3])
        max_gap = max(gaps, key=lambda t: t[3])

        # Pega os valores de maior e menor gap, caso exista
        min_gap = min_gap[3] if min_gap else 0
        max_gap = max_gap[3] if max_gap else 0

        # Cria a lista de max e min buscando na lista de vencedores quem tem o maior e menor gap
        list_producers_max_gap = []
        list_producers_min_gap = []
        for row in gaps:
            if row[3] == max_gap:
                aux = {
                    'producer': row[0],
                    'interval': max_gap,
                    'previousWin': row[2],
                    'followingWin': row[1]
                }
                list_producers_max_gap.append(aux)

            if row[3] == min_gap:
                aux = {
                    'producer': row[0],
                    'interval': min_gap,
                    'previousWin': row[2],
                    'followingWin': row[1]
                }
                list_producers_min_gap.append(aux)

        # Monta o dicinario final que sera retornado
        self.response = {
            'min': list_producers_min_gap,
            'max': list_producers_max_gap
        }

        return self.response

    def _create_winers_temp_table(self, dataframe):
        # Cria uma tabela temporária para simplificação do cálculo de produtores que ganharam o premio
        #   e o gap entre cada vitória
        self.conn.execute('CREATE TABLE IF NOT EXISTS WINNERS_TEMP (year INTEGER , producer TEXT)')
        for row in [tuple(x) for x in dataframe.to_numpy()]:
            self.cursor.execute('insert into WINNERS_TEMP (producer, year) values {}'.format(tuple(row)))
        self.conn.commit()

    def _drop_temp_table(self):
        # Deleta a tabela temporária
        self.conn.execute('DROP TABLE WINNERS_TEMP')
        self.conn.commit()


api.add_resource(ReadCsv, '/worstmovie/gap/find', '/worstmovie/gap/find/<path>')

if __name__ == '__main__':
    app.run()
