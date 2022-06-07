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
        # Busca os produtores que ganharam mais de uma vez e os respectivos anos
        queryset_ganhadores = self.conn.execute('select producer, year '
                                                'from WINNERS_TEMP '
                                                'where producer in (select producer '
                                                '                       from WINNERS_TEMP'
                                                '                       group by producer '
                                                '                       having count(year) > 1) '
                                                'group by producer, year '
                                                'order by producer, year')
        ganhadores = queryset_ganhadores.fetchall()

        # Define o max e min gap que ocorreram por produtor que ganhou o prêmio
        ls_max = []
        ls_min = []
        producers_gap = {}
        last_win = 0
        lower_gap_found = 0
        higher_gap_found = 0
        for idx, row in enumerate(ganhadores):
            if producers_gap.get(row[0]) is None:
                last_win = row[1]
                producers_gap[row[0]] = {
                    'min': 0,
                    'max': 0
                }
            else:
                current_win = row[1]

                # Busca os gaps por produtor
                if (current_win - last_win < producers_gap[row[0]]['min']) or producers_gap[row[0]]['min'] == 0:
                    producers_gap[row[0]]['min'] = current_win - last_win

                if current_win - last_win > producers_gap[row[0]]['max']:
                    producers_gap[row[0]]['max'] = current_win - last_win

                # Se houver um gap menor limpa a lista e adiciona o ganhador com esse gap
                # Se tiver um ganhador com o mesmo gap faz append na lista de ganhadores do gap minimo
                if producers_gap[row[0]]['min'] < lower_gap_found or lower_gap_found == 0:
                    lower_gap_found = producers_gap[row[0]]['min']
                    ls_min = [(row[0], current_win, last_win, current_win - last_win)]
                elif (producers_gap[row[0]]['min'] == lower_gap_found) and (current_win - last_win == lower_gap_found):
                    ls_min.append((row[0], current_win, last_win, current_win - last_win))

                # Se houver um gap maior limpa a lista e adiciona o ganhador com esse gap
                # Se tiver um ganhador com o mesmo gap faz append na lista de ganhadores do gap maximo
                if producers_gap[row[0]]['max'] > higher_gap_found:
                    higher_gap_found = producers_gap[row[0]]['max']
                    ls_max = [(row[0], current_win, last_win, current_win - last_win)]
                elif producers_gap[row[0]]['max'] == higher_gap_found and (current_win - last_win == higher_gap_found):
                    ls_max.append((row[0], current_win, last_win, current_win - last_win))

                last_win = current_win

        # Cria as listas para retorno
        list_producers_max_gap = [
            {
                'producer': i[0],
                'interval': i[3],
                'previousWin': i[2],
                'followingWin': i[1],
            }
            for i in ls_max
        ]

        list_producers_min_gap = [
            {
                'producer': i[0],
                'interval': i[3],
                'previousWin': i[2],
                'followingWin': i[1],
            }
            for i in ls_min
        ]

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
        self.conn.execute('DELETE FROM WINNERS_TEMP')
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
