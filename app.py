import sqlite3

import numpy as np
import pandas as pd
from flask import Flask
from flask_restful import Api, Resource
from pandas import Series

app = Flask(__name__)
api = Api(app)


class ReadCsv(Resource):
    def get(self):
        db = Database()
        db.create_database()
        db.update_data()
        db.get_data()

        # with open('movielist.csv', 'r') as file:
        #     my_reader = csv.reader(file, delimiter=';')
        #     data = []
        #     for row in my_reader:
        #         data.append(row)
        #
        #     self.save_csv(data)
        #
        # return True

    # def save_csv(self, data):
    #     conn = sqlite3.connect('worst_movie.db')
    #     cursor = conn.cursor()
    #
    #     try:
    #         with open('movielist.csv', 'r') as file:
    #             my_reader = csv.reader(file, delimiter=';')
    #             data = []
    #             for row in my_reader:
    #                 print(row)
    #                 cursor.execute('insert into awards values {}'.format(tuple(row)))
    #
    #             conn.commit()
    #
    #         WorstMovies().getMaxMin()
    #         # data_ = [tuple(i) for i in data]
    #         #
    #         # #
    #         # records_list_template = ','.join(['?'] * len(data_))
    #         # insert_query = 'insert into awards values {}'.format(records_list_template)
    #         # cursor.execute(insert_query, data_[1:])
    #         # conn.commit()
    #     except Exception as e:
    #         print(e)
    #
    # def _create_database(self):
    #     conn = sqlite3.connect('worst_movie.db')
    #     print("Opened database successfully")
    #
    #     conn.execute('CREATE TABLE IF NOT EXISTS awards (year INTEGER , title TEXT, studios TEXT, producers TEXT, winner TEXT)')
    #     conn.execute('DELETE FROM awards')
    #     conn.commit()
    #     print("Table created successfully")
    #     conn.close()
    #
    #     return True


class WorstMovies:
    def __int__(self):
        pass

    def getMaxMin(self):
        conn = sqlite3.connect('worst_movie.db')

        cur = conn.cursor()
        cur.execute("SELECT * FROM awards")

        rows = cur.fetchall()
        df = pd.DataFrame([])
        for row in rows:
            print(row)


class Database:
    def __init__(self):
        self.conn = sqlite3.connect('worst_movie.db')
        self.cursor = self.conn.cursor()

    def create_database(self):
        try:
            # Cria a tabela caso ainda não exista
            self.conn.execute('CREATE TABLE IF NOT EXISTS awards (year INTEGER , title TEXT, studios TEXT, producers TEXT, winner TEXT)')
            # Apaga todos os dados
            self.conn.execute('DELETE FROM awards')
            self.conn.commit()
        except Exception as e:
            print(e)
            return False

        return True

    def update_data(self, path='movielist.csv', delimiter=';'):
        # Busca as informações do arquivo CSV
        df = pd.read_csv(path, delimiter=delimiter)
        df2 = df.replace({np.nan: ''})
        data_ = [tuple(x) for x in df2.to_numpy()]
        for row in data_:
            print(row)
            self.cursor.execute('insert into awards values {}'.format(tuple(row)))
        self.conn.commit()

        # records_list_template = ','.join([''] * len(data_))
        # insert_query = 'insert into awards values {};'.format(records_list_template)
        # self.cursor.execute(insert_query, data_)
        # self.conn.commit()

    def get_data(self):
        winners = pd.DataFrame(self.conn.execute('SELECT replace(producers, \'and\', \',\'), year FROM awards where winner = \'yes\''))
        winners = pd.concat([Series(row[1], row[0].split(',')) for _, row in winners.iterrows()]).reset_index()
        winners[0] = winners[0].apply(lambda x: x.strip())

api.add_resource(ReadCsv, '/teste')

if __name__ == '__main__':
    app.run()
