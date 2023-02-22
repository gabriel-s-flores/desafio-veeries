from flask import Flask, jsonify
import sqlite3
import json
import desafio_veeries.banco as banco

app = Flask(__name__)

@app.route('/centro_oeste')
def get_clima():
    conn = sqlite3.connect('clima.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM centro_oeste')
    rows = cursor.fetchall()
    cidades = []
    for row in rows:
        cidade = {
            'nome': row[0],
            'uf': row[1],
            'codigo': row[2],
            'umidade': row[3],
            'temperatura': row[4],
            'chuvas': row[5]
        }
        cidades.append(cidade)
    conn.close()
    response = jsonify(cidades)
    response.headers['Content-Type'] = 'application/json; charset=utf-8-sig'
    response.set_data((json.dumps(cidades, ensure_ascii=False, indent=None).encode('utf-8-sig')))
    return response

banco.inicializaBanco()

if __name__ == '__main__':
    app.run(debug=True)