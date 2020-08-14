from flask import Flask;
import pandas as pd
import numpy as np
from scipy import integrate
from flask import jsonify
import requests
import json
from flask_cors import CORS
app = Flask(__name__)
CORS(app)

##todas las siguientes funciones trabajan sobre arreglos de numpy es decir los parametros de entrada deben ser np.arrays
##Convertir las fechas en unix


##Lo unico que cambia es el metodo que trabaja con las fechas pasa de usar to_timeStamp a to_timeStampH, el resto
##se manteiene igual

##Implementación del algoritmo de cesar
##todas las siguientes funciones trabajan sobre arreglos de numpy es decir los parametros de entrada deben ser np.arrays
##Convertir las fechas en unix
def to_timeStampH(arr):
    return (pd.to_datetime(arr).hour) + ((pd.to_datetime(arr).minute/60) + (pd.to_datetime(arr).second/3600))
def to_timeStamp(arr):
    return ((pd.to_datetime(arr) - pd.Timestamp("1970-01-01T00:00:00Z")) / pd.Timedelta('1h'))
def integrateSimpsUT(x,y):
    x = to_timeStamp(x)
    return integrate.simps(y,x)
def to_json(df):
    parsed = df.to_json(orient="table")
    result = json.loads(parsed)
    del result['schema']
    return result
def integrateDF(df):
    dfn = pd.DataFrame(columns=['estacion','radiacion','maximo','minimo'])
    estacion = df["estacion"].unique()
    for i in estacion:
        es = df[df['estacion'] == i]
        x = np.array(es["fecha"])
        y = np.array(es["radiacion"])
        dfn.loc[len(dfn)] = {'estacion':i, 'radiacion':integrateSimpsUT(x,y), 'maximo':np.amax(y), 'minimo':np.amin(y)}
    return  to_json(dfn)


## Consultas a la API
BASE_URI_SERVER = "http://localhost/api"

print()

# retorna el potencial en una fecha espesifica
def getPotencialByDateFunction(specificDate):
    date = specificDate + " 00:00:00" # radiacion de todo el día
    print("--Calculando potencial para fecha: " +date)
    response = requests.get(url = BASE_URI_SERVER + "/getRadiacionByDate/"+date)
    data = {}
    if (response.status_code == 200):
         data = response.json()
         if(len(data) > 0):
             df = pd.DataFrame.from_dict(data, orient='columns')
             data = integrateDF(df)
    return jsonify(data)

# Potencial para todas las estaciones
def getPotencialFunction():
    print("--Calculando potencial para todas las estaciones: ")
    response = requests.get(url = BASE_URI_SERVER + "/getRadiacion")
    data = {}
    if (response.status_code == 200):
         data = response.json()
         if(len(data) > 0):
             df = pd.DataFrame.from_dict(data, orient='columns')
             data = integrateDF(df)
    return jsonify(data)

# routes
@app.route("/getPotencial")
def getPotencial():
    return getPotencialFunction()

@app.route("/getPotencialByDate/<specificDate>")
def getPotencialByDate(specificDate):
    return getPotencialByDateFunction(specificDate)







if __name__ == "__main__":
    app.run(debug=True, port=5000)