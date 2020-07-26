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
# install dependecies doc -> https://medium.com/python-pandemonium/better-python-dependency-and-package-management-b5d8ea29dff1




# ---- Integral ----
def to_timeStamp(arr):
    return ((pd.to_datetime(arr) - pd.Timestamp("1970-01-01T00:00:00Z")) / pd.Timedelta('1s')) 
def integrate(x,y):
    x = to_timeStamp(x)
    pot = 0
    for i in range(1,x.shape[0]):
        pot += ((x[i]-x[i-1])*y[i])/(60*60)
    return pot
def integrateDF(df):
    dfn = pd.DataFrame(columns=['estacion','radiacion','maximo','minimo'])
    estacion = df["estacion"].unique()
    for i in estacion:
        es = df[df['estacion'] == i]
        x = np.array(es["fecha"])
        y = np.array(es["radiacion"])
        dfn.loc[len(dfn)] = {'estacion':i, 'radiacion':integrate(x,y), 'maximo':np.amax(y), 'minimo':np.amin(y)}
    return dfn 


## Consultas a la API
BASE_URI_SERVER = "http://localhost/api"

print()

def getPotencialByDate(specificDate):
    date = specificDate + " 00:00:00" # para recuperar la radiacion de todo el día
    response = requests.get(url = BASE_URI_SERVER + "/getRadiacionByDate/"+date)
    data = {}
    if (response.status_code == 200):
         data = response.json()
         df = pd.DataFrame.from_dict(data, orient='columns')
         data = integrateDF(df).to_json(orient='records')[1:-1].replace('},{', '} {')
    return jsonify(data)

def getRadiacionByStation(station, specificDate):
    date = specificDate + " 00:00:00"
    response = requests.get(url = BASE_URI_SERVER + "/getRadiacionByEstacionAndDate/"+station+"/"+date)
    data = response.json()
    return jsonify(data)

# routes
@app.route("/get-potencial/<specificDate>")
def getPotencial(specificDate):
    return getPotencialByDate(specificDate)

@app.route("/get-radiationByStation/<station>/<specificDate>")
def getRadiaction(station, specificDate):
    return getRadiacionByStation(station, specificDate)





if __name__ == "__main__":
    app.run(debug=True, port=5000)