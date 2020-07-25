from flask import Flask;
import pandas as pd
import numpy as np
from scipy import integrate
from flask import jsonify
import requests
app = Flask(__name__)
##todas las siguientes funciones trabajan sobre arreglos de numpy es decir los parametros de entrada deben ser np.arrays
##Convertir las fechas en unix
# install dependecies doc -> https://medium.com/python-pandemonium/better-python-dependency-and-package-management-b5d8ea29dff1




# ---- Integral ----
##Lo unico que cambia es el metodo que trabaja con las fechas pasa de usar to_timeStamp a to_timeStampH, el resto
##se manteiene igual
def to_timeStampH(arr):
    return (pd.to_datetime(arr).hour) + ((pd.to_datetime(arr).minute/60) + (pd.to_datetime(arr).second/3600))
def to_timeStamp(arr):
    return (pd.to_datetime(arr) - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
def integrateSimpsUT(x,y):
    x = to_timeStampH(x)
    return integrate.simps(y,x)


def integrateDF(df):
    dfn = pd.DataFrame(columns=['estacion','radiacion'])
    estacion = df["estacion"].unique()
    for i in estacion:
        es = df[df['estacion'] == i]
        x = np.array(es["fecha"])
        y = np.array(es["radiacion"])
        dfn.loc[len(dfn)] = {'estacion':i, 'radiacion':integrateSimpsUT(x,y)}
    print(dfn)
    return dfn


## Consultas a la API
BASE_URI_SERVER = "http://localhost/api"

print()

def getDataByDate(specificDate):
    date = specificDate + " 00:00:00" # para recuperar la radiación de todo el día
    response = requests.get(url = BASE_URI_SERVER + "/getRadiacionByDate/"+date)
    data = {}
    if (response.status_code == 200):
         data = response.json()
         df = pd.DataFrame.from_dict(data, orient='columns')
         data = integrateDF(df).to_json(orient='records')[1:-1].replace('},{', '} {')
    return jsonify(data)



# routes
@app.route("/get-radiation/<specificDate>")
def test(specificDate):
    return getDataByDate(specificDate)




if __name__ == "__main__":
    app.run(debug=True, port=5000)