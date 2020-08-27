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
    return ((pd.to_datetime(arr) - pd.Timestamp("1970-01-01T00:00:00")) / pd.Timedelta('1h')) 
def integrateSimpsUT(x,y):
    x = to_timeStamp(x)
    return np.trapz(y,x=x)##integrate.simps(y,x)
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
        dfn.loc[len(dfn)] = {'estacion':i, 'radiacion':integrateSimpsUT(x,y), 'maximo':np.amax(y), 'minimo':np.amin(y[y != 0])}
    return dfn
def integrateDfJSON(df):
    return to_json(integrateDF(df))

## Consultas a la API
BASE_URI_SERVER = "http://localhost/api"


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
             data = integrateDfJSON(df)
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
             data = integrateDfJSON(df)
    return jsonify(data)


##Funciones de integracion por día, mes o año
def integrateByDay(df):
    dfn = pd.DataFrame(columns=['estacion','fecha','radiacion','maximo','minimo'])
    estacion = df["estacion"].unique()
    ran = pd.to_datetime((df['fecha'].apply(lambda x:(pd.to_datetime(x)).strftime('%Y-%m-%d') )).unique())
    for i in estacion:
        es = df[df['estacion'] == i]
        for j in ran:
            ess = es[((pd.to_datetime(es['fecha'])).dt.year == j.year) & ((pd.to_datetime(es['fecha'])).dt.month == j.month) 
                    & ((pd.to_datetime(es['fecha'])).dt.day == j.day)]
            x = np.array(ess["fecha"])
            y = np.array(ess["radiacion"])
            if x.shape[0] > 0:
                try:
                    dfn.loc[len(dfn)] = {'estacion':i,'fecha':j.strftime('%Y-%m-%d') ,'radiacion':integrateSimpsUT(x,y), 'maximo':np.amax(y), 'minimo':np.amin(y[y != 0])}
                except:
                    dfn.loc[len(dfn)] = {'estacion':i,'fecha':j.strftime('%Y-%m-%d') ,'radiacion':integrateSimpsUT(x,y), 'maximo':np.amax(y), 'minimo':np.amin(y)}
    return to_json(dfn)
def integrateByMonth(df):
    dfn = pd.DataFrame(columns=['estacion','fecha','radiacion','maximo','minimo'])
    estacion = df["estacion"].unique()
    ran = pd.to_datetime((df['fecha'].apply(lambda x:(pd.to_datetime(x)).strftime('%Y-%m') )).unique())
    for i in estacion:
        es = df[df['estacion'] == i]
        for j in ran:
            ess = es[((pd.to_datetime(es['fecha'])).dt.year == j.year) & ((pd.to_datetime(es['fecha'])).dt.month == j.month)]
            x = np.array(ess["fecha"])
            y = np.array(ess["radiacion"])
            if x.shape[0] > 0:
                try:
                    dfn.loc[len(dfn)] = {'estacion':i,'fecha':j.strftime('%Y-%m') ,'radiacion':integrateSimpsUT(x,y), 'maximo':np.amax(y), 'minimo':np.amin(y[y != 0])}
                except:
                    dfn.loc[len(dfn)] = {'estacion':i,'fecha':j.strftime('%Y-%m') ,'radiacion':integrateSimpsUT(x,y), 'maximo':np.amax(y), 'minimo':np.amin(y)}
    return to_json(dfn)
def integrateByYear(df):
    dfn = pd.DataFrame(columns=['estacion','fecha','radiacion','maximo','minimo'])
    ran = pd.to_datetime((df['fecha'].apply(lambda x:(pd.to_datetime(x)).strftime('%Y') )).unique())
    estacion = df["estacion"].unique()
    for i in estacion:
        es = df[df['estacion'] == i]
        for j in ran:
            ess = es[(pd.to_datetime(es['fecha'])).dt.year == j.year]
            x = np.array(ess["fecha"])
            y = np.array(ess["radiacion"])
            if x.shape[0] > 0:
                try:
                    dfn.loc[len(dfn)] = {'estacion':i,'fecha':j.strftime('%Y') ,'radiacion':integrateSimpsUT(x,y), 'maximo':np.amax(y), 'minimo':np.amin(y[y != 0])}
                except:
                    dfn.loc[len(dfn)] = {'estacion':i,'fecha':j.strftime('%Y') ,'radiacion':integrateSimpsUT(x,y), 'maximo':np.amax(y), 'minimo':np.amin(y)}
    return to_json(dfn)
def integrateBy(df, by='day'):
    if by == 'day':
        return integrateByDay(df)
    elif by == 'month':
        return integrateByMonth(df)
    elif by == 'year':
        return integrateByYear(df)
    else:
        return "{data:'error'}"

# Funciones para potencial por rango de fechas
def getPotencialByDateRangeDayFunction(start_date, last_date):
    print("--Calculando potencial por día para el rango de fechas: " + start_date + " - " + last_date)
    response = requests.get(url = BASE_URI_SERVER + "/getRadiacionByRangeDate/" + start_date + " 00:00:00" + "/" + last_date + " 00:00:00")
    data = {}
    if (response.status_code == 200):
         data = response.json()
         if(len(data) > 0):
             df = pd.DataFrame.from_dict(data, orient='columns')
             data = integrateByDay(df)
    return jsonify(data)

def getPotencialByDateRangeYearFunction(start_date, last_date):
    print("--Calculando potencial por año para el rango de fechas: " + start_date + " - " + last_date)
    response = requests.get(url = BASE_URI_SERVER + "/getRadiacionByRangeDate/" + start_date + " 00:00:00" + "/" + last_date + " 00:00:00")
    data = {}
    if (response.status_code == 200):
         data = response.json()
         if(len(data) > 0):
             df = pd.DataFrame.from_dict(data, orient='columns')
             data = integrateByYear(df)
    return jsonify(data)

def getPotencialByDateRangeMonthFunction(start_date, last_date):
    print("--Calculando potencial por mes para el rango de fechas: " + start_date + " - " + last_date)
    response = requests.get(url = BASE_URI_SERVER + "/getRadiacionByRangeDate/" + start_date + " 00:00:00" + "/" + last_date + " 00:00:00")
    data = {}
    if (response.status_code == 200):
         data = response.json()
         if(len(data) > 0):
             df = pd.DataFrame.from_dict(data, orient='columns')
             data = integrateByMonth(df)
    return jsonify(data)

# routes
@app.route("/getPotencial")
def getPotencial():
    return getPotencialFunction()

@app.route("/getPotencialByDate/<specificDate>")
def getPotencialByDate(specificDate):
    return getPotencialByDateFunction(specificDate)

@app.route("/getPotencialByDateRangeDay/<start_date>/<last_date>")
def getPotencialByDateRangeDay(start_date, last_date):
    return getPotencialByDateRangeDayFunction(start_date, last_date)

@app.route("/getPotencialByDateRangeYear/<start_date>/<last_date>")
def getPotencialByDateRangeYear(start_date, last_date):
    return getPotencialByDateRangeYearFunction(start_date, last_date)

@app.route("/getPotencialByDateRangeMonth/<start_date>/<last_date>")
def getPotencialByDateRangeMonth(start_date, last_date):
    return getPotencialByDateRangeMonthFunction(start_date, last_date)







if __name__ == "__main__":
    app.run(debug=True, port=5000)