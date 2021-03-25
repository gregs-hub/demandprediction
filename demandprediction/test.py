# pip install -U "C:/Users/grse/OneDrive - DHI/Python/demandprediction/demandprediction-main"

from demandprediction import demand
from demandprediction import utils
import numpy as np

## MANUAL INPUTS
dbflav = 'sqlite'
dbpath = 'C:/dhi/demand/WDONLINE.db'
offtime = "0min" # Offset time TODO
dformat = '%Y-%m-%d %H:%M:%S'

## DB CONFIGURATION NAMES
config = utils.dbConfig(dbflav, dbpath)
colsens = config['INIDFLDNAME']
coldate = config['INDATETIMEFLDNAME']
colvalue = config['INVALUEFLDNAME']
coldemand = config['INQUALITYFLDNAME']
colID = config['AUTOKEYID']

## DB DEMAND PARAMETERS
df_wd = utils.dbParam(dbflav, dbpath)

## LOOP
for index, row in df_wd.iterrows():
    ## Sensor currently forecasted
    print(row['ID'], row ['SENSORTABLE'], row['SENSORID'], row['PREDICTSTEP'], row['PREDICTDURATION'], row['HISTORYDURATION'], row['PREDICTTYPE'])

    ## Get parameters
    tabsensor = row['SENSORTABLE']
    sensor = row['SENSORID']
    rstime = row['PREDICTSTEP']
    leadtime = row['PREDICTDURATION']
    histtime = row['HISTORYDURATION']
    runs = row['RUNS']
    param1 = row['PARAM1']
    param2 = row['PARAM2']
    param3 = row['PARAM3']
    param4 = row['PARAM4']
    param5 = row['PARAM5']
    param6 = row['PARAM6']
    param7 = row['PARAM7']
    param8 = row['PARAM8']

    ## Delete previous forecast from sql
    utils.dbDelete(dbflav, dbpath, sensor, tabsensor, colsens, coldemand)

    ## Get last historical date
    ldate = utils.lastDate(dbflav, dbpath, sensor, tabsensor, coldate, colsens)

    ## Compute water demand prediction dates
    start_train, stop_train, start_pred, stop_pred = utils.datesMgt(ldate, rstime, offtime, leadtime, histtime, dformat)

    ## Data read
    df_in = utils.dbRead(dbflav, dbpath, sensor, tabsensor, colID, coldate, colsens)

    ## Pre-process
    X_train, y_train, df_X, df_y, X_pred = utils.preProcess(df_in, coldate, colID, colsens, coldemand, colvalue, rstime, offtime, start_train, stop_train, start_pred, stop_pred)

    ## Demand computation
    if row['PREDICTTYPE'] == 'HOLTWINTERS': # Need historical data
        fcst = np.zeros((X_pred.shape[1],int(runs)))
        for r in range(runs):
            print('Run '+str(r))
            model = demand.HoltWinters(param1, param2, int(param3), X_pred.shape[1])
            # model = HoltWinters('add','add', 7*24, 24)
            model.learn(y_train)
            fcst[:,r] = model.predict()
        fcst = np.mean(fcst,axis=1)
    elif row['PREDICTTYPE'] == 'MLPDYNAMIC':
        fcst = np.zeros((X_pred.shape[1],int(runs)))
        for r in range(runs):
            print('Run '+str(r))
            model = demand.MLPDynamic(param1, eval(param2), param3, param4, float(param5), int(param6), int(param7), float(param8))
            # model = demand.MLPDynamic('relu', (64,128,64), 'adam', 'adaptive', 0.01, 100000, 1000, 0.01)
            model.learn(X_train.T,y_train)
            fcst[:,r] = model.predict(X_pred.T)
        fcst = np.mean(fcst,axis=1)

    ## Post-process
    df_out = utils.postProcess(fcst, df_X, sensor, coldate, colID, colsens, coldemand, colvalue, start_pred, stop_pred)

    ## Write new forecast to sql
    utils.dbWrite(df_out, dbflav, dbpath, sensor, tabsensor)

    # import pdb; pdb.set_trace() # continue


# CREATE TABLE WDO_DEMANDPREDICTION (
# 	ID INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
# 	SENSORTABLE TEXT NOT NULL,
# 	SENSORID TEXT NOT NULL,
# 	PREDICTSTEP INTEGER NOT NULL,
# 	PREDICTDURATION INTEGER NOT NULL,
# 	HISTORYDURATION INTEGER NOT NULL,
# 	PREDICTTYPE TEXT NOT NULL,
# 	RUNS INTEGER NOT NULL,
# 	PARAM1 TEXT,
# 	PARAM2 TEXT,
# 	PARAM3 TEXT,
# 	PARAM4 TEXT,
# 	PARAM5 TEXT,
# 	PARAM6 TEXT,
# 	PARAM7 TEXT,
# 	PARAM8 TEXT
# )

# CREATE TABLE SENSORS (
# 	ID INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
# 	LIBELLE_MESURE STRING(40) NOT NULL,
# 	VALEUR_MESURE REAL NOT NULL,
# 	DATE_MESURE TIMESTAMP NOT NULL,
# 	CODE INTEGER
# )