# pip install -U "C:/Users/grse/OneDrive - DHI/Python/demandprediction/demandprediction-main"
# import pdb; pdb.set_trace() # continue

from demandprediction import demand
from demandprediction import utils
import numpy as np
import configparser

## INFOS AND TODO
# for now only sqlite, with database path manually provided
# for now, resampling time (rstime) is both computational time step (water demand algo) and output predictions time step
# offset time (for resampling) can b eprovided manually
# date format is manually provided
# two algorithms with some parameters editable : Holt Winters prediction (exponential smoothing) and Multi Layer Perceptron Regressor
# supposed to work with < 1h timesteps, but not tested intensively

## READ CONFIGURATION FILE
iniconfig = configparser.ConfigParser()
# config['DEMAND'] = {'databaseFlavour': 'sqlite', 'databasePath': 'C:/dhi/demand/WDONLINE.db', 'resamplingOffset': '0min', 'dateFormat': 'YYYY-mm-dd HH:MM:SS'}
# with open('config.ini', 'w') as configfile:
#     config.write(configfile)
iniconfig.read('config.ini')
dbflav = iniconfig['DEMAND']['databaseFlavour'] # 'sqlite'
dbpath = iniconfig['DEMAND']['databasePath'] # 'C:/dhi/demand/WDONLINE.db'
offtime = iniconfig['DEMAND']['resamplingOffset'] # "0min"
dformat = iniconfig['DEMAND']['dateFormat'] # '%Y-%m-%d %H:%M:%S'
dformat = dformat.replace('yyyy','%Y').replace('MM','%m').replace('dd','%d').replace('hh','%H').replace('mm','%M').replace('ss','%S')

import pdb; pdb.set_trace() # continue

## DB CONFIGURATION NAMES
dbconfig = utils.dbConfig(dbflav, dbpath)
colsens = dbconfig['INIDFLDNAME']
coldate = dbconfig['INDATETIMEFLDNAME']
colvalue = dbconfig['INVALUEFLDNAME']
colqual = dbconfig['INQUALITYFLDNAME']
colID = dbconfig['AUTOKEYID']

## DB DEMAND PARAMETERS
df_wd = utils.dbParam(dbflav, dbpath)

## LOOP
for index, row in df_wd.iterrows():
    ## Sensor currently forecasted
    print(row['ID'], row ['SENSORTABLE'], row['SENSORID'], row ['PREDICTTABLE'], row['PREDICTSTEP'], row['PREDICTDURATION'], row['HISTORYDURATION'], row['PREDICTTYPE'])

    ## Get parameters
    tabsensor = row['SENSORTABLE']
    sensor = row['SENSORID']
    tabdemand = row['PREDICTTABLE']
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
    utils.dbDelete(dbflav, dbpath, sensor, tabdemand, colsens)
    
    ## Get last historical date
    ldate = utils.lastDate(dbflav, dbpath, sensor, tabsensor, coldate, colsens)

    ## Compute water demand prediction dates
    start_train, stop_train, start_pred, stop_pred = utils.datesMgt(ldate, rstime, offtime, leadtime, histtime, dformat)

    ## Data read
    df_in = utils.dbRead(dbflav, dbpath, sensor, tabsensor, colID, coldate, colsens)

    ## Pre-process
    X_train, y_train, df_X, df_y, X_pred = utils.preProcess(df_in, coldate, colID, colsens, colqual, colvalue, rstime, offtime, start_train, stop_train, start_pred, stop_pred)

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
    df_out = utils.postProcess(fcst, df_X, sensor, coldate, colID, colsens, colqual, colvalue, start_pred, stop_pred)

    ## Write new forecast to sql
    utils.dbWrite(df_out, dbflav, dbpath, sensor, tabdemand)

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