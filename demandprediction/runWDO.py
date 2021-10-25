from demandprediction import demand
from demandprediction import utils
import numpy as np

## READ CONFIGURATION FILE
cfgpath, dbflav, dbpath, offtime, dformat = utils.iniConfig('config.ini')

## DB CONFIGURATION NAMES
dbconfig = utils.dbConfig(cfgpath)
colsens = dbconfig['INIDFLDNAME']
coldate = dbconfig['INDATETIMEFLDNAME']
colvalue = dbconfig['INVALUEFLDNAME']
colqual = dbconfig['INQUALITYFLDNAME']
colID = dbconfig['AUTOKEYID']

## DB DEMAND PARAMETERS
df_wd = utils.dbParam(cfgpath)

## LOOP
delist = []
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
    if sensor in delist:
        pass
    else:
        utils.dbDelete(dbflav, dbpath, sensor, tabdemand, colsens)
        delist.append(sensor)
    
    ## Get last historical date
    ldate, fdate = utils.histDate(dbflav, dbpath, sensor, tabsensor, coldate, colsens)

    ## Compute water demand prediction dates
    realtime = True # Real time mode True/False (True means Time Of Forecast is now, False means Time of Forecast is the Last date in the database)
    start_train, stop_train, start_pred, stop_pred, toftime = utils.datesMgt(ldate, fdate, rstime, offtime, leadtime, histtime, dformat, realtime)
    print('>>> Water demand prediction : TOF '+str(toftime))
    
    ## Data read
    df_in = utils.dbRead(dbflav, dbpath, sensor, tabsensor, colID, coldate, colsens)

    ## Pre-process
    X_train, y_train, df_X, df_y, X_pred = utils.preProcess(df_in, coldate, colID, colsens, colqual, colvalue, rstime, offtime, start_train, stop_train, start_pred, stop_pred)
    
    ## Demand computation
    if row['PREDICTTYPE'] == 'HOLTWINTERS':
        fcst = np.zeros((X_pred.shape[1],int(runs)))
        for r in range(runs):
            print('Run '+str(r))
            model = demand.HoltWinters(param1, param2, int(param3), X_pred.shape[1])
            # model = HoltWinters('add','add', 7*24, 24)
            model.learn(y_train)
            fcst[:,r] = model.predict()
        fcstm = np.mean(fcst,axis=1)
    elif row['PREDICTTYPE'] == 'MLPDYNAMIC':
        fcst = np.zeros((X_pred.shape[1],int(runs)))
        for r in range(runs):
            print('Run '+str(r))
            model = demand.MLPDynamic(param1, eval(param2), param3, param4, float(param5), int(param6), int(param7), float(param8))
            # model = demand.MLPDynamic('relu', (64,128,64), 'adam', 'adaptive', 0.01, 100000, 1000, 0.01)
            model.learn(X_train.T,y_train)
            fcst[:,r] = model.predict(X_pred.T)
        fcstm = np.mean(fcst,axis=1)
    
    ## Post-process and write
    for increm in range(fcst.shape[1]):
        if fcst.shape[1] > 1:
            # Multiple members
            if increm == 0:
                # Members' average
                df_out = utils.postProcess(fcstm, increm, df_X, sensor, coldate, colID, colsens, colqual, colvalue, start_pred, stop_pred, row['PREDICTTYPE'])
            else:
                df_out = utils.postProcess(fcst[:,increm], increm, df_X, sensor, coldate, colID, colsens, colqual, colvalue, start_pred, stop_pred, row['PREDICTTYPE'])
        else:
            # Single member
            df_out = utils.postProcess(fcst, increm, df_X, sensor, coldate, colID, colsens, colqual, colvalue, start_pred, stop_pred, row['PREDICTTYPE'])

        # Write new forecast to sql
        utils.dbWrite(df_out, dbflav, dbpath, sensor, tabdemand)