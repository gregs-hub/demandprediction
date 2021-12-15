from demandprediction import demand
from demandprediction import utils
import numpy as np

## READ CONFIGURATION FILE
[cfgpath, dbflav, dbpath, offtime, dformat, tsettings, tdemand,
 namesect, nameid, namedate, nameval, namequal, namekey, iddem, tsensor,
 idsensor, toutput, steppred, durpred, durhist, typpred, nbruns,
 nparam1, nparam2, nparam3, nparam4, nparam5, nparam6, nparam7, nparam8] = utils.iniConfig('config.ini')

## DB CONFIGURATION NAMES
dbconfig = utils.dbConfig(cfgpath, tsettings, namesect, nameid, namedate, nameval, namequal, namekey)
colsens = dbconfig[nameid]
coldate = dbconfig[namedate]
colvalue = dbconfig[nameval]
colqual = dbconfig[namequal]
colID = dbconfig[namekey]

## DB DEMAND PARAMETERS
df_wd = utils.dbParam(cfgpath, tdemand)

## LOOP
delist = []
for index, row in df_wd.iterrows():
    ## Sensor currently forecasted
    print(row[iddem], row [tsensor], row[idsensor], row [toutput], row[toutput], row[durpred], row[durhist], row[typpred])

    ## Get parameters
    tabsensor = row[tsensor]
    sensor = row[idsensor]
    tabdemand = row[toutput]
    rstime = row[steppred]
    leadtime = row[durpred]
    histtime = row[durhist]
    runs = row[nbruns]
    param1 = row[nparam1]
    param2 = row[nparam2]
    param3 = row[nparam3]
    param4 = row[nparam4]
    param5 = row[nparam5]
    param6 = row[nparam6]
    param7 = row[nparam7]
    param8 = row[nparam8]

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
    if row[typpred] == 'HOLTWINTERS':
        fcst = np.zeros((X_pred.shape[1],int(runs)))
        for r in range(runs):
            print('Run '+str(r))
            model = demand.HoltWinters(param1, param2, int(param3), X_pred.shape[1])
            # model = HoltWinters('add','add', 7*24, 24)
            model.learn(y_train)
            fcst[:,r] = model.predict()
        fcstm = np.mean(fcst,axis=1)
    elif row[typpred] == 'MLPDYNAMIC':
        fcst = np.zeros((X_pred.shape[1],int(runs)))
        for r in range(runs):
            print('Run '+str(r))
            model = demand.MLPDynamic(param1, eval(param2), param3, param4, float(param5), int(param6), int(param7), float(param8))
            # model = demand.MLPDynamic('relu', (64,128,64), 'adam', 'adaptive', 0.01, 100000, 1000, 0.01)
            model.learn(X_train.T,y_train)
            fcst[:,r] = model.predict(X_pred.T)
        fcstm = np.mean(fcst,axis=1)
    
    ## Post-process and write
    for increm in range(fcst.shape[1]+1):
        if fcst.shape[1] > 1:
            # Multiple members
            if increm == 0:
                # Members' average
                df_out = utils.postProcess(fcstm, increm, df_X, sensor, coldate, colID, colsens, colqual, colvalue, start_pred, stop_pred, row[typpred])
            else:
                df_out = utils.postProcess(fcst[:,increm-1], increm, df_X, sensor, coldate, colID, colsens, colqual, colvalue, start_pred, stop_pred, row[typpred])
        else:
            # Single member
            df_out = utils.postProcess(fcst, increm, df_X, sensor, coldate, colID, colsens, colqual, colvalue, start_pred, stop_pred, row[typpred])

        # Write new forecast to sql
        utils.dbWrite(df_out, dbflav, dbpath, sensor, tabdemand)