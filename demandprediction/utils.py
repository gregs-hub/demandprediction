import configparser
import pandas as pd
from pandas.tseries.frequencies import to_offset
import numpy as np
import datetime
from datetime import datetime, timedelta
import sqlite3
import psycopg2
import psycopg2.extras as extras

def iniConfig(configFile):
    ## READ CONFIGURATION FILE
    iniconfig = configparser.ConfigParser()
    iniconfig.read(configFile)
    cfgpath = iniconfig['DEMAND']['configPath']
    dbflav = iniconfig['DEMAND']['databaseFlavour']
    dbpath = iniconfig['DEMAND']['databasePath']
    dbpath = dbpath.replace("'","")
    offtime = iniconfig['DEMAND']['resamplingOffset']
    dformat = iniconfig['DEMAND']['dateFormat']
    dformat = dformat.replace('yyyy','%Y').replace('MM','%m').replace('dd','%d').replace('hh','%H').replace('mm','%M').replace('ss','%S')
    tsettings = iniconfig['DEMAND']['settingsTable']
    tdemand = iniconfig['DEMAND']['demandTable']
    namesect = iniconfig['DEMAND']['sectionName']
    nameid = iniconfig['DEMAND']['idFldName']
    namedate = iniconfig['DEMAND']['dateFldName']
    nameval = iniconfig['DEMAND']['valFldName']
    namequal = iniconfig['DEMAND']['qualFldName']
    namekey = iniconfig['DEMAND']['keyName']
    iddem = iniconfig['DEMAND']['demId']
    tsensor = iniconfig['DEMAND']['sensorTable']
    idsensor = iniconfig['DEMAND']['sensorId']
    toutput = iniconfig['DEMAND']['outputTable']
    steppred = iniconfig['DEMAND']['predStep']
    durpred = iniconfig['DEMAND']['predDur']
    durhist = iniconfig['DEMAND']['histDur']
    typpred = iniconfig['DEMAND']['predType']
    nbruns = iniconfig['DEMAND']['runsNb']
    nparam1 = iniconfig['DEMAND']['param1']
    nparam2 = iniconfig['DEMAND']['param2']
    nparam3 = iniconfig['DEMAND']['param3']
    nparam4 = iniconfig['DEMAND']['param4']
    nparam5 = iniconfig['DEMAND']['param5']
    nparam6 = iniconfig['DEMAND']['param6']
    nparam7 = iniconfig['DEMAND']['param7']
    nparam8 = iniconfig['DEMAND']['param8']
    return cfgpath, dbflav, dbpath, offtime, dformat, tsettings, tdemand, namesect, nameid, namedate, nameval, namequal, namekey, iddem, tsensor, idsensor, toutput, steppred, durpred, durhist, typpred, nbruns, nparam1, nparam2, nparam3, nparam4, nparam5, nparam6, nparam7, nparam8

def dbRead(dbflav, dbpath, sensor, tabsensor, colID, coldate, colsens):
    ### READ
    if dbflav == 'sqlite':
        # Read sqlite query results into a pandas DataFrame
        con = sqlite3.connect(dbpath)
    elif dbflav == 'postgreSQL':
        # Read postgreSQL query results into a pandas DataFrame
        con = psycopg2.connect(dbpath)
    print("SELECT * FROM "+tabsensor+" WHERE "+colsens+"='"+sensor+"' ORDER BY "+coldate+" ASC, "+colID+" ASC", end='')
    try:
        df = pd.read_sql_query("SELECT * FROM "+tabsensor+" WHERE "+colsens+"='"+sensor+"' ORDER BY "+coldate+" ASC, "+colID+" ASC", con)
        con.close()
        print('... done')
    except Exception as e:
        con.close()
        print(' '+str(e))
        print('... fail')
    return df

def dbWrite(df, dbflav, dbpath, sensor, tabdemand):
    ### WRITE
    if dbflav == 'sqlite':
        # Write pandas to a sqlite table
        con = sqlite3.connect(dbpath)
        print('WRITE TO DATABASE FOR '+sensor, end='')
        try:
            df.to_sql(tabdemand, con, if_exists="append", index=False)
            con.execute('REINDEX '+tabdemand)
            con.commit()
            con.close()
            print('... done')
        except Exception as e:
            con.close()
            print(' '+str(e))
            print('... fail')
    elif dbflav == 'postgreSQL':
        # Write pandas to a postgreSQL table
        con = psycopg2.connect(dbpath)
        print('WRITE TO DATABASE FOR '+sensor, end='')
        try:
            df.drop(columns='',inplace=True)
        except Exception:
            pass
        try:
            # Create a list of tuples from the dataframe values
            tuples = []
            for x in df.to_numpy():
                try:
                    x[1] = x[1][0]
                except Exception:
                    pass
                tuples.append(tuple(x))
            # Comma-separated dataframe columns
            cols = ','.join(list(df.columns))
            # SQL quert to execute
            query  = "INSERT INTO %s(%s) VALUES %%s" % (tabdemand, cols)
            cur = con.cursor()
            try:
                extras.execute_values(cur, query, tuples)
                con.commit()
            except Exception as e:
                con.rollback()
                print(' '+str(e))
                print('... fail')
            con.close()
        except Exception as e:
            con.close()
            print(' '+str(e))
            print('... fail')

def dbDelete(dbflav, dbpath, sensor, tabdemand, colsens):
    ### DELETE
    if dbflav == 'sqlite':
        # Delete sqlite rows based on a condition
        con = sqlite3.connect(dbpath)
        print("DELETE FROM "+tabdemand+" WHERE "+colsens+"='"+sensor+"'", end='')
        try:
            con.execute("DELETE FROM "+tabdemand+" WHERE "+colsens+"='"+sensor+"'")
            con.execute('REINDEX '+tabdemand)
            con.commit()
            con.close()
            print('... done')
        except Exception as e:
            con.close()
            print(' '+str(e))
            print('... fail')
    elif dbflav == 'postgreSQL':
        # Delete postgreSQL rows based on a condition
        con = psycopg2.connect(dbpath)
        cur = con.cursor()
        print("DELETE FROM "+tabdemand+" WHERE "+colsens+"='"+sensor+"'", end='')
        try:
            cur.execute("DELETE FROM "+tabdemand+" WHERE "+colsens+"='"+sensor+"'")
            con.commit()
            con.close()
            print('... done')
        except Exception as e:
            con.close()
            print(' '+str(e))
            print('... fail')

def dbConfig(cfgpath, tsettings, namesect, nameid, namedate, nameval, namequal, namekey):
    ### GET CONFIG FROM DATABASE, ALWAYS IN SQLITE
    # Read sqlite configuration
    lkey = [nameid,namedate,nameval,namequal,namekey]
    dkey = {}
    for l in lkey:
        con = sqlite3.connect(cfgpath)
        print("SELECT keyvalue FROM "+tsettings+" WHERE keyname == "+l+" AND section == "+namesect, end='')
        try:
            tag = pd.read_sql_query("SELECT keyvalue FROM "+tsettings+" WHERE keyname = '"+l+"' AND section = '"+namesect+"'", con)
            dkey[l] = tag.keyvalue[0]
            con.commit()
            con.close()
            print('... done')
        except Exception as e:
            con.close()
            print(' '+str(e))
            print('... fail')
    return dkey

def dbParam(cfgpath, tdemand):
    ### GET WATER DEMAND PARAMETERS FROM DATABASE, ALWAYS IN SQLITE
    # Read sqlite demand parameters
    con = sqlite3.connect(cfgpath)
    print("SELECT * FROM "+tdemand+" ORDER BY ID ASC", end='')
    try:
        df = pd.read_sql_query("SELECT * FROM "+tdemand+" ORDER BY ID ASC", con)
        con.commit()
        con.close()
        print('... done')
    except Exception as e:
        con.close()
        print(' '+str(e))
        print('... fail')
    return df

def histDate(dbflav, dbpath, sensor, tabsensor, coldate, colsens):
    ### READ LAST DATE FOR CURRENT SENSOR
    if dbflav == 'sqlite':
        # Read sqlite query results into a pandas DataFrame
        con = sqlite3.connect(dbpath)
    elif dbflav == 'postgreSQL':
        # Read postgreSQL query results into a pandas DataFrame
        con = psycopg2.connect(dbpath)
    print("SELECT * FROM "+tabsensor+" WHERE "+colsens+"='"+sensor+"' ORDER BY "+coldate+" DESC", end='')
    try:
        df = pd.read_sql_query("SELECT * FROM "+tabsensor+" WHERE "+colsens+"='"+sensor+"' ORDER BY "+coldate+" DESC", con)
        ldate = str(df[coldate][0]) # Last available historical date
        fdate = str(df[coldate][len(df)-1]) # First available historical date
        con.commit()
        con.close()
        print('... done')
    except Exception as e:
        con.close()
        print(' '+str(e))
        print('... fail')
    return ldate, fdate

def datesMgt(ldate, fdate, rstime, offtime, leadtime, histtime, dformat, realtime):
    ### DATES MANAGEMENT
    def _roundto(rst, date, direct):
        # Round to nearest (up : ceil)
        new_minute = (date.minute // rst + (direct)) * rst # direct = direction => floor is 0, ceil is 1
        return date + timedelta(minutes=new_minute - date.minute, seconds=-date.second, microseconds=-date.microsecond)
    if realtime:
        toftime = _roundto(rstime, datetime.now().replace(microsecond=0, second=0),0)
    else:
        toftime = _roundto(rstime, datetime.strptime(ldate, dformat).replace(microsecond=0, second=0),0)
    lstdate = datetime.strptime(ldate, dformat)
    fstdate = datetime.strptime(fdate, dformat)
    start_train = datetime.strftime(max(_roundto(rstime, lstdate+timedelta(minutes=-histtime),1), _roundto(rstime, fstdate,1)),dformat)
    stop_train = datetime.strftime(_roundto(rstime, datetime.strptime(ldate, dformat),1),dformat)
    start_pred = stop_train
    stop_pred = datetime.strftime(_roundto(rstime, toftime+timedelta(minutes=leadtime+rstime),0),dformat)
    return start_train, stop_train, start_pred, stop_pred, toftime

def preProcess(df, coldate, colID, colsens, colqual, colvalue, rstime, offtime, start_train, stop_train, start_pred, stop_pred):
    ### TODO (future: add anomaly detection filter)
    ### TODO (future: data normalization, data scaling)
    # Define coldate as datetime index
    df = df.set_index(pd.DatetimeIndex(df[coldate])).drop(columns=coldate)
    # Drop unused columns : coldID, colsens, colqual
    try:
        df.drop(columns=[colID,colsens,colqual], inplace=True)
    except Exception: # in case codequal is empty
        df.drop(columns=[colID,colsens], inplace=True)
    # Data filtering
    df_mod = df.copy(deep=True)
    df_mod.replace(0,np.nan,inplace=True)
    df_mod[df_mod[colvalue]>df_mod[colvalue].quantile(0.999)] = np.nan
    df_mod[df_mod[colvalue]<df_mod[colvalue].quantile(0.001)] = np.nan
    df_mod.dropna(inplace=True)
    # Data resampling
    df_rs = df_mod.resample(str(min(60,rstime))+'min').mean() # If timestep is higher than 60 min, we use 60 min for resampling before reindexing to asked rstime
    df_rs.index = df_rs.index+to_offset(offtime)
    # Gap filling (fill hourly missing values with WD/HR couples' observed averages during the training period)
    if rstime <= 60 and df_rs[df_rs[colvalue].isnull()].sum().sum() > 0:
        for index, row in df_rs.iterrows():
            if str(row[colvalue])=='nan':
                df_rs.at[index,colvalue] = df_rs.groupby([df_rs.WD,df_rs.HR]).get_group((row['WD'], row['HR'])).mean()[colvalue]
    # Data interpolation (in case still missing values) and reindexing
    df_rs.interpolate(inplace=True)
    df_rs = df_rs.reindex(pd.date_range(start = start_train, end = stop_pred, freq=str(rstime)+'min'),fill_value=0.0)
    # Feature engineering (methodology using DOY/WD/HR)
    df_rs['DOY'] = df_rs.index.dayofyear.astype(float)
    df_rs['WD'] = df_rs.index.dayofweek.astype(float)+1
    df_rs['HR'] = df_rs.index.hour.astype(float)
    # Data formatting for water demand algorithms
    df_X = df_rs.drop(colvalue, axis=1)
    df_y = df_rs[colvalue]
    X = np.transpose(df_X.values)
    y = df_y.values
    y = y.reshape(1,len(y))
    # Dates management
    trainFrom = np.where(df_X.index == start_train)[0][0]
    trainTo = np.where(df_X.index == stop_train)[0][0]#+1
    predFrom = np.where(df_X.index == start_pred)[0][0]
    try:
        predTo = np.where(df_X.index == stop_pred)[0][0]+3
    except Exception:
        predTo = df_X.shape[0]
    # Data selection
    X_train, X_pred, y_train, y_pred = X[trainFrom:trainTo], X[predFrom:predTo], y[trainFrom:trainTo], y[predFrom:predTo]
    X_train, y_train = X[:,trainFrom:trainTo], y[:,trainFrom:trainTo]
    X_pred, y_pred = X[:,predFrom:predTo], y[:,predFrom:predTo]
    y_train = np.squeeze(y_train, axis=0)
    return X_train, y_train, df_X, df_y, X_pred

def postProcess(fcst, increm, df_X, sensor, coldate, colID, colsens, colqual, colvalue, start_pred, stop_pred, predtype):
    steps = len(df_X.loc[start_pred:stop_pred].index)
    # Code int(100) is used to warn it's a HOLTWINTERS predicted value, int(200) is used to warn it's a MLPDYNAMIC predicted value
    # TODO post-process anomalies, for now only > 0
    fcst[fcst < 0] = 0
    try :
        if predtype == 'HOLTWINTERS':
            df_out = pd.DataFrame([[sensor]*steps, fcst, df_X.loc[start_pred:stop_pred].index, [100+increm]*steps],[colsens, colvalue, coldate, colqual]).transpose()
        elif predtype == 'MLPDYNAMIC':
            df_out = pd.DataFrame([[sensor]*steps, fcst, df_X.loc[start_pred:stop_pred].index, [200+increm]*steps],[colsens, colvalue, coldate, colqual]).transpose()
    except Exception: # in case codequal is empty
        df_out = pd.DataFrame([[sensor]*steps, fcst, df_X.loc[start_pred:stop_pred].index],[colsens, colvalue, coldate]).transpose()
    print(df_out)
    return df_out
