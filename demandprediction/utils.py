import pandas as pd
from pandas.tseries.frequencies import to_offset
import numpy as np
import datetime
from datetime import datetime, timedelta
import sqlite3

def dbRead(dbflav, dbpath, sensor, tabsensor, colID, coldate, colsens):
    ### READ
    if dbflav == 'sqlite':
        # Read sqlite query results into a pandas DataFrame
        con = sqlite3.connect(dbpath)
        print("SELECT * FROM "+tabsensor+" WHERE "+colsens+"='"+sensor+"' ORDER BY "+coldate+" ASC, "+colID+" ASC", end='')
        try:
            df = pd.read_sql_query("SELECT * FROM "+tabsensor+" WHERE "+colsens+"='"+sensor+"' ORDER BY "+coldate+" ASC, "+colID+" ASC", con)
            con.execute('REINDEX '+tabsensor)
            con.commit()
            con.close()
            print('... done')
        except Exception as e:
            con.close()
            print(e)
            print('... fail')
        return df
    else:
        pass

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
            print(e)
            print('... fail')
    else:
        pass

def dbDelete(dbflav, dbpath, sensor, tabdemand, colsens):
    ### DELETE
    if dbflav == 'sqlite':
        # Delete sqlite rows based on a condition
        con = sqlite3.connect(dbpath)
        print("DELETE FROM "+tabdemand+" WHERE "+colsens+"=='"+sensor+"'", end='')
        try:
            con.execute("DELETE FROM "+tabdemand+" WHERE "+colsens+"=='"+sensor+"'")
            con.execute('REINDEX '+tabdemand)
            con.commit()
            con.close()
            print('... done')
        except Exception as e:
            con.close()
            print(e)
            print('... fail')
    else:
        pass

def dbConfig(dbflav, dbpath):
    ### GET CONFIG FROM DATABASE
    if dbflav == 'sqlite':
        # Read sqlite configuration
        lkey = ['INIDFLDNAME','INDATETIMEFLDNAME','INVALUEFLDNAME','INQUALITYFLDNAME','AUTOKEYID','DMIDFLDNAME','DMDATETIMEFLDNAME','DMVALUEFLDNAME','DMQUALITYFLDNAME']
        dkey = {}
        for l in lkey:
            con = sqlite3.connect(dbpath)
            print("SELECT KEYVALUE FROM WDO_SETTINGS WHERE KEYNAME == "+l, end='')
            try:
                tag = pd.read_sql_query("SELECT KEYVALUE FROM WDO_SETTINGS WHERE KEYNAME = '"+l+"'", con)
                dkey[l] = tag.KEYVALUE[0]
                con.commit()
                con.close()
                print('... done')
            except Exception as e:
                con.close()
                print(e)
                print('... fail')
        return dkey
    else:
        pass

def dbParam(dbflav, dbpath):
    ### GET WATER DEMAND PARAMETERS FROM DATABASE
    if dbflav == 'sqlite':
        # Read sqlite water demand parameters
        con = sqlite3.connect(dbpath)
        print("SELECT * FROM WDO_DEMANDPREDICTION ORDER BY ID ASC", end='')
        try:
            df = pd.read_sql_query("SELECT * FROM WDO_DEMANDPREDICTION ORDER BY ID ASC", con)
            con.commit()
            con.close()
            print('... done')
        except Exception as e:
            con.close()
            print(e)
            print('... fail')
        return df
    else:
        pass

def lastDate(dbflav, dbpath, sensor, tabsensor, coldate, colsens):
    ### READ LAST DATE FOR CURRENT SENSOR
    if dbflav == 'sqlite':
        # Read sqlite query results into a pandas DataFrame
        con = sqlite3.connect(dbpath)
        print("SELECT * FROM "+tabsensor+" WHERE "+colsens+"='"+sensor+"' ORDER BY "+coldate+" DESC LIMIT 1", end='')
        try:
            df = pd.read_sql_query("SELECT * FROM "+tabsensor+" WHERE "+colsens+"='"+sensor+"' ORDER BY "+coldate+" DESC LIMIT 1", con)
            ldate = df[coldate][0]
            con.commit()
            con.close()
            print('... done')
        except Exception as e:
            con.close()
            print(e)
            print('... fail')
        return ldate
    else:
        pass

def datesMgt(ldate, rstime, offtime, leadtime, histtime, dformat):
    ### DATES MANAGEMENT
    def _roundto(rst, date):
        # Round to nearest (up : ceil)
        new_minute = (date.minute // rst + (1)) * rst # for direction down (floor) : new_minute = (date.minute // rst + (0)) * rst
        return date + timedelta(minutes=new_minute - date.minute, seconds=-date.second, microseconds=-date.microsecond)
    toftime = _roundto(rstime, datetime.strptime(ldate, dformat)) # toftime = roundto(rstime, datetime.now().replace(microsecond=0, second=0, minute=0))
    lstdate = datetime.strptime(ldate, dformat)
    start_train = datetime.strftime(_roundto(rstime, lstdate+timedelta(minutes=-histtime)),dformat)
    stop_train = datetime.strftime(toftime,dformat)
    start_pred = stop_train
    stop_pred = datetime.strftime(_roundto(rstime, lstdate+timedelta(minutes=leadtime+rstime)),dformat)
    return start_train, stop_train, start_pred, stop_pred

def preProcess(df, coldate, colID, colsens, colqual, colvalue, rstime, offtime, start_train, stop_train, start_pred, stop_pred):
    # Define coldate as datetime index
    df = df.set_index(pd.DatetimeIndex(df[coldate])).drop(columns=coldate)
    # Drop unused columns : coldID, colsens, colqual
    df.drop(columns=[colID,colsens,colqual], inplace=True)
    # Data filtering ### TODO
    df_mod = df
    df_mod.replace(0,np.nan,inplace=True)
    df_mod[df_mod[colvalue]>df_mod[colvalue].quantile(0.999)] = np.nan
    df_mod[df_mod[colvalue]<df_mod[colvalue].quantile(0.001)] = np.nan
    df_mod.dropna(inplace=True)
    # Data resampling ### TODO
    df_rs = df.resample(str(rstime)+'min').mean()
    df_rs.index = df_rs.index+to_offset(offtime)
    df_rs.interpolate()
    df_rs = df_rs.reindex(pd.date_range(start = start_train, end = stop_pred, freq=str(rstime)+'min'),fill_value=0.0)
    # TODO Cases where available data don't cover asked historical dates
    # Methodology using DOY/WD/HR, no data scaling ### TODO
    df_rs['DOY'] = df_rs.index.dayofyear.astype(float)
    df_rs['WD'] = df_rs.index.dayofweek.astype(float)+1
    df_rs['HR'] = df_rs.index.hour.astype(float)
    # df_rs['MN'] = df_rs.index.minute.astype(float)
    # Data formatting for water demand algorithms
    df_X = df_rs.drop(colvalue, axis=1)
    df_y = df_rs[colvalue]
    X = np.transpose(df_X.values)
    y = df_y.values
    y = y.reshape(1,len(y))
    # Dates management
    trainFrom = np.where(df_X.index == start_train)[0][0]
    trainTo = np.where(df_X.index == stop_train)[0][0]+1
    predFrom = np.where(df_X.index == start_pred)[0][0]
    predTo = np.where(df_X.index == stop_pred)[0][0]+3
    # Data selection
    X_train, X_pred, y_train, y_pred = X[trainFrom:trainTo], X[predFrom:predTo], y[trainFrom:trainTo], y[predFrom:predTo]
    X_train, y_train = X[:,trainFrom:trainTo], y[:,trainFrom:trainTo]
    X_pred, y_pred = X[:,predFrom:predTo], y[:,predFrom:predTo]
    y_train = np.squeeze(y_train, axis=0)
    return X_train, y_train, df_X, df_y, X_pred

def postProcess(fcst, df_X, sensor, coldate, colID, colsens, colqual, colvalue, start_pred, stop_pred):
    steps = len(df_X.loc[start_pred:stop_pred].index)
    # Code int(9) is used to warn it's a predicted value
    # TODO post-process anomalies, for now only > 0
    fcst[fcst < 0] = 0
    df_out = pd.DataFrame([[sensor]*steps, fcst, df_X.loc[start_pred:stop_pred].index, [9]*steps],[colsens, colvalue, coldate, colqual]).transpose()
    print(df_out)
    return df_out


## IN PROGRESS

# df[colqual] = int(9)
# dbWrite(df, dbflav, dbpath, sensor, tabsensor, colID, coldate, colsens, colqual)

# dbDelete(dbflav, dbpath, sensor, tabsensor, colsens, colqual)

# # UdlRead('./dev/toulon/Online/Data/rtc-in.udl')
# def UdlRead(filepath):
#     udl = open(filepath)
#     connstring = udl.readlines()[4]
#     connstring = connstring.replace('\x00','')
#     connstring = connstring.split(';')
#     return connstring

# udl = open('./dev/rtc-in.udl')
# constr = udl.readlines()[4]
# constr = constr.replace('\x00','')
# constr = constr.replace('\n','')
# # connstring = connstring.split(';')


# import adodbapi
# databasename = 'D:\directorypath\DatabaseName.accdb'  
# constr = 'Provider=Microsoft.ACE.OLEDB.12.0;Data Source=%s'  % databasename 
# db = adodbapi.connect(constr)


# db = adodbapi.connect('Provider=MSDASQL.1;Persist Security Info=False;Extended Properties="DSN=SQLite3 Datasource;Database=C:\\dhi\\demand\\SCADA.sqlite;StepAPI=0;SyncPragma=NORMAL;NoTXN=0;Timeout=100000;ShortNames=0;LongNames=0;NoCreat=0;NoWCHAR=0;FKSupport=0;JournalMode=;OEMCP=0;LoadExt=;BigInt=0;JDConv=0;')


# import re
# dbconf = list(filter(re.compile("Database=*").match, connstring))
# dbflav = list(filter(re.compile("*DSN=*").match, connstring))


# ; Everything after this line is an OLE DB initstring
# Provider=PGNP.1;Password=12345;Persist Security Info=True;User ID=postgres;Initial Catalog=postgres;Data Source=localhost;Extended Properties=""


# import pyodbc
# databasename = 'D:\otherdirectorypath\OtherDatabaseName.mdb'
# constr = 'DRIVER={Microsoft Access Driver (*.mdb)};DBQ=%s;'  % databasename
# db = pyodbc.connect(constr)



# import adodbapi
# myhost = r".\SQLEXPRESS"
# mydatabase = "Northwind"
# myuser = "guest"
# mypassword = "12345678"
# connStr = """Provider=SQLOLEDB.1; User ID=%s; Password=%s;
# Initial Catalog=%s;Data Source= %s"""
# myConnStr = connStr % (myuser, mypassword, mydatabase, myhost)
# myConn = adodbapi.connect(myConnStr)

# connStr = """Provider=SQLOLEDB.1; User ID=%(user)s;
# Password=%(password)s;"Initial Catalog=%(database)s;
# Data Source= %(host)s"""
# myConn=adodbapi.connect(connStr, myuser, mypassword, myhost, mydatabase)


# myConn = adodbapi.connect(connStr, user=myuser, password=mypassword,
# host=myhost, database=mydatabase)



