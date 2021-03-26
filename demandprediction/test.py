# pip install -U "C:/Users/grse/OneDrive - DHI/Python/demandprediction/demandprediction-main"
# import pdb; pdb.set_trace() # continue

from demandprediction import demand
from demandprediction import utils
import numpy as np

## INFOS AND TODO
# for now only sqlite, with database path manually provided
# for now, resampling time (rstime) is both computational time step (water demand algo) and output predictions time step
# offset time (for resampling) can b eprovided manually
# date format is manually provided
# two algorithms with some parameters editable : Holt Winters prediction (exponential smoothing) and Multi Layer Perceptron Regressor
# supposed to work with < 1h timesteps, but not tested intensively

## READ CONFIGURATION FILE
dbflav, dbpath, offtime, dformat = utils.iniConfig('config.ini')

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


##### UTILS
# import pyodbc


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
# # constr = constr.split(';')
# cnxn = pyodbc.connect(constr)

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



##### DEMAND
# import pyrenn as prn

# class ANNDyn:
#     # nmemb = 5
#     # niter = 500
#     # [X.shape[0],12,1]

#     # def __init__(self, trend: str = 'mul', seasonal: str = 'mul', seasonal_periods: int = 24, horizon: int = 72):
#     #     self.trend = trend
#     #     self.seasonal = seasonal
#     #     self.seasonal_periods = seasonal_periods
#     #     self.horizon = horizon
#     #     pass

#     # def learn(self, trainY: pd.Series):
#     #     # self.validate(data)
#     #     self.model = ExponentialSmoothing(trainY, trend = self.trend, seasonal = self.seasonal, seasonal_periods = self.seasonal_periods).fit()
#     #     return self

#     # def predict(self):
#     #     predY = self.model.forecast(self.horizon)
#     #     return predY
#     pass

# from demandprediction.demand import HoltWinters
# model = HoltWinters('mul','add', 24, 72)
# model.learn(y_train)
# fcst = model.predict()

# from demandprediction.demand import MLPDynamic
# model = MLPDynamic('relu', (64,128,64), 'adam', 'adaptive', 0.01, 100000, 1000, 1)
# model.learn(X_train.T,y_train)
# fcst = model.predict(X_test.T)




# class WrongInputDataType(Exception):
#     def __init__(self, message="Input data must be a pandas Series !"):
#         self.message = message
#         super().__init__(self.message)

# class Rectangle:
#     def __init__(self, length, width):
#         self.length = length
#         self.width = width

#     def area(self):
#         return self.length * self.width

#     def perimeter(self):
#         return 2 * self.length + 2 * self.width

# class Square:
#     def __init__(self, length):
#         self.length = length

#     def area(self):
#         return self.length * self.length

#     def perimeter(self):
#         return 4 * self.length

# >>> square = Square(4)
# >>> square.area()
# 16
# >>> rectangle = Rectangle(2,4)
# >>> rectangle.area()




# class Rectangle:
#     def __init__(self, length, width):
#         self.length = length
#         self.width = width

#     def area(self):
#         return self.length * self.width

#     def perimeter(self):
#         return 2 * self.length + 2 * self.width

# # Here we declare that the Square class inherits from the Rectangle class
# class Square(Rectangle):
#     def __init__(self, length):
#         super().__init__(length, length)
# Here, you’ve used super() to call the __init__() of the Rectangle class, allowing you to use it in the Square class without repeating code. Below, the core functionality remains after making changes:

# >>> square = Square(4)
# >>> square.area()
# 16