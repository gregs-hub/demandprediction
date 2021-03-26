import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.neural_network import MLPRegressor

class HoltWinters:
    def __init__(self, trend: str = 'mul', seasonal: str = 'mul', seasonal_periods: int = 24, horizon: int = 72):
        self.trend = trend
        self.seasonal = seasonal
        self.seasonal_periods = seasonal_periods
        self.horizon = horizon
        pass

    def learn(self, data: pd.Series):
        # self.validate(data)
        self.model = ExponentialSmoothing(data, trend = self.trend, seasonal = self.seasonal, seasonal_periods = self.seasonal_periods).fit()
        print(self.model.summary())
        return self

    def predict(self):
        fcst = self.model.forecast(self.horizon)
        return fcst

    # def validate(self, data):
    #     if not isinstance(data, pd.Series):
    #         raise WrongInputDataType()

class MLPDynamic:
    def __init__(self, activation: str = 'relu', hidden_layer_sizes: tuple = (100,), solver: str = 'sgd', learning_rate: str = 'adaptive', learning_rate_init: float = 0.001, max_iter: int = 1000, n_iter_no_change: int = 10, tol: float = 0.0000001):
        self.activation = activation
        # alpha= 0.0001, 
        # batch_size= 'auto', 
        # beta_1= 0.9, 
        # beta_2= 0.999, 
        # early_stopping= False, 
        # epsilon= 1e-08, 
        self.hidden_layer_sizes = hidden_layer_sizes
        self.solver = solver
        self.learning_rate = learning_rate
        self.learning_rate_init = learning_rate_init
        self.max_iter = max_iter
        # momentum= 0.9, 
        self.n_iter_no_change = n_iter_no_change
        # nesterovs_momentum= True, 
        # power_t= 0.5, 
        # random_state= None, 
        # shuffle= True, 
        self.tol = tol
        # validation_fraction= 0.1, 
        # verbose= True, 
        # warm_start= False
        pass

    def learn(self, trainX: pd.Series, trainY: pd.Series):
        # self.validate(data)
        self.model = MLPRegressor(activation=self.activation, hidden_layer_sizes=self.hidden_layer_sizes, learning_rate=self.learning_rate, learning_rate_init=self.learning_rate_init, max_iter=self.max_iter, n_iter_no_change=self.n_iter_no_change, solver=self.solver, tol=self.tol, verbose = True).fit(trainX, trainY)
        return self

    def predict(self, predX: pd.Series):
        # self.validate(data)
        predY = self.model.predict(predX)
        return predY

