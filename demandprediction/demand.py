import pandas as pd
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.neural_network import MLPRegressor
import pyrenn as prn

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

class ANNDyn:
    # nmemb = 5
    # niter = 500
    # [X.shape[0],12,1]

    # def __init__(self, trend: str = 'mul', seasonal: str = 'mul', seasonal_periods: int = 24, horizon: int = 72):
    #     self.trend = trend
    #     self.seasonal = seasonal
    #     self.seasonal_periods = seasonal_periods
    #     self.horizon = horizon
    #     pass

    # def learn(self, trainY: pd.Series):
    #     # self.validate(data)
    #     self.model = ExponentialSmoothing(trainY, trend = self.trend, seasonal = self.seasonal, seasonal_periods = self.seasonal_periods).fit()
    #     return self

    # def predict(self):
    #     predY = self.model.forecast(self.horizon)
    #     return predY
    pass

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