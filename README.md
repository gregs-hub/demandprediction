# demandprediction: Water Demand Prediction algorithms associated to Waternet Advisor
This package aims to provide algorithms for water demand prediction specifically tailored to DHI users and the water domain. It is simple to install and deploy operationally. Not for now accessible to everyone (private).

# Installation
- Install Python 3 (> 3.7)
- Open a Command Prompt (Terminal) as adiministrator
- Go to demandprediction-master unzipped directory (cd /Path/)
- Install demandprediction package to 
<!-- pip install git+https://github.com/DHIgrse/demandprediction.git -->

# Definitions
Two algorithms are implemented for now

- Holt Winters (exponential smoothing)
Simple and light algorithm better suited when historical data are available (at least months). Uses statsmodels.tsa.holtwinters (ExponentialSmoothing)

- Artificial Neural Network (Multi Layer Perceptron)
More advanced algorithm with a large number of parameters (few are open in this version). Uses sklearn.neural_network (MLPRegressor)
