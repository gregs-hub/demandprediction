# demandprediction: Water Demand Prediction algorithms associated to Waternet Advisor
This package aims to provide algorithms for water demand prediction specifically tailored to DHI users and the water domain. It is simple to install and deploy operationally. Not for now accessible to everyone (private).

# Installation
1. Unzip package at the desired location
2. Install Python 3 (> 3.7)
3. Open a Command Prompt (Terminal) as administrator
4. Go to demandprediction-master unzipped directory
`cd /Path/to/unzipped/package/`
5. Upgrade pip (if needed)
`python -m pip install --upgrade pip`
6. Create a virtual environment named 'demand': \
`python -m venv demand`
7. Activate the virtual environment: \
`demand\Scripts\activate`
8. Install required external packages
`pip install -r requirements.txt`
9. Install local demandprediction package
<!-- pip install git+https://github.com/DHIgrse/demandprediction.git -->
`python setup.py install`
10. Edit config.ini file
11. Edit WDO database to add demand prediction sensors
11. Run the command line to launch a water demand rediction run:
`python demandprediction/runWDO.py`

# Definitions
Two algorithms are implemented for now
- Holt Winters (exponential smoothing)
Simple and light algorithm better suited when historical data are available (at least months). Uses statsmodels.tsa.holtwinters (ExponentialSmoothing)
- Artificial Neural Network (Multi Layer Perceptron)
More advanced algorithm with a large number of parameters (few are open in this version). Uses sklearn.neural_network (MLPRegressor)
