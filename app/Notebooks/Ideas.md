#### ETL Ideas  
- Take plant level data and divide net generation by net fuel consumption
- Make consumption fuel ratios (%).  This might be unnecessary as most plants might use mostly only one fuel source for generation, and the grain of the table may be lower than this aggregation.
- Get CO2 emissions conversions from CO2 emission set
- Combine data to give the following by plant
  - State
  - Net Generation
  - Net Fuel Consumption
  - Generation / consumptions
  - Fuel ratios
  - co2 emission by fuel
- Grain is probably going to be one row per plant per fuel type per month

#### Analyses
- Cluster plants based on available attributes
- Time series analysis via python and statsmodels (using pandas_udf https://medium.com/walmartglobaltech/multi-time-series-forecasting-in-spark-cc42be812393)
- Overall data manipulation and analysis on the resulting granular representation of U.S. electric generation and fuel consumption.

#### Notebooks
- ETL
- K-means grouping plants in order to make grouped ts analysis easier.  Possibly create a new table with inputs for analysis as well as useful time series statistics (month low, month high, kpss output?, acf?, pacf?) to be used.
- Implementation of methods and prediction on just one plant
- Implementation on a k-group of plants and analysis
- Implementaiton on all plants and provide predictions
