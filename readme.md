# Housing Data Engineering Pipeline 

## Description of problem
The housing market in Singapore is facing several challenges, such as the fragmentation of information across different actors and sources, and the volatility of the market due to external factors such as COVID-19, inflation, and interest rates. 

A combination of these challenges makes it difficult for potential buyers, sellers, and investors to make swift and informed decisions about their property transactions.

As such, there is a need for a data pipeline solution that can gather housing information from various sources and analyse it to provide a comprehensive and timely overview of the state of the market in Singapore. 

## Usefulness and importance of gathering and analysing housing data 
Gathering and analysing housing data from various sources are important and useful because it can help to reduce information asymmetry and increase transparency in the property market, increasing confidence in decision-making.  

Furthermore, it can help to identify emerging trends and patterns in various segments of the housing market, such as price movements, supply and demand dynamics, and rental yields. This has widespread benefits for various actors in the market, helping them make data-driven decisions. 

## Application 1: PowerBI Dashboard


## Application 2: Modelling Housing Prices 

We attempt to model housing prices for the following:

1. Monthly median resale flat prices
2. Monthly median private property prices 
3. Monthly median flat rental prices
4. Monthly median private property rental prices

- A full DAG run on Airflow (ETL) was carried out to prepare the data for modelling
- The data was then modelled outside of Airflow using:
    - SARIMA (Seasonal AutoRegressive Integrated Moving Average)
    - SARIMAX (Seasonal AutoRegressive Integrated Moving Average with Exogenous Variables)
    - LSTM (Long Short-Term Memory)
- An example process is outlined in the [notebook](modelling/sarima.ipynb) where resale flat prices are modelled.
- The best models for each district are chosen based on the lowest RMSE (Root Mean Squared Error) scores.
- The models are then saved as pickle files for future use.
- The models are then imported for inference in the [Housing Prices Visualisation Dashboard Notebook](housing_prices_viz.ipynb). An interactive version of the notebook can be accessed [here](https://nbviewer.org/github/ivankqw/housing-data-engineering/blob/main/housing_prices_viz.ipynb?flush_cache=True) 