# Housing Data Engineering Pipeline 

![image](https://user-images.githubusercontent.com/66941932/232530471-451cba3b-a0e0-4fa6-99bd-b9a85276f689.png)

## Description of problem
The housing market in Singapore is facing several challenges, such as the fragmentation of information across different actors and sources, and the volatility of the market due to external factors such as COVID-19, inflation, and interest rates. 

A combination of these challenges makes it difficult for potential buyers, sellers, and investors to make swift and informed decisions about their property transactions.

As such, there is a need for a data pipeline solution that can gather housing information from various sources and analyse it to provide a comprehensive and timely overview of the state of the market in Singapore. 

## Usefulness and importance of gathering and analysing housing data 
Gathering and analysing housing data from various sources are important and useful because it can help to reduce information asymmetry and increase transparency in the property market, increasing confidence in decision-making.  

Furthermore, it can help to identify emerging trends and patterns in various segments of the housing market, such as price movements, supply and demand dynamics, and rental yields. This has widespread benefits for various actors in the market, helping them make data-driven decisions. 

## Application 1: PowerBI Dashboard
The dashboards aim to ascertain the following hypotheses:

-  Property prices are correlated to the property’s district
    - Certain areas in Singapore are known to be more affluent than others, due to their factors such as population demographics and the proximity to the central business district. For example, neighbourhoods such as Marina Bay and Orchard could have higher demand than neighbourhoods such as Jurong and Woodlands, due to their closer proximity to the central business district. This implies a difference in prices. This hypothesis can be confirmed through the creation of maps, categorical bar plots, and histograms. 

- Popularity of districts changes over time
    - The team believes that the popularity of districts changes over time will suggest that the demand for properties in certain districts may fluctuate. There could be various factors affecting the popularity such as new developments, and shifts in population demographics. As a result, the rental and resale values of properties in these districts may also change over time. To test this hypothesis, bar charts will be used to visualise any trends and identify changes in district popularity and changes in property prices. 

- Proximity to key locations contribute to the prices of Rental and Resale market 
    - The team believes that being closer to important locations can increase the rental and resale values of properties. This is due to the convenience and accessibility these locations offer to residents. Key locations such as Central Business District, Changi Airport, Marina Bay Sands, and Orchard Road will be selected for this hypothesis. In order to test this hypothesis, the team will use a line chart to examine the relationship between property prices and their distance from these key locations. 

- The public and private transactions follow similar distributions
    - The team believes that the patterns of transactions and prices in both public and private housing markets are likely to be similar. This is because external factors such as changes in the overall economy or government policies are expected to affect both markets in a similar way, leading to similar trends in transactions and prices. As such, we expect these two distributions to be similar, with both markets experiencing similar fluctuations in transactions and prices. Visual tests such as histograms can be created to verify this hypothesis. 

- Property transactions follow a Pareto distribution
    - We expect that approximately 80% of transactions are performed by 20% of all salespersons, or in other words, there are certain salespersons who dominate the property market. This can be reasoned by the structure of real estate companies in Singapore, where top performers eventually start their own practice and build a strong following over time. An empirical Pareto analysis can be applied to confirm this hypothesis. For example, visual plots such as top-K property agents, and empirical Pareto analysis can be applied to ascertain this hypothesis. 

- Property prices are non-stationary
    - We expect property prices to exhibit seasonal variation due to fluctuating business cycles. For instance, we expect prices to pick up where demand is higher - and that could happen when the economy “booms”. The inverse occurs when the economy “busts”. These cycles are known as “boom and bust” cycles. Because seasonality implies that the mean of these price processes is dependent on season, we expect that property prices are non-stationary. 

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
