# Housing Data Engineering Pipeline 

## Description of problem
The housing market in Singapore is facing several challenges, such as the fragmentation of information across different actors and sources, and the volatility of the market due to external factors such as COVID-19, inflation, and interest rates. 

A combination of these challenges makes it difficult for potential buyers, sellers, and investors to make swift and informed decisions about their property transactions.

As such, there is a need for a data pipeline solution that can gather housing information from various sources and analyse it to provide a comprehensive and timely overview of the state of the market in Singapore. 

## Usefulness and importance of gathering and analysing housing data 
Gathering and analysing housing data from various sources are important and useful because it can help to reduce information asymmetry and increase transparency in the property market, increasing confidence in decision-making.  

Furthermore, it can help to identify emerging trends and patterns in various segments of the housing market, such as price movements, supply and demand dynamics, and rental yields. This has widespread benefits for various actors in the market, helping them make data-driven decisions. 


## Application 2: Modelling Housing Prices 

We attempt to model housing prices for the following:

1. [Monthly median resale flat prices](modelling/sarima.ipynb)
2. Monthly median private property prices 
3. Monthly median flat rental prices
4. Monthly median private property rental prices

- Before integrating extract and transform processes into the pipeline, a full process from data extraction to modelling was carried out.
- This was done to ensure that the pipeline would be able to handle the data and produce the desired results
- An example process is outlined in the [notebook](modelling/sarima.ipynb) where resale flat prices are modelled.
