# banks_market_cap

ETL pipeline to compile banks market capitalization to other currencies

This repository contains a data engineering project that builds an end-to-end ETL pipeline to compile, transform, and store market capitalization data of the top 10 largest banks worldwide. The pipeline extracts the list of banks ranked by market capitalization in USD, converts these values into GBP, EUR, and INR using exchange rates from a provided CSV file, and saves the processed data both as a local CSV file and in a database table.
