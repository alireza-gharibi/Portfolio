# Create a Machine Learning Pipeline for a Regression Project

 ## Scenario
 I am a data engineer at a data analytics consulting company. My company prides itself in being able to efficiently handle huge datasets. Data scientists in my office need to work with different algorithms and data in different formats. While they are good at Machine Learning, they count on me to be able do ETL jobs and build ML pipelines. \
 I use modified version of car mileage dataset. Original dataset available [here](https://archive.ics.uci.edu/ml/datasets/auto+mpg ).
 ## preparing the environment:
- I start a **spark** container and run **pyspark** in that container:
```
docker run --rm -it spark:python3 /opt/spark/bin/pyspark
```
- make sure you have **java** installed on your system.
 ## Project Overview
Welcome to the project, where i will create an end-to-end solution using machine learning pipelines for regression. My objective is to clean the dataset, create a model that predicts the **SoundLevel** based on the other columns, evaluate its performance, and persist the model for future use.
This project has four parts, each building on the previous one. 

- In part one, i will perform ETL activities, including loading the CSV dataset, removing duplicate rows, if any, dropping rows with null values, applying necessary transformations, and storing the cleaned data in the parquet format. 

- Moving on to part two, i will create a machine learning pipeline with three stages, including a regression stage. This pipeline will be the backbone of my model development, enabling me to process the data and train a predictive model efficiently. 

- Once i’ve trained the model, i will proceed to part three to evaluate its performance using relevant metrics. This step is crucial in understanding how well my model predicts **SoundLevel** and identifying areas for improvement. 

- Finally, in part four, i will persist the model, allowing me to load and utilize it in real-world applications when predicting new data. As the final step, i will load and verify the stored model to ensure its integrity and usability in future tasks or deployments.


