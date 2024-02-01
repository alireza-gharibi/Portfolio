## Project Overview
I am a data engineer at an aeronautics consulting company. My company prides itself in being able to efficiently design airfoils for use in planes and sports cars. Data scientists in my office need to work with different algorithms and data in different formats. While they are good at Machine Learning, they count on me to be able to do ETL jobs and build ML pipelines. In this project i will use the **modified version** of the NASA Airfoil Self Noise dataset. 
[My Project Code](https://github.com/alireza-gharibi/Portfolio/blob/main/Spark%20machine%20learning/Spark%20Project%202/Spark%20project%202.py)

In this lab i will be using dataset(s):
 - The original dataset can be found here [NASA airfoil self noise dataset](https://archive.ics.uci.edu/dataset/291/airfoil+self+noise). 
  - This dataset is licensed under a Creative Commons Attribution 4.0 International (CC BY 4.0) license.

  Diagram of an airfoil. - For informational purpose: 

![Alt text](https://github.com/alireza-gharibi/Portfolio/blob/main/Spark%20machine%20learning/Spark%20Project%202/Airfoil_with_flow.png)

Diagram showing the Angle of attack. - For informational purpose:

![Alt text](https://github.com/alireza-gharibi/Portfolio/blob/main/Spark%20machine%20learning/Spark%20Project%202/Airfoil_angle_of_attack.jpg)

- I will begin by cleaning the dataset and removing duplicate rows and rows with null values. Doing this step ensures the data is consistent and reliable for further analysis. 
- Next, i will construct a Machine Learning pipeline with three stages, including a regression stage. This pipeline will enable me to build a model that predicts the SoundLevel based on other columns in the dataset. 
- Once i’ve trained the model, i will evaluate its performance using appropriate metrics to assess its accuracy and effectiveness. 
- Finally, i will persist with the model, saving it for future use. This step ensures that the trained model can be stored and retrieved later, enabling its deployment in real-world applications and making predictions on new data.
- ## preparing the environment:
- I start a **spark** container and run **pyspark** in that container:
```
docker run --rm -it spark:python3 /opt/spark/bin/pyspark
```
- make sure you have **java** installed on your system.
 ## Project Overview
## Objectives
There are four parts to this project. 
- In part one, i will perform ETL activities, including loading the CSV dataset, cleaning it, applying transformations, and storing the cleaned data in the parquet format. 
- Part two involves creating the Machine Learning pipeline, which forms the backbone of my model development. 
- In part three, i will evaluate the model using relevant metrics to gain insights into its performance. 
- Finally, in part four, i will save the model for future production use and verify its successful loading

