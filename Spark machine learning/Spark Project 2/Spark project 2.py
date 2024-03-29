

### Objectives

# In this 4 part project i will:
#
# - Part 1 Perform ETL activity
#   - Load a csv dataset
#   - Remove duplicates if any
#   - Drop rows with null values if any
#   - Make transformations
#   - Store the cleaned data in parquet format
# - Part 2 Create a  Machine Learning Pipeline
#   - Create a machine learning pipeline for prediction
# - Part 3 Evaluate the Model
#   - Evaluate the model using relevant metrics
# - Part 4 Persist the Model 
#   - Save the model for future production use
#   - Load and verify the stored model




### Installing Required Libraries

#!pip install pyspark==3.1.2 -q
#!pip install findspark -q


### Importing Required Libraries
# i can use this section to suppress warnings generated by code:
def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn
warnings.filterwarnings('ignore')

# FindSpark simplifies the process of using Apache Spark with Python

import findspark
findspark.init()


# ## Part 1 - Perform ETL activity


### Task 1 - Import required libraries
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator
import wget
import os


### Task 2 - Create a spark session
spark = SparkSession.builder.appName("Spark project 2").getOrCreate()


### Task 3 - Load the csv file into a dataframe



# Download the data file.
wget.download('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-BD0231EN-Coursera/datasets/NASA_airfoil_noise_raw.csv')

df = spark.read.csv("NASA_airfoil_noise_raw.csv", header=True, inferSchema=True) # Load the dataset into the spark dataframe



### Task 4 - Print top 5 rows of the dataset
df.show(5)


### Task 6 - Print the total number of rows in the dataset
rowcount1 = df.count()
print(rowcount1)


### Task 7 - Drop all the duplicate rows from the dataset
df = df.dropDuplicates()


### Task 8 - Print the total number of rows in the dataset
rowcount2 = df.count()
print(rowcount2)


### Task 9 - Drop all the rows that contain null values from the dataset
df = df.na.drop()


### Task 10 - Print the total number of rows in the dataset
rowcount3 = df.count()
print(rowcount3)


### Task 11 - Rename the column "SoundLevel" to "SoundLevelDecibels"Drop
df = df.withColumnRenamed('SoundLevel','SoundLevelDecibels')

### Task 12 - Save the dataframe in parquet formant, name the file as "NASA_airfoil_noise_cleaned.parquet"
df.write.parquet("NASA_airfoil_noise_cleaned.parquet")


#### Part 1 - Evaluation

print("Part 1 - Evaluation")
print("Total rows = ", rowcount1)
print("Total rows after dropping duplicate rows = ", rowcount2)
print("Total rows after dropping duplicate rows and rows with null values = ", rowcount3)
print("New column name = ", df.columns[-1])
print("NASA_airfoil_noise_cleaned.parquet exists :", os.path.isdir("NASA_airfoil_noise_cleaned.parquet"))



### Part - 2 Create a  Machine Learning Pipeline


### Task 1 - Load data from "NASA_airfoil_noise_cleaned.parquet" into a dataframe
df = spark.read.parquet("NASA_airfoil_noise_cleaned.parquet")


### Task 2 - Print the total number of rows in the dataset
rowcount4 = df.count()
print(rowcount4)


### Task 3 - Define the VectorAssembler pipeline stage
# Stage 1 - Assemble the input columns into a single column "features". Use all the columns except SoundLevelDecibels as input features.
assembler = VectorAssembler(inputCols=['Frequency', 'AngleOfAttack', 'ChordLength', 'FreeStreamVelocity', 'SuctionSideDisplacement'], outputCol="features")

### Task 4 - Define the StandardScaler pipeline stage
# Stage 2 - Scale the "features" using standard scaler and store in "scaledFeatures" column
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")


### Task 5 - Define the LinearRegression pipeline stage
# Stage 3 - Create a LinearRegression stage to predict "SoundLevelDecibels"
lr = LinearRegression(featuresCol="scaledFeatures", labelCol="SoundLevelDecibels")


### Task 6 - Build the pipeline
# Build a pipeline using the above three stages
pipeline = Pipeline(stages=[assembler, scaler, lr]) 


### Task 7 - Split the data
# Split the data into training and testing sets with 70:30 split. set the value of seed to 42

(trainingData, testingData) = df.randomSplit([0.7, 0.3], seed=42)


### Task 8 - Fit the pipeline
# Fit the pipeline using the training data
pipelineModel = pipeline.fit(trainingData) # returns trained(fitted) model



### Part 2 - Evaluation

print("Part 2 - Evaluation")
print("Total rows = ", rowcount4)
ps = [str(x).split("_")[0] for x in pipeline.getStages()]
print("Pipeline Stage 1 = ", ps[0])
print("Pipeline Stage 2 = ", ps[1])
print("Pipeline Stage 3 = ", ps[2])
print("Label column = ", lr.getLabelCol())



### Part 3 - Evaluate the Model


### Task 1 - Predict using the model
predictions = pipelineModel.transform(testingData)



### Task 2 - Print the MSE
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="SoundLevelDecibels", metricName="mse")
mse = evaluator.evaluate(predictions)
print(mse)


### Task 3 - Print the MAE
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="SoundLevelDecibels", metricName="mae")
mae = evaluator.evaluate(predictions)
print(mae)


### Task 4 - Print the R-Squared(R2)
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="SoundLevelDecibels", metricName="r2")
r2 = evaluator.evaluate(predictions)
print(r2)


### Part 3 - Evaluation

print("Part 3 - Evaluation")
print("Mean Squared Error = ", round(mse,2))
print("Mean Absolute Error = ", round(mae,2))
print("R Squared = ", round(r2,2))
lrModel = pipelineModel.stages[-1]
print("Intercept = ", round(lrModel.intercept,2))



### Part 4 - Persist the Model


### Task 1 - Save the model to the path "Spark_Project_2"
pipelineModel.write().save("Spark_Project_2")


### Task 2 - Load the model from the path "Spark_Project_2"
# Load the pipeline model you have created in the previous step
loadedPipelineModel = PipelineModel.load("Spark_Project_2")


### Task 3 - Make predictions using the loaded model on the testingData
predictions = loadedPipelineModel.transform(testingData)


### Task 4 - Show the predictions
#show top 5 rows from the predections dataframe. Display only the label column and predictions
predictions.select("SoundLevelDecibels","prediction").show()



#### Part 4 - Evaluation

print("Part 4 - Evaluation")
loadedmodel = loadedPipelineModel.stages[-1]
totalstages = len(loadedPipelineModel.stages)
inputcolumns = loadedPipelineModel.stages[0].getInputCols()
print("Number of stages in the pipeline = ", totalstages)
for i,j in zip(inputcolumns, loadedmodel.coefficients):
    print(f"Coefficient for {i} is {round(j,4)}")


### Stop Spark Session
spark.stop()



