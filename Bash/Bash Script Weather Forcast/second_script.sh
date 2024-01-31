#! /bin/bash
# Creating some sample data:
echo -e "year\tmonth\tday\thour\tcurrent_tmp\tfc_temp">"weather/weather_data.log" #table headers, this line is not appending it is overwriting the file
echo -e "2022\t11\t1\t10\t9\t\t10">>weather/weather_data.log  
echo -e "2022\t11\t2\t10\t8\t\t12">>weather/weather_data.log
echo -e "2022\t11\t3\t10\t13\t\t7">>weather/weather_data.log
echo -e "2022\t11\t4\t10\t8\t\t7">>weather/weather_data.log
echo -e "2022\t11\t5\t10\t9\t\t6">>weather/weather_data.log
echo -e "2022\t11\t6\t10\t6\t\t8">>weather/weather_data.log
echo -e "2022\t11\t7\t10\t9\t\t7">>weather/weather_data.log
echo -e "2022\t11\t8\t10\t6\t\t8">>weather/weather_data.log

#obs_tmp is observed temp.
#adding column names to the file and creating the file: 
echo -e "year\tmonth\tday\tobs_tmp\tfc_temp\taccuracy\taccuracy_label" > weather/historical_fc_accuracy.tsv

line_num=$(wc -l < weather/weather_data.log) #counting lines
# passing  $line_num-1 to tail results in erasing the column names from file:
tail -n $(($line_num-1)) weather/weather_data.log > weather/weather_data_dummy.log # writing to a dummy file

yesterday_fc=0   # there is no yesterday_fc data for the first line so i default it to 0

# to map accuracy variable to acuracy labels we use dictionary aka associative array:
declare -A accuracy_label
accuracy_label=([0]=exact [1]=excellent [-1]=excellent [2]=good [-2]=good [3]=fair [-3]=fair [4]=poor [-4]=poor )

# reading the file line by line using while:
while IFS= read -r lines;
 do
 year=$(echo $lines | cut -d ' ' -f1) #extracting year from each line
 month=$( echo $lines | cut -d ' ' -f2) # extracting month from each line
 day=$( echo $lines | cut -d ' ' -f3) #extracting day from each line
 today_temp=$( echo $lines | cut -d ' ' -f5) #extracting today_temp from each line
 accuracy=$(($yesterday_fc-$today_temp)) # calculating accuracy for each line
 #inserting each row into file:
 echo -e "$year\t$month\t$day\t$today_temp\t$yesterday_fc\t$accuracy\t\t${accuracy_label[$accuracy]}" >> weather/historical_fc_accuracy.tsv
 #extracting yseterday_fc is tricky. since all the row values are extracted from current line in the file
 #(continued) but yesterday_fc is extracted from previous line.
 #(continued) we assign value to yesterday_fc variable after row insertion. so this line's yesterday_fc value will be used in the next iteration
 yesterday_fc=$( echo $lines | cut -d ' ' -f6) # puting yesterday_fc in the last line of the loop so it will be used in the next iteration
 done < weather/weather_data_dummy.log # passing dummy file into 'while' to iterate over
cat weather/historical_fc_accuracy.tsv
echo "there is no fc_temp for first line so i default it to 0"
echo "obs_tmp:observed temperature"
echo "fc_temp:forecasted temperature"
rm weather/weather_data_dummy.log # deleting dummy file


#ideas to extract lines from 'weather_data.log' file and write each row to output file:
#1)we can use tail | head in a for loop to iterate over lines. each time 'tail' returns lots of lines which is not efficient.
#2)use grep based on date columns. grep has to read all the lines each time to find the desired line. this is not efficient either.
#3) we iterate over lines using 'while'. this is good. because we input line number and the intended line is returned.
