#! /bin/bash
mkdir weather #
# adding header to weather_data.log:
echo -e "year\tmonth\tday\thour\tcurrent_tmp\tfc_temp">>"weather/weather_data.log"  #table headers

today=$(date +%Y-%m-%d)
report_name=weather/raw_data_$today.json

city=munich
curl https://wttr.in/$city?format=j1 --output $report_name   #downloading data from the website
current_tmp=$(cat $report_name | grep -E 'temp_C' | grep -oE [0-9]+)
fc_temp=$(cat $report_name | grep -wE 'date|tempC|time' | sed -n '27p' | grep -oE [0-9]+) #tomorrow noon temperature
hour=$(TZ='Europe/Berlin' date +%H) # munich time zone is 'Europe/Berlin'
day=$(TZ='Europe/Berlin' date +%d)
month=$(TZ='Europe/Berlin' date +%m)
year=$(TZ='Europe/Berlin' date +%Y)
echo -e "$year\t$month\t$day\t$hour\t$current_tmp\t\t$fc_temp">>weather/weather_data.log # writing the row to table

# this shell script should be scheduled using cron for 12:00 munich time:
#local time:  date +%T
#munich time:  TZ='Europe/Berlin' date +%T
#calculating time difference between local time and munich time
# since cronjob needs to run at 12:00 munich time i need to convert munich time to my local time.


