#!/bin/bash
week_data=($(tail -7 weather/historical_fc_accuracy.tsv  | cut -f6)) #extracting last week data
#validate result:
echo "last week accuracy data:"
printf "%d\n" "${week_data[@]}" # printing last week data

# calculating absolute value for last week data:
for i in {0..6}; do
  if [[ ${week_data[$i]} < 0 ]]
  then
    week_data[$i]=$(((-1)*week_data[$i])) # multiplying negative numbers by -1 in place
  fi
  echo ${week_data[$i]} >> weather/accu_data.txt     #writing each element of the array to separate lines in a file so we can sort them later
done

# to find minimum and maximum we use sort then head -1 for minimum and tail -1 for maximum:
minimum=$(cat weather/accu_data.txt | sort | head -1)
maximum=$(cat weather/accu_data.txt | sort | tail -1)
echo "**************************"
echo -e "in this week:\nminimum absolute error is: $minimum \nmaximum absolute error is: $maximum"
rm weather/accu_data.txt # deleting dummy file