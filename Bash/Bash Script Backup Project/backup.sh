#!/bin/bash

# This checks if the number of arguments is correct
# If the number of arguments is incorrect ( $# != 1) print a message and exit 
if [[ $# != 1 ]]
then
  echo "Enter one Backup directory path"
  exit
fi
# This checks if destination directory exists and make a directory if not
if [ ! -d $1 ]
then
  mkdir $1
fi
 
backup_directory_path=$1   # absolute path to a directory to store backup files
echo "Backup directory path is: $backup_directory_path"

currentTS=$(date +%s)  #current timestamp
backupFileName="backup-$currentTS.tar.gz" # name of the archived and compressed backup file that the script will create

origAbsPath=$(pwd) #absolute path of the current directory

yesterdayTS=$(date -d "-1 day" +%s) #timestamp (in seconds) 24 hours prior to the current timestamp(now)

declare -a toBackup # creating an array named 'toBackup'
echo "*************************"
echo -e "\nmodified files and folders:\n"

for file in $(ls) # returns all files and directories in the current folder (including their child folders and files )
do
  if (( $(date -r $file +%s) > $yesterdayTS ))   #comparing each file's last-modified-date "date -r $file +%s" with yesterdayTS
  then
    toBackup+=($file)  # adding the file that was updated in the past 24-hours to the toBackup array.
  fi
done

tar -czvf $backupFileName ${toBackup[@]} # compressing and archiving the files, using the $toBackup array of filenames, to a file with the name 'backupFileName'
mv $backupFileName $backup_directory_path
echo '***Backup file Created!***'

#The script should be scheduled using cron to run every 24 hours