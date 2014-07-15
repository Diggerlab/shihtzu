#!/bin/bash
today=$(date +%Y%m%d)
pfile=/tmp/shihtzu.minute.$today.pid

# Kill previous running process of TODAY
if [ -f $pfile ]; then 
  kill -HUP $(cat $pfile)
fi

echo "$$" > "$pfile"
cd /home/webuser/www/shihtzu/current && bundle exec ruby etl/minute.rb
rm -f "$pfile" 