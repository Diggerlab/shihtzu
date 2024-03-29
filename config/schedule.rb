# Use this file to easily define all of your cron jobs.
#
# It's helpful, but not entirely necessary to understand cron before proceeding.
# http://en.wikipedia.org/wiki/Cron

# Example:
#
set :output, "/home/webuser/www/shihtzu/shared/log/cron_log.log"
every 25.minutes do 
	command "/home/webuser/www/shihtzu/current/etl/job_minute.sh"
end

every "*/57 * * * *" do 
  command "/home/webuser/www/shihtzu/current/etl/job_hour.sh"
end
#
# every 2.hours do
#   command "/usr/bin/some_great_command"
#   runner "MyModel.some_method"
#   rake "some:great:rake:task"
# end
#
# every 4.days do
#   runner "AnotherModel.prune_old_records"
# end

# Learn more: http://github.com/javan/whenever
