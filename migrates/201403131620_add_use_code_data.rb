require 'mysql2'
require 'etl'
require 'date'
require 'dotenv'
Dotenv.load

connection_bernard = Mysql2::Client.new host: ENV['META_HOST'],
                                username: ENV['META_USER'],
                                password: ENV['META_PASSWORD'],
                                database: ENV['META_DB']

connection = Mysql2::Client.new host: ENV['DW_HOST'],
                                username: ENV['DW_USER'],
                                password: ENV['DW_PASSWORD'],
                                database: ENV['DW_DB'], 
                                reconnect: true


GAME_ID = ENV['GAME_ID']

START = Date.new(2014, 3, 1)
START_DATE = START.strftime("%Y-%m-%d")
START_DATE_ID = START.strftime('%Y%m%d')

FINISH = Time.now.to_date
FINISH_DATE = FINISH.strftime("%Y-%m-%d")
FINISH_DATE_ID = FINISH.strftime('%Y%m%d')

# set up the ETL
etl = ETL.new(description: "migrate data", connection:  connection)
per_coin = 0
result_coins = connection_bernard.query  "select rewards.coins from rewards where rewards.code = 'reward_code_connect' limit 1 "
per_coin = result_coins.first['coins']
# add code_used and use_code data 
 result1 = etl.query "select id from dim_income_sources where name = 'use_code' limit 1 "
 result2 = etl.query "select id from dim_income_sources where name = 'code_used' limit 1 "
 use_code_id = result1.first['id']
 code_used_id = result2.first['id']

etl.before_etl do |etl|
  etl.query "delete from fact_user_incomes where fact_user_incomes.dim_income_source_id = '#{use_code_id}' "  
  etl.query "delete from fact_user_incomes where fact_user_incomes.dim_income_source_id = '#{code_used_id}' "  
end

etl.etl do |etl|
  (START..FINISH).each do |date|
    date_id = date.strftime('%Y%m%d')
    date_name = date.strftime('%Y-%m-%d')
     ##get_by_use_code
    result = connection_bernard.query "select count(*) as count from friends where date(friends.created_at)='#{date_name}' and provider = 'code' and state = 'asked' "
    code_coins = per_coin * (result.first['count'] || 0)

    income_id = etl.query "select id from dim_income_sources where name = 'use_code' "
      etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
      #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{code_coins})"
    ##get_by_code_used
    result = connection_bernard.query "select count(*) as count from friends where date(friends.created_at)='#{date_name}' and provider = 'code' and state = 'answered' "
    code_used_coins = per_coin * (result.first['count'] || 0)

    income_id = etl.query "select id from dim_income_sources where name = 'code_used' "
      etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
      #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{code_used_coins})"   
  end
end



# ship it
etl.run
