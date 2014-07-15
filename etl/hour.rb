require 'mysql2'
require 'etl'
require 'date'
require 'dotenv'
Dotenv.load
CONNECTION_BERNARD = Mysql2::Client.new host: ENV['META_HOST'],
                                username: ENV['META_USER'],
                                password: ENV['META_PASSWORD'],
                                database: ENV['META_DB']

connection = Mysql2::Client.new host: ENV['DW_HOST'],
                                username: ENV['DW_USER'],
                                password: ENV['DW_PASSWORD'],
                                database: ENV['DW_DB'], 
                                reconnect: true


GAME_ID = ENV['GAME_ID']
UNIT_REVENUE = 0 # Free from Jun 28
# FINISH==START for only importing data of this day
# to keep original logins data. Change START to reload everything but logins.
# 
#START = Date.new(2014, 3, 5)
START = Time.now.to_date
START_DATE = START.strftime("%Y-%m-%d")
START_DATE_ID = START.strftime('%Y%m%d')

FINISH = Time.now.to_date
FINISH_DATE = FINISH.strftime("%Y-%m-%d")
FINISH_DATE_ID = FINISH.strftime('%Y%m%d')

# set up the ETL
etl = ETL.new(description: "Executing slow queries..", connection:  connection)

# configure ETL
etl.config do |etl|

  def fact_accounts(start,finish,etl)
    (start..finish).each do |date|
      date_id = date.strftime('%Y%m%d')
      date_name = date.strftime('%Y-%m-%d')
        # income & expense & balance
      total_users = CONNECTION_BERNARD.query "select count(id) as count from users where date(created_at) <= '#{date_name}' and date(created_at) >= '2013-05-01' and status <> 'racing-bot'"
      balance = CONNECTION_BERNARD.query "select sum(gems) as gems_balance, sum(coins) as coins_balance from bernard.accounts"
      expense = CONNECTION_BERNARD.query "select sum(total_gems) as gems_expense, sum(total_coins) as coins_expense from bernard.purchases join bernard.items on items.id=purchases.item_id where date(purchases.created_at) = '#{date_name}' and items.tag <> 'coins' and state = 'completed'"
      exchange = CONNECTION_BERNARD.query "select sum(total_gems) as gems_exchange from bernard.purchases join bernard.items on items.id=purchases.item_id where date(purchases.created_at) = '#{date_name}' and state = 'completed' and items.tag = 'coins'"
      etl.query "insert into fact_accounts(dim_game_id, dim_date_id, total_users, gems_balance, coins_balance, gems_expense, coins_expense, gems_exchange) values(
        #{GAME_ID}, #{date_id}, #{total_users.first['count']||0}, #{balance.first['gems_balance']||0}, #{balance.first['coins_balance']||0}, #{expense.first['gems_expense']||0}, #{expense.first['coins_expense']||0}, #{exchange.first['gems_exchange']||0})"     
    end
  end

  start, finish, methods = ARGV
  start_date = Date.parse(start||START_DATE)
  finish_date = Date.parse(finish||FINISH_DATE)
  fact_tables = %w(fact_accounts)

  etl.before_etl do |etl|
    fact_tables.each do |method|
        etl.query "delete from #{method} where dim_date_id >= #{start_date.strftime('%Y%m%d')} 
          and dim_date_id <= #{finish_date.strftime('%Y%m%d')} and dim_game_id=#{GAME_ID}" if methods.nil? || methods.include?(method) 
    end 
  end

  etl.etl do |etl|
    fact_tables.each do |method|
      eval "#{method}(start_date, finish_date, etl)" if methods.nil? || methods.include?(method)
    end 
  end

  etl.after_etl do |etl|
  end
end

# ship it
 etl.run
