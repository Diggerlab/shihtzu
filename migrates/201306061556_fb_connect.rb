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

START = Date.new(2013, 5, 1)
START_DATE = START.strftime("%Y-%m-%d")
START_DATE_ID = START.strftime('%Y%m%d')

FINISH = Time.now.to_date
FINISH_DATE = FINISH.strftime("%Y-%m-%d")
FINISH_DATE_ID = FINISH.strftime('%Y%m%d')

# set up the ETL
etl = ETL.new(description: "migrate data", connection:  connection)

# configure ETL
etl.config do |etl|
  etl.etl do |etl|
    etl.query "truncate table fact_accounts"
    etl.query "truncate table fact_racings"
    

    (START..FINISH).each do |date|
      date_id = date.strftime('%Y%m%d')
      date_name = date.strftime('%Y-%m-%d')
      puts "migrating #{date_name}.."

      fb_users = connection_bernard.query "select count(distinct(uid)) count from bernard.authentications where provider='facebook' and date(created_at) = '#{date_name}'"
      etl.query "update fact_active_users set fb_connected_users=#{fb_users.first['count']} where dim_date_id=#{date_id} and dim_game_id=#{GAME_ID}"

      total_users = connection_bernard.query "select count(id) as count from users where date(created_at) <= '#{date_name}' and date(created_at) >= '2013-05-01' and status <> 'racing-bot'"
      total_users = total_users.first['count']||0
      balance = connection_bernard.query "select sum(gems) as gems_balance, sum(coins) as coins_balance from bernard.accounts join bernard.users on users.id=accounts.user_id where date(users.created_at) <= '#{date_name}' and date(users.created_at) >= '2013-05-01' and users.status <> 'racing-bot'"
      gems_balance = balance.first['gems_balance']||0
      coins_balance = balance.first['coins_balance']||0
      expense = connection_bernard.query "select sum(total_gems) as gems_expense, sum(total_coins) as coins_expense from bernard.purchases join bernard.items on items.id=purchases.item_id where date(created_at) = '#{date_name}' and items.tag <> 'coins' and state = 'completed'"
      gems_expense = expense.first['gems_expense']||0
      coins_expense = expense.first['coins_expense']||0
      exchange = connection_bernard.query "select sum(total_gems) as gems_exchange from bernard.purchases join bernard.items on items.id=purchases.item_id where date(created_at) = '#{date_name}' and state = 'completed' and items.tag = 'coins'"
      gems_exchange = exchange.first['gems_exchange']||0
      etl.query "insert into fact_accounts(dim_game_id, dim_date_id, total_users, gems_balance, coins_balance, gems_expense, coins_expense, gems_exchange) values(
        #{GAME_ID}, #{date_id}, #{total_users}, #{gems_balance}, #{coins_balance}, #{gems_expense}, #{coins_expense}, #{gems_exchange})"      


      new_matches = connection_bernard.query "select count(id) as count from bernard.racings where date(created_at) = '#{date_name}' and state='new'"
      pending_matches = connection_bernard.query "select count(id) as count from bernard.racings where date(created_at) = '#{date_name}' and state='matched'"
      finished_matches = connection_bernard.query "select count(id) as count from bernard.racings where date(created_at) = '#{date_name}' and state='finished'"
      players = connection_bernard.query "select count(distinct(user_id)) as count from bernard.racings where date(created_at) = '#{date_name}'"
      etl.query "insert into fact_racings(dim_game_id, dim_date_id, new_matches, pending_matches, finished_matches, players) values(
        #{GAME_ID}, #{date_id}, #{new_matches.first['count']||0}, #{pending_matches.first['count']||0}, #{finished_matches.first['count']||0}, #{players.first['count']||0})"      
    end
  end

end

# ship it
etl.run
