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

MONTH_START = Time.new(FINISH.year, FINISH.month, 1)
MONTH_START_DATE = MONTH_START.strftime("%Y-%m-%d")

if FINISH.month == 12
  MONTH_END = Time.new(FINISH.year + 1, 1, 1)
else
  MONTH_END = Time.new(FINISH.year, FINISH.month + 1, 1)
end
MONTH_END_DATE = MONTH_END.strftime("%Y-%m-%d")

# SHARDING_TABLES = %w(quests racings racing_results pop_results)
SHARDING_START = Date.parse('2013-10-09')
SHARDING_SPAN = 7 

def get_sharding_table(date, table)
  sharding_date = SHARDING_START + ((date - SHARDING_START).to_i/SHARDING_SPAN.to_i + 1).to_i * SHARDING_SPAN
  if Time.now.to_date > sharding_date
    "#{table}_#{sharding_date.strftime('%Y%m%d')}"
  else
    table
  end
end

# set up the ETL
etl = ETL.new(description: "Loading dimention and fact tables", connection:  connection)

# configure ETL
etl.config do |etl|
  etl.ensure_destination do |etl|
    etl.query %[
      CREATE TABLE IF NOT EXISTS shihtzu.dim_games (
          id SMALLINT NOT NULL AUTO_INCREMENT
        , name VARCHAR(20) NOT NULL
        , PRIMARY KEY (id))]

    etl.query %[
      CREATE TABLE IF NOT EXISTS shihtzu.dim_users (
          id INT NOT NULL AUTO_INCREMENT
        , name VARCHAR(50) NOT NULL
        , remote_ip VARCHAR(50)
        , email VARCHAR(50) 
        , created_at DATETIME NOT NULL
        , PRIMARY KEY (id))]
    
    etl.query %[
      CREATE TABLE IF NOT EXISTS shihtzu.dim_dates (
          id INT NOT NULL 
        , full_date date NOT NULL
        , year_number SMALLINT NOT NULL
        , month_number SMALLINT NOT NULL
        , day_number SMALLINT NOT NULL
        , week_number SMALLINT NOT NULL
        , PRIMARY KEY (id))]

    etl.query %[
       CREATE TABLE IF NOT EXISTS shihtzu.dim_income_sources (
        id INT NOT NULL AUTO_INCREMENT
        , dim_game_id INT NOT NULL
        , name VARCHAR(50) NOT NULL
        , PRIMARY KEY (id))]

    etl.query %[
       CREATE TABLE IF NOT EXISTS shihtzu.dim_items (
        id INT NOT NULL AUTO_INCREMENT
        , dim_game_id INT NOT NULL
        , name VARCHAR(50) NOT NULL
        , code VARCHAR(50) NOT NULL
        , PRIMARY KEY (id))]

    etl.query %[
      CREATE TABLE IF NOT EXISTS shihtzu.fact_active_users (
          dim_game_id INT NOT NULL
        , dim_date_id INT NOT NULL
        , today_users INT DEFAULT 0
        , total_users INT DEFAULT 0
        , today_logins INT DEFAULT 0
        , one_days_retention INT DEFAULT 0
        , three_days_retention INT DEFAULT 0
        , seven_days_retention INT DEFAULT 0
        , fourteen_days_retention INT DEFAULT 0
        , thirty_days_retention INT DEFAULT 0
        , fb_connected_users INT DEFAULT 0)]

    etl.query %[
      CREATE TABLE IF NOT EXISTS shihtzu.fact_purchases (
          dim_game_id INT NOT NULL
        , dim_date_id INT NOT NULL
        , item VARCHAR(50)
        , total_count INT DEFAULT 0
        , total_coins INT DEFAULT 0
        , total_gems INT DEFAULT 0)]

    etl.query %[
      CREATE TABLE IF NOT EXISTS shihtzu.fact_iap (
          dim_game_id INT NOT NULL
        , dim_date_id INT NOT NULL
        , product VARCHAR(50)
        , product_id VARCHAR(50)
        , count INT DEFAULT 0)]

    etl.query %[
      CREATE TABLE IF NOT EXISTS shihtzu.fact_revenues (
          dim_game_id INT NOT NULL
        , dim_date_id INT NOT NULL
        , paid_users INT DEFAULT 0
        , iap_users INT DEFAULT 0
        , iap_revenues float DEFAULT 0
        , month_arpu float DEFAULT 0
        , month_arppu float DEFAULT 0 )]

    etl.query %[
      CREATE TABLE IF NOT EXISTS shihtzu.fact_accounts (
          dim_game_id INT NOT NULL
        , dim_date_id INT NOT NULL
        , total_users INT DEFAULT 0
        , coins_balance INT DEFAULT 0
        , gems_balance INT DEFAULT 0
        , gems_exchange INT DEFAULT 0
        , coins_expense INT DEFAULT 0
        , gems_expense INT DEFAULT 0 )]

    etl.query %[
      CREATE TABLE IF NOT EXISTS shihtzu.fact_racings (
          dim_game_id INT NOT NULL
        , dim_date_id INT NOT NULL
        , new_matches INT DEFAULT 0
        , pending_matches INT DEFAULT 0
        , finished_matches INT DEFAULT 0
        , players INT DEFAULT 0
        , init_wins INT DEFAULT 0
        , total_awards INT DEFAULT 0
        , init_awards INT DEFAULT 0 )]

    etl.query %[
      CREATE TABLE IF NOT EXISTS shihtzu.fact_pops (
          dim_game_id INT NOT NULL
        , dim_date_id INT NOT NULL
        , matches INT DEFAULT 0
        , points INT DEFAULT 0
        , coins INT DEFAULT 0 )]

    etl.query %[
      CREATE TABLE IF NOT EXISTS shihtzu.fact_lotteries (
          dim_game_id INT NOT NULL
        , dim_date_id INT NOT NULL
        , dim_item_id INT NOT NULL
        , total_count INT DEFAULT 0 )]

    etl.query %[
    CREATE TABLE IF NOT EXISTS shihtzu.fact_user_incomes (
        dim_game_id INT NOT NULL
      , dim_income_source_id INT NOT NULL
      , dim_date_id INT NOT NULL
      , total_coins INT DEFAULT 0 
      , total_gems INT DEFAULT 0 )]


    ###
    # load dimention data
    ###
    chichi_game = etl.query %[SELECT count(id) as rows FROM dim_games where name='chichi']
    if chichi_game.first['rows'].to_i == 0
      etl.query "INSERT INTO dim_games(id, name) 
        VALUES(#{GAME_ID}, 'chichi')"  
    end

    dates = etl.query %[SELECT count(id) as rows FROM dim_dates]
    if dates.first['rows'].to_i == 0
      start = DateTime.new(2013, 4, 1)
      finish = DateTime.new(2013, 12, 31)
      (start..finish).each do |date|
        etl.query "INSERT INTO dim_dates(id, full_date, year_number, month_number, day_number, week_number) 
        VALUES(#{date.strftime('%Y%m%d')}, '"+ date.strftime("%Y-%m-%d %H:%M:%S")  +"', #{date.year}, #{date.month}, #{date.day}, #{date.strftime('%U')})"  
      end
    end

    year = etl.query %[SELECT year_number FROM dim_dates order by year_number desc limit 1]
    if year.first['year_number'].to_i != 2014
      start = DateTime.new(2014, 1, 1)
      finish = DateTime.new(2014, 12, 31)
      (start..finish).each do |date|
        etl.query "INSERT INTO dim_dates(id, full_date, year_number, month_number, day_number, week_number) 
        VALUES(#{date.strftime('%Y%m%d')}, '"+ date.strftime("%Y-%m-%d %H:%M:%S")  +"', #{date.year}, #{date.month}, #{date.day}, #{date.strftime('%U')})"  
      end
    end

    income_sources = etl.query %[SELECT count(id) as rows FROM dim_income_sources]
    if income_sources.first['rows'].to_i == 0
      sources = %w(lottery_coins racing_active racing_passive pop daily_login share_home share_photo share_racing share_pop first_team first_bind_sns first_feedback first_buy_starfish use_code code_used iap_gems lottery_gems)
      sources.each do |source|
        etl.query "INSERT INTO dim_income_sources(name, dim_game_id) VALUES('#{source}', #{GAME_ID})"
      end 
    end

    items = etl.query %[SELECT count(id) as rows FROM dim_items]
    if items.first['rows'].to_i == 0
      game_items = CONNECTION_BERNARD.query %[SELECT code, name FROM items]
      game_items.each do |row|
        etl.query "INSERT INTO dim_items(dim_game_id, code, name) VALUES(#{GAME_ID}, '#{row['code']}', '#{row['name']}') "
      end
    end
  end

 
  def fact_lotteries(start,finish,etl)
    (start..finish).each do |date|
      date_id = date.strftime('%Y%m%d')
      date_name = date.strftime('%Y-%m-%d')
      ## load fact_lottery
      results = CONNECTION_BERNARD.query "select items.code, sum(prizes.count) as prize_count from lotteries 
      join prizes on lotteries.prize_id=prizes.id 
      join items on prizes.item_id = items.id 
      where date(lotteries.created_at)='#{date_name}' group by items.id"
      results.each do |result|
        dim_item = etl.query "select id from dim_items where code = '#{result['code']}' "
        etl.query "insert into fact_lotteries(dim_game_id, dim_date_id, dim_item_id, total_count) values(
        #{GAME_ID}, #{date_id}, #{dim_item.first['id']}, #{result['prize_count']||0})"
      end
    end
  end

  def fact_user_incomes(start,finish,etl)
    per_coin = 0
    result_coins = CONNECTION_BERNARD.query  "select rewards.coins from rewards where rewards.code = 'reward_code_connect' limit 1 "
    per_coin = result_coins.first['coins']                          
    (start..finish).each do |date|
      quests_table = get_sharding_table(date, 'quests')
      racings_table = get_sharding_table(date, 'racings')
      racing_results_table = get_sharding_table(date, 'racing_results')
      pop_results_table = get_sharding_table(date, 'pop_results')

      ##coins get_by_lottery
      date_id = date.strftime('%Y%m%d')
      date_name = date.strftime('%Y-%m-%d')
      coins = 0
      results = CONNECTION_BERNARD.query "select items.coins_got, prizes.count from lotteries
      join prizes on lotteries.prize_id=prizes.id 
      join items on prizes.item_id = items.id
      where date(lotteries.created_at)='#{date_name}' and items.tag = 'coins' "
    
      results.each do |result|
        coins += result['coins_got'].to_i * result['count'].to_i
      end
      income_id = etl.query "select id from dim_income_sources where name = 'lottery_coins' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{coins})"

      ##add get_by_racing_active
      results = CONNECTION_BERNARD.query "select sum(#{racing_results_table}.coins) as racing_coins from users
      join #{racings_table} on #{racings_table}.user_id = users.id  
      join #{racing_results_table} on #{racing_results_table}.racing_id=#{racings_table}.id 
      where date(#{racings_table}.created_at)='#{date_name}' "

      income_id = etl.query "select id from dim_income_sources where name = 'racing_active' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{results.first["racing_coins"]||0})"

      ##add get_by_racing_passive
      results = CONNECTION_BERNARD.query "select sum(#{racing_results_table}.coins) as r_coins, sum(#{racing_results_table}.total_coins) as t_coins from users
      join #{racings_table} on #{racings_table}.user_id = users.id  
      join #{racing_results_table} on #{racing_results_table}.racing_id=#{racings_table}.id 
      where date(#{racings_table}.created_at)='#{date_name}' "

      racing_passive_count = results.first["t_coins"].to_i - results.first["r_coins"].to_i

      income_id = etl.query "select id from dim_income_sources where name = 'racing_passive' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{racing_passive_count||0})"
      
      ##add get_by_pop pop_results

      results = CONNECTION_BERNARD.query "select sum(#{pop_results_table}.coins) as pop_coins from users
      join #{pop_results_table} on #{pop_results_table}.user_id = users.id  
      where date(#{pop_results_table}.created_at)='#{date_name}' "


      income_id = etl.query "select id from dim_income_sources where name = 'pop' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{results.first["pop_coins"]||0})"

      ##add get_by_daily_login
      results = CONNECTION_BERNARD.query "select quest_templates.award from users
      join #{quests_table} on #{quests_table}.user_id = users.id
      join quest_templates on #{quests_table}.quest_template_id = quest_templates.id
      where date(#{quests_table}.taken_at)='#{date_name}' and quest_templates.category = 'daily_login' and #{quests_table}.state = 'finish'"
      daily_login_coins = 0
      results.each do |result|
        r_split = "'#{result}'".split('\n').select{|item| item.match /coins/}
        next if r_split == nil
        daily_login_coins += r_split.first.split(':').last.strip.to_i
      end

      income_id = etl.query "select id from dim_income_sources where name = 'daily_login' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{daily_login_coins})"

      ##add get_by_share_photo
      share_photo_coins = 0
      results = CONNECTION_BERNARD.query "select quest_templates.award from users
      join #{quests_table} on #{quests_table}.user_id = users.id
      join quest_templates on #{quests_table}.quest_template_id = quest_templates.id
      where date(#{quests_table}.taken_at)='#{date_name}' and quest_templates.code = 'photo_share' and #{quests_table}.state = 'finish'"
      results.each do |result|
        r_split = "'#{result}'".split('\n').select{|item| item.match /coins/}
        next if r_split == nil
        share_photo_coins += r_split.first.split(':').last.strip.to_i
      end

      income_id = etl.query "select id from dim_income_sources where name = 'share_photo' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{share_photo_coins})"

      ##add get_by_share_home
      share_home_coins = 0
      results = CONNECTION_BERNARD.query "select quest_templates.award from users
      join #{quests_table} on #{quests_table}.user_id = users.id
      join quest_templates on #{quests_table}.quest_template_id = quest_templates.id
      where date(#{quests_table}.taken_at)='#{date_name}' and quest_templates.code = 'tools_share' and #{quests_table}.state = 'finish'"
      results.each do |result|
         r_split = "'#{result}'".split('\n').select{|item| item.match /coins/}
        next if r_split == nil
        share_home_coins += r_split.first.split(':').last.strip.to_i
      end

      income_id = etl.query "select id from dim_income_sources where name = 'share_home' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{share_home_coins})"

       ##add get_by_share_racing
      share_racing_coins = 0
      results = CONNECTION_BERNARD.query "select quest_templates.award from users
      join #{quests_table} on #{quests_table}.user_id = users.id
      join quest_templates on #{quests_table}.quest_template_id = quest_templates.id
      where date(#{quests_table}.taken_at)='#{date_name}' and quest_templates.code = 'racing_share' and #{quests_table}.state = 'finish'"
      results.each do |result|
        r_split = "'#{result}'".split('\n').select{|item| item.match /coins/}
        next if r_split == nil
        share_racing_coins += r_split.first.split(':').last.strip.to_i
      end

      income_id = etl.query "select id from dim_income_sources where name = 'share_racing' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{share_racing_coins})"

      ##add get_by_share_pop
      share_pop_coins = 0
      results = CONNECTION_BERNARD.query "select quest_templates.award from users
      join #{quests_table} on #{quests_table}.user_id = users.id
      join quest_templates on #{quests_table}.quest_template_id = quest_templates.id
      where date(#{quests_table}.taken_at)='#{date_name}' and quest_templates.code = 'pop_share' and #{quests_table}.state = 'finish'"
      results.each do |result|
        r_split = "'#{result}'".split('\n').select{|item| item.match /coins/}
        next if r_split == nil
        share_pop_coins += r_split.first.split(':').last.strip.to_i
      end

      income_id = etl.query "select id from dim_income_sources where name = 'share_pop' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{share_pop_coins})"

      ##get_by_first_team teamup_friend
      teamup_friend_coins = 0
      results = CONNECTION_BERNARD.query "select quest_templates.award from users
      join #{quests_table} on #{quests_table}.user_id = users.id
      join quest_templates on #{quests_table}.quest_template_id = quest_templates.id
      where date(#{quests_table}.taken_at)='#{date_name}' and quest_templates.code = 'teamup_friend' and #{quests_table}.state = 'finish'"
      results.each do |result|
        r_split = "'#{result}'".split('\n').select{|item| item.match /coins/}
        next if r_split == nil
        teamup_friend_coins += r_split.first.split(':').last.strip.to_i
      end

      income_id = etl.query "select id from dim_income_sources where name = 'first_team' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{teamup_friend_coins})"

      ##get_by_first_bind_sns
      bind_social_network_coins = 0
      results = CONNECTION_BERNARD.query "select quest_templates.award from users
      join #{quests_table} on #{quests_table}.user_id = users.id
      join quest_templates on #{quests_table}.quest_template_id = quest_templates.id
      where date(#{quests_table}.taken_at)='#{date_name}' and quest_templates.code = 'bind_social_network' and #{quests_table}.state = 'finish'"
      results.each do |result|
        r_split = "'#{result}'".split('\n').select{|item| item.match /coins/}
        next if r_split == nil
        bind_social_network_coins += r_split.first.split(':').last.strip.to_i
      end

      income_id = etl.query "select id from dim_income_sources where name = 'first_bind_sns' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{bind_social_network_coins})"
    
      ##get_by_first_feedback
      first_feedback_coins = 0
      results = CONNECTION_BERNARD.query "select quest_templates.award from users
      join #{quests_table} on #{quests_table}.user_id = users.id
      join quest_templates on #{quests_table}.quest_template_id = quest_templates.id
      where date(#{quests_table}.taken_at)='#{date_name}' and quest_templates.code = 'first_feedback' and #{quests_table}.state = 'finish'"
      results.each do |result|
        r_split = "'#{result}'".split('\n').select{|item| item.match /coins/}
        next if r_split == nil
        first_feedback_coins += r_split.first.split(':').last.strip.to_i
      end

      income_id = etl.query "select id from dim_income_sources where name = 'first_feedback' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{first_feedback_coins})"

      ##get_by_first_buy_starfish
      purchase_gems_coins = 0
      results = CONNECTION_BERNARD.query "select quest_templates.award from users
      join #{quests_table} on #{quests_table}.user_id = users.id
      join quest_templates on #{quests_table}.quest_template_id = quest_templates.id
      where date(#{quests_table}.taken_at)='#{date_name}' and quest_templates.code = 'purchase_gems' and #{quests_table}.state = 'finish'"
      results.each do |result|
        r_split = "'#{result}'".split('\n').select{|item| item.match /coins/}
        next if r_split == nil
        purchase_gems_coins += r_split.first.split(':').last.strip.to_i
      end

      income_id = etl.query "select id from dim_income_sources where name = 'first_buy_starfish' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{purchase_gems_coins})"

      ##get_by_use_code

      result = CONNECTION_BERNARD.query "select count(*) as count from friends where date(friends.created_at)='#{date_name}' and provider = 'code' and state = 'asked' "
      code_coins = per_coin * (result.first['count'] || 0)
  
      income_id = etl.query "select id from dim_income_sources where name = 'use_code' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{code_coins})"
      ##get_by_code_used
      result = CONNECTION_BERNARD.query "select count(*) as count from friends where date(friends.created_at)='#{date_name}' and provider = 'code' and state = 'answered' "
      code_used_coins = per_coin * (result.first['count'] || 0)
  
      income_id = etl.query "select id from dim_income_sources where name = 'code_used' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_coins) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{code_used_coins})"
  
      ##gems_get_by_iap
      results = CONNECTION_BERNARD.query "select sum(gems) as p_gems from bernard.recharges join bernard.products on products.id = recharges.product_id 
      where date(recharges.created_at) = '#{date_name}' and state = 'completed'"
      income_id = etl.query "select id from dim_income_sources where name = 'iap_gems' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_gems) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{results.first["p_gems"]||0})"

      ##gems_get_by_lottery
      gems = 0
      results = CONNECTION_BERNARD.query "select items.gems, prizes.count from lotteries
      join prizes on lotteries.prize_id=prizes.id 
      join items on prizes.item_id = items.id
      where date(lotteries.created_at)='#{date_name}' and items.tag = 'gems' "
    
      results.each do |result|
        gems += result['gems'].to_i * result['count'].to_i
      end
      income_id = etl.query "select id from dim_income_sources where name = 'lottery_gems' "
        etl.query "insert into fact_user_incomes(dim_game_id, dim_date_id, dim_income_source_id, total_gems) values(
        #{GAME_ID}, #{date_id}, #{income_id.first['id']}, #{gems})"
    end 
  end

  def fact_active_users(start,finish,etl)
    (start..finish).each do |date|
      date_id = date.strftime('%Y%m%d')
      date_name = date.strftime('%Y-%m-%d')
      ### load fact_active_users
      etl.query "insert into fact_active_users(dim_game_id, dim_date_id, today_users, today_logins, total_users) values(
      #{GAME_ID}, #{date_id}, 0, 0, 0)"
    
      # total users rolling today
      total_users = CONNECTION_BERNARD.query "select count(id) as count from users where date(created_at) <= '#{date_name}' and date(created_at) >= '2013-05-01' and status <> 'racing-bot'"
      etl.query "update fact_active_users set total_users=#{total_users.first['count']} where dim_date_id=#{date_id} and dim_game_id=#{GAME_ID}"

      # logins today
      logins = CONNECTION_BERNARD.query "select count(id) as count from users where date(current_sign_in_at) = '#{date_name}' and status <> 'racing-bot'"
      etl.query "update fact_active_users set today_logins=#{logins.first['count']} where dim_date_id=#{date_id} and dim_game_id=#{GAME_ID}"

      # new users today
      users = CONNECTION_BERNARD.query "select count(id) as count from bernard.users where date(created_at) = '#{date_name}' and status <> 'racing-bot'"
      etl.query "update fact_active_users set today_users=#{users.first['count']} where dim_date_id=#{date_id} and dim_game_id=#{GAME_ID}"

      # fb connected users
      fb_users = CONNECTION_BERNARD.query "select count(distinct(uid)) count from bernard.authentications where provider='facebook' and date(created_at) = '#{date_name}'"
      etl.query "update fact_active_users set fb_connected_users=#{fb_users.first['count']} where dim_date_id=#{date_id} and dim_game_id=#{GAME_ID}"

      # yesterday retention
      created_date_name = (date - 1).strftime('%Y-%m-%d')
      created_date_id = (date - 1).strftime('%Y%m%d')
      users = CONNECTION_BERNARD.query "select count(id) as count from bernard.users where date(created_at) = '#{created_date_name}' and date(current_sign_in_at)='#{date_name}' and status <> 'racing-bot'"
      initial_users = etl.query "select today_users as count from shihtzu.fact_active_users where dim_date_id=#{created_date_id} and dim_game_id=#{GAME_ID}"
      etl.query "update fact_active_users set one_days_retention=#{users.first['count']}, one_days_retention_rate=(#{users.first['count']}/#{initial_users.first['count']}) where dim_date_id=#{date_id} and dim_game_id=#{GAME_ID}"

      # there days retention
      created_date_name = (date - 3).strftime('%Y-%m-%d')
      created_date_id = (date - 3).strftime('%Y%m%d')
      users = CONNECTION_BERNARD.query "select count(id) as count from bernard.users where date(created_at) = '#{created_date_name}' and date(current_sign_in_at) = '#{date_name}' and status <> 'racing-bot'"
      initial_users = etl.query "select today_users as count from shihtzu.fact_active_users where dim_date_id=#{created_date_id} and dim_game_id=#{GAME_ID}"
      etl.query "update fact_active_users set three_days_retention=#{users.first['count']}, three_days_retention_rate=(#{users.first['count']}/#{initial_users.first['count']}) where dim_date_id=#{date_id} and dim_game_id=#{GAME_ID}"

      # seven days retention
      created_date_name = (date - 7).strftime('%Y-%m-%d')
      created_date_id = (date - 7).strftime('%Y%m%d')
      users = CONNECTION_BERNARD.query "select count(id) as count from bernard.users where date(created_at) = '#{created_date_name}' and date(current_sign_in_at) = '#{date_name}' and status <> 'racing-bot'"
      initial_users = etl.query "select today_users as count from shihtzu.fact_active_users where dim_date_id=#{created_date_id} and dim_game_id=#{GAME_ID}"
      etl.query "update fact_active_users set seven_days_retention=#{users.first['count']}, seven_days_retention_rate=(#{users.first['count']}/#{initial_users.first['count']}) where dim_date_id=#{date_id} and dim_game_id=#{GAME_ID}"

      # fourteen days retention
      created_date_name = (date - 14).strftime('%Y-%m-%d')
      created_date_id = (date - 14).strftime('%Y%m%d')
      users = CONNECTION_BERNARD.query "select count(id) as count from bernard.users where date(created_at) = '#{created_date_name}' and date(current_sign_in_at) = '#{date_name}' and status <> 'racing-bot'"
      initial_users = etl.query "select today_users as count from shihtzu.fact_active_users where dim_date_id=#{created_date_id} and dim_game_id=#{GAME_ID}"
      etl.query "update fact_active_users set fourteen_days_retention=#{users.first['count']}, fourteen_days_retention_rate=(#{users.first['count']}/#{initial_users.first['count']}) where dim_date_id=#{date_id} and dim_game_id=#{GAME_ID}"

      # thirty days retention
      created_date_name = (date - 30).strftime('%Y-%m-%d')
      created_date_id = (date - 30).strftime('%Y%m%d')
      users = CONNECTION_BERNARD.query "select count(id) as count from bernard.users where date(created_at) = '#{created_date_name}' and date(current_sign_in_at) = '#{date_name}' and status <> 'racing-bot'"
      initial_users = etl.query "select today_users as count from shihtzu.fact_active_users where dim_date_id=#{created_date_id} and dim_game_id=#{GAME_ID}"
      etl.query "update fact_active_users set thirty_days_retention=#{users.first['count']}, thirty_days_retention_rate=(#{users.first['count']}/#{initial_users.first['count']}) where dim_date_id=#{date_id} and dim_game_id=#{GAME_ID}"
    end
  end

  def fact_revenues(start, finish, etl)
    (start..finish).each do |date|
      ### ARPU && ARPPU
      date_id = date.strftime('%Y%m%d')
      date_name = date.strftime('%Y-%m-%d')
      results = CONNECTION_BERNARD.query "select count(distinct(device_token)) as count from bernard.users where date(created_at) >= '#{MONTH_START_DATE}' and date(created_at) < '#{MONTH_END_DATE}'"
      pu = results.first['count'].to_i

      results = CONNECTION_BERNARD.query "select count(distinct(user_id)) as count from bernard.recharges where date(created_at) >= '#{MONTH_START_DATE}' and state = 'completed' and date(created_at) < '#{MONTH_END_DATE}'"
      iu = results.first['count'].to_i

      results = CONNECTION_BERNARD.query "select sum(gross) as income from bernard.recharges join bernard.products on products.id = recharges.product_id where date(created_at) >= '#{MONTH_START_DATE}' and state = 'completed' and date(created_at) < '#{MONTH_END_DATE}'"
      iap_revenue = results.first['income'].to_f
      arppu = iu > 0 ? (iu*UNIT_REVENUE + iap_revenue/iu).to_f : 0.00
      arpu = pu > 0 ? ((pu*UNIT_REVENUE + iap_revenue)/pu).to_f : 0.00

      etl.query "insert into fact_revenues(dim_game_id, dim_date_id, paid_users, iap_users, iap_revenues, month_arpu, month_arppu) values(
        #{GAME_ID}, #{date_id}, #{pu}, #{iu}, #{iap_revenue}, #{arpu}, #{arppu})"
    end
  end

  def fact_racings(start,finish,etl)
    # 100m racings
    (start..finish).each do |date|
      racings_table = get_sharding_table(date, 'racings')
      racing_results_table = get_sharding_table(date, 'racing_results')
      date_id = date.strftime('%Y%m%d')
      date_name = date.strftime('%Y-%m-%d')
      new_matches = CONNECTION_BERNARD.query "select count(id) as count from bernard.#{racings_table} where date(created_at) = '#{date_name}' and state='new'"
      pending_matches = CONNECTION_BERNARD.query "select count(id) as count from bernard.#{racings_table} where date(created_at) = '#{date_name}' and state='matched'"
      finished_matches = CONNECTION_BERNARD.query "select count(id) as count from bernard.#{racings_table} where date(created_at) = '#{date_name}' and state='finished'"
      players = CONNECTION_BERNARD.query "select count(distinct(user_id)) as count from bernard.#{racings_table} where date(created_at) = '#{date_name}'"
      init_wins = CONNECTION_BERNARD.query "select count(id) as count from bernard.#{racing_results_table} where date(created_at) = '#{date_name}' and rank=1"
      init_awards = CONNECTION_BERNARD.query "select sum(coins) as count from bernard.#{racing_results_table} where date(created_at) = '#{date_name}'"
      total_awards = CONNECTION_BERNARD.query "select sum(total_coins) as count from bernard.#{racing_results_table} where date(created_at) = '#{date_name}'"
      etl.query "insert into fact_racings(dim_game_id, dim_date_id, new_matches, pending_matches, finished_matches, players, init_wins, init_awards, total_awards) values(
        #{GAME_ID}, #{date_id}, #{new_matches.first['count']||0}, #{pending_matches.first['count']||0}, #{finished_matches.first['count']||0}, #{players.first['count']||0}, #{init_wins.first['count']||0}, #{init_awards.first['count']||0}, #{total_awards.first['count']||0})"
    end
  end

  def fact_pops(start,finish,etl)
   # pop game
    (start..finish).each do |date|
        pop_results_table = get_sharding_table(date, 'pop_results')
        date_id = date.strftime('%Y%m%d')
        date_name = date.strftime('%Y-%m-%d')
        matches = CONNECTION_BERNARD.query "select count(id) as count from bernard.#{pop_results_table} where date(created_at) = '#{date_name}'"
        points = CONNECTION_BERNARD.query "select sum(points) as count from bernard.#{pop_results_table} where date(created_at) = '#{date_name}'"
        coins = CONNECTION_BERNARD.query "select sum(coins) as count from bernard.#{pop_results_table} where date(created_at) = '#{date_name}'"
        etl.query "insert into fact_pops(dim_game_id, dim_date_id, matches, points, coins) values(
          #{GAME_ID}, #{date_id}, #{matches.first['count']||0}, #{points.first['count']||0}, #{coins.first['count']||0})"      
    end
  end

  def fact_purchases(start,finish,etl)
    (start..finish).each do |date|
      date_id = date.strftime('%Y%m%d')
      date_name = date.strftime('%Y-%m-%d')
      results = CONNECTION_BERNARD.query "select code as name, date(purchases.created_at) as created_date, sum(count) as total_count, sum(total_coins) as total_coins, sum(total_gems) as total_gems from bernard.purchases join bernard.items on items.id = purchases.item_id where date(purchases.created_at) = '#{date_name}' and state = 'completed' group by date(purchases.created_at), item_id"
      results.each do |result|
        # new users today
        etl.query "insert into fact_purchases(dim_game_id, dim_date_id, item, total_count, total_coins, total_gems) values(
        #{GAME_ID}, #{date_id}, '#{result['name']}', #{result['total_count']}, #{result['total_coins']}, #{result['total_gems']})"
      end 

      results = CONNECTION_BERNARD.query "select count(id) as total_count, lotteries.created_at as created_date, sum(lotteries.coins) as total_coins from bernard.lotteries 
      where date(lotteries.created_at) = '#{date_name}' group by date(lotteries.created_at) "
      results.each do |result|
        etl.query "insert into fact_purchases(dim_game_id, dim_date_id, item, total_count, total_coins, total_gems) values(
         #{GAME_ID}, #{date_id}, 'consume_by_lottery', #{result['total_count']}, #{result['total_coins']}, 0)"
      end
    end 
  end

  def fact_iap(start,finish,etl) 
    (start..finish).each do |date|
      date_id = date.strftime('%Y%m%d')
      date_name = date.strftime('%Y-%m-%d') 
      results = CONNECTION_BERNARD.query "select short_name as name, sk_product_id, date(recharges.created_at) as created_date, sum(iap_quantity) as total_count from bernard.recharges join bernard.products on products.id = recharges.product_id where date(recharges.created_at) = '#{date_name}' and state = 'completed' group by date(recharges.created_at), product_id"
      results.each do |result|
        # new users today
        etl.query "insert into fact_iap(dim_game_id, dim_date_id, product, count, product_id) values(
        #{GAME_ID}, #{date_id}, '#{result['name']}', #{result['total_count']}, '#{result['sk_product_id']}')"
      end  
    end 
  end

  start, finish, methods = ARGV
  start_date = Date.parse(start||START_DATE)
  finish_date = Date.parse(finish||FINISH_DATE)
  fact_tables = %w(fact_lotteries fact_user_incomes fact_active_users 
      fact_revenues fact_racings fact_pops fact_purchases fact_iap)

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
    puts "-- done --"
  end
end

# ship it
 etl.run
