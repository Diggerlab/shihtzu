require 'mysql2'
require 'date'
require 'dotenv'
Dotenv.load

GAME_ID = ENV['GAME_ID']
connection = Mysql2::Client.new host: ENV['DW_HOST'],
                                username: ENV['DW_USER'],
                                password: ENV['DW_PASSWORD'],
                                database: ENV['DW_DB'], 
                                reconnect: true

SCHEDULER.every '10s' do
  today_date_id = Time.now.strftime("%Y%m%d")
  results = connection.query "select total_users, today_users, today_logins from fact_active_users where dim_date_id = #{today_date_id} and dim_game_id=#{GAME_ID}"
  # results_login = connection.query "select max(today_logins) as max_logins from fact_active_users"
  if results.count > 0 
  	total_users = results.first['total_users']
  	today_users = results.first['today_users']
  	today_logins = results.first['today_logins']
  	# max_logins = results_login.first['max_logins']
  end
  send_event 'today_login', {value: today_logins}
  send_event 'today_users', {current: today_users}
  send_event 'total_users', {current: total_users, moreinfo: "value = #{total_users}"}

  # Populate daily users
  results = connection.query "select today_users, DATE_FORMAT(full_date, '%Y-%m-%d') as full_date from fact_active_users f join dim_dates d on d.id=f.dim_date_id where dim_game_id=#{GAME_ID} order by dim_date_id desc limit 10"
  points = []
  results.each do |row|
	  points << { x: Date.parse(row['full_date']).to_time.to_i, y: row['today_users'] }
	end
	send_event('today_users', points: points.reverse)

  # Populate retention
  results = connection.query "select one_days_retention, three_days_retention, seven_days_retention, DATE_FORMAT(full_date, '%Y-%m-%d') as full_date from fact_active_users f join dim_dates d on d.id=f.dim_date_id where dim_game_id=#{GAME_ID} order by dim_date_id desc limit 10"
  points1 = points2 = points3 = []
  results.each do |row|
    points1 << { x: Date.parse(row['full_date']).to_time.to_i, y: row['one_days_retention'] }
    points2 << { x: Date.parse(row['full_date']).to_time.to_i, y: row['three_days_retention'] }
    points3 << { x: Date.parse(row['full_date']).to_time.to_i, y: row['seven_days_retention'] }
  end
  send_event('retention', {points_one: points1.reverse, points_two: points2.reverse, points_three: points3.reverse})

	# populate items sales
	results = connection.query "select sum(total_count) as total, item from fact_purchases where dim_game_id=#{GAME_ID} group by item order by total desc limit 30"
	sales = []
	results.each do |row|
		sales << {label: row['item'], value: row['total'].to_i}
	end
	send_event('items', { items: sales })
  
  # populate IAP
  results = connection.query "select sum(count) as total, product from fact_iap where dim_game_id=#{GAME_ID} group by product order by total desc limit 10"
  sales = []
  results.each do |row|
    sales << {label: row['product'], value: row['total'].to_i}
  end
  send_event('iap', { items: sales })

  #arpu 
  results = connection.query "select month_arpu, month_arppu, iap_users, paid_users from fact_revenues where dim_date_id = #{today_date_id} and dim_game_id=#{GAME_ID}"
  if results.count > 0 
    month_arpu = results.first['month_arpu']
    month_arppu = results.first['month_arppu']
    send_event 'month_arpu', {current: month_arpu.round(2), moreinfo: "Per month - #{Time.now.strftime('%Y/%m')}"}
    send_event 'month_arppu', {current: month_arppu.round(2), moreinfo: "Per month - #{Time.now.strftime('%Y/%m')}"}

  end

  
  # account balance
  results = connection.query "select gems_balance, coins_balance, gems_exchange from fact_accounts where dim_date_id = #{today_date_id} and dim_game_id=#{GAME_ID}"
  if results.count > 0 
    gems_balance = results.first['gems_balance']
    coins_balance = results.first['coins_balance']
    gems_exchange = results.first['gems_exchange']

  end
  send_event 'gems_balance', {current: gems_balance}
  send_event 'coins_balance', {current: coins_balance}
  send_event 'gems_exchange', {value: gems_exchange}

  results = connection.query "select gems_expense, coins_expense, DATE_FORMAT(full_date, '%Y-%m-%d') as full_date from fact_accounts f join dim_dates d on d.id=f.dim_date_id where dim_game_id=#{GAME_ID} order by dim_date_id desc limit 10"
  points = []
  results.each do |row|
    points << { x: Date.parse(row['full_date']).to_time.to_i, y: row['coins_expense'] }
  end
  send_event('coins_expenses', points: points.reverse)

  # daily racing total coins
  results = connection.query "select total_awards, DATE_FORMAT(full_date, '%Y-%m-%d') as full_date from fact_racings f join dim_dates d on d.id=f.dim_date_id where dim_game_id=#{GAME_ID} order by dim_date_id desc limit 10"
  points = []
  results.each do |row|
    points << { x: Date.parse(row['full_date']).to_time.to_i, y: row['total_awards'] }
  end
  send_event('racing_coins', points: points.reverse)

  # daily pop total coins
  results = connection.query "select coins, DATE_FORMAT(full_date, '%Y-%m-%d') as full_date from fact_pops f join dim_dates d on d.id=f.dim_date_id where dim_game_id=#{GAME_ID} order by dim_date_id desc limit 10"
  points = []
  results.each do |row|
    points << { x: Date.parse(row['full_date']).to_time.to_i, y: row['coins'] }
  end
  send_event('pop_coins', points: points.reverse)

end