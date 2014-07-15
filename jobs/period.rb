require 'mysql2'
require 'date'
require 'dotenv'
Dotenv.load

connection = Mysql2::Client.new host: ENV['DW_HOST'],
                                username: ENV['DW_USER'],
                                password: ENV['DW_PASSWORD'],
                                database: ENV['DW_DB'], 
                                reconnect: true

IAP_HASH = {"com.diggerlab.bernard.a1" => '1', "com.diggerlab.bernard.a2" => '3', "com.diggerlab.bernard.a3" => '5', "com.diggerlab.bernard.a4" => '11', "com.diggerlab.bernard.a5" => '21'}
SCHEDULER.every '1m' do
  FINISH = Time.now.to_date - 1
  START = FINISH - 6
  ##new_user
  start_date_id = START.strftime('%Y%m%d')
  finish_date_id = FINISH.strftime('%Y%m%d')
  results = connection.query "select today_users, DATE_FORMAT(dim_date_id, '%Y-%m-%d') as full_date from fact_active_users where dim_date_id >= #{start_date_id} and dim_date_id <= #{finish_date_id} and dim_game_id=#{GAME_ID} order by dim_date_id desc"
  points = []
  results.each do |row|
    points << { x: Date.parse(row['full_date']).to_time.to_i, y: row['today_users'] }
  end
  send_event('weekly_new_user', points: points.reverse)
 ##active_user
  results = connection.query "select today_logins, DATE_FORMAT(dim_date_id, '%Y-%m-%d') as full_date from fact_active_users where dim_date_id >= #{start_date_id} and dim_date_id <= #{finish_date_id} and dim_game_id=#{GAME_ID} order by dim_date_id desc"
  points = []
  results.each do |row|
    points << { x: Date.parse(row['full_date']).to_time.to_i, y: row['today_logins'] }
  end
  send_event('weekly_login_user', points: points.reverse)

  # populate IAP
  points = []
  (START..FINISH).each do |date|
    total = 0
    start_date_id = date.strftime('%Y%m%d')
    full_date_id = date.strftime('%Y-%m-%d')
    results = connection.query "select count, product_id, DATE_FORMAT(dim_date_id, '%Y-%m-%d') as full_date from fact_iap where dim_date_id = #{start_date_id} and dim_game_id=#{GAME_ID} group by product_id order by dim_date_id desc"

    results.each do |row| 
      total += IAP_HASH["#{row['product_id']}"].to_i * row['count'].to_i
    end
    points << { x: Date.parse(full_date_id).to_time.to_i, y: total}
  end
 
  send_event('weekly_iap', points: points, displayValue: points.first["y"])
end
