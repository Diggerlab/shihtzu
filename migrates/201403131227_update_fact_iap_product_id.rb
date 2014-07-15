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


# set up the ETL
etl = ETL.new(description: "migrate data", connection:  connection)

# configure ETL
etl.etl do |etl|
    ## update fact_iap
    etl.query " update fact_iap set product_id = 'com.diggerlab.bernard.a1' where fact_iap.product = 'Tier 1' " 
    etl.query " update fact_iap set product_id = 'com.diggerlab.bernard.a2' where fact_iap.product = 'Tier 3' "
    etl.query " update fact_iap set product_id = 'com.diggerlab.bernard.a3' where fact_iap.product = 'Tier 5' "
    etl.query " update fact_iap set product_id = 'com.diggerlab.bernard.a4' where fact_iap.product = 'Tier 11' "
    etl.query " update fact_iap set product_id = 'com.diggerlab.bernard.a5' where fact_iap.product = 'Tier 21' "   
end


# ship it
etl.run
