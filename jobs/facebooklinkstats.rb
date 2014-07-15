#!/usr/bin/env ruby
require 'net/http'
require 'json'
 
# The url you are tracking
sharedlink = URI::encode('chichiapp.com')
 
SCHEDULER.every '10m', :first_in => 0 do
  fbstat = []
 
  http = Net::HTTP.new('graph.facebook.com')
  response = http.request(Net::HTTP::Get.new("/fql?q=SELECT%20share_count,%20like_count,%20comment_count,%20total_count%20FROM%20link_stat%20WHERE%20url=%22#{sharedlink}%22"))
  fbcounts = JSON.parse(response.body)['data']
 
  fbcounts[0].each do |stat|
    fbstat << {:label=>stat[0], :value=>stat[1]}
  end
 
	send_event('fblinkstat', { items: fbstat })
 
end