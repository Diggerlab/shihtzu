require 'dashing'
require 'dotenv'
Dotenv.load

configure do
  set :auth_token, ENV['AUTH_TOKEN']
  set :default_dashboard, 'index'

  helpers do
      def protected!
		    unless authorized?
		      response['WWW-Authenticate'] = %(Basic realm="Restricted Area")
		      throw(:halt, [401, "Not authorized\n"])
		    end
		  end

		  def authorized?
		    @auth ||=  Rack::Auth::Basic::Request.new(request.env)
		    @auth.provided? && @auth.basic? && @auth.credentials && 
          @auth.credentials == [ENV['LOGIN_USER'], ENV['LOGIN_PASSWORD']]
		  end

  end
end

map Sinatra::Application.assets_prefix do
  run Sinatra::Application.sprockets
end

run Sinatra::Application