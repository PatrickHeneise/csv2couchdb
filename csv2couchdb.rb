#!/usr/bin/ruby

require 'rubygems'
require 'json' # http://flori.github.com/json/doc/index.html
require 'net/http'

# --- !!! Change this to your database !!! ---
database = "testdb"

module Couch # Full example class available here: http://wiki.apache.org/couchdb/Getting_started_with_Ruby
    
    class Server
        def initialize(host, port, user, password)
            @host = host
            @port = port
            @user = user
            @password = password
        end
                
        def post(uri, json)
            req = Net::HTTP::Post.new(uri)
            req["content-type"] = "application/json"
            req.body = json
            req.basic_auth @user, @password
            request(req)
        end
        
        def request(req)
            res = Net::HTTP.start(@host, @port) { |http|http.request(req) }
            unless res.kind_of?(Net::HTTPSuccess)
                handle_error(req, res)
            end
            res
        end
        
        private
        
        def handle_error(req, res)
            e = RuntimeError.new("#{res.code}:#{res.message}\nMETHOD:#{req.method}\nURI:#{req.path}\n#{res.body}")
            raise e
        end
    end
end

# From specious @ http://railsforum.com/viewtopic.php?pid=64564#p64564
def is_a_number?(s)
    s.to_s.match(/\A[+-]?\d+?(\.\d+)?\Z/) == nil ? false : true 
end

server = Couch::Server.new("127.0.0.1", "5984", "admin", "admin")

counter = 1

begin
    # Read file example used by Michael Williams, http://www.abbeyworkshop.com/howto/ruby/rb-readfile/index.html
    file = File.new("data.csv", "r")
    
    while (line = file.gets)
        if (counter == 1)
            line = line.chomp
            headers = line.split(",")
            counter = counter + 1
        else
            line = line.chomp
            line = line.delete "\""
            values = line.split(",")
            num = 0
            data = ""
            
            headers.each do |value|
                if(is_a_number?(values[num]))
                   data << "\"#{headers[num]}\": #{values[num]},"
                else
                   data << "\"#{headers[num]}\": \"#{values[num]}\","
                end
                num = num + 1
            end
            
            # Remove linebreak and last comma
            data = data[0..-2]
            
            # Add JSON object brackets
            data = data.insert(0, '{')
            data = data.insert(-1, '}')

            obj = JSON data
            counter = counter + 1
        end
        if(obj)
            send = JSON.generate(obj)
            server.post("/#{database}/", send) 
            puts "Importing: #{send}"
        end
    end
    
    puts "#{counter-2} rows successfully imported."
    
    file.close
    
rescue => err
    puts "Exception: #{err}"
    err
end