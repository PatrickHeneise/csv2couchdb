#!/usr/bin/ruby

require 'rubygems'
require 'json' # http://flori.github.com/json/doc/index.html
require 'net/http'
require 'csv'

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

server = Couch::Server.new("127.0.0.1", "5984", "admin", "admin")

begin
    
    csv_data = CSV.read 'data.csv', :encoding => "UTF-8"
    headers = csv_data.shift.map {|i| i.to_s }
    string_data = csv_data.map {|row| row.map {|cell| cell.to_s } }
    array_of_hashes = string_data.map {|row| Hash[*headers.zip(row).flatten] }
    
    array_of_hashes.each do |row|
      row.delete_if{ |key,value| value.empty?}
      server.post("/#{database}/", row.to_json) 
    end
    
rescue => err
    puts "Exception: #{err}"
    err
end