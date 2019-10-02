require 'mqtt'
require 'fluent/plugin/input'
require 'fluent/plugin/parser'

module Fluent::Plugin
  class MqttInput < Input
    Fluent::Plugin.register_input('mqtt', self)

    helpers :thread, :inject, :compat_parameters, :parser

    DEFAULT_PARSER_TYPE = 'none'

    config_set_default :include_tag_key, false
    config_set_default :include_time_key, true

    config_param :port, :integer, :default => 1883
    config_param :bind, :string, :default => '127.0.0.1'
    config_param :topics, :array, :default => ['#'], value_type: :string
    config_param :format, :string, :default => DEFAULT_PARSER_TYPE
    config_param :client_id, :string, :default => nil
    config_param :username, :string, :default => nil
    config_param :password, :string, :default => nil
    config_param :ssl, :bool, :default => nil
    config_param :ca, :string, :default => nil
    config_param :key, :string, :default => nil
    config_param :cert, :string, :default => nil

    config_section :parse do
      config_set_default :@type, DEFAULT_PARSER_TYPE
    end

    def configure(conf)
      compat_parameters_convert(conf, :inject, :parser)
      super
      configure_parser(conf)
    end

    def configure_parser(conf)
      @parser = parser_create(usage: 'in_mqtt_parser', type: @format, conf: conf)
    end

    # Return [time (if not available return now), message]
    def parse(message)
      @parser.parse(message) {|time, record|
        return (time || Fluent::Engine.now), record
      }
    end

    def start
      super
      log.debug "start mqtt #{@bind}"
      opts = {host: @bind,
              port: @port}
      opts[:client_id] = @client_id if @client_id
      opts[:username] =  @username if @username
      opts[:password] = @password if @password
      opts[:ssl] = @ssl if @ssl
      opts[:ca_file] = @ca if @ca
      opts[:cert_file] = @cert if @cert
      opts[:key_file] = @key if @key
      @connect = MQTT::Client.connect(opts)
      log.debug "subscribing on topics #{@topics}"
      @topics.each do |topic|
        @connect.subscribe(topic)
      end

      thread_create(:in_mqtt_worker) do
        @connect.get do |topic,message|
          topic.gsub!("/","\.")
          log.debug "#{topic}: #{message}"
          begin
            time, record = self.parse(message)
            record = inject_values_to_record(topic, time, record)
          rescue Exception => e
            log.error e
          end
          emit topic, record, time
        end
      end
    end


    def emit topic, message, time = Fluent::Engine.now
      if message.class == Array
        message.each do |data|
          log.debug "#{topic}: #{data}"
          router.emit(topic , time , data)
        end
      else
        router.emit(topic , time , message)
      end
    end

    def shutdown
      @connect.disconnect
      super
    end
  end
end
