module Fluent
  class MqttInput < Input
    Plugin.register_input('mqtt', self)

    include Fluent::SetTagKeyMixin
    config_set_default :include_tag_key, false
    
    include Fluent::SetTimeKeyMixin
    config_set_default :include_time_key, true
    
    config_param :port, :integer, :default => 1883
    config_param :bind, :string, :default => '127.0.0.1'
    config_param :topic, :string, :default => '#'
    config_param :username, :string, :default => nil
    config_param :password, :string, :default => nil

    require 'mqtt'

    def configure(conf)
      super
      @bind ||= conf['bind']
      @topic ||= conf['topic']
      @port ||= conf['port']
      @username ||= conf['username']
      @password ||= conf['password']
    end

    def start
      $log.debug "start mqtt host: #{@bind}, port: #{@port}, username: #{@username}, password: #{@password}"
      @connect = MQTT::Client.connect(host: @bind, port: @port,
                                      username: @username, password: @password)
      @connect.subscribe(@topic)

      @thread = Thread.new do
        @connect.get do |topic,message|
          topic.gsub!("/","\.")
          $log.debug "#{topic}: #{message}"
          emit topic, json_parse(message)
        end
      end
    end

    def emit topic, message , time = Fluent::Engine.now
      if message.class == Array
        message.each do |data|
          $log.debug "#{topic}: #{data}"
          Fluent::Engine.emit(topic , time , data)
        end
      else
        Fluent::Engine.emit(topic , time , message)
      end
    end

    def json_parse message
      begin
        y = Yajl::Parser.new
        y.parse(message)
      rescue
        $log.error "JSON parse error", :error => $!.to_s, :error_class => $!.class.to_s
        $log.warn_backtrace $!.backtrace         
      end
    end
    def shutdown
      @thread.kill
      @connect.disconnect
    end
  end
end

