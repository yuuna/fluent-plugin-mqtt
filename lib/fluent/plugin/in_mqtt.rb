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
    config_param :format, :string, :default => 'none'

    require 'mqtt'

    def configure(conf)
      super
      @bind ||= conf['bind']
      @topic ||= conf['topic']
      @port ||= conf['port']

      configure_parser(conf)
    end

    def configure_parser(conf)
      @parser = Plugin.new_parser(@format)
      @parser.configure(conf)
    end

    def start
      $log.debug "start mqtt #{@bind}"
      @connect = MQTT::Client.connect({remote_host: @bind, remote_port: @port})
      @connect.subscribe(@topic)

      @thread = Thread.new do
        @connect.get do |topic,message|
          topic.gsub!("/","\.")
          $log.debug "#{topic}: #{message}"
          emit topic, @parser.parse(message)[1]
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

    def shutdown
      @thread.kill
      @connect.disconnect
    end
  end
end

