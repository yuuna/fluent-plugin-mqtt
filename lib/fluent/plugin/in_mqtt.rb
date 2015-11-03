module Fluent
  class MqttInput < Input
    Plugin.register_input('mqtt', self)

    #include Fluent::SetTagKeyMixin
    #config_set_default :include_tag_key, false
    #
    #include Fluent::SetTimeKeyMixin
    #config_set_default :include_time_key, true
    
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
      configure_parser(conf)
    end

    def configure_parser(conf)
      @parser = Plugin.new_parser(conf['format'])
      @parser.configure(conf)
    end

    def start
      $log.debug "start mqtt host: #{@bind}, port: #{@port}, username: #{@username}, password: #{@password}"
      @connect = MQTT::Client.connect(host: @bind, port: @port,
                                      username: @username, password: @password)
      @connect.subscribe(@topic)

      @thread = Thread.new do
        @connect.get do |topic,message|
          #topic.gsub!("/","\.")
          $log.debug "#{topic}: #{message}"
          emit topic, message
        end
      end
    end

    def emit topic, message, time = Fluent::Engine.now
      begin
        @parser.parse(message) {|time, record|
          $log.debug "#{time}, #{record}"
          router.emit(topic, time, record)
        }
      rescue => e
        $log.warn :error => e.to_s
        $log.debug_backtrace(e.backtrace)
      end
    end

    def shutdown
      @thread.kill
      @connect.disconnect
    end
  end
end

