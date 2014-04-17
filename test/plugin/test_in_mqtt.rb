require 'helper'

class Fluent::MqttInput 
  def emit topic, message , time = Fluent::Engine.now
    if message.class == Array
      message.each do |data|
        $log.debug "#{topic}: #{data}"
        Fluent::Engine.emit(topic, message["t"], data)
      end
    else
      Fluent::Engine.emit(topic, message["t"], message)
    end
  end

end

class MqttInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end


 CONFIG = %[
  ]

  def create_driver(conf = CONFIG) 
    Fluent::Test::InputTestDriver.new(Fluent::MqttInput).configure(conf)
  end
  
  def test_configure
    d = create_driver(
      %[ bind 127.0.0.1
         port 1300 ] 
    )
    assert_equal '127.0.0.1', d.instance.bind
    assert_equal 1300, d.instance.port
  end


  def sub_client
    connect = MQTT::Client.connect
    connect.subscribe('#')
    return connect
  end


  def test_client
    d = create_driver
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i    
    d.expect_emit "tag1", time, {"t" => time, "v" => {"a"=>1}}
    d.expect_emit "tag2", time, {"t" => time, "v" => {"a"=>2}}
    d.expect_emit "tag3", time, {"t" => time, "v" => {"a"=>31}}
    d.expect_emit "tag3", time, {"t" => time, "v" => {"a"=>32}}

    d.run do
      d.expected_emits.each {|tag,time,record|
        send_data tag, time, record
      }
      send_data "tag3", time , [{"t" => time, "v" => {"a"=>31}} , {"t" => time, "v" => {"a"=>32}}] 
      sleep 0.5
    end

  end

  def send_data tag, time, record
    sub_client.publish(tag, record.to_json)
  end
end
