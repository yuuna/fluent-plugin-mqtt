require_relative '../helper'

class MqttOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[ bind 127.0.0.1
              port 1883
              format json ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::OutMqtt).configure(conf)
  end

  def sub_client(topic = "td-agent/#")
    connect = MQTT::Client.connect("localhost")
    connect.subscribe(topic)
    return connect
  end

  def test_configure_1
    d = create_driver(
      %[ bind 127.0.0.1
         port 1883
         format json]
    )
    assert_equal '127.0.0.1', d.instance.bind
    assert_equal 1883, d.instance.port
    assert_equal 'json', d.instance.instance_variable_get(:@format)

    d2 = create_driver(
      %[ bind 127.0.0.1
         port 1883
         format csv
         fields time,message
         time_key time]
    )
    assert_equal 'csv', d2.instance.instance_variable_get(:@format)
    assert_equal 'time', d2.instance.inject_config.time_key
  end

  def test_format_csv
    d = create_driver(
      %[ bind 127.0.0.1
         port 1883
         format csv
         fields time,message]
    )

    client = sub_client
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    data = [
      {tag: "tag1", message: "#{time},hello world" },
      {tag: "tag2", message: "#{time},hello to you to" },
      {tag: "tag3", message: "#{time}," },
    ]

    d.run do
      data.each do |record|
        d.emit(record, time)
      end
    end
    3.times do |i|
      record = client.get
      assert_equal "td-agent", record[0]
      assert_equal "\"2011-01-02T13:14:15Z\",\"#{data[i][:message]}\"\n", record[1]
    end

  end
end
