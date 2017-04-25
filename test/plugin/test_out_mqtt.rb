require_relative '../helper'
require 'fluent/test/driver/output'

class MqttOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[ bind 127.0.0.1
              port 1883
              format json ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::OutMqtt).configure(conf)
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

  class TestWithTimeZone < self
    def setup
      @timeZone = ENV['TZ']
    end
    def teardown
      ENV['TZ'] = @timeZone
    end

    def test_format_csv
      ENV['TZ'] = 'Asia/Tokyo'

      d = create_driver(
        %[ bind 127.0.0.1
           port 1883
           format csv
           time_type string
           time_format %Y-%m-%dT%H:%M:%S%z
           fields time,message]
      )

      client = sub_client
      time = event_time("2011-01-02 13:14:15 UTC")
      data = [
        {tag: "tag1", message: "#{time},hello world" },
        {tag: "tag2", message: "#{time},hello to you to" },
        {tag: "tag3", message: "#{time}," },
      ]

      d.run(default_tag: "test") do
        data.each do |record|
          d.feed(time, record)
        end
      end
      3.times do |i|
        record = client.get
        assert_equal "td-agent", record[0]
        assert_equal "\"2011-01-02T22:14:15+0900\",\"#{data[i][:message]}\"\n", record[1]
      end
    end

    def test_format_json
      ENV['TZ'] = 'Asia/Tokyo'

      d = create_driver(
        %[ bind 127.0.0.1
           port 1883
           format json
           time_type string
           time_format %Y-%m-%dT%H:%M:%S%z
           fields time,message]
      )

      client = sub_client
      time = event_time("2011-01-02 13:14:15 UTC")
      data = [
        {tag: "tag1", message: "#{time},hello world" },
        {tag: "tag2", message: "#{time},hello to you to" },
        {tag: "tag3", message: "#{time}," },
      ]

      d.run(default_tag: "test") do
        data.each do |record|
          d.feed(time, record)
        end
      end
      3.times do |i|
        record = client.get
        assert_equal "td-agent", record[0]
        assert_equal "{\"tag\":\"#{data[i][:tag]}\",\
\"message\":\"#{data[i][:message]}\",\
\"time\":\"2011-01-02T22:14:15+0900\"}\n", record[1]
      end
    end
  end
end
