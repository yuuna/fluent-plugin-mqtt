require 'helper'
require 'fluent/test/driver/input'

class Fluent::Plugin::MqttInput
  #def emit topic, message , time = Fluent::Engine.now
    #if message.class == Array
      #message.each do |data|
        #$log.debug "#{topic}: #{data}"
        #Fluent::Engine.emit(topic, message["t"], data)
      #end
    #else
      #Fluent::Engine.emit(topic, message["t"], message)
    #end
  #end

end

include Fluent::Test::Helpers

class MqttInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

 CONFIG = %[ bind 127.0.0.1
             port 1883
             format json ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::MqttInput).configure(conf)
  end

  def test_configure_1
    d = create_driver(
      %[ bind 127.0.0.1
         port 1883
         format none]
    )
    assert_equal '127.0.0.1', d.instance.bind
    assert_equal 1883, d.instance.port
    assert_equal 'none', d.instance.format

    d2 = create_driver(
      %[ bind 127.0.0.1
         port 1883
         format csv
         keys time,message
         time_key time]
    )
    assert_equal 'csv', d2.instance.format
    assert_equal 'time', d2.instance.time_key
  end

  def test_configure_2
    d = create_driver(
      %[ bind 127.0.0.1
         port 1883
         format csv
         keys time,message ]
    )
    assert_equal '127.0.0.1', d.instance.bind
    assert_equal 1883, d.instance.port
    assert_equal 'csv', d.instance.format
  end

  def sub_client
    connect = MQTT::Client.connect("localhost")
    connect.subscribe('#')
    return connect
  end

  def test_format_json_without_time_key
    d = create_driver(
      %[ bind 127.0.0.1
         port 1883
         format json ]
    )
    time = event_time("2011-01-02 13:14:15 UTC")
    data = [
      {tag: "tag1", message: {"t" => time, "v" => {"a"=>1}}},
      {tag: "tag2", message: {"t" => time, "v" => {"a"=>1}}},
      {tag: "tag3", message: {"t" => time, "v" => {"a"=>32}}},
    ]

    d.run(expect_emits: 3, timeout: 5) do
      data.each do |record|
        send_data record[:tag], record[:message], d.instance.format
      end
    end

    emits = d.events
    assert_equal('tag1', emits[0][0])
    assert_equal({"t" => time, "v" => {"a"=>1}}, emits[0][2])

    assert_equal('tag2', emits[1][0])
    assert_equal({"t" => time, "v" => {"a"=>1}}, emits[1][2])

    assert_equal('tag3', emits[2][0])
    assert_equal({"t" => time, "v" => {"a"=>32}}, emits[2][2])
  end

  def test_format_json_with_time_key
    d = create_driver(
      %[ bind 127.0.0.1
         port 1883
         format json
         time_key t ]
    )
    time = event_time("2011-01-02 13:14:15 UTC")
    data = [
      {tag: "tag1", message: {"t" => time, "v" => {"a"=>1}}},
      {tag: "tag2", message: {"t" => time, "v" => {"a"=>1}}},
      {tag: "tag3", message: {"t" => time, "v" => {"a"=>31}}},
      {tag: "tag3", message: {"t" => time, "v" => {"a"=>32}}},
    ]

    d.run(expect_emits: 4, timeout: 5) do
      data.each do |record|
        send_data record[:tag], record[:message], d.instance.format
      end
    end

    emits = d.events
    assert_equal('tag1', emits[0][0])
    assert_equal(time, emits[0][1])
    assert_equal({"v" => {"a"=>1}}, emits[0][2])
  end

  def test_format_none
    d = create_driver(
      %[ bind 127.0.0.1
         port 1883
         format none]
    )

    data = [
      {tag: "tag1", message: 'hello world'},
      {tag: "tag2", message: 'another world'},
      {tag: "tag3", message: ''},
    ]

    d.run(expect_emits: 3, timeout: 5) do
      data.each do |record|
        send_data record[:tag], record[:message], d.instance.format
      end
    end

    emits = d.events
    time = Fluent::Engine.now
    assert_equal('tag1', emits[0][0])
    assert_equal({'message' => 'hello world'}, emits[0][2])
    assert_equal('tag2', emits[1][0])
    assert_equal({'message' => 'another world'}, emits[1][2])
    assert_equal('tag3', emits[2][0])
    assert_equal({'message' => ''}, emits[2][2])
  end

  def test_format_csv
    d = create_driver(
      %[ bind 127.0.0.1
         port 1883
         format csv
         keys time,message]
    )

    time = event_time("2011-01-02 13:14:15 UTC")
    data = [
      {tag: "tag1", message: "#{time},hello world" },
      {tag: "tag2", message: "#{time},hello to you to" },
      {tag: "tag3", message: "#{time}," },
    ]

    d.run(expect_emits: 3, timeout: 5) do
      data.each do |record|
        send_data record[:tag], record[:message], d.instance.format
      end
    end

    emits = d.events
    #puts 'emits length', emits.length.to_s
    assert_equal('tag1', emits[0][0])
    assert_equal({'time' => time.to_s, 'message' => 'hello world'}, emits[0][2])

    assert_equal('tag2', emits[1][0])
    assert_equal({'time' => time.to_s, 'message' => 'hello to you to'}, emits[1][2])
    assert_equal('tag3', emits[2][0])
    assert_equal({'time' => time.to_s, 'message' => nil}, emits[2][2])
  end

  def test_format_csv_with_time_key
    d = create_driver(
      %[ bind 127.0.0.1
         port 1883
         format csv
         keys time2,message
         time_key time2
         time_format %S]
    )

    time = event_time("2011-01-02 13:14:15 UTC")
    data = [
      {tag: "tag1", message: "#{time},abc" },
      {tag: "tag2", message: "#{time},def" },
      {tag: "tag3", message: "#{time},ghi" },
      {tag: "tag3", message: "#{time}," },
    ]

    d.run(expect_emits: 4, timeout: 5) do
      data.each do |record|
        send_data record[:tag], record[:message], d.instance.format
      end
    end

    emits = d.events
    assert_equal('tag1', emits[0][0])
    assert_equal({'message' => 'abc'}, emits[0][2])

    assert_equal('tag2', emits[1][0])
    assert_equal({'message' => 'def'}, emits[1][2])

    assert_equal('tag3', emits[2][0])
    assert_equal({'message' => 'ghi'}, emits[2][2])

    assert_equal('tag3', emits[3][0])
    assert_equal({'message' => nil}, emits[3][2])
  end

  def send_data tag, record, format
    case format
      when 'none'
        sub_client.publish(tag, record)
      when 'json'
        sub_client.publish(tag, record.to_json)
      when 'csv'
        sub_client.publish(tag, record)
      else
        sub_client.publish(tag, record)
    end
  end
end
