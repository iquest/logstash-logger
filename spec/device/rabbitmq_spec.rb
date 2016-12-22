require 'logstash-logger'
require 'bunny'

describe LogStashLogger::Device::Rabbitmq do
  include_context 'device'

  let(:exchange) { double("Bunny::Exchange") }

  before(:each) do
    allow(Bunny).to receive(:new) { rabbitmq }
    allow(rabbitmq).to receive(:start)
  end

  it "writes to a RabbitMQ" do
    expect(exchange).to receive(:publish)
    rabbitmq_device.write "foo"
  end

  it "defaults the RabbitMQ key to 'logstash'" do
    expect(rabbit_device.key).to eq('logstash')
  end

  it "defaults the RabbitMQ hosts to ['localhost:5672']" do
    expect(rabbitmq_device.hosts).to eq(['localhost:5672'])
  end

  it "defaults the RabbitMQ topic to 'logstash'" do
    expect(rabbitmq_device.topic).to eq('logstash')
  end

  it "defaults the RabbitMQ producer to 'logstash-logger'" do
    expect(rabbitmq_device.producer).to eq('logstash-logger')
  end
end
