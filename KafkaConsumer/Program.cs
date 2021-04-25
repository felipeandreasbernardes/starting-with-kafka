using System;
using System.Threading;
using Confluent.Kafka;

namespace KafkaConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("quickstart-events");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);
                            System.Console.WriteLine($"Consumed message: '{cr.Value}' at: '{cr.TopicPartitionOffset}.'");
                        }
                        catch (ConsumeException ex)
                        {
                            Console.WriteLine($"Error occured: {ex.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException ex)
                {
                    consumer.Close();
                }
            }
        }
    }
}
