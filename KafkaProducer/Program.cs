using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaProducer
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    while (true)
                    {
                        System.Console.WriteLine("Insira uma mensagem para ser publicada!");
                        var message = Console.ReadLine();
                        var dr = await producer.ProduceAsync("quickstart-events", new Message<Null, string> { Value = message });
                        System.Console.WriteLine($"Delivered: '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                    }
                }
                catch (ProduceException<Null, string> ex)
                {
                    Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
                }
            }
        }
    }
}
