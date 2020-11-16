using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace producer_kafka_case_bari
{
    class Program
    {
        public static async Task Main()
        {
            var config = new ProducerConfig { BootstrapServers = "127.0.0.1:9092" };

            using var p = new ProducerBuilder<Null, string>(config).Build();

            {
                try
                {
                    var count = 0;
                    while (true)
                    {
                        var dr = await p.ProduceAsync("test-topic", 
                            new Message<Null, string> { Value = $"Hello World: {count++} - Enviado em: {DateTime.Now}" });

                        Console.WriteLine($"'{dr.Value}' para '{dr.TopicPartitionOffset} | {count}'");

                        Thread.Sleep(5000);
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Falha ao enviar: {e.Error.Reason}");
                }
            }
        }
    }
}
