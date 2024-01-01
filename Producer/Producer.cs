using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

IConfiguration configuration = new ConfigurationBuilder()
    .AddInMemoryCollection()
    .Build();

configuration["bootstrap.servers"] = "kafka:9092";

const string topic = "purchases";
const int messagesToProduceCount = 10;

string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };

using var producer = new ProducerBuilder<string, string>(configuration.AsEnumerable()).Build();

var producedMessagesCount = 0;

for (int i = 0; i < messagesToProduceCount; ++i)
{
    var user = users[Random.Shared.Next(users.Length)];
    var item = items[Random.Shared.Next(items.Length)];

    producer.Produce(topic, new Message<string, string> { Key = user, Value = item },
        (deliveryReport) =>
        {
            if (deliveryReport.Error.Code != ErrorCode.NoError)
            {
                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
            }
            else
            {
                Console.WriteLine($"Produced event to topic {topic}: key = {user,-10} value = {item}");
                producedMessagesCount += 1;
            }
        });
}

producer.Flush(TimeSpan.FromSeconds(30));

Console.WriteLine($"{producedMessagesCount} messages were produced to topic {topic}");
