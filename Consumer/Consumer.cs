using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

IConfiguration configuration = new ConfigurationBuilder()
    .AddInMemoryCollection()
    .Build();

configuration["bootstrap.servers"] = "kafka:9092";
configuration["group.id"] = "kafka-dotnet-getting-started";
configuration["auto.offset.reset"] = "earliest";

const string topic = "purchases";

CancellationTokenSource cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // prevent the process from terminating.
    cts.Cancel();
};

using var consumer = new ConsumerBuilder<string, string>(configuration.AsEnumerable()).Build();

consumer.Subscribe(topic);

try
{
    while (true)
    {
        var cr = consumer.Consume(cts.Token);
        Console.WriteLine($"Consumed event from topic {topic}: key = {cr.Message.Key,-10} value = {cr.Message.Value}");
    }
}
catch (OperationCanceledException)
{
    // Ctrl-C was pressed.
}
finally
{
    consumer.Close();
}
