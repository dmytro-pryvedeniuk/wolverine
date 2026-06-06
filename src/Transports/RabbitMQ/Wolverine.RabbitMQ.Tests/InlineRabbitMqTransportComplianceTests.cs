using JasperFx.Core;
using Wolverine.ComplianceTests.Compliance;
using Xunit;

namespace Wolverine.RabbitMQ.Tests;

public class InlineRabbitMqTransportFixture : TransportComplianceFixture
{
    public InlineRabbitMqTransportFixture() : base($"rabbitmq://queue/{RabbitTesting.NextQueueName()}".ToUri())
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        var queueName = RabbitTesting.NextQueueName() + "_inline";
        OutboundAddress = $"rabbitmq://queue/{queueName}".ToUri();

        await SenderIs(opts =>
        {
            var listener = RabbitTesting.NextQueueName() + "_inline";

            opts
                .ListenToRabbitQueue(listener)
                .ProcessInline().TelemetryEnabled(false);

            opts.UseRabbitMq().AutoProvision().AutoPurgeOnStartup();
        });

        await ReceiverIs(opts =>
        {
            opts.UseRabbitMq().AutoProvision().AutoPurgeOnStartup();

            opts.ListenToRabbitQueue(queueName).ProcessInline().TelemetryEnabled(false);
        });
    }
}

public class InlineRabbitMqTransportComplianceTests(InlineRabbitMqTransportFixture fixture) 
    : TransportCompliance<InlineRabbitMqTransportFixture>(fixture);