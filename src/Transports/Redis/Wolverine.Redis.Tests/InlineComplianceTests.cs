using System;
using System.Threading.Tasks;
using JasperFx.Core;
using Wolverine.ComplianceTests.Compliance;
using Wolverine.Configuration;
using Wolverine.Redis;
using Xunit;

namespace Wolverine.Redis.Tests;

public class RedisInlineComplianceFixture : TransportComplianceFixture
{
    public RedisInlineComplianceFixture() : base(new Uri("redis://stream/0/receiver"), 120)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        var receiverStream = $"wolverine-tests-inline-receiver-{Guid.NewGuid():N}";
        OutboundAddress = new Uri($"redis://stream/0/{receiverStream}");

        await ReceiverIs(opts =>
        {
            opts.UseRedisTransport(RedisContainerFixture.ConnectionString).AutoProvision();
            opts.ListenToRedisStream(receiverStream, "g1").ProcessInline().StartFromBeginning();
        });

        await SenderIs(opts =>
        {
            opts.UseRedisTransport(RedisContainerFixture.ConnectionString).AutoProvision();
            opts.PublishAllMessages().ToRedisStream(receiverStream).SendInline();
        });
    }
}

public class InlineSendingAndReceivingCompliance(RedisInlineComplianceFixture fixture)
    : TransportCompliance<RedisInlineComplianceFixture>(fixture);

