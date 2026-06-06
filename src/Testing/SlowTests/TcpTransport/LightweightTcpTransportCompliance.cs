using JasperFx.Core;
using Wolverine.ComplianceTests;
using Wolverine.ComplianceTests.Compliance;
using Wolverine.Transports.Tcp;
using Wolverine.Util;
using Xunit;

namespace SlowTests.TcpTransport;

public class LightweightTcpFixture : TransportComplianceFixture
{
    public LightweightTcpFixture() : base($"tcp://localhost:{PortFinder.GetAvailablePort()}/incoming".ToUri())
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await SenderIs(opts => { opts.ListenAtPort(PortFinder.GetAvailablePort()); });

        await ReceiverIs(opts => { opts.ListenAtPort(OutboundAddress.Port); });
    }
}

[Collection("compliance")]
public class LightweightTcpTransportCompliance(LightweightTcpFixture fixture)
    : TransportCompliance<LightweightTcpFixture>(fixture);