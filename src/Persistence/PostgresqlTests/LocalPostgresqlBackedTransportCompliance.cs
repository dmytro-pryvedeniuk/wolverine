using IntegrationTests;
using JasperFx.Core;
using Wolverine.ComplianceTests.Compliance;
using Wolverine;
using Wolverine.Postgresql;

namespace PostgresqlTests;

public class LocalPostgresqlBackedFixture : TransportComplianceFixture
{
    public LocalPostgresqlBackedFixture() : base("local://one/durable".ToUri())
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await TheOnlyAppIs(opts =>
        {
            opts.PersistMessagesWithPostgresql(Servers.PostgresConnectionString);
            opts.Durability.Mode = DurabilityMode.Solo;
        });
    }
}

[Collection("marten")]
public class LocalPostgresqlBackedTransportCompliance(LocalPostgresqlBackedFixture fixture)
    : TransportCompliance<LocalPostgresqlBackedFixture>(fixture);