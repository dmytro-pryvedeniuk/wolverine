using IntegrationTests;
using JasperFx.Core;
using JasperFx.Resources;
using Shouldly;
using Wolverine.ComplianceTests.Compliance;
using Wolverine.Configuration;
using Wolverine.Postgresql;
using Wolverine.Tracking;

namespace PostgresqlTests.Transport;

public class PostgresqlTransportDurableFixture : TransportComplianceFixture
{
    public PostgresqlTransportDurableFixture() : base("postgresql://receiver".ToUri(), 10)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await SenderIs(opts =>
        {
            opts.UsePostgresqlPersistenceAndTransport(Servers.PostgresConnectionString, "durable", transportSchema:"durable")
                .AutoProvision().AutoPurgeOnStartup();

            opts.ListenToPostgresqlQueue("sender");
            opts.Policies.UseDurableOutboxOnAllSendingEndpoints();
            
            opts.Durability.ScheduledJobPollingTime = 250.Milliseconds();
            opts.Durability.ScheduledJobFirstExecution = 0.Seconds();
        });

        await ReceiverIs(opts =>
        {
            opts.UsePostgresqlPersistenceAndTransport(Servers.PostgresConnectionString, "durable", transportSchema:"durable");

            opts.ListenToPostgresqlQueue("receiver").UseDurableInbox();
            
            opts.Durability.ScheduledJobPollingTime = 250.Milliseconds();
            opts.Durability.ScheduledJobFirstExecution = 0.Seconds();
        });
    }
}

[Collection("marten")]
public class PostgresqlTransport_Durable_Compliance(PostgresqlTransportDurableFixture fixture) 
    : TransportCompliance<PostgresqlTransportDurableFixture>(fixture);

public class PostgresqlTransportBufferedFixture : TransportComplianceFixture
{
    public PostgresqlTransportBufferedFixture() : base("postgresql://receiver".ToUri(), 10)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await SenderIs(opts =>
        {
            opts.UsePostgresqlPersistenceAndTransport(Servers.PostgresConnectionString, "buffered_compliance")
                .AutoProvision().AutoPurgeOnStartup().DisableInboxAndOutboxOnAll();

            #region sample_setting_postgres_queue_to_buffered
            opts.ListenToPostgresqlQueue("sender").BufferedInMemory();

            #endregion
            
            opts.Durability.ScheduledJobPollingTime = 250.Milliseconds();
            opts.Durability.ScheduledJobFirstExecution = 0.Seconds();

            opts.Services.AddResourceSetupOnStartup();

        });

        await ReceiverIs(opts =>
        {
            opts.UsePostgresqlPersistenceAndTransport(Servers.PostgresConnectionString, "buffered_compliance")
                .AutoProvision().AutoPurgeOnStartup().DisableInboxAndOutboxOnAll();

            opts.ListenToPostgresqlQueue("receiver").BufferedInMemory();
            
            opts.Durability.ScheduledJobPollingTime = 250.Milliseconds();
            opts.Durability.ScheduledJobFirstExecution = 0.Seconds();
            
            opts.Services.AddResourceSetupOnStartup();
        });
    }
}

[Collection("sqlserver")]
public class PostgresqlTransport_Buffered_Compliance(PostgresqlTransportBufferedFixture fixture) 
    : TransportCompliance<PostgresqlTransportBufferedFixture>(fixture)
{
    [Fact]
    public void endpoints_are_all_buffered()
    {
        theReceiver.GetRuntime().Endpoints.GetOrBuildSendingAgent(new Uri("postgresql://sender")).Endpoint.Mode.ShouldBe(EndpointMode.BufferedInMemory);
    }
}