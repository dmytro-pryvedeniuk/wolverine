using System.Net;
using JasperFx.Core;
using Microsoft.Extensions.Logging;
using Wolverine.Configuration;
using Wolverine.Runtime;
using Wolverine.Transports.Sending;

namespace Wolverine.Transports.Tcp;

public class TcpEndpoint : Endpoint
{
    public TcpEndpoint() : this("localhost", 2000)
    {
    }

    public TcpEndpoint(int port) : this("localhost", port)
    {
    }

    public TcpEndpoint(string hostName, int port) : base(ToUri(port, hostName), EndpointRole.Application)
    {
        HostName = hostName;
        Port = port;

        // ReSharper disable once VirtualMemberCallInConstructor
        EndpointName = Uri.ToString();
        BrokerRole = "socket";
    }

    public string HostName { get; }

    public int Port { get; }

    protected override bool supportsMode(EndpointMode mode)
    {
        return mode != EndpointMode.Inline;
    }

    public static Uri ToUri(int port, string hostName = "localhost")
    {
        return $"tcp://{hostName}:{port}".ToUri();
    }

    public override ValueTask<IListener> BuildListenerAsync(IWolverineRuntime runtime, IReceiver receiver)
    {
        // check the uri for an ip address to bind to
        var cancellation = runtime.DurabilitySettings.Cancellation;

        var hostNameType = Uri.CheckHostName(HostName);

        var logger = runtime.LoggerFactory.CreateLogger<TcpEndpoint>();

        if (hostNameType != UriHostNameType.IPv4 && hostNameType != UriHostNameType.IPv6)
        {
#pragma warning disable IDISP001 // Dispose created
            var listener = HostName == "localhost"
                ? new SocketListener(this, receiver, logger, IPAddress.Loopback, Port, cancellation)
                : new SocketListener(this, receiver, logger, IPAddress.Any, Port, cancellation);
#pragma warning restore IDISP001 // Dispose created

            return ValueTask.FromResult<IListener>(listener);
        }

        var ipaddr = IPAddress.Parse(HostName);
#pragma warning disable IDISP004 // Don't ignore created IDisposable
        return ValueTask.FromResult<IListener>(new SocketListener(this, receiver, logger, ipaddr, Port,
            cancellation));
#pragma warning restore IDISP004 // Don't ignore created IDisposable
    }

    protected override ISender CreateSender(IWolverineRuntime runtime)
    {
        return new BatchedSender(this, new SocketSenderProtocol(), runtime.DurabilitySettings.Cancellation,
            runtime.LoggerFactory.CreateLogger<SocketSenderProtocol>());
    }
}