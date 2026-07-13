using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((ctx, cfg) =>
    {
        cfg.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
    })
    .ConfigureServices((context, services) =>
    {
        services.Configure<KafkaSettings>(context.Configuration.GetSection("Kafka"));
        services.Configure<ForwardingSettings>(context.Configuration.GetSection("Forwarding"));
        services.AddHttpClient("forwarder");
        services.AddSingleton<IEndpointResolver, ConfigurationEndpointResolver>();
        services.AddHostedService<KafkaForwarderWorker>();
    })
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
    })
    .Build();

await host.RunAsync();
