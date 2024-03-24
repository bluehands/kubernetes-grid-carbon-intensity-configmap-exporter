using System.CommandLine;
using System.CommandLine.NamingConventionBinder;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using static System.Runtime.InteropServices.JavaScript.JSType;
using System.Net;
using System.Text;
using System.Text.Json;
using CarbonAware.Model;
using k8s.Models;
using CarbonAwareComputing.ExecutionForecast;
using KubeOps.KubernetesClient;
using Prometheus.HttpClientMetrics;

namespace Exporter
{
    internal class Program
    {
        static int Main(string[] args)
        {
            var root = BuildCommandLine();
            return root.Invoke(args);

        }

        private static RootCommand BuildCommandLine()
        {
            var root = new RootCommand()
            {
                new Option<string>("--computing-location", ()=>"de","Specifies the grid for which the carbon intensity is requested"),
                new Option<string>("--forecast-data-endpoint-template", ()=>"https://carbonawarecomputing.blob.core.windows.net/forecasts/{0}.json","Specifies url of the forecast data. {0} will be replaced by the computing location"),
                new Option<string>("--configmap-namespace", ()=>"kube-system","Specifies the configmap namespace the grid carbon intensity is set"),
                new Option<string>("--configmap-name", ()=>"carbon-intensity","Specifies the configmap name the grid carbon intensity is set"),
                new Option<string>("--configmap-key", ()=>"data","Specifies the configmap data key the grid carbon intensity is set"),

            };
            root.Handler = CommandHandler.Create(ExecuteRootCommand);
            return root;
        }
        private static async Task<int> ExecuteRootCommand(string computingLocation, string forecastDataEndpointTemplate, string configmapNamespace, string configmapName, string configmapKey)
        {
            try
            {
                if (!ComputingLocations.TryParse(computingLocation, out ComputingLocation? location))
                {
                    await Console.Error.WriteLineAsync($"No supported computing location found for {computingLocation}");
                    await Console.Error.WriteLineAsync($"See https://github.com/bluehands/Carbon-Aware-Computing");
                    await Console.Error.WriteLineAsync($"To get the list of locations: https://forecast.carbon-aware-computing.com/locations");
                    return -1;
                }

                var emissionData = await GetEmissionData(location, forecastDataEndpointTemplate);
                var exporterData = TransformData(emissionData);
                var json = JsonSerializer.Serialize(exporterData, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });

                await UpdateConfigmap(configmapNamespace, configmapName, configmapKey, json, exporterData.Count, exporterData.FirstOrDefault()?.Timestamp, exporterData.LastOrDefault()?.Timestamp);
                return 0;
            }
            catch (Exception ex)
            {
                await Console.Error.WriteLineAsync(ex.ToString());
                return -1;
            }
        }

        private static List<ExporterData> TransformData(List<EmissionsData> emissionData)
        {
            var exporterData = emissionData.
                Where(e => e.Rating > 0).
                Select(e => new ExporterData(e.Time, Convert.ToInt32(e.Duration.TotalMinutes), e.Rating)).
                ToList();
            Console.WriteLine($"Transform {exporterData.Count} data points");
            return exporterData;
        }

        private static async Task UpdateConfigmap(string configmapNamespace, string configmapName, string configmapKey, string json, int numOfRecords, DateTimeOffset? minForecast, DateTimeOffset? maxForecast)
        {
            var client = new KubernetesClient();
            var configMap = await GetConfigMap(configmapNamespace, configmapName, client);
            UpdateMetadata("lastHeartbeatTime", DateTimeOffset.Now.ToString("O"), configMap);
            UpdateMetadata("numOfRecords", numOfRecords.ToString(CultureInfo.InvariantCulture), configMap);
            UpdateMetadata("minForecast", $"{minForecast:O}", configMap);
            UpdateMetadata("maxForecast", $"{maxForecast:O}", configMap);
            UpdateData(configmapKey, json, configMap);
            await client.UpdateAsync(configMap);
            Console.WriteLine($"Update configmap {configmapNamespace}/{configmapName}");
        }
        private static void UpdateMetadata(string configmapKey, string data, V1ConfigMap configMap)
        {
            configMap.Data ??= new Dictionary<string, string>();
            configMap.Data[configmapKey] = data;
        }
        private static void UpdateData(string configmapKey, string json, V1ConfigMap configMap)
        {
            configMap.BinaryData ??= new Dictionary<string, byte[]>();
            configMap.BinaryData[configmapKey] = Encoding.UTF8.GetBytes(json);
        }

        private static async Task<V1ConfigMap> GetConfigMap(string configmapNamespace, string configmapName, KubernetesClient client)
        {
            var configMap = await client.GetAsync<V1ConfigMap>(configmapName, configmapNamespace);
            if (configMap is null)
            {
                configMap = new V1ConfigMap(metadata: new V1ObjectMeta(name: configmapName, namespaceProperty: configmapNamespace));
                configMap = await client.CreateAsync(configMap);
            }

            return configMap;
        }

        private static async Task<List<EmissionsData>> GetEmissionData(ComputingLocation? location, string forecastDataEndpointTemplate)
        {
            var provider = new CarbonAwareDataProviderOpenData(forecastDataEndpointTemplate);
            var emissionData = await provider.GetForecastData(location!);
            Console.WriteLine($"Download {emissionData.Count} data points");
            return emissionData;
        }
    }

    public record ExporterData(DateTimeOffset Timestamp, int Duration, double Value);
}
