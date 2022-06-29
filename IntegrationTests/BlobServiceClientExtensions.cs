using Azure.Storage.Blobs;

namespace AutoNumber.IntegrationTests
{
    internal static class BlobServiceClientExtensions
    {
        public static BlobServiceClient DevelopmentStorageAccount => new BlobServiceClient(
            "UseDevelopmentStorage=true");
    }
}