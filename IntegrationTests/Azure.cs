using System;
using System.Text;
using AutoNumber.Interfaces;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using NUnit.Framework;

namespace AutoNumber.IntegrationTests
{
    [TestFixture]
    public class Azure : Scenarios<TestScope>
    {
        private readonly BlobServiceClient storageAccount = BlobServiceClientExtensions.DevelopmentStorageAccount;

        protected override TestScope BuildTestScope()
        {
            return new TestScope(BlobServiceClientExtensions.DevelopmentStorageAccount);
        }

        protected override IOptimisticDataStore BuildStore(TestScope scope)
        {
            var blobOptimisticDataStore = new BlobOptimisticDataStore(storageAccount, scope.ContainerName);
            blobOptimisticDataStore.Init().GetAwaiter().GetResult();
            return blobOptimisticDataStore;
        }
    }

    public sealed class TestScope : ITestScope
    {
        private readonly BlobContainerClient blobClient;

        public TestScope(BlobServiceClient account)
        {
            var ticks = DateTime.UtcNow.Ticks;
            IdScopeName = string.Format("autonumbertest{0}", ticks);
            ContainerName = string.Format("autonumbertest{0}", ticks);

            blobClient = account.GetBlobContainerClient(ContainerName);
        }

        public string ContainerName { get; }

        public string IdScopeName { get; }

        public string ReadCurrentPersistedValue()
        {
            var blobContainer = blobClient;
            var blob = blobContainer.GetBlockBlobClient(IdScopeName);
            return Encoding.UTF8.GetString(blob.DownloadContent().Value.Content.ToArray());
        }

        public void Dispose()
        {
            blobClient.DeleteAsync().GetAwaiter().GetResult();
        }
    }
}