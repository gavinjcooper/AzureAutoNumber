using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using AutoNumber.Extensions;
using AutoNumber.Interfaces;
using AutoNumber.Options;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Options;

namespace AutoNumber
{
    public class BlobOptimisticDataStore : IOptimisticDataStore
    {
        private const string SeedValue = "1";
        private readonly BlobContainerClient blobContainer;
        private readonly ConcurrentDictionary<string, ICloudBlob> blobReferences;
        private readonly object blobReferencesLock = new object();

        public BlobOptimisticDataStore(BlobServiceClient account, string containerName)
        {
            blobContainer = account.GetBlobContainerClient(containerName.ToLower());
            blobReferences = new ConcurrentDictionary<string, ICloudBlob>();
        }

        public BlobOptimisticDataStore(BlobServiceClient client, IOptions<AutoNumberOptions> options)
            : this(client, options.Value.StorageContainerName)
        {
        }

        public string GetData(string blockName)
        {
            return GetDataAsync(blockName).GetAwaiter().GetResult();
        }

        public async Task<string> GetDataAsync(string blockName)
        {
            var blobReference = GetBlobReference(blockName);

            using (var stream = new MemoryStream())
            {
                await blobReference.DownloadToStreamAsync(stream).ConfigureAwait(false);
                return Encoding.UTF8.GetString(stream.ToArray());
            }
        }

        public async Task Init()
        {
            await blobContainer.CreateIfNotExistsAsync().ConfigureAwait(false);
        }

        public bool TryOptimisticWrite(string blockName, string data)
        {
            return TryOptimisticWriteAsync(blockName, data).GetAwaiter().GetResult();
        }

        public async Task<bool> TryOptimisticWriteAsync(string blockName, string data)
        {
            var blobReference = GetBlobReference(blockName);

            try
            {
                await blobReference.UploadTextAsync(
                       data
                       ).ConfigureAwait(false);

            }
            catch (RequestFailedException ex)
            {
                if (ex.Status == (int)HttpStatusCode.PreconditionFailed)
                    return false;

                throw;
            }
           

            return true;
        }

        private ICloudBlob GetBlobReference(string blockName)
        {
            return blobReferences.GetValue(
                blockName,
                blobReferencesLock,
                () => InitializeBlobReferenceAsync(blockName).GetAwaiter().GetResult());
        }

        private async Task<ICloudBlob> InitializeBlobReferenceAsync(string blockName)
        {
            var blobClient = blobContainer.GetBlockBlobClient(blockName);

            if (await blobClient.ExistsAsync().ConfigureAwait(false))
                return new BlobReference(blobClient);

            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(SeedValue)))
            {

                try
                {
                    await blobClient.UploadAsync(stream, new BlobHttpHeaders
                    {
                        ContentType = "text/plain"
                    }, conditions: new BlobRequestConditions { IfNoneMatch = ETag.All }
                          ).ConfigureAwait(false);
                }
                catch (RequestFailedException uploadException)
                {
                    if (uploadException.Status != (int) HttpStatusCode.Conflict)
                        throw;
                }

                return new BlobReference(blobClient);
            }
        }
        
    }

   


    internal class BlobReference : ICloudBlob
    {
        public BlobReference(BlockBlobClient client)
        {
            this.client = client;
        }


        private readonly BlockBlobClient client;
        private ETag eTag;

        public async Task DownloadToStreamAsync(Stream stream)
        {
            var result = await client.DownloadStreamingAsync();
            eTag = result.Value.Details.ETag;
            await result.Value.Content.CopyToAsync(stream);
        }

        public async Task UploadTextAsync(string text)
        {
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(text)))
            {
                await client.UploadAsync(stream, new BlobHttpHeaders
                    {
                        ContentType = "text/plain"
                    }, conditions: new BlobRequestConditions { IfMatch = eTag }
                ).ConfigureAwait(false);
            }
        }
    }

    internal interface ICloudBlob
    {
        Task DownloadToStreamAsync(Stream stream);
        Task UploadTextAsync(string text);
    }
}