# Azure AutoNumber

[![Build Status](https://alibahraminezhad0485.visualstudio.com/AutoNumber/_apis/build/status/0x414c49.AzureAutoNumber?branchName=master)](https://alibahraminezhad0485.visualstudio.com/AutoNumber/_build/latest?definitionId=5&branchName=master)
[![Build Status](https://img.shields.io/github/license/0x414c49/AzureAutoNumber)]()
[![NuGet version (AzureAutoNumber)](https://img.shields.io/nuget/v/AzureAutoNumber.svg?style=flat-square)](https://www.nuget.org/packages/AzureAutoNumber/)


High performance, distributed unique thread-safe id generator for Azure.

- Human-friendly generated ids (number)
- High performant and fast
- 100% guarantee that won't cause any duplicate ids

## How to use

The project is rely on Azure Blob Storage. `AutoNumber` package will generate ids by using a single text file on the Azure Blob Storage.


```
var blobStorageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(connectionString);

var blobOptimisticDataStore = new BlobOptimisticDataStore(blobStorageAccount, "unique-ids");

var idGen = new UniqueIdGenerator(blobOptimisticDataStore);

// generate ids with different scopes

var id = idGen.NextId("urls");
var id2 = idGen.NextId("orders");
```

### With Microsoft DI
The project has an extension method to add it and its dependencies to Microsoft ASP.NET DI. The only caveat is you need to registry type of  `CloudStorageAccount` in DI before registring `AutoNumber`.


```
// configure the services
serviceCollection.AddAutoNumber();

// Inject `IUniqueIdGenerator` in constructor

public class Foo
{
  public Foo(IUniqueIdGenerator idGenerator)
  {
      _idGenerator = idGenerator;
  }
}
```

### Configuration
These are default configuration for `AutoNumber`. If you prefer registering AutoNumber with `AddAddNumber` method, these options can be set via `appsettings.json`.

```
{
  "AutoNumber": {
    "BatchSize": 50,
    "MaxWriteAttempts": 25,
    "StorageContainerName": "unique-urls"
  }
}
```


## Credits
Most of the credits of this library goes to [Tatham Oddie](https://tatham.blog/2011/07/14/released-snowmaker-a-unique-id-generator-for-azure-or-any-other-cloud-hosting-environment/) for making SnowMaker. I forked his work and made lots of change to make it available on .NET Standard (2.0 and 2.1). SnowMaker is out-dated and is using very old version of Azure Packages.
