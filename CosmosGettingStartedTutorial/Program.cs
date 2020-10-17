using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CosmosGettingStartedTutorial.Models.CosmosGettingStartedTutorial;
using Microsoft.Azure.Cosmos;

namespace CosmosGettingStartedTutorial
{
    class Program
    {
        private static readonly string EndpointUri = "https://singhwongfirst.documents.azure.com:443/";
        private static readonly string PrimarykEY = "sxuhAlszswsaiJZx6I3lqdlcddOW33BLtrX90wSY7cp2bowidtLmjYgja18v4v3D4XgiKmlUIlwKdbMiI3j2VQ==";
        private CosmosClient cosmosClient;
        private Database database;
        private Container container;
        private string databaseId = "FamilyDatabase";
        private string containerId = "FamilyContainer";
        static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("Beginning operations...\n");
                Program p = new Program();
                await p.GetStartedDemoAsync();
            }
            catch (CosmosException ex)
            {
                var baseException = ex.GetBaseException();
                Console.WriteLine($"{ex.StatusCode} error occurred: {baseException.Message}");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }
        public async Task GetStartedDemoAsync()
        {
            cosmosClient = new CosmosClient(EndpointUri,PrimarykEY);
            await CreatedDatabaseAsync();
            await CreateContainerAsync();
            //await AddItemsToContainerAsync();
            //Console.WriteLine("Is query?");
            //Console.ReadKey();
            //await QueryItemsAsync();
            //Console.WriteLine("After updated wakefield family:");
            //Console.ReadKey();
            //await ReplaceFamilyItemAsync();
            //Console.WriteLine("Is Delete andersen family:");
            //Console.ReadKey();
            //await DeleteFamilyItemAsync();
            //Console.WriteLine("andersen family is deleted.");
            //await QueryItemsAsync();
            Console.WriteLine("Is delete database:");
            Console.ReadKey();
            await DeleteDatabaseAndCleanupAsync();
        }
        private async Task CreatedDatabaseAsync()
        {
            database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId+2);
            Console.WriteLine($"Created Database: {database.Id+2}");
        }
        private async Task CreateContainerAsync()
        {
            container = await database.CreateContainerIfNotExistsAsync(containerId,"/LastName");
            Console.WriteLine($"Created Container: {container}");
        }
        private async Task AddItemsToContainerAsync()
        {
            Family andersenFamily = new Family
            {
                Id = "Andersen.1",
                LastName = "Andersen",
                Parents = new Parent[]
                {
                    new Parent{FirstName = "Thomas"},
                    new Parent{FirstName = "Mary kAY"}
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FirstName = "Henriette Thaulow",
                        Gender = "female",
                        Grade = 5,
                        Pets = new Pet[]
                        {
                            new Pet{GivenName = "Fluffy"}
                        }
                    }
                },
                Address = new Address { State = "WA",County="King",City="Seattle"},
                IsRegistered = false
            };
            try
            {
                ItemResponse<Family> andersenFamilyResponse =
                    await container.ReadItemAsync<Family>(andersenFamily.Id,new PartitionKey(andersenFamily.LastName));
                Console.WriteLine($"item in database with id: {andersenFamilyResponse.Resource.Id} already exists\n");
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                ItemResponse<Family> andersenFamilyResponse =
                    await container.CreateItemAsync<Family>(andersenFamily,new PartitionKey(andersenFamily.LastName));
                Console.WriteLine($"Created item in database with id: {andersenFamilyResponse.Resource.Id} Operation consumed {andersenFamilyResponse.RequestCharge} RUs.\n");
            }
            Family wakefieldFamily = new Family
            {
                Id = "Wakefield.7",
                LastName = "Wakefield",
                Parents = new Parent[]
                {
                    new Parent{FamilyName = "Wakefield",FirstName="Robin"},
                    new Parent{FamilyName="Miller",FirstName="Ben"}
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FamilyName = "Meriam",
                        FirstName = "Jesse",
                        Gender = "female",
                        Grade=8,
                        Pets = new Pet[]
                        {
                            new Pet{GivenName="Goofy" },
                            new Pet{GivenName="Shadow" }
                        }
                    },
                    new Child
                    {
                        FamilyName="Miller",
                        FirstName="Lisa",
                        Gender="female",
                        Grade=1
                    }
                },
                Address = new Address { State="NY",County="Manhattan",City="NY"},
                IsRegistered = true
            };
            try
            {
                ItemResponse<Family> wakefieldFamilyResponse =
                    await container.ReadItemAsync<Family>(wakefieldFamily.Id, new PartitionKey(wakefieldFamily.LastName));
                Console.WriteLine($"item in database with id: {wakefieldFamilyResponse.Resource.Id} already exists\n");
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                ItemResponse<Family> wakefieldFamilyResponse =
                    await container.CreateItemAsync<Family>(wakefieldFamily, new PartitionKey(wakefieldFamily.LastName));
                Console.WriteLine($"Created item in database with id: {wakefieldFamilyResponse.Resource.Id} Operation consumed {wakefieldFamilyResponse.RequestCharge} RUs.\n");
            }
        }
        private async Task QueryItemsAsync()
        {
            var sqlQueryText = "SELECT * FROM c";
            Console.WriteLine($"Running query: {sqlQueryText}");
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<Family> queryResultSetIterator = container.GetItemQueryIterator<Family>(queryDefinition);
            var families = new List<Family>();
            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<Family> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (var family in currentResultSet)
                {
                    families.Add(family);
                    Console.WriteLine($"\tRead{family}");
                }
            }
        }
        private async Task ReplaceFamilyItemAsync()
        {
            ItemResponse<Family> wakefieldFamilyResponse =
                await container.ReadItemAsync<Family>("Wakefield.7",new PartitionKey("Wakefield"));
            var itemBody = wakefieldFamilyResponse.Resource;
            itemBody.IsRegistered = true;
            itemBody.IsRegistered = false;
            itemBody.Children[0].Grade = 6;
            wakefieldFamilyResponse = await container.ReplaceItemAsync<Family>(itemBody,itemBody.Id,new PartitionKey(itemBody.LastName));
            Console.WriteLine($"Updated Family[{itemBody.LastName},{itemBody.Id}].\nBody is now: {wakefieldFamilyResponse.Resource}");
        }
        private async Task DeleteFamilyItemAsync()
        {
            var andersenFamilyResponse = await container.DeleteItemAsync<Family>("Andersen.1", new PartitionKey("Andersen"));
            Console.WriteLine("Deleted Family ['Andersen','Andersen.1']");
        }
        private async Task DeleteDatabaseAndCleanupAsync()
        {
            DatabaseResponse databaseResourceResponse = await database.DeleteAsync();
            Console.WriteLine($"Deleted Database: {databaseId}");
            cosmosClient.Dispose();
        }
    }
}
