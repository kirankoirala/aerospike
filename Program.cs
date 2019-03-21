using System;
using System.Collections.Generic;
using System.Linq;
using Aerospike.Client;
using Newtonsoft.Json;
using System.Diagnostics;

namespace Aerospike
{
    class Program
    {
        static AerospikeClient client = null;

        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            Stopwatch counter = new Stopwatch();


            string asServerIP = "localhost";
            int asServerPort = 3000;

            try
            {
                client = new AerospikeClient(asServerIP, asServerPort);
                //InsertRows();
                //CreateIndex();

                //InsertOneMasterRow();
                //InsertIntoFoo();

                ////never register again
                ////register_udf();
                ////register_hello_udf();
                ////registerUdfFilterBySymbol();

                //getFromUdf();
                counter.Start();
                var filteredPositions = getFilteredPositions("amzn");
                var result = JsonConvert.DeserializeObject<List<SubAcctDetail>>(filteredPositions);
                Console.WriteLine($"Total count: {result.Count()}");
                //Console.WriteLine(filteredPositions);
                counter.Stop();
                Console.WriteLine($"Total time taken using udf is: {counter.ElapsedMilliseconds}");

                counter.Restart();
                RunQuery("amzn");
                counter.Stop();
                Console.WriteLine($"Total time taken without using udf is: {counter.ElapsedMilliseconds}");


                //getAll();
                //CreateIndex();
                //getFromHelloUdf();


                //counter.Start();
                //RunQuery();
                // counter.Start();
                //Console.WriteLine("Total time taken: " + counter.ElapsedMilliseconds.ToString());


                client.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                client.Close();
            }

        }

        private static void registerUdfFilterBySymbol()
        {
            RegisterTask task = client.Register(null, "./filterpositions.lua", "filterpositions.lua", Language.LUA);
            task.Wait();
            Console.WriteLine("UDF is registered...");
        }

        private static void getAll()
        {
            Key key = new Key("test", "SubAcctPos", 50243432);
            var record = client.Get(null, key);
            Console.WriteLine(record);
        }
        private static string getFilteredPositions(string symbol)
        {
            Key key = new Key("test", "SubAcctPos", 50243432);
            var result = client.Execute(null, key, "filterpositions", "filter", Value.Get(symbol));
            return result.ToString();
        }

        private static void getFromUdf()
        {
            Key key = new Key("test", "foo", "Kiran");
            var result = client.Execute(null, key, "example", "readBin", Value.Get("name"));

            Console.WriteLine(result.ToString());
        }
      

        private static void CreateIndex()
        {
            IndexTask task = client.CreateIndex(null, "test", "MastAcctPos", "idx_master_acct", "MasterAcctId", IndexType.NUMERIC);
            Console.WriteLine("The following index is created:");
            Console.WriteLine("Namsespace: test, Set: MastAcctPos, index_name: idx_mast_acct_pos, bin: MasterAcctId");
        }



        private static void getFromHelloUdf()
        {
            Key key = new Key("test", "foo", "Kiran");
            var result = client.Execute(null, key, "hello", "hello", Value.Get("name"));

            Console.WriteLine(result);
        }

        private static void register_udf()
        {
            RegisterTask task = client.Register(null, "./example.lua", "example.lua", Language.LUA);
            task.Wait();
            Console.WriteLine("UDF is registered...");

        }

        private static void register_hello_udf()
        {
            RegisterTask task = client.Register(null, "./hello.lua", "hello.lua", Language.LUA);
            task.Wait();
            Console.WriteLine("hwllo UDF is registered...");

        }
        private static void InsertIntoFoo()
        {
            Key key = new Key("test", "foo", "Kiran");

            client.Put(null, key, new Bin("name", "Kiran"), new Bin("age", 15));

            Console.WriteLine("1 foo is inserted");
        }
        private static void InsertOneMasterRow()
        {
            Random rnd = new Random();
            var subAccounts = new List<SubAcctDetail>();

            var subaccountIds = new int[100];
            string[] symbols = new[] { "APPL", "AMZN", "SWAB", "SW", "AMEX" };

            for (var x = 0; x < 100; x++)
            {
                subaccountIds[x] = rnd.NextInt32();
            }

            for (var i = 0; i < 5; i++)
            {
                foreach (var s in subaccountIds)
                {
                    subAccounts.Add(new SubAcctDetail
                    {
                        SubAcctId = s,
                        Symbol = symbols[i],
                        Cash = rnd.NextDecimal(),
                        margin = rnd.NextDecimal()
                    });
                }
            }

            var subDetail = JsonConvert.SerializeObject(subAccounts);
            var masterKey = getNextUniqueInt();
            // Create Bins
            Bin bin1 = new Bin("MasterAcctId", masterKey);
            Bin bin2 = new Bin("SubAccountBlob", subDetail);

            // Write record
            Key key = new Key("test", "SubAcctPos", masterKey);

            client.Put(null, key, bin1, bin2);

            Console.WriteLine("hundred rows successfully inserted");
        }



        //Inserts multiple master/sub and symbols
        private static void InsertRows()
        {
            Random rnd = new Random();
            for (var i = 0; i < 100; i++)
            {
                Key key = new Key("test", "MastAcctPos", Guid.NewGuid().ToString());


                var subDetail = JsonConvert.SerializeObject("{'SubAcctId':" + rnd.NextInt32().ToString() + ", 'Symbol':'" + rnd.NextString(4) + "', 'cash':" + rnd.NextDecimal() + ", 'margin':" + rnd.NextDecimal() + "}");

                // Create Bins
                Bin bin1 = new Bin("MasterAcctId", 1234001);
                Bin bin2 = new Bin("SubAccountBlob", subDetail);

                // Write record
                client.Put(null, key, bin1, bin2);
            }

            Console.WriteLine("hundred rows successfully inserted");
        }



        private static int getNextUniqueInt()
        {
            var now = DateTime.Now;
            var zeroDate = DateTime.MinValue.AddHours(now.Hour).AddMinutes(now.Minute).AddSeconds(now.Second).AddMilliseconds(now.Millisecond);
            return (int)(zeroDate.Ticks / 10000);
        }
        private static void RunQuery(string symbol)
        {
            var binName = "SubAccountBlob";

            Console.WriteLine("Query Started...");


            Key key = new Key("test", "SubAcctPos", 50243432);
            var subAccounts = new List<SubAcctDetail>();
            var record = client.Get(null, key);
            if (record != null && record.bins.TryGetValue(binName, out Object obj))
            {
                subAccounts = JsonConvert.DeserializeObject<List<SubAcctDetail>>(obj.ToString());
            }
            var result = subAccounts.Where(s => s.Symbol == symbol.ToUpper());
            Console.WriteLine("Record found: ns=" + key.ns +
                          " set=" + key.setName +
                          " bin=" + binName +
                          " digest=" + ByteUtil.BytesToHexString(key.digest) +
                          " counts=" + result.Count());


            Console.WriteLine("Query Ended...");

        }
    }
    public static class DecExtension
    {
        public static int NextInt32(this Random rng)
        {
            int firstBits = rng.Next(1000000, 9999999);
            //int lastBits = rng.Next(0, 1 << 28);
            return firstBits; //| lastBits;
        }

        public static int NextInt32WithRange(this Random rng, Int32 min, Int32 max)
        {
            int firstBits = rng.Next(min, max);
            int lastBits = rng.Next(0, 1 << 28);
            return firstBits | lastBits;
        }
        public static decimal NextDecimal(this Random rng)
        {
            //byte scale = (byte)rng.Next(29);
            //bool sign = rng.Next(2) == 1;
            return new decimal(rng.NextInt32WithRange(1, 100),
                               rng.NextInt32WithRange(100, 200),
                               rng.NextInt32WithRange(200, 500),
                               false,
                               0);
        }

        public static string NextString(this Random rng, int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[rng.Next(s.Length)]).ToArray());
        }
    }

    public class SubAcctDetail
    {
        public int SubAcctId { get; set; }
        public string Symbol { get; set; }
        public decimal Cash { get; set; }
        public decimal margin { get; set; }
    }
}
