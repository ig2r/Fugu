using Microsoft.Extensions.Configuration;
using System;

namespace Fugu.Core.Benchmarks
{
    class Program
    {
        static void Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .AddCommandLine(args)
                .Build();
        }
    }
}