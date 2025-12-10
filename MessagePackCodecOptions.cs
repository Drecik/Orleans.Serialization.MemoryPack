using MemoryPack;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Orleans.Serialization.MemoryPack
{
    /// <summary>
    /// Options for configuring MemoryPack serialization in Orleans.
    /// </summary>
    public sealed class MemoryPackCodecOptions
    {
        //
        // Summary:
        //     Gets or sets the MemoryPackSerializerOptions
        public MemoryPackSerializerOptions SerializerOptions { get; set; } = MemoryPackSerializerOptions.Default;

        //
        // Summary:
        //     Gets or sets a delegate used to determine if a type is supported by the MemoryPack
        //     serializer for serialization and deserialization.
        public Func<Type, bool?> IsSerializableType { get; set; }

        //
        // Summary:
        //     Gets or sets a delegate used to determine if a type is supported by the MemoryPack
        //     serializer for copying.
        public Func<Type, bool?> IsCopyableType { get; set; }
    }
}