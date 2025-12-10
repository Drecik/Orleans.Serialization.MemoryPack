using MemoryPack;
using Microsoft.Extensions.Options;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Buffers.Adaptors;
using Orleans.Serialization.Cloning;
using Orleans.Serialization.Codecs;
using Orleans.Serialization.Serializers;
using Orleans.Serialization.WireProtocol;
using System.Collections.Concurrent;
using System.Reflection;

namespace Orleans.Serialization.MemoryPack
{
    /// <summary>
    /// A generic Orleans field codec which uses MemoryPack for serialization.
    /// </summary>
    /// <typeparam name="T">The type being serialized.</typeparam>
    [Alias(WellKnownAlias)]
    internal sealed class MemoryPackCodec : IGeneralizedCodec, IGeneralizedCopier, ITypeFilter
    {
        private static readonly ConcurrentDictionary<Type, bool> SupportedTypes = new();
        private static readonly Type SelfType = typeof(MemoryPackCodec);

        /// <summary>
        /// The well-known type alias for this codec.
        /// </summary>
        public const string WellKnownAlias = "memorypack";

        private readonly MemoryPackCodecOptions _options;

        public MemoryPackCodec(IOptions<MemoryPackCodecOptions> options)
        {
            _options = options.Value;
        }

        void IFieldCodec.WriteField<TBufferWriter>(ref Writer<TBufferWriter> writer, uint fieldIdDelta, Type expectedType, object value)
        {
            if (ReferenceCodec.TryWriteReferenceField(ref writer, fieldIdDelta, expectedType, value))
            {
                return;
            }

            // The schema type when serializing the field is the type of the codec.
            writer.WriteFieldHeader(fieldIdDelta, expectedType, SelfType, WireType.TagDelimited);

            // Write the type name
            ReferenceCodec.MarkValueField(writer.Session);
            writer.WriteFieldHeaderExpected(0, WireType.LengthPrefixed);
            writer.Session.TypeCodec.WriteLengthPrefixed(ref writer, value.GetType());

            var bufferWriter = new BufferWriterBox<PooledBuffer>(new());
            try
            {

                MemoryPackSerializer.Serialize(value.GetType(), bufferWriter, value);

                ReferenceCodec.MarkValueField(writer.Session);
                writer.WriteFieldHeaderExpected(1, WireType.LengthPrefixed);
                writer.WriteVarUInt32((uint)bufferWriter.Value.Length);
                bufferWriter.Value.CopyTo(ref writer);
            }
            finally
            {
                bufferWriter.Value.Dispose();
            }

            writer.WriteEndObject();
        }

        object IFieldCodec.ReadValue<TInput>(ref Reader<TInput> reader, Field field)
        {
            if (field.IsReference)
            {
                return ReferenceCodec.ReadReference(ref reader, field.FieldType);
            }

            field.EnsureWireTypeTagDelimited();

            var placeholderReferenceId = ReferenceCodec.CreateRecordPlaceholder(reader.Session);
            object result = null;
            Type type = null;
            uint fieldId = 0;
            while (true)
            {
                var header = reader.ReadFieldHeader();
                if (header.IsEndBaseOrEndObject)
                {
                    break;
                }

                fieldId += header.FieldIdDelta;
                switch (fieldId)
                {
                    case 0:
                        ReferenceCodec.MarkValueField(reader.Session);
                        type = reader.Session.TypeCodec.ReadLengthPrefixed(ref reader);
                        break;
                    case 1:
                        if (type is null)
                        {
                            ThrowTypeFieldMissing();
                        }

                        ReferenceCodec.MarkValueField(reader.Session);
                        var length = reader.ReadVarUInt32();

                        var bufferWriter = new BufferWriterBox<PooledBuffer>(new());
                        try
                        {
                            reader.ReadBytes(ref bufferWriter, (int)length);
                            result = MemoryPackSerializer.Deserialize(type, bufferWriter.Value.AsReadOnlySequence());
                        }
                        finally
                        {
                            bufferWriter.Value.Dispose();
                        }

                        break;
                    default:
                        reader.ConsumeUnknownField(header);
                        break;
                }
            }

            ReferenceCodec.RecordObject(reader.Session, result, placeholderReferenceId);
            return result;
        }

        bool IGeneralizedCodec.IsSupportedType(Type type)
        {
            if (type == SelfType)
            {
                return true;
            }

            if (CommonCodecTypeFilter.IsAbstractOrFrameworkType(type))
            {
                return false;
            }

            if (_options.IsSerializableType?.Invoke(type) is bool value)
            {
                return value;
            }

            return IsMemoryPackContract(type);
        }

        object IDeepCopier.DeepCopy(object input, CopyContext context)
        {
            if (context.TryGetCopy(input, out object result))
            {
                return result;
            }

            var bufferWriter = new BufferWriterBox<PooledBuffer>(new());
            try
            {
                MemoryPackSerializer.Serialize(input.GetType(), bufferWriter, input, _options.SerializerOptions);

                var sequence = bufferWriter.Value.AsReadOnlySequence();
                result = MemoryPackSerializer.Deserialize(input.GetType(), sequence, _options.SerializerOptions);
            }
            finally
            {
                bufferWriter.Value.Dispose();
            }

            context.RecordCopy(input, result);
            return result;
        }

        bool IGeneralizedCopier.IsSupportedType(Type type)
        {
            if (CommonCodecTypeFilter.IsAbstractOrFrameworkType(type))
            {
                return false;
            }

            if (_options.IsCopyableType?.Invoke(type) is bool value)
            {
                return value;
            }

            return IsMemoryPackContract(type);
        }

        /// <inheritdoc/>
        bool? ITypeFilter.IsTypeAllowed(Type type) => (((IGeneralizedCopier)this).IsSupportedType(type) || ((IGeneralizedCodec)this).IsSupportedType(type)) ? true : null;

        private static bool IsMemoryPackContract(Type type)
        {
            if (SupportedTypes.TryGetValue(type, out bool isMemoryPackContract))
            {
                return isMemoryPackContract;
            }

            isMemoryPackContract = type.GetCustomAttribute<MemoryPackableAttribute>() is not null;

            SupportedTypes.TryAdd(type, isMemoryPackContract);
            return isMemoryPackContract;
        }

        private static void ThrowTypeFieldMissing() => throw new RequiredFieldMissingException("Serialized value is missing its type field.");
    }
}