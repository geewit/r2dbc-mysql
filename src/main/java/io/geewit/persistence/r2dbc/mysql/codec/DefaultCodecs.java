package io.geewit.persistence.r2dbc.mysql.codec;

import io.geewit.persistence.r2dbc.mysql.MySqlParameter;
import io.geewit.persistence.r2dbc.mysql.api.MySqlReadableMetadata;
import io.geewit.persistence.r2dbc.mysql.constant.MySqlType;
import io.geewit.persistence.r2dbc.mysql.internal.util.InternalArrays;
import io.geewit.persistence.r2dbc.mysql.message.FieldValue;
import io.geewit.persistence.r2dbc.mysql.message.LargeFieldValue;
import io.geewit.persistence.r2dbc.mysql.message.NormalFieldValue;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Parameter;

import javax.annotation.concurrent.GuardedBy;
import java.lang.reflect.ParameterizedType;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Codecs}.
 */
final class DefaultCodecs implements Codecs {

    private static final List<Codec<?>> DEFAULT_CODECS = InternalArrays.asImmutableList(
            ByteCodec.INSTANCE,
            ShortCodec.INSTANCE,
            IntegerCodec.INSTANCE,
            LongCodec.INSTANCE,
            BigIntegerCodec.INSTANCE,

            BigDecimalCodec.INSTANCE, // Only all decimals
            FloatCodec.INSTANCE, // Decimal (precision < 7) or float
            DoubleCodec.INSTANCE, // Decimal (precision < 16) or double or float

            BooleanCodec.INSTANCE,
            BitSetCodec.INSTANCE,

            ZonedDateTimeCodec.INSTANCE,
            LocalDateTimeCodec.INSTANCE,
            InstantCodec.INSTANCE,
            OffsetDateTimeCodec.INSTANCE,

            LocalDateCodec.INSTANCE,

            LocalTimeCodec.INSTANCE,
            DurationCodec.INSTANCE,
            OffsetTimeCodec.INSTANCE,

            YearCodec.INSTANCE,

            StringCodec.INSTANCE,

            EnumCodec.INSTANCE,
            SetCodec.INSTANCE,

            ClobCodec.INSTANCE,
            BlobCodec.INSTANCE,

            ByteBufferCodec.INSTANCE,
            ByteArrayCodec.INSTANCE
    );

    private final List<Codec<?>> codecs;

    private final ParameterizedCodec<?>[] parameterizedCodecs;

    private final MassiveCodec<?>[] massiveCodecs;

    private final MassiveParameterizedCodec<?>[] massiveParameterizedCodecs;

    private final Map<Class<?>, Codec<?>> fastPath;

    private DefaultCodecs(List<Codec<?>> codecs) {
        requireNonNull(codecs, "codecs must not be null");

        Map<Class<?>, Codec<?>> fastPath = new HashMap<>();
        List<ParameterizedCodec<?>> parameterizedCodecs = new ArrayList<>();
        List<MassiveCodec<?>> massiveCodecs = new ArrayList<>();
        List<MassiveParameterizedCodec<?>> massiveParamCodecs = new ArrayList<>();

        for (Codec<?> codec : codecs) {
            Class<?> mainClass = codec.getMainClass();

            if (mainClass != null) {
                fastPath.putIfAbsent(mainClass, codec);
            }

            if (codec instanceof AbstractPrimitiveCodec<?> abstractPrimitiveCodec) {
                fastPath.putIfAbsent(abstractPrimitiveCodec.getPrimitiveClass(), abstractPrimitiveCodec);
            } else if (codec instanceof ParameterizedCodec<?> parameterizedCodec) {
                parameterizedCodecs.add(parameterizedCodec);
            }

            if (codec instanceof MassiveCodec<?> massiveCodec) {
                massiveCodecs.add(massiveCodec);

                if (codec instanceof MassiveParameterizedCodec<?> massiveParameterizedCodec) {
                    massiveParamCodecs.add(massiveParameterizedCodec);
                }
            }
        }

        this.fastPath = fastPath;
        this.codecs = codecs;
        this.massiveCodecs = massiveCodecs.toArray(new MassiveCodec<?>[0]);
        this.massiveParameterizedCodecs = massiveParamCodecs.toArray(new MassiveParameterizedCodec<?>[0]);
        this.parameterizedCodecs = parameterizedCodecs.toArray(new ParameterizedCodec<?>[0]);
    }

    /**
     * Note: this method should NEVER release {@code buf} because of it come from {@code MySqlRow} which will release
     * this buffer.
     */
    @Override
    public <T> T decode(FieldValue value,
                        MySqlReadableMetadata metadata,
                        Class<?> type, boolean binary,
                        CodecContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(metadata, "info must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(type, "type must not be null");

        if (value.isNull()) {
            // T is always an object, so null should be returned even if the type is a primitive class.
            // See also https://github.com/mirromutth/r2dbc-mysql/issues/184 .
            return null;
        }

        Class<?> target = chooseClass(metadata, type, context);

        if (value instanceof NormalFieldValue normalFieldValue) {
            return decodeNormal(normalFieldValue, metadata, target, binary, context);
        } else if (value instanceof LargeFieldValue largeFieldValue) {
            return decodeMassive(largeFieldValue, metadata, target, binary, context);
        }

        throw new IllegalArgumentException("Unknown value " + value.getClass().getSimpleName());
    }

    @Override
    public <T> T decode(FieldValue value, MySqlReadableMetadata metadata, ParameterizedType type,
                        boolean binary, CodecContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(metadata, "info must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(type, "type must not be null");

        if (value.isNull()) {
            return null;
        } else if (value instanceof NormalFieldValue normalFieldValue) {
            return decodeNormal(normalFieldValue, metadata, type, binary, context);
        } else if (value instanceof LargeFieldValue largeFieldValue) {
            return decodeMassive(largeFieldValue, metadata, type, binary, context);
        }

        throw new IllegalArgumentException("Unknown value " + value.getClass().getSimpleName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decodeLastInsertId(long value, Class<?> type) {
        requireNonNull(type, "type must not be null");

        if (Byte.TYPE == type || Byte.class == type) {
            return (T) Byte.valueOf((byte) value);
        } else if (Short.TYPE == type || Short.class == type) {
            return (T) Short.valueOf((short) value);
        } else if (Integer.TYPE == type || Integer.class == type) {
            return (T) Integer.valueOf((int) value);
        } else if (Long.TYPE == type || Long.class == type) {
            return (T) Long.valueOf(value);
        } else if (BigInteger.class == type) {
            if (value < 0) {
                return (T) CodecUtils.unsignedBigInteger(value);
            }

            return (T) BigInteger.valueOf(value);
        } else if (type.isAssignableFrom(Number.class)) {
            if (value < 0) {
                return (T) CodecUtils.unsignedBigInteger(value);
            }

            return (T) Long.valueOf(value);
        }

        throw new IllegalArgumentException(String.format("Cannot decode %s with last inserted ID %s", type,
                value < 0 ? Long.toUnsignedString(value) : value));
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(context, "context must not be null");

        Object valueToEncode = getValueToEncode(value);

        if (null == valueToEncode) {
            return encodeNull();
        }

        Codec<?> fast = encodeFast(valueToEncode);

        if (fast != null && fast.canEncode(valueToEncode)) {
            return fast.encode(valueToEncode, context);
        }

        for (Codec<?> codec : codecs) {
            if (codec != fast && codec.canEncode(valueToEncode)) {
                return codec.encode(valueToEncode, context);
            }
        }

        throw new IllegalArgumentException("Cannot encode " + valueToEncode.getClass());
    }

    private static Object getValueToEncode(Object value) {
        if (value instanceof Parameter parameter) {
            return parameter.getValue();
        }
        return value;
    }

    @Override
    public MySqlParameter encodeNull() {
        return NullMySqlParameter.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    private <T> Codec<T> decodeFast(Class<?> type) {
        Codec<T> codec = (Codec<T>) fastPath.get(type);

        if (codec == null && type.isEnum()) {
            return (Codec<T>) fastPath.get(Enum.class);
        }

        return codec;
    }

    @SuppressWarnings("unchecked")
    private <T> Codec<T> encodeFast(Object value) {
        Codec<T> codec = (Codec<T>) fastPath.get(value.getClass());

        if (codec == null) {
            switch (value) {
                case ByteBuffer _ -> {
                    return (Codec<T>) fastPath.get(ByteBuffer.class);
                }
                case Blob _ -> {
                    return (Codec<T>) fastPath.get(Blob.class);
                }
                case Clob _ -> {
                    return (Codec<T>) fastPath.get(Clob.class);
                }
                case Enum<?> _ -> {
                    return (Codec<T>) fastPath.get(Enum.class);
                }
                default -> {
                }
            }
        }

        return codec;
    }

    private <T> T decodeNormal(NormalFieldValue value,
                               MySqlReadableMetadata metadata,
                               Class<?> type,
                               boolean binary,
                               CodecContext context) {
        Codec<T> fast = decodeFast(type);

        if (fast != null && fast.canDecode(metadata, type)) {
            return fast.decode(value.getBufferSlice(), metadata, type, binary, context);
        }

        for (Codec<?> codec : codecs) {
            if (codec != fast && codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                Codec<T> c = (Codec<T>) codec;
                return c.decode(value.getBufferSlice(), metadata, type, binary, context);
            }
        }

        throw new IllegalArgumentException("Cannot decode " + type + " for " + metadata.getType());
    }

    private <T> T decodeNormal(NormalFieldValue value,
                               MySqlReadableMetadata metadata,
                               ParameterizedType type,
                               boolean binary,
                               CodecContext context) {
        for (ParameterizedCodec<?> codec : parameterizedCodecs) {
            if (codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                T result = (T) codec.decode(value.getBufferSlice(), metadata, type, binary, context);
                return result;
            }
        }

        throw new IllegalArgumentException("Cannot decode " + type + " for " + metadata.getType());
    }

    private <T> T decodeMassive(LargeFieldValue value,
                                MySqlReadableMetadata metadata,
                                Class<?> type,
                                boolean binary,
                                CodecContext context) {
        Codec<T> fast = decodeFast(type);

        if (fast instanceof MassiveCodec<?> && fast.canDecode(metadata, type)) {
            return ((MassiveCodec<T>) fast).decodeMassive(value.getBufferSlices(), metadata, type, binary, context);
        }

        for (MassiveCodec<?> codec : massiveCodecs) {
            if (codec != fast && codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                MassiveCodec<T> c = (MassiveCodec<T>) codec;
                return c.decodeMassive(value.getBufferSlices(), metadata, type, binary, context);
            }
        }

        throw new IllegalArgumentException("Cannot decode massive " + type + " for " + metadata.getType());
    }

    private <T> T decodeMassive(LargeFieldValue value,
                                MySqlReadableMetadata metadata,
                                ParameterizedType type,
                                boolean binary,
                                CodecContext context) {
        for (MassiveParameterizedCodec<?> codec : massiveParameterizedCodecs) {
            if (codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                T result = (T) codec.decodeMassive(value.getBufferSlices(), metadata, type, binary, context);
                return result;
            }
        }

        throw new IllegalArgumentException("Cannot decode massive  " + type + " for " + metadata.getType());
    }

    /**
     * Chooses the {@link Class} to use for decoding. It helps to find {@link Codec} on the fast path. e.g.
     * {@link Object} -> {@link String} for {@code TEXT}, {@link Number} -> {@link Integer} for {@code INT}, etc.
     *
     * @param metadata the metadata of the column or the {@code OUT} parameter.
     * @param type     the {@link Class} specified by the user.
     * @return the {@link Class} to use for decoding.
     */
    private static Class<?> chooseClass(final MySqlReadableMetadata metadata,
                                        Class<?> type,
                                        final CodecContext codecContext) {
        final Class<?> javaType = getDefaultJavaType(metadata, codecContext);
        return type.isAssignableFrom(javaType) ? javaType : type;
    }


    private static boolean shouldBeTreatedAsBoolean(final Integer precision,
                                                    final MySqlType type,
                                                    final CodecContext context) {
        if (precision == null || precision != 1) {
            return false;
        }
        // ref: https://github.com/asyncer-io/r2dbc-mysql/issues/277
        // BIT(1) should be treated as Boolean by default.
        return type == MySqlType.BIT || type == MySqlType.TINYINT && context.isTinyInt1isBit();
    }

    private static Class<?> getDefaultJavaType(final MySqlReadableMetadata metadata,
                                               final CodecContext codecContext) {
        final MySqlType type = metadata.getType();
        final Integer precision = metadata.getPrecision();

        if (shouldBeTreatedAsBoolean(precision, type, codecContext)) {
            return Boolean.class;
        }

        return type.getJavaType();
    }

    static final class Builder implements CodecsBuilder {

        @GuardedBy("lock")
        private final ArrayList<Codec<?>> codecs = new ArrayList<>();

        private final ReentrantLock lock = new ReentrantLock();

        @Override
        public CodecsBuilder addFirst(Codec<?> codec) {
            lock.lock();
            try {
                if (codecs.isEmpty()) {
                    codecs.ensureCapacity(DEFAULT_CODECS.size() + 1);
                    // Add first.
                    codecs.add(codec);
                    codecs.addAll(DEFAULT_CODECS);
                } else {
                    codecs.addFirst(codec);
                }
            } finally {
                lock.unlock();
            }
            return this;
        }

        @Override
        public CodecsBuilder addLast(Codec<?> codec) {
            lock.lock();
            try {
                if (codecs.isEmpty()) {
                    codecs.addAll(DEFAULT_CODECS);
                }
                codecs.add(codec);
            } finally {
                lock.unlock();
            }
            return this;
        }

        @Override
        public Codecs build() {
            lock.lock();
            try {
                try {
                    if (codecs.isEmpty()) {
                        return new DefaultCodecs(DEFAULT_CODECS);
                    }
                    return new DefaultCodecs(InternalArrays.asImmutableList(codecs.toArray(new Codec<?>[0])));
                } finally {
                    codecs.clear();
                    codecs.trimToSize();
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
