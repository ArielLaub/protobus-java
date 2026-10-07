package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import io.github.ariellaub.protobus.types.bigint;
import io.github.ariellaub.protobus.types.timestamp;
import java.math.BigInteger;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import pbtest.Wallet;

class CustomTypesTest {
    @Test
    void bigintIsThirtyTwoBigEndianBytes() {
        bigint one = CustomTypes.bigint(1);
        byte[] expected = new byte[32];
        expected[31] = 1;
        assertArrayEquals(expected, one.getValue().toByteArray());
        assertEquals(BigInteger.ONE, CustomTypes.toBigInteger(one));
        assertEquals(CustomTypes.BIGINT_MAX, CustomTypes.toBigInteger(CustomTypes.bigint(CustomTypes.BIGINT_MAX)));
        assertEquals(new BigInteger("255"), CustomTypes.toBigInteger(CustomTypes.bigint("0xff")));
    }

    @Test
    void bigintRefusesWhatNoPortCanCarry() {
        assertThrows(CustomTypeRangeError.class, () -> CustomTypes.bigint(BigInteger.valueOf(-1)));
        assertThrows(CustomTypeRangeError.class, () -> CustomTypes.bigint(CustomTypes.BIGINT_MAX.add(BigInteger.ONE)));
        assertThrows(CustomTypeRangeError.class, () -> CustomTypes.bigint("12x"));
        bigint wide = bigint.newBuilder().setValue(ByteString.copyFrom(new byte[33])).build();
        assertThrows(CustomTypeRangeError.class, () -> CustomTypes.toBigInteger(wide));
    }

    @Test
    void anUnsetOrShortBigintDecodes() {
        assertEquals(BigInteger.ZERO, CustomTypes.toBigInteger(bigint.getDefaultInstance()));
        bigint shortValue = bigint.newBuilder().setValue(ByteString.copyFrom(new byte[] {1, 0})).build();
        assertEquals(BigInteger.valueOf(256), CustomTypes.toBigInteger(shortValue));
    }

    @Test
    void timestampIsSignedMillis() {
        Instant before = Instant.parse("1969-07-20T20:17:40Z");
        timestamp t = CustomTypes.timestamp(before);
        assertEquals(before.toEpochMilli(), t.getValue());
        assertEquals(before, CustomTypes.toInstant(t));
        assertThrows(CustomTypeRangeError.class, () -> CustomTypes.timestampMillis(8_640_000_000_000_001L));
    }

    @Test
    void validationWalksMapsRepeatedFieldsAndNesting() {
        Wallet ok = Wallet.newBuilder().setAmount(CustomTypes.bigint(5))
                .putBalances("k", CustomTypes.bigint(7)).addParts(CustomTypes.bigint(1)).build();
        CustomTypes.validate(ok);
        bigint wide = bigint.newBuilder().setValue(ByteString.copyFrom(new byte[40])).build();
        assertThrows(CustomTypeRangeError.class, () -> CustomTypes.validate(Wallet.newBuilder().putBalances("k", wide)
                .build()));
        assertThrows(CustomTypeRangeError.class, () -> CustomTypes.validate(Wallet.newBuilder().addParts(wide)
                .build()));
        assertThrows(CustomTypeRangeError.class, () -> MessageFactory.encodeMessage(Wallet.newBuilder()
                .setAt(timestamp.newBuilder().setValue(Long.MAX_VALUE)).build()));
    }
}
