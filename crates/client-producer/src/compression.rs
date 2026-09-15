//! The `Compression` enum, and the mapping from the producer's choice to a
//! `RecordBatch` v2 `attributes` value and a
//! `krabka-compression::CompressionType`.

use std::{fmt, str::FromStr};

use bytes::Bytes;

use crate::error::ProducerError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl fmt::Display for Compression {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        })
    }
}

impl FromStr for Compression {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "none" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "snappy" => Ok(Self::Snappy),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            _ => Err(format!("unsupported producer compression: {value}")),
        }
    }
}

impl Compression {
    #[must_use]
    pub(crate) fn compression_type(self) -> krabka_compression::CompressionType {
        match self {
            Compression::None => krabka_compression::CompressionType::None,
            Compression::Gzip => krabka_compression::CompressionType::Gzip,
            Compression::Snappy => krabka_compression::CompressionType::Snappy,
            Compression::Lz4 => krabka_compression::CompressionType::Lz4,
            Compression::Zstd => krabka_compression::CompressionType::Zstd,
        }
    }

    /// The 3-bit `compression_type` field that goes into the `RecordBatch` v2
    /// `attributes`, at bits 0..3.
    #[must_use]
    pub fn attribute_bits(self) -> i16 {
        match self {
            Compression::None => 0,
            Compression::Gzip => 1,
            Compression::Snappy => 2,
            Compression::Lz4 => 3,
            Compression::Zstd => 4,
        }
    }

    /// Compress the encoded record body. Returns the byte payload that goes
    /// into the `RecordBatch.records_body` slot.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn compress(self, raw: &[u8]) -> Result<Bytes, ProducerError> {
        Ok(krabka_compression::compress(self.compression_type(), raw)?)
    }
}

/// Default gzip compression level. Kafka's `compression.gzip.level` default
/// is `CompressionType.GZIP.defaultLevel()`, which is
/// `Deflater.DEFAULT_COMPRESSION` (-1).
pub const DEFAULT_PRODUCER_COMPRESSION_GZIP_LEVEL: i32 = -1;
/// Default lz4 compression level. Kafka's `compression.lz4.level` default is
/// 9.
pub const DEFAULT_PRODUCER_COMPRESSION_LZ4_LEVEL: i32 = 9;
/// Default zstd compression level. Kafka's `compression.zstd.level` default
/// is 3.
pub const DEFAULT_PRODUCER_COMPRESSION_ZSTD_LEVEL: i32 = 3;

/// The lowest and highest gzip levels: `Deflater.BEST_SPEED` and
/// `Deflater.BEST_COMPRESSION`.
const GZIP_LEVELS: (i32, i32) = (1, 9);
/// The lowest and highest lz4 levels, from `net.jpountz.lz4.LZ4Constants`.
const LZ4_LEVELS: (i32, i32) = (1, 17);
/// The lowest and highest zstd levels: `ZSTD_minCLevel` and `ZSTD_MAX_CLEVEL`.
const ZSTD_LEVELS: (i32, i32) = (-131_072, 22);

/// Validated compression levels of the producer (KIP-390).
///
/// Kafka's `ProducerConfig` defines `compression.gzip.level`,
/// `compression.lz4.level` and `compression.zstd.level`, and checks each one
/// with `CompressionType.levelValidator`, whatever `compression.type` is.
/// `KafkaProducer.configureCompression` then gives the level of the chosen
/// codec to the codec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompressionLevels {
    gzip: i32,
    lz4: i32,
    zstd: i32,
}

impl CompressionLevels {
    /// Validate the three levels as Kafka's `ProducerConfig` does.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `ConfigException` message, with the krabka option name,
    /// for the first level that is out of range: gzip 1 to 9 or -1, lz4 1 to
    /// 17, zstd -131072 to 22.
    pub fn new(gzip: i32, lz4: i32, zstd: i32) -> Result<Self, String> {
        let (gzip_min, gzip_max) = GZIP_LEVELS;
        if gzip > gzip_max || (gzip < gzip_min && gzip != DEFAULT_PRODUCER_COMPRESSION_GZIP_LEVEL) {
            return Err(invalid_level(
                "compression_gzip_level",
                gzip,
                &format!(
                    "Value must be between {gzip_min} and {gzip_max} or equal to {DEFAULT_PRODUCER_COMPRESSION_GZIP_LEVEL}"
                ),
            ));
        }
        check_range("compression_lz4_level", lz4, LZ4_LEVELS)?;
        check_range("compression_zstd_level", zstd, ZSTD_LEVELS)?;
        Ok(Self { gzip, lz4, zstd })
    }

    #[must_use]
    pub const fn gzip(self) -> i32 {
        self.gzip
    }

    #[must_use]
    pub const fn lz4(self) -> i32 {
        self.lz4
    }

    #[must_use]
    pub const fn zstd(self) -> i32 {
        self.zstd
    }

    /// The level of `compression`, or `None` for a codec with no levels.
    #[must_use]
    pub const fn level(self, compression: Compression) -> Option<i32> {
        match compression {
            Compression::Gzip => Some(self.gzip),
            Compression::Lz4 => Some(self.lz4),
            Compression::Zstd => Some(self.zstd),
            Compression::None | Compression::Snappy => None,
        }
    }
}

impl Default for CompressionLevels {
    fn default() -> Self {
        Self {
            gzip: DEFAULT_PRODUCER_COMPRESSION_GZIP_LEVEL,
            lz4: DEFAULT_PRODUCER_COMPRESSION_LZ4_LEVEL,
            zstd: DEFAULT_PRODUCER_COMPRESSION_ZSTD_LEVEL,
        }
    }
}

/// Kafka's `ConfigDef.Range.ensureValid`.
fn check_range(name: &str, level: i32, (min, max): (i32, i32)) -> Result<(), String> {
    if level < min {
        Err(invalid_level(
            name,
            level,
            &format!("Value must be at least {min}"),
        ))
    } else if level > max {
        Err(invalid_level(
            name,
            level,
            &format!("Value must be no more than {max}"),
        ))
    } else {
        Ok(())
    }
}

/// Kafka's `ConfigException(name, value, message)` text.
fn invalid_level(name: &str, level: i32, message: &str) -> String {
    format!("Invalid value {level} for configuration {name}: {message}")
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn none_round_trip_is_identity() {
        let raw = b"hello producer";
        let out = Compression::None.compress(raw).unwrap();
        assert2::assert!(out.as_ref() == raw);
    }

    /// Each row is one set of levels and the result of Kafka's validators:
    /// `CompressionType.GZIP.levelValidator` for gzip, and
    /// `ConfigDef.Range.ensureValid` for lz4 and zstd.
    #[test]
    fn compression_levels_follow_kafka_validators() {
        let levels = |gzip, lz4, zstd| CompressionLevels { gzip, lz4, zstd };
        let gzip_error = |level: i32| {
            Err(format!(
                "Invalid value {level} for configuration compression_gzip_level: Value must be \
                 between 1 and 9 or equal to -1"
            ))
        };
        let cases = [
            ("defaults", (-1, 9, 3), Ok(levels(-1, 9, 3))),
            (
                "lowest levels",
                (1, 1, -131_072),
                Ok(levels(1, 1, -131_072)),
            ),
            ("highest levels", (9, 17, 22), Ok(levels(9, 17, 22))),
            ("gzip zero", (0, 9, 3), gzip_error(0)),
            ("gzip below the default", (-2, 9, 3), gzip_error(-2)),
            ("gzip above nine", (10, 9, 3), gzip_error(10)),
            (
                "lz4 zero",
                (-1, 0, 3),
                Err(
                    "Invalid value 0 for configuration compression_lz4_level: Value must be at \
                     least 1"
                        .to_owned(),
                ),
            ),
            (
                "lz4 above seventeen",
                (-1, 18, 3),
                Err(
                    "Invalid value 18 for configuration compression_lz4_level: Value must be \
                     no more than 17"
                        .to_owned(),
                ),
            ),
            (
                "zstd below the minimum",
                (-1, 9, -131_073),
                Err(
                    "Invalid value -131073 for configuration compression_zstd_level: Value must \
                     be at least -131072"
                        .to_owned(),
                ),
            ),
            (
                "zstd above twenty-two",
                (-1, 9, 23),
                Err(
                    "Invalid value 23 for configuration compression_zstd_level: Value must be \
                     no more than 22"
                        .to_owned(),
                ),
            ),
        ];
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, (gzip, lz4, zstd), expected) in cases {
            actual.push((name, CompressionLevels::new(gzip, lz4, zstd)));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
        assert2::assert!(CompressionLevels::default() == levels(-1, 9, 3));
        assert2::assert!(
            [
                Compression::None,
                Compression::Gzip,
                Compression::Snappy,
                Compression::Lz4,
                Compression::Zstd,
            ]
            .map(|compression| levels(5, 12, 19).level(compression))
                == [None, Some(5), None, Some(12), Some(19)]
        );
        let set = levels(5, 12, 19);
        assert2::assert!([set.gzip(), set.lz4(), set.zstd()] == [5, 12, 19]);
    }

    #[test]
    fn attribute_bits_match_kafka_table() {
        for (_name, compression, want) in [
            ("none", Compression::None, 0),
            ("gzip", Compression::Gzip, 1),
            ("snappy", Compression::Snappy, 2),
            ("lz4", Compression::Lz4, 3),
            ("zstd", Compression::Zstd, 4),
        ] {
            assert2::assert!(compression.attribute_bits() == want);
        }
    }

    #[test]
    fn gzip_round_trip_via_decoder() {
        use krabka_compression::CompressionType;
        let raw = b"the quick brown fox";
        let compressed = Compression::Gzip.compress(raw).unwrap();
        let decoded = krabka_compression::decompress(
            CompressionType::Gzip,
            &compressed,
            krabka_units::convert::ByteSizeExt::from_bytes(u64::MAX),
        )
        .unwrap();
        assert2::assert!(decoded.as_ref() == raw);
    }

    #[test]
    fn compression_parses_and_displays_canonical_names() {
        for (name, compression) in [
            ("none", Compression::None),
            ("gzip", Compression::Gzip),
            ("snappy", Compression::Snappy),
            ("lz4", Compression::Lz4),
            ("zstd", Compression::Zstd),
        ] {
            assert_eq!(name.parse::<Compression>(), Ok(compression));
            assert_eq!(compression.to_string(), name);
        }
        assert_eq!(
            "brotli".parse::<Compression>(),
            Err("unsupported producer compression: brotli".to_owned())
        );
    }
}
