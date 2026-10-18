//! The `x-amz-checksum-*` algorithms S3 accepts. One table drives hashing, verification and
//! storage, so an algorithm cannot be hashed without also being verified and stored.
//!
//! A multipart object's checksum derives from its parts' in one of two ways
//! (`x-amz-checksum-type`). COMPOSITE is the digest of the concatenated binary part checksums
//! followed by `-<part count>`; base64 never contains `-`, so the value itself tells the two
//! apart. FULL_OBJECT is the checksum of the whole object, as a single PUT gets, which only the
//! CRCs can derive from their parts'.

use base64_simd::STANDARD;
use s3s::checksum::ChecksumHasher;
use s3s::dto::{Checksum, ChecksumAlgorithm, ChecksumType};

pub type Field = fn(&mut Checksum) -> &mut Option<String>;
pub type Enable = fn(&mut ChecksumHasher);

/// `(algorithm name, Checksum field, enable on a hasher)`.
pub const ALGORITHMS: [(&str, Field, Enable); 10] = [
    (
        ChecksumAlgorithm::CRC32,
        |c| &mut c.checksum_crc32,
        |h| h.crc32 = Some(Default::default()),
    ),
    (
        ChecksumAlgorithm::CRC32C,
        |c| &mut c.checksum_crc32c,
        |h| h.crc32c = Some(Default::default()),
    ),
    (
        ChecksumAlgorithm::CRC64NVME,
        |c| &mut c.checksum_crc64nvme,
        |h| h.crc64nvme = Some(Default::default()),
    ),
    (
        ChecksumAlgorithm::SHA1,
        |c| &mut c.checksum_sha1,
        |h| h.sha1 = Some(Default::default()),
    ),
    (
        ChecksumAlgorithm::SHA256,
        |c| &mut c.checksum_sha256,
        |h| h.sha256 = Some(Default::default()),
    ),
    (
        ChecksumAlgorithm::SHA512,
        |c| &mut c.checksum_sha512,
        |h| h.sha512 = Some(Default::default()),
    ),
    (
        ChecksumAlgorithm::MD5,
        |c| &mut c.checksum_md5,
        |h| h.md5 = Some(Default::default()),
    ),
    (
        ChecksumAlgorithm::XXHASH64,
        |c| &mut c.checksum_xxhash64,
        |h| h.xxhash64 = Some(Default::default()),
    ),
    (
        ChecksumAlgorithm::XXHASH3,
        |c| &mut c.checksum_xxhash3,
        |h| h.xxhash3 = Some(Default::default()),
    ),
    (
        ChecksumAlgorithm::XXHASH128,
        |c| &mut c.checksum_xxhash128,
        |h| h.xxhash128 = Some(Default::default()),
    ),
];

/// The [`ALGORITHMS`] entry named `name`.
pub fn algorithm(name: &str) -> Option<(&'static str, Field, Enable)> {
    ALGORITHMS.into_iter().find(|(n, ..)| *n == name)
}

/// Whether `value` is a composite checksum (`<base64>-<part count>`).
pub fn is_composite(value: &str) -> bool {
    value.contains('-')
}

/// The type a multipart upload gets when it names an algorithm but no type, as on AWS.
pub fn default_type(algorithm: &str) -> &'static str {
    if algorithm == ChecksumAlgorithm::CRC64NVME {
        ChecksumType::FULL_OBJECT
    } else {
        ChecksumType::COMPOSITE
    }
}

/// Whether a multipart object's checksum can be of `checksum_type` with `algorithm`. AWS has no
/// composite CRC64NVME, and only a CRC combines into a full-object checksum.
pub fn supports(algorithm: &str, checksum_type: &str) -> bool {
    match checksum_type {
        ChecksumType::COMPOSITE => algorithm != ChecksumAlgorithm::CRC64NVME,
        ChecksumType::FULL_OBJECT => crc(algorithm).is_some(),
        _ => false,
    }
}

/// The COMPOSITE checksum of parts with checksums `parts` (base64, in part order). `None` when
/// the algorithm is unknown or a part value is not base64.
pub fn composite<'a>(algorithm: &str, parts: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let (_, field, enable) = self::algorithm(algorithm)?;
    let mut hasher = ChecksumHasher::default();
    enable(&mut hasher);
    let mut count = 0;
    for part in parts {
        hasher.update(&STANDARD.decode_to_vec(part).ok()?);
        count += 1;
    }
    let mut digest = hasher.finalize();
    Some(format!("{}-{count}", field(&mut digest).take()?))
}

/// The FULL_OBJECT checksum of parts with CRCs and sizes `parts` (in part order): the CRC of the
/// concatenated parts. `None` when the algorithm is not a CRC, a part value is malformed, or
/// there is no part.
pub fn combine<'a>(
    algorithm: &str,
    parts: impl IntoIterator<Item = (&'a str, u64)>,
) -> Option<String> {
    let crc = crc(algorithm)?;
    let mut combined = None;
    for (value, len) in parts {
        let part = crc.decode(value)?;
        combined = Some(match combined {
            None => part,
            Some(head) => crc.combine(head, part, len),
        });
    }
    Some(crc.encode(combined?))
}

/// A reflected CRC whose initial value and final XOR are both all ones, as every S3 CRC is.
struct Crc {
    width: u32,
    /// The polynomial without its top term, bit-reversed.
    poly: u64,
}

fn crc(algorithm: &str) -> Option<Crc> {
    let (width, poly) = match algorithm {
        ChecksumAlgorithm::CRC32 => (32, 0xEDB8_8320),
        ChecksumAlgorithm::CRC32C => (32, 0x82F6_3B78),
        ChecksumAlgorithm::CRC64NVME => (64, 0x9A6C_9329_AC4B_C9B5),
        _ => return None,
    };
    Some(Crc { width, poly })
}

impl Crc {
    /// The CRC of `A ‖ B` from the CRCs of `A` and `B` and the length of `B`, as zlib's
    /// `crc32_combine`: `crc(A)` carried past `len_b` more bytes is `crc(A) · x^(8·len_b)` mod P,
    /// and the initial value and final XOR cancel because they are equal. Square-and-multiply
    /// keeps this to a few thousand bit operations per part, where zlib's older matrix method
    /// (crc-fast's `checksum_combine`) costs about a hundred times more, which a Complete of
    /// 10000 parts would feel.
    fn combine(&self, crc_a: u64, crc_b: u64, len_b: u64) -> u64 {
        self.multiply(self.x_to_8n(len_b), crc_a) ^ crc_b
    }

    /// `x^(8n)` mod P.
    fn x_to_8n(&self, mut n: u64) -> u64 {
        let one = 1 << (self.width - 1);
        let mut result = one;
        let mut power = one >> 8;
        while n != 0 {
            if n & 1 == 1 {
                result = self.multiply(result, power);
            }
            power = self.multiply(power, power);
            n >>= 1;
        }
        result
    }

    /// `a · b` mod P. Bit `width - 1` holds `x^0`, as the reflected register does, so
    /// multiplying by `x` is a right shift.
    fn multiply(&self, a: u64, mut b: u64) -> u64 {
        let mut product = 0;
        let mut term = 1 << (self.width - 1);
        while term != 0 {
            if a & term != 0 {
                product ^= b;
            }
            b = if b & 1 == 1 {
                (b >> 1) ^ self.poly
            } else {
                b >> 1
            };
            term >>= 1;
        }
        product
    }

    fn decode(&self, value: &str) -> Option<u64> {
        let bytes = STANDARD.decode_to_vec(value).ok()?;
        (bytes.len() * 8 == self.width as usize)
            .then(|| bytes.iter().fold(0, |n, &b| (n << 8) | u64::from(b)))
    }

    fn encode(&self, crc: u64) -> String {
        let bytes = crc.to_be_bytes();
        STANDARD.encode_to_string(&bytes[bytes.len() - self.width as usize / 8..])
    }
}

#[cfg(test)]
mod tests {
    use s3s::crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme};

    use super::*;

    fn b64(digest: &[u8]) -> String {
        STANDARD.encode_to_string(digest)
    }

    #[test]
    fn combined_crcs_equal_the_crc_of_the_whole_object() {
        // A FULL_OBJECT checksum is what a client computes over the object it downloads; SDKs
        // validate GET bodies against it, so a wrong combination fails every download.
        let data: Vec<u8> = (0..10_000u32).map(|i| (i * 7 % 251) as u8).collect();
        let whole = |name: &str, bytes: &[u8]| match name {
            ChecksumAlgorithm::CRC32 => b64(&Crc32::checksum(bytes)),
            ChecksumAlgorithm::CRC32C => b64(&Crc32c::checksum(bytes)),
            _ => b64(&Crc64Nvme::checksum(bytes)),
        };
        // Includes an empty part and a one-byte part, and a single part on its own.
        for cuts in [
            &[0, 10_000][..],
            &[0, 1, 10_000],
            &[0, 4_096, 4_096, 9_999, 10_000],
            &[0, 3, 5_000, 7_777, 10_000],
        ] {
            for name in [
                ChecksumAlgorithm::CRC32,
                ChecksumAlgorithm::CRC32C,
                ChecksumAlgorithm::CRC64NVME,
            ] {
                let parts: Vec<(String, u64)> = cuts
                    .windows(2)
                    .map(|w| (whole(name, &data[w[0]..w[1]]), (w[1] - w[0]) as u64))
                    .collect();
                let combined = combine(name, parts.iter().map(|(v, n)| (v.as_str(), *n)));
                assert_eq!(combined, Some(whole(name, &data)), "{name} {cuts:?}");
            }
        }
    }

    #[test]
    fn multipart_checksums_match_the_s3_tests_vectors() {
        // ceph s3-tests (test_multipart_use_cksum_helper_*, test_multipart_checksum_sha256):
        // three 5 MiB parts of 'A', 'B' and 'C', and one 1 KiB part of 'A'. The values are
        // what S3 reports, so clients that verify them compute these.
        const PART: u64 = 5 * 1024 * 1024;
        let crcs = [
            (
                ChecksumAlgorithm::CRC32,
                ["JRTCyQ==", "QoZTGg==", "YAgjqw=="],
                "WgDhBQ==",
            ),
            (
                ChecksumAlgorithm::CRC32C,
                ["MDaLrw==", "TH4EZg==", "Z7mBIQ=="],
                "xU+Krw==",
            ),
            (
                ChecksumAlgorithm::CRC64NVME,
                ["L/E4WYn8v98=", "xW1l19VobYM=", "cK5MnNaWrW4="],
                "i+6LR0y3eFo=",
            ),
        ];
        for (name, parts, expected) in crcs {
            let combined = combine(name, parts.iter().map(|p| (*p, PART)));
            assert_eq!(combined.as_deref(), Some(expected), "{name}");
        }
        let composites = [
            (
                ChecksumAlgorithm::SHA256,
                &[
                    "275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=",
                    "mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=",
                    "Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=",
                ][..],
                "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
            ),
            (
                ChecksumAlgorithm::SHA1,
                &[
                    "iIaTCGbm+vdVjNqIMF2S0T7ibMk=",
                    "LS/TJ32bAVKEwRu+sE3X7awh/lk=",
                    "6DDwovUaHwrKNXDMzOGbuvj9kxI=",
                ],
                "sizjvY4eud3MrcHdZM3cQ/ol39o=-3",
            ),
            (
                ChecksumAlgorithm::SHA256,
                &["arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0="],
                "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1",
            ),
        ];
        for (name, parts, expected) in composites {
            let value = composite(name, parts.iter().copied());
            assert_eq!(value.as_deref(), Some(expected), "{name}");
            assert!(is_composite(expected));
        }
    }

    #[test]
    fn malformed_part_values_give_no_object_checksum() {
        // A manifest value that does not decode must fail the Complete, not yield a checksum
        // the object never had.
        assert_eq!(
            combine(ChecksumAlgorithm::CRC32, [("not base64!", 1)]),
            None
        );
        // A CRC64 value is no CRC32.
        assert_eq!(
            combine(ChecksumAlgorithm::CRC32, [("L/E4WYn8v98=", 1)]),
            None
        );
        assert_eq!(combine(ChecksumAlgorithm::CRC32, []), None);
        assert_eq!(combine(ChecksumAlgorithm::SHA256, [("JRTCyQ==", 1)]), None);
        assert_eq!(composite(ChecksumAlgorithm::SHA256, ["not base64!"]), None);
    }

    #[test]
    fn checksum_types_follow_aws() {
        // AWS refuses the combinations it cannot compute; accepting one would store an object
        // checksum that no other S3 would produce.
        assert_eq!(
            default_type(ChecksumAlgorithm::CRC64NVME),
            ChecksumType::FULL_OBJECT
        );
        assert_eq!(
            default_type(ChecksumAlgorithm::CRC32),
            ChecksumType::COMPOSITE
        );
        assert!(supports(
            ChecksumAlgorithm::CRC32C,
            ChecksumType::FULL_OBJECT
        ));
        assert!(supports(ChecksumAlgorithm::CRC32C, ChecksumType::COMPOSITE));
        assert!(!supports(
            ChecksumAlgorithm::CRC64NVME,
            ChecksumType::COMPOSITE
        ));
        assert!(!supports(
            ChecksumAlgorithm::SHA256,
            ChecksumType::FULL_OBJECT
        ));
        assert!(!supports(ChecksumAlgorithm::SHA256, "PARTIAL"));
    }
}
