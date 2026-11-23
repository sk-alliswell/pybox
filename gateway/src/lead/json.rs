//! A minimal JSON scanner: finds where values start and end without building
//! a tree. The gateway only needs block boundaries, and it must never
//! re-serialize a request body (that would change bytes the provider's prompt
//! cache keys on).

use std::ops::Range;

#[inline]
pub fn ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\n' | b'\r' | b'\t') {
        i += 1;
    }
    i
}

/// `b[i]` is the opening quote. Returns the index after the closing quote.
#[inline]
pub fn skip_string(b: &[u8], mut i: usize) -> Option<usize> {
    i += 1;
    loop {
        // Strings are most of a prompt: scan for the next quote or escape.
        let n = b.get(i..)?.iter().position(|&c| c == b'"' || c == b'\\')?;
        i += n;
        if b[i] == b'"' {
            return Some(i + 1);
        }
        i += 2;
    }
}

/// `i` is at the first byte of a value. Returns its exclusive end.
pub fn skip_value(b: &[u8], i: usize) -> Option<usize> {
    match *b.get(i)? {
        b'"' => skip_string(b, i),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut j = i;
            while j < b.len() {
                match b[j] {
                    b'"' => {
                        j = skip_string(b, j)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(j + 1);
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            None
        }
        _ => {
            let mut j = i;
            while j < b.len() && !matches!(b[j], b',' | b'}' | b']' | b' ' | b'\n' | b'\r' | b'\t')
            {
                j += 1;
            }
            (j > i).then_some(j)
        }
    }
}

/// Members of the object starting at `b[start] == '{'` as (key without
/// quotes, value) byte ranges.
pub fn members(b: &[u8], start: usize) -> Option<Vec<(Range<usize>, Range<usize>)>> {
    if b.get(start) != Some(&b'{') {
        return None;
    }
    let mut out = Vec::new();
    let mut i = ws(b, start + 1);
    if b.get(i) == Some(&b'}') {
        return Some(out);
    }
    loop {
        if b.get(i) != Some(&b'"') {
            return None;
        }
        let kend = skip_string(b, i)?;
        let key = i + 1..kend - 1;
        i = ws(b, kend);
        if b.get(i) != Some(&b':') {
            return None;
        }
        i = ws(b, i + 1);
        let vend = skip_value(b, i)?;
        out.push((key, i..vend));
        i = ws(b, vend);
        match b.get(i) {
            Some(b',') => i = ws(b, i + 1),
            Some(b'}') => return Some(out),
            _ => return None,
        }
    }
}

/// Elements of the array starting at `b[start] == '['`.
pub fn elems(b: &[u8], start: usize) -> Option<Vec<Range<usize>>> {
    if b.get(start) != Some(&b'[') {
        return None;
    }
    let mut out = Vec::new();
    let mut i = ws(b, start + 1);
    if b.get(i) == Some(&b']') {
        return Some(out);
    }
    loop {
        let vend = skip_value(b, i)?;
        out.push(i..vend);
        i = ws(b, vend);
        match b.get(i) {
            Some(b',') => i = ws(b, i + 1),
            Some(b']') => return Some(out),
            _ => return None,
        }
    }
}

const K: u64 = 0x9E37_79B9_7F4A_7C15;

#[inline]
fn fold(a: u64, b: u64) -> u64 {
    let m = u128::from(a) * u128::from(b);
    (m as u64) ^ ((m >> 64) as u64)
}

/// Fast non-cryptographic hash, chained through `seed`.
#[inline]
pub fn hash(seed: u64, bytes: &[u8]) -> u64 {
    let mut h = seed ^ (bytes.len() as u64).wrapping_mul(K);
    let mut chunks = bytes.chunks_exact(8);
    for c in &mut chunks {
        let w = u64::from_le_bytes(c.try_into().unwrap());
        h = fold(h ^ w, K);
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut buf = [0u8; 8];
        buf[..rest.len()].copy_from_slice(rest);
        h = fold(h ^ u64::from_le_bytes(buf), K ^ 0xA5A5_A5A5);
    }
    fold(h, K ^ 0x5851_F42D_4C95_7F2D)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_nested_values_and_escapes() {
        let b = br#"{"a": "x\"}]", "b": [1, {"c": "]"}, null], "d": {"e": []}}"#;
        let m = members(b, 0).unwrap();
        let keys: Vec<&[u8]> = m.iter().map(|(k, _)| &b[k.clone()]).collect();
        assert_eq!(keys, [b"a".as_slice(), b"b", b"d"]);
        assert_eq!(&b[m[0].1.clone()], br#""x\"}]""#);
        let arr = elems(b, m[1].1.start).unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(&b[arr[1].clone()], br#"{"c": "]"}"#);
        assert!(members(b"{\"a\": ", 0).is_none());
    }

    #[test]
    fn hash_depends_on_seed_and_content() {
        assert_ne!(hash(1, b"abc"), hash(2, b"abc"));
        assert_ne!(hash(1, b"abc"), hash(1, b"abd"));
        assert_ne!(hash(1, b"abcdefgh"), hash(1, b"abcdefgh\0"));
        assert_eq!(hash(7, b"same"), hash(7, b"same"));
    }
}
