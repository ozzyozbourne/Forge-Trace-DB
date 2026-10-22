//! Tokenizer: Unicode word segmentation (UAX #29), lowercase, ASCII folding, no stemming.
//!
//! The design asked for tantivy's tokenizer; the full `tantivy` crate is a large dependency for
//! three small transforms, so this implements the same pipeline (`SimpleTokenizer` +
//! `LowerCaser` + `AsciiFoldingFilter`) on `unicode-segmentation` directly.

use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::is_combining_mark;
use unicode_segmentation::UnicodeSegmentation;

/// Tokens longer than this are dropped (base64 blobs, hashes).
pub const MAX_TOKEN_BYTES: usize = 64;

/// Text past this many bytes per field per span is not indexed.
pub const MAX_FIELD_BYTES: usize = 1 << 20;

fn fold(word: &str) -> String {
    if word.is_ascii() {
        return word.to_ascii_lowercase();
    }
    let lower = word.to_lowercase();
    let folded: String = lower.nfkd().filter(|c| !is_combining_mark(*c)).collect();
    // Keep the folded form only when folding made it ASCII (é -> e); otherwise keep the
    // lowercase original (e.g. CJK, Cyrillic).
    if folded.is_ascii() {
        folded
    } else {
        lower.nfc().collect()
    }
}

/// Split text into normalized tokens, calling `f` for each.
pub fn for_each_token(text: &str, mut f: impl FnMut(&str)) {
    let text = truncate(text, MAX_FIELD_BYTES);
    for w in text.unicode_words() {
        let t = fold(w);
        if !t.is_empty() && t.len() <= MAX_TOKEN_BYTES {
            f(&t);
        }
    }
}

pub fn tokenize(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for_each_token(text, |t| out.push(t.to_string()));
    out
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline() {
        assert_eq!(
            tokenize("Hello, World! Refund-policy v2.1"),
            vec!["hello", "world", "refund", "policy", "v2.1"]
        );
        assert_eq!(tokenize("Crème Brûlée"), vec!["creme", "brulee"]);
        assert_eq!(tokenize("Привет мир"), vec!["привет", "мир"]);
        assert!(tokenize(&"a".repeat(100)).is_empty());
    }
}
