//! Cost computed at ingest from a pricing table: USD per million tokens, matched by the longest
//! model-name prefix. The built-in table is a convenience snapshot of public list prices and will
//! drift; override or extend it with `[pricing."<model>"]` in `forge.toml`.

use std::collections::BTreeMap;

use crate::config::ModelPrice;

const BUILTIN: &[(&str, f64, f64)] = &[
    ("gpt-5-nano", 0.05, 0.40),
    ("gpt-5-mini", 0.25, 2.00),
    ("gpt-5", 1.25, 10.00),
    ("gpt-4.1-nano", 0.10, 0.40),
    ("gpt-4.1-mini", 0.40, 1.60),
    ("gpt-4.1", 2.00, 8.00),
    ("gpt-4o-mini", 0.15, 0.60),
    ("gpt-4o", 2.50, 10.00),
    ("gpt-4-turbo", 10.00, 30.00),
    ("gpt-3.5-turbo", 0.50, 1.50),
    ("o4-mini", 1.10, 4.40),
    ("o3-mini", 1.10, 4.40),
    ("o3", 2.00, 8.00),
    ("o1-mini", 1.10, 4.40),
    ("o1", 15.00, 60.00),
    ("text-embedding-3-small", 0.02, 0.0),
    ("text-embedding-3-large", 0.13, 0.0),
    ("claude-opus-4", 15.00, 75.00),
    ("claude-sonnet-4", 3.00, 15.00),
    ("claude-haiku-4", 1.00, 5.00),
    ("claude-3-7-sonnet", 3.00, 15.00),
    ("claude-3-5-sonnet", 3.00, 15.00),
    ("claude-3-5-haiku", 0.80, 4.00),
    ("claude-3-opus", 15.00, 75.00),
    ("claude-3-haiku", 0.25, 1.25),
    ("gemini-2.5-pro", 1.25, 10.00),
    ("gemini-2.5-flash", 0.30, 2.50),
    ("gemini-2.0-flash", 0.10, 0.40),
    ("gemini-1.5-pro", 1.25, 5.00),
    ("gemini-1.5-flash", 0.075, 0.30),
    ("mistral-large", 2.00, 6.00),
    ("mistral-small", 0.20, 0.60),
];

#[derive(Debug, Clone)]
pub struct Pricing {
    prices: BTreeMap<String, ModelPrice>,
}

impl Default for Pricing {
    fn default() -> Self {
        Pricing::new(&BTreeMap::new())
    }
}

impl Pricing {
    pub fn new(overrides: &BTreeMap<String, ModelPrice>) -> Self {
        let mut prices: BTreeMap<String, ModelPrice> = BUILTIN
            .iter()
            .map(|(m, i, o)| {
                (
                    m.to_string(),
                    ModelPrice {
                        input: *i,
                        output: *o,
                    },
                )
            })
            .collect();
        for (k, v) in overrides {
            prices.insert(k.to_ascii_lowercase(), *v);
        }
        Pricing { prices }
    }

    pub fn price(&self, model: &str) -> Option<ModelPrice> {
        let m = model.to_ascii_lowercase();
        // Strip provider prefixes like "openai/" or "anthropic.".
        let m = m.rsplit('/').next().unwrap_or(&m);
        let m = m.strip_prefix("anthropic.").unwrap_or(m);
        self.prices
            .iter()
            .filter(|(k, _)| m.starts_with(k.as_str()))
            .max_by_key(|(k, _)| k.len())
            .map(|(_, v)| *v)
    }

    pub fn cost(
        &self,
        model: Option<&str>,
        prompt: Option<i64>,
        completion: Option<i64>,
    ) -> Option<f64> {
        let p = self.price(model?)?;
        if prompt.is_none() && completion.is_none() {
            return None;
        }
        let cost = prompt.unwrap_or(0) as f64 * p.input / 1e6
            + completion.unwrap_or(0) as f64 * p.output / 1e6;
        Some(cost)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn longest_prefix() {
        let p = Pricing::default();
        assert_eq!(p.price("gpt-4o-mini-2024-07-18").unwrap().input, 0.15);
        assert_eq!(p.price("gpt-4o-2024-08-06").unwrap().input, 2.5);
        assert_eq!(p.price("openai/gpt-4o").unwrap().input, 2.5);
        assert!(p.price("my-local-llama").is_none());
        let c = p
            .cost(Some("gpt-4o"), Some(1_000_000), Some(1_000_000))
            .unwrap();
        assert!((c - 12.5).abs() < 1e-9);
        let mut o = BTreeMap::new();
        o.insert(
            "my-local-llama".to_string(),
            ModelPrice {
                input: 1.0,
                output: 1.0,
            },
        );
        assert!(Pricing::new(&o).price("my-local-llama-3").is_some());
    }
}
