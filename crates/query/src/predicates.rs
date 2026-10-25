//! Turns pushed-down DataFusion filters into a pruning predicate.
//!
//! Pruning is always conservative: every filter is reported as `Inexact`, so DataFusion
//! re-applies it on the rows we return.

use std::collections::BTreeSet;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use model::schema;
use table::AddSegment;

use crate::udf::HEADER;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicFilter {
    Any(BTreeSet<String>),
    Prefix(String),
}

impl TopicFilter {
    pub fn matches(&self, topic: &str) -> bool {
        match self {
            Self::Any(set) => set.contains(topic),
            Self::Prefix(p) => topic.starts_with(p.as_str()),
        }
    }
}

/// Conjunction of everything we understood from the filters.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Predicate {
    pub topics: Vec<TopicFilter>,
    pub partitions: Option<BTreeSet<i32>>,
    pub offset_min: Option<i64>,
    pub offset_max: Option<i64>,
    pub ts_min: Option<i64>,
    pub ts_max: Option<i64>,
    /// `header(headers, key)` must equal one of the values.
    pub headers: Vec<(String, Vec<Vec<u8>>)>,
    /// `key` must equal one of the values.
    pub keys: Option<Vec<Vec<u8>>>,
}

fn strip_cast(e: &Expr) -> &Expr {
    match e {
        Expr::Cast(c) => strip_cast(&c.expr),
        Expr::TryCast(c) => strip_cast(&c.expr),
        Expr::Alias(a) => strip_cast(&a.expr),
        other => other,
    }
}

fn column_name(e: &Expr) -> Option<&str> {
    match strip_cast(e) {
        Expr::Column(c) => Some(c.name.as_str()),
        _ => None,
    }
}

/// `header(headers, 'k')` → `k`
fn header_key(e: &Expr) -> Option<String> {
    match strip_cast(e) {
        Expr::ScalarFunction(f) if f.func.name() == HEADER && f.args.len() == 2 => {
            column_name(&f.args[0]).filter(|c| *c == schema::HEADERS)?;
            literal_string(&f.args[1])
        }
        _ => None,
    }
}

fn literal(e: &Expr) -> Option<&ScalarValue> {
    match strip_cast(e) {
        Expr::Literal(v, _) => Some(v),
        _ => None,
    }
}

fn literal_string(e: &Expr) -> Option<String> {
    match literal(e)? {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::Utf8View(Some(s))
        | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

fn literal_bytes(e: &Expr) -> Option<Vec<u8>> {
    match literal(e)? {
        ScalarValue::Binary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => Some(b.clone()),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::Utf8View(Some(s))
        | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

/// Integers, and timestamps of any unit, as i64 (timestamps in milliseconds).
fn literal_i64(e: &Expr) -> Option<i64> {
    let v = literal(e)?;
    match v {
        ScalarValue::Int8(Some(x)) => Some(*x as i64),
        ScalarValue::Int16(Some(x)) => Some(*x as i64),
        ScalarValue::Int32(Some(x)) => Some(*x as i64),
        ScalarValue::Int64(Some(x)) => Some(*x),
        ScalarValue::UInt8(Some(x)) => Some(*x as i64),
        ScalarValue::UInt16(Some(x)) => Some(*x as i64),
        ScalarValue::UInt32(Some(x)) => Some(*x as i64),
        ScalarValue::UInt64(Some(x)) => i64::try_from(*x).ok(),
        ScalarValue::TimestampSecond(Some(x), _) => Some(x.saturating_mul(1000)),
        ScalarValue::TimestampMillisecond(Some(x), _) => Some(*x),
        ScalarValue::TimestampMicrosecond(Some(x), _) => Some(x.div_euclid(1000)),
        ScalarValue::TimestampNanosecond(Some(x), _) => Some(x.div_euclid(1_000_000)),
        _ => None,
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Cmp {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

fn flip(c: Cmp) -> Cmp {
    match c {
        Cmp::Lt => Cmp::Gt,
        Cmp::LtEq => Cmp::GtEq,
        Cmp::Gt => Cmp::Lt,
        Cmp::GtEq => Cmp::LtEq,
        Cmp::Eq => Cmp::Eq,
    }
}

fn cmp_of(op: Operator) -> Option<Cmp> {
    Some(match op {
        Operator::Eq => Cmp::Eq,
        Operator::Lt => Cmp::Lt,
        Operator::LtEq => Cmp::LtEq,
        Operator::Gt => Cmp::Gt,
        Operator::GtEq => Cmp::GtEq,
        _ => return None,
    })
}

impl Predicate {
    pub fn from_filters(filters: &[Expr]) -> Self {
        let mut p = Self::default();
        for f in filters {
            p.add(f);
        }
        p
    }

    /// Whether a filter can be used for pruning.
    pub fn understands(filter: &Expr) -> bool {
        Self::default().add(filter)
    }

    fn narrow_range(min: &mut Option<i64>, max: &mut Option<i64>, cmp: Cmp, v: i64) {
        let lo = |m: &mut Option<i64>, x: i64| *m = Some(m.map_or(x, |c| c.max(x)));
        let hi = |m: &mut Option<i64>, x: i64| *m = Some(m.map_or(x, |c| c.min(x)));
        match cmp {
            Cmp::Eq => {
                lo(min, v);
                hi(max, v);
            }
            Cmp::Gt => lo(min, v.saturating_add(1)),
            Cmp::GtEq => lo(min, v),
            Cmp::Lt => hi(max, v.saturating_sub(1)),
            Cmp::LtEq => hi(max, v),
        }
    }

    fn add_comparison(&mut self, col: &Expr, cmp: Cmp, value: &Expr) -> bool {
        if let Some(key) = header_key(col) {
            if cmp != Cmp::Eq {
                return false;
            }
            return match literal_bytes(value) {
                Some(v) => {
                    self.headers.push((key, vec![v]));
                    true
                }
                None => false,
            };
        }
        match column_name(col) {
            Some(schema::TOPIC) if cmp == Cmp::Eq => match literal_string(value) {
                Some(t) => {
                    self.topics.push(TopicFilter::Any(BTreeSet::from([t])));
                    true
                }
                None => false,
            },
            Some(schema::PARTITION) if cmp == Cmp::Eq => match literal_i64(value) {
                Some(v) => self.add_partitions([v as i32]),
                None => false,
            },
            Some(schema::OFFSET) => match literal_i64(value) {
                Some(v) => {
                    Self::narrow_range(&mut self.offset_min, &mut self.offset_max, cmp, v);
                    true
                }
                None => false,
            },
            Some(schema::TIMESTAMP) => match literal_i64(value) {
                Some(v) => {
                    Self::narrow_range(&mut self.ts_min, &mut self.ts_max, cmp, v);
                    true
                }
                None => false,
            },
            Some(schema::KEY) if cmp == Cmp::Eq => match literal_bytes(value) {
                Some(v) => self.add_keys(vec![v]),
                None => false,
            },
            _ => false,
        }
    }

    fn add_partitions(&mut self, values: impl IntoIterator<Item = i32>) -> bool {
        let set: BTreeSet<i32> = values.into_iter().collect();
        self.partitions = Some(match self.partitions.take() {
            Some(cur) => cur.intersection(&set).copied().collect(),
            None => set,
        });
        true
    }

    fn add_keys(&mut self, values: Vec<Vec<u8>>) -> bool {
        self.keys = Some(match self.keys.take() {
            Some(cur) => cur.into_iter().filter(|k| values.contains(k)).collect(),
            None => values,
        });
        true
    }

    /// Adds one filter; returns false if it was not understood (and nothing was added).
    fn add(&mut self, filter: &Expr) -> bool {
        match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if *op == Operator::And {
                    let mut tmp = self.clone();
                    if tmp.add(left) && tmp.add(right) {
                        *self = tmp;
                        return true;
                    }
                    return false;
                }
                let Some(cmp) = cmp_of(*op) else {
                    return false;
                };
                if literal(right).is_some() {
                    self.add_comparison(left, cmp, right)
                } else if literal(left).is_some() {
                    self.add_comparison(right, flip(cmp), left)
                } else {
                    false
                }
            }
            Expr::Between(b) if !b.negated => {
                let mut tmp = self.clone();
                if tmp.add_comparison(&b.expr, Cmp::GtEq, &b.low)
                    && tmp.add_comparison(&b.expr, Cmp::LtEq, &b.high)
                {
                    *self = tmp;
                    true
                } else {
                    false
                }
            }
            Expr::InList(l) if !l.negated => {
                if let Some(key) = header_key(&l.expr) {
                    let values: Option<Vec<Vec<u8>>> = l.list.iter().map(literal_bytes).collect();
                    return match values {
                        Some(v) => {
                            self.headers.push((key, v));
                            true
                        }
                        None => false,
                    };
                }
                match column_name(&l.expr) {
                    Some(schema::TOPIC) => {
                        let values: Option<BTreeSet<String>> =
                            l.list.iter().map(literal_string).collect();
                        match values {
                            Some(v) => {
                                self.topics.push(TopicFilter::Any(v));
                                true
                            }
                            None => false,
                        }
                    }
                    Some(schema::PARTITION) => {
                        let values: Option<Vec<i64>> = l.list.iter().map(literal_i64).collect();
                        match values {
                            Some(v) => self.add_partitions(v.into_iter().map(|x| x as i32)),
                            None => false,
                        }
                    }
                    Some(schema::KEY) => {
                        let values: Option<Vec<Vec<u8>>> =
                            l.list.iter().map(literal_bytes).collect();
                        match values {
                            Some(v) => self.add_keys(v),
                            None => false,
                        }
                    }
                    _ => false,
                }
            }
            Expr::Like(like)
                if !like.negated && !like.case_insensitive && like.escape_char.is_none() =>
            {
                if column_name(&like.expr) != Some(schema::TOPIC) {
                    return false;
                }
                match literal_string(&like.pattern) {
                    Some(p) => {
                        let prefix = p.strip_suffix('%').unwrap_or(&p);
                        if prefix.contains(['%', '_']) {
                            return false;
                        }
                        self.topics.push(if p.ends_with('%') {
                            TopicFilter::Prefix(prefix.to_owned())
                        } else {
                            TopicFilter::Any(BTreeSet::from([p.clone()]))
                        });
                        true
                    }
                    None => false,
                }
            }
            _ => false,
        }
    }

    pub fn topic_matches(&self, topic: &str) -> bool {
        self.topics.iter().all(|f| f.matches(topic))
    }

    fn ranges_overlap(min: Option<i64>, max: Option<i64>, lo: i64, hi: i64) -> bool {
        min.is_none_or(|m| hi >= m) && max.is_none_or(|m| lo <= m)
    }

    /// File-level pruning from the commit log manifest only.
    pub fn may_match_segment(&self, seg: &AddSegment) -> bool {
        if self
            .headers
            .iter()
            .any(|(k, _)| !seg.header_keys.iter().any(|h| h == k))
        {
            return false;
        }
        seg.topics.iter().any(|t| {
            self.topic_matches(&t.topic)
                && Self::ranges_overlap(self.ts_min, self.ts_max, t.ts_min, t.ts_max)
                && t.partitions.iter().any(|p| {
                    self.partitions
                        .as_ref()
                        .is_none_or(|s| s.contains(&p.partition))
                        && Self::ranges_overlap(
                            self.offset_min,
                            self.offset_max,
                            p.first_offset,
                            p.last_offset,
                        )
                })
        })
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}
