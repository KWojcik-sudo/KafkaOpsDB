/// How Kafka assigned the record timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampType {
    #[default]
    CreateTime = 0,
    LogAppendTime = 1,
}

impl TimestampType {
    pub fn from_code(c: i64) -> Self {
        if c == 1 {
            Self::LogAppendTime
        } else {
            Self::CreateTime
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::CreateTime => "CreateTime",
            Self::LogAppendTime => "LogAppendTime",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

impl Header {
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }
}

/// Indexed metadata of one Kafka record. The value itself is never stored.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Record {
    pub offset: i64,
    pub timestamp: i64,
    pub timestamp_type: TimestampType,
    pub key: Option<Vec<u8>>,
    /// `None` for tombstones (null value).
    pub value_size: Option<u32>,
    /// Schema id from the Confluent wire format, if the value uses it.
    pub value_schema_id: Option<i32>,
    pub headers: Vec<Header>,
}

impl Record {
    /// Approximate in-memory size of the indexed metadata, used for buffer accounting.
    pub fn metadata_size(&self) -> usize {
        48 + self.key.as_ref().map_or(0, |k| k.len())
            + self
                .headers
                .iter()
                .map(|h| 8 + h.key.len() + h.value.as_ref().map_or(0, |v| v.len()))
                .sum::<usize>()
    }
}

/// Extracts the schema id from a value in the Confluent wire format
/// (magic byte 0x00 followed by a 4-byte big-endian schema id).
pub fn confluent_schema_id(value: &[u8]) -> Option<i32> {
    if value.len() >= 5 && value[0] == 0 {
        Some(i32::from_be_bytes(value[1..5].try_into().unwrap()))
    } else {
        None
    }
}
