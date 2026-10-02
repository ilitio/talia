//! Shared parsing primitives for the procfs TCP counter files.
//!
//! Both `/proc/net/snmp` and `/proc/net/netstat` repeat the same layout: a
//! header line carrying field names, followed by a values line carrying the
//! counters. Fields are always paired **by name, never by position**, so a
//! kernel that reorders or extends the columns keeps parsing correctly.

use thiserror::Error;

/// Failures while parsing `/proc/net/snmp` or `/proc/net/netstat`.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    /// No header line (e.g. `Tcp:`) was found in the input.
    #[error("missing header line '{prefix}'")]
    MissingHeader {
        /// The expected line prefix, e.g. `"Tcp:"`.
        prefix: &'static str,
    },
    /// A header line was found but no following values line with the same
    /// prefix exists.
    #[error("missing values line for '{prefix}'")]
    MissingValues {
        /// The expected line prefix, e.g. `"Tcp:"`.
        prefix: &'static str,
    },
    /// The header and values lines carry a different number of fields, so
    /// name-based pairing is impossible.
    #[error("field count mismatch for '{prefix}': {headers} headers vs {values} values")]
    FieldCountMismatch {
        /// The section prefix being parsed, e.g. `"Tcp:"`.
        prefix: &'static str,
        /// Number of fields on the header line.
        headers: usize,
        /// Number of fields on the values line.
        values: usize,
    },
    /// A required counter name is absent from the header line.
    #[error("required field '{field}' missing from '{prefix}' headers")]
    MissingField {
        /// The section prefix being parsed, e.g. `"Tcp:"`.
        prefix: &'static str,
        /// The counter name that was not found.
        field: &'static str,
    },
    /// A counter value is not a valid unsigned integer.
    #[error("invalid integer for field '{field}' in '{prefix}': '{value}'")]
    InvalidInteger {
        /// The section prefix being parsed, e.g. `"Tcp:"`.
        prefix: &'static str,
        /// The counter name whose value failed to parse.
        field: &'static str,
        /// The offending raw value.
        value: String,
    },
}

/// Extracts the `name -> raw value` mapping for one procfs section.
///
/// `prefix` is the line prefix including the colon (`"Tcp:"`, `"TcpExt:"`).
/// The first matching line is the header, the next matching line the values.
pub(crate) fn section_values<'a>(
    content: &'a str,
    prefix: &'static str,
) -> Result<Vec<(&'a str, &'a str)>, ParseError> {
    let mut lines = content.lines().filter(|line| line.starts_with(prefix));
    let headers_line = lines.next().ok_or(ParseError::MissingHeader { prefix })?;
    let values_line = lines.next().ok_or(ParseError::MissingValues { prefix })?;

    let headers: Vec<&str> = headers_line[prefix.len()..].split_whitespace().collect();
    let values: Vec<&str> = values_line[prefix.len()..].split_whitespace().collect();
    if headers.len() != values.len() {
        return Err(ParseError::FieldCountMismatch {
            prefix,
            headers: headers.len(),
            values: values.len(),
        });
    }
    Ok(headers.into_iter().zip(values).collect())
}

/// Looks up one required counter by name and parses it as `u64`.
pub(crate) fn required_u64(
    values: &[(&str, &str)],
    prefix: &'static str,
    field: &'static str,
) -> Result<u64, ParseError> {
    let raw = values
        .iter()
        .find(|(name, _)| *name == field)
        .map(|(_, value)| *value)
        .ok_or(ParseError::MissingField { prefix, field })?;
    raw.parse::<u64>().map_err(|_| ParseError::InvalidInteger {
        prefix,
        field,
        value: raw.to_string(),
    })
}
