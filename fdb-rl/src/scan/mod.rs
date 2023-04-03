//! Provides types for out-of-band scan limits in Record Layer.

mod scan_limiter;
mod scan_properties;

pub use scan_limiter::{ByteScanLimiter, KeyValueScanLimiter, ScanLimiter, TimeScanLimiter};

pub use scan_properties::{ScanProperties, ScanPropertiesBuilder};
