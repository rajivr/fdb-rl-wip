use fdb::range::RangeOptions;

use crate::scan::ScanLimiter;

/// Build [`ScanProperties`] with custom [`RangeOptions`] and
/// [`ScanLimiter`].
///
/// Methods can be chained. Value of [`ScanProperties`] is constructed
/// by calling [`build`] method.
///
/// [`build`]: ScanPropertiesBuilder::build
#[derive(Debug)]
pub struct ScanPropertiesBuilder {
    range_options: RangeOptions,
    scan_limiter: ScanLimiter,
}

impl ScanPropertiesBuilder {
    /// Returns a new builder.
    pub fn new() -> ScanPropertiesBuilder {
        ScanPropertiesBuilder {
            range_options: RangeOptions::default(),
            scan_limiter: ScanLimiter::new(None, None, None),
        }
    }

    /// Set [`RangeOptions`] using a closure.
    ///
    /// # Safety
    ///
    /// There is **no way** in the [`RangeOptions`] API to set a limit
    /// of `0`. *Infact* if you set the limit to `0`, you are
    /// indicating that you want [unlimited] rows, which almost always
    /// is not the behavior that you want.
    ///
    /// [unlimited]: fdb::range::KEYVALUE_LIMIT_UNLIMITED
    pub unsafe fn set_range_options<F>(&mut self, f: F) -> &mut ScanPropertiesBuilder
    where
        F: FnOnce(&mut RangeOptions),
    {
        f(&mut self.range_options);
        self
    }

    /// Set [`ScanLimiter`].
    pub fn set_scan_limiter(&mut self, scan_limiter: ScanLimiter) -> &mut ScanPropertiesBuilder {
        self.scan_limiter = scan_limiter;
        self
    }

    /// Create the configured [`ScanProperties`].
    pub fn build(self) -> ScanProperties {
        let ScanPropertiesBuilder {
            range_options,
            scan_limiter,
        } = self;

        ScanProperties {
            range_options,
            scan_limiter,
        }
    }
}

impl Default for ScanPropertiesBuilder {
    fn default() -> ScanPropertiesBuilder {
        ScanPropertiesBuilder::new()
    }
}

/// Properties that pertain to a scan. It consists of [`RangeOptions`]
/// and a [`ScanLimiter`].

// Note: Unlike Java RecordLayer, we do not have
//       `ScanProperties.FORWARD_SCAN` or
//       `ScanProperties.FORWARD_SCAN` as that is taken care of
//       `reverse` option in `RangeOptions`. We also do not have Java
//       class `ExecuteProperties` as concerns covered by it is taken
//       care of by `RangeOptions` and `ScanLimiter`.
//
//       In addition there is no unit test for this type as
//       `scan_properties_builder_build` takes care of exercising the
//       API.
//
//       Also, see comment mentioned for type `KeyValueCursorBuilder`
//       regarding `PartialEq` and unit testing.
#[cfg(not(test))]
#[derive(Debug, Clone)]
pub struct ScanProperties {
    range_options: RangeOptions,
    scan_limiter: ScanLimiter,
}

/// We need to derive `PartialEq` for testing.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub struct ScanProperties {
    range_options: RangeOptions,
    scan_limiter: ScanLimiter,
}

impl ScanProperties {
    /// Get reference to [`RangeOptions`] used by the scan.
    pub fn get_range_options_ref(&self) -> &RangeOptions {
        &self.range_options
    }

    /// Get reference to [`ScanLimiter`] used by the scan.
    pub fn get_scan_limiter_ref(&self) -> &ScanLimiter {
        &self.scan_limiter
    }

    pub(crate) fn into_parts(self) -> (RangeOptions, ScanLimiter) {
        let ScanProperties {
            range_options,
            scan_limiter,
        } = self;
        (range_options, scan_limiter)
    }
}

#[cfg(test)]
mod tests {
    use fdb::range::StreamingMode;

    use crate::scan::{ByteScanLimiter, KeyValueScanLimiter, ScanLimiter};

    use super::ScanPropertiesBuilder;

    #[test]
    fn builder_build() {
        // Smoke test on the API usage
        let scan_properties = {
            let mut builder = ScanPropertiesBuilder::new();
            unsafe {
                builder.set_range_options(|r| {
                    r.set_limit(100);
                    r.set_reverse(true);
                })
            }
            .set_scan_limiter(ScanLimiter::new(
                Some(KeyValueScanLimiter::tracking()),
                Some(ByteScanLimiter::tracking()),
                None,
            ));
            builder.build()
        };

        assert!(scan_properties.get_range_options_ref().get_reverse());
        assert_eq!(scan_properties.get_range_options_ref().get_limit(), 100);
        assert_eq!(
            scan_properties.get_range_options_ref().get_mode(),
            StreamingMode::Iterator
        );

        // Safety: Safe to unwrap because we are setting `Some` value
        //         above.
        assert_eq!(
            scan_properties
                .get_scan_limiter_ref()
                .get_keyvalue_scan_limiter_ref()
                .unwrap()
                .get_limit(),
            usize::MAX
        );
        assert_eq!(
            scan_properties
                .get_scan_limiter_ref()
                .get_byte_scan_limiter_ref()
                .unwrap()
                .get_limit(),
            usize::MAX
        );
        assert!(scan_properties
            .get_scan_limiter_ref()
            .get_time_scan_limiter_ref()
            .is_none());
    }
}
