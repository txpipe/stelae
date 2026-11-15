//! The scope of a `blocks` layer: one whole epoch of one network.
//!
//! The layer header carries `[network_magic, epoch, start_slot, end_slot]`;
//! the inscription's descriptor carries `{"epoch", "startSlot", "endSlot"}`,
//! the network being the inscription's `position`. Both slot bounds are
//! inclusive and are the epoch's own bounds, not the first and last block.

use minicbor::Decoder;
use serde_json::json;
use stelae::{
    frame::{self, CanonicalCbor},
    LayerSpec,
};

use crate::{Error, BLOCKS};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochScope {
    pub network_magic: u64,
    pub epoch: u64,
    pub start_slot: u64,
    pub end_slot: u64,
}

impl EpochScope {
    /// A scope over `start_slot..=end_slot`, refused when the bounds are
    /// reversed.
    pub fn new(
        network_magic: u64,
        epoch: u64,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<Self, Error> {
        if start_slot > end_slot {
            return Err(Error::MalformedScope(format!(
                "epoch {epoch} starts at slot {start_slot}, after its end slot {end_slot}"
            )));
        }

        Ok(Self {
            network_magic,
            epoch,
            start_slot,
            end_slot,
        })
    }

    /// Whether the epoch covers `slot`.
    pub fn covers(&self, slot: u64) -> bool {
        (self.start_slot..=self.end_slot).contains(&slot)
    }

    /// The CBOR form, for the layer header record.
    pub fn header(&self) -> Result<CanonicalCbor, Error> {
        Ok(frame::encode(|e| {
            e.array(4)?
                .u64(self.network_magic)?
                .u64(self.epoch)?
                .u64(self.start_slot)?
                .u64(self.end_slot)?;
            Ok(())
        })?)
    }

    /// The JSON form, for the inscription's layer descriptor.
    pub fn descriptor(&self) -> serde_json::Value {
        json!({"epoch": self.epoch, "startSlot": self.start_slot, "endSlot": self.end_slot})
    }

    /// Both encodings under the `blocks` kind, ready for a layer sink.
    pub fn layer_spec(&self) -> Result<LayerSpec, Error> {
        Ok(LayerSpec::new(BLOCKS, self.header()?, self.descriptor()))
    }

    /// Read the scope out of a layer header's scope bytes.
    pub fn read_header(scope: &[u8]) -> Result<Self, Error> {
        let malformed = |e: minicbor::decode::Error| Error::MalformedScope(e.to_string());
        let mut decoder = Decoder::new(scope);

        match decoder.array().map_err(malformed)? {
            Some(4) => {}
            other => {
                return Err(Error::MalformedScope(format!(
                    "expected an array of 4 fields, found {other:?}"
                )))
            }
        }

        let mut field = || decoder.u64().map_err(malformed);
        let (network_magic, epoch, start_slot, end_slot) = (field()?, field()?, field()?, field()?);

        if decoder.position() != scope.len() {
            return Err(Error::MalformedScope(format!(
                "{} trailing byte(s) after the scope",
                scope.len() - decoder.position()
            )));
        }

        Self::new(network_magic, epoch, start_slot, end_slot)
    }

    /// Read the scope out of a layer descriptor, on the network the
    /// inscription's `position` names.
    pub fn read_descriptor(network_magic: u64, scope: &serde_json::Value) -> Result<Self, Error> {
        let object = scope
            .as_object()
            .ok_or_else(|| Error::MalformedScope("descriptor scope is not an object".into()))?;

        if object.len() != 3 {
            return Err(Error::MalformedScope(format!(
                "expected epoch, startSlot and endSlot, found {scope}"
            )));
        }

        let uint = |name: &str| {
            object.get(name).and_then(|v| v.as_u64()).ok_or_else(|| {
                Error::MalformedScope(format!("{name} is missing or not a u64 in {scope}"))
            })
        };

        Self::new(
            network_magic,
            uint("epoch")?,
            uint("startSlot")?,
            uint("endSlot")?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_encodings_round_trip() {
        let scope = EpochScope::new(764824073, 667, 172_800_000, 173_231_999).unwrap();

        assert_eq!(
            EpochScope::read_header(scope.header().unwrap().as_bytes()).unwrap(),
            scope
        );
        assert_eq!(
            EpochScope::read_descriptor(764824073, &scope.descriptor()).unwrap(),
            scope
        );
    }

    #[test]
    fn bounds_are_inclusive() {
        let scope = EpochScope::new(2, 1, 100, 199).unwrap();

        assert!(!scope.covers(99));
        assert!(scope.covers(100));
        assert!(scope.covers(199));
        assert!(!scope.covers(200));
    }

    #[test]
    fn malformed_scopes_are_refused() {
        assert!(EpochScope::new(2, 1, 200, 100).is_err());

        let three = frame::encode(|e| {
            e.array(3)?.u64(2)?.u64(1)?.u64(0)?;
            Ok(())
        })
        .unwrap();
        assert!(EpochScope::read_header(three.as_bytes()).is_err());

        for bad in [
            json!({"epoch": 1, "startSlot": 0}),
            json!({"epoch": 1, "startSlot": 0, "endSlot": -1}),
            json!({"epoch": 1, "startSlot": 0, "endSlot": 9, "shard": 0}),
        ] {
            assert!(EpochScope::read_descriptor(2, &bad).is_err(), "{bad}");
        }
    }
}
