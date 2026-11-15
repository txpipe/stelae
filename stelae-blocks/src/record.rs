//! The `blocks` record: `[slot, hash: bytes(32), body: bytes]`.
//!
//! Records run in ascending slot, and blocks that share a slot keep the order
//! the chain gave them: a Byron epoch boundary block shares its slot with the
//! block after it. That order is a property of the sequence, not of a record,
//! so [`OrderCheck`] enforces only that slots never go backwards.
//!
//! The codec takes the hash from its caller and does not derive it: deriving
//! it means decoding a Cardano header, which belongs to whoever reads the
//! blocks out of a node, not to the format.

use minicbor::Decoder;
use stelae::{
    codec::{blob, close, fixed, open, uint},
    frame::{self, CanonicalCbor},
};

use crate::{Error, BLOCKS};

/// One block, as the layer carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRecord {
    pub slot: u64,
    /// The block's header hash.
    pub hash: [u8; 32],
    /// The block as the node stores it, verbatim and never re-encoded.
    pub body: Vec<u8>,
}

impl BlockRecord {
    pub fn new(slot: u64, hash: [u8; 32], body: impl Into<Vec<u8>>) -> Self {
        Self {
            slot,
            hash,
            body: body.into(),
        }
    }
}

pub fn encode(record: &BlockRecord) -> Result<CanonicalCbor, Error> {
    Ok(frame::encode(|e| {
        e.array(3)?
            .u64(record.slot)?
            .bytes(&record.hash)?
            .bytes(&record.body)?;
        Ok(())
    })?)
}

pub fn decode(bytes: &[u8]) -> Result<BlockRecord, Error> {
    let shape = |e: stelae::Error| match e {
        stelae::Error::MalformedRecord { reason, .. } => Error::MalformedRecord(reason),
        other => Error::Stelae(other),
    };

    let mut decoder = Decoder::new(bytes);

    open(BLOCKS, &mut decoder, 3).map_err(shape)?;

    let slot = uint(BLOCKS, "slot", &mut decoder).map_err(shape)?;
    let hash = fixed::<32>(BLOCKS, "hash", &mut decoder).map_err(shape)?;
    let body = blob(BLOCKS, "body", &mut decoder).map_err(shape)?.to_vec();

    close(BLOCKS, &decoder, bytes).map_err(shape)?;

    Ok(BlockRecord { slot, hash, body })
}

/// Ascending slot, with same-slot runs left in stream order.
#[derive(Debug, Default, Clone, Copy)]
pub struct OrderCheck {
    last: Option<u64>,
}

impl OrderCheck {
    pub fn check(&mut self, slot: u64) -> Result<(), Error> {
        if let Some(last) = self.last {
            if slot < last {
                return Err(Error::OutOfOrder(format!(
                    "slot {slot} follows slot {last}"
                )));
            }
        }

        self.last = Some(slot);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(slot: u64, byte: u8) -> BlockRecord {
        BlockRecord::new(slot, [byte; 32], vec![byte; 4])
    }

    #[test]
    fn round_trips() {
        for original in [
            record(0, 0),
            record(u32::MAX as u64 + 1, 0xff),
            BlockRecord::new(7, [0; 32], Vec::new()),
        ] {
            let encoded = encode(&original).unwrap();
            assert_eq!(decode(encoded.as_bytes()).unwrap(), original);
        }
    }

    #[test]
    fn a_wrong_width_hash_is_refused() {
        let bad = frame::encode(|e| {
            e.array(3)?.u64(1)?.bytes(&[0u8; 28])?.bytes(&[])?;
            Ok(())
        })
        .unwrap();

        let err = decode(bad.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::MalformedRecord(_)), "{err:?}");
    }

    #[test]
    fn a_wrong_arity_or_a_trailing_byte_is_refused() {
        let short = frame::encode(|e| {
            e.array(2)?.u64(1)?.bytes(&[0u8; 32])?;
            Ok(())
        })
        .unwrap();
        assert!(matches!(
            decode(short.as_bytes()),
            Err(Error::MalformedRecord(_))
        ));

        let mut bytes = encode(&record(1, 1)).unwrap().into_bytes();
        bytes.push(0x00);
        assert!(matches!(decode(&bytes), Err(Error::MalformedRecord(_))));
    }

    /// The Byron case: two blocks at one slot, in the order they arrived.
    #[test]
    fn same_slot_blocks_are_accepted_in_stream_order() {
        let mut order = OrderCheck::default();

        for slot in [10, 10, 11] {
            order.check(slot).unwrap();
        }
    }

    #[test]
    fn a_slot_going_backwards_is_refused() {
        let mut order = OrderCheck::default();

        order.check(11).unwrap();

        let err = order.check(10).unwrap_err();
        assert!(matches!(err, Error::OutOfOrder(_)), "{err:?}");
    }
}
