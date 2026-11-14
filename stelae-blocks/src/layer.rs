//! Writing a `blocks` layer from records, and reading one back.
//!
//! Both ends hold the same two rules: slots never go backwards, and every
//! block falls inside the layer's epoch. The writer refuses a block before it
//! reaches the sink, so a refused block leaves the layer as it was; the reader
//! refuses a layer a careless publisher wrote anyway.

use std::io::Read;

use stelae::{
    frame::Limits, LayerDescriptor, LayerDigests, LayerReader, Profile, RecordSink, SnapshotWriter,
    WrittenLayer,
};

use crate::{
    record::{self, BlockRecord, OrderCheck},
    scope::EpochScope,
    BlocksProfile, Error, BLOCKS, PROFILE_NAME,
};

/// The record ceiling both ends read under: the profile's own.
pub fn limits() -> Limits {
    Limits {
        max_record: BlocksProfile.max_record(),
        ..Limits::default()
    }
}

/// One epoch's layer being written, one block at a time.
pub struct BlocksWriter<S: RecordSink> {
    sink: S,
    scope: EpochScope,
    order: OrderCheck,
}

impl<S: RecordSink> BlocksWriter<S> {
    /// Open the layer for `scope` on `writer`, compressing at `level`.
    pub fn open<W>(writer: &W, scope: EpochScope, level: i32) -> Result<Self, Error>
    where
        W: SnapshotWriter<Sink = S>,
    {
        let sink = writer.layer_sink(&BlocksProfile, &scope.layer_spec()?, level)?;

        Ok(Self {
            sink,
            scope,
            order: OrderCheck::default(),
        })
    }

    /// Append the next block in chain order.
    pub fn push(&mut self, block: &BlockRecord) -> Result<(), Error> {
        check_in_epoch(&self.scope, block.slot)?;

        let mut order = self.order;
        order.check(block.slot)?;

        self.sink.write_record(&record::encode(block)?)?;
        self.order = order;

        Ok(())
    }

    /// Close the layer and hand back its descriptor.
    pub fn finish(self) -> Result<WrittenLayer, Error> {
        Ok(self.sink.finish()?)
    }
}

/// Write one epoch's blocks, in chain order, as a layer.
pub fn write_layer<W, I>(
    writer: &W,
    scope: EpochScope,
    level: i32,
    blocks: I,
) -> Result<WrittenLayer, Error>
where
    W: SnapshotWriter,
    I: IntoIterator<Item = BlockRecord>,
{
    let mut layer = BlocksWriter::open(writer, scope, level)?;

    for block in blocks {
        layer.push(&block)?;
    }

    layer.finish()
}

/// A `blocks` layer being read: its scope up front, then its blocks.
///
/// Blocks are yielded before the layer is proven; only
/// [`BlocksReader::finish`] confirms the layer against its descriptor.
pub struct BlocksReader<R: Read> {
    layer: LayerReader<R>,
    scope: EpochScope,
    order: OrderCheck,
    failed: bool,
}

impl<R: Read> BlocksReader<R> {
    /// Open the compressed blob `source` holds, as `descriptor` describes it.
    pub fn open(source: R, descriptor: &LayerDescriptor) -> Result<Self, Error> {
        Self::from_layer(LayerReader::new(
            source,
            &BlocksProfile,
            descriptor,
            limits(),
        )?)
    }

    /// Read a layer a transport has already opened.
    pub fn from_layer(layer: LayerReader<R>) -> Result<Self, Error> {
        let header = layer.header();

        if header.profile != PROFILE_NAME || header.kind != BLOCKS {
            return Err(Error::Stelae(stelae::Error::LayerMismatch {
                kind: header.kind.clone(),
                reason: format!(
                    "header names profile {:?}, kind {:?}; this reader reads {PROFILE_NAME:?}, \
                     kind {BLOCKS:?}",
                    header.profile, header.kind
                ),
            }));
        }

        let scope = EpochScope::read_header(header.scope.as_bytes())?;

        Ok(Self {
            layer,
            scope,
            order: OrderCheck::default(),
            failed: false,
        })
    }

    /// The epoch and slot bounds the layer's header declares.
    pub fn scope(&self) -> &EpochScope {
        &self.scope
    }

    fn next_block(&mut self) -> Option<Result<BlockRecord, Error>> {
        let bytes = match self.layer.next_record()? {
            Ok(bytes) => bytes,
            Err(e) => return Some(Err(e.into())),
        };

        let block = match record::decode(bytes) {
            Ok(block) => block,
            Err(e) => return Some(Err(e)),
        };

        let checked = check_in_epoch(&self.scope, block.slot)
            .and_then(|()| self.order.check(block.slot))
            .map(|()| block);

        Some(checked)
    }

    /// Drain what is left and confirm the layer's digest, size and record
    /// count. Refused if any block failed to read.
    pub fn finish(self) -> Result<LayerDigests, Error> {
        if self.failed {
            return Err(Error::Stelae(stelae::Error::LayerMismatch {
                kind: BLOCKS.to_owned(),
                reason: "a block failed validation; the layer cannot be confirmed".to_owned(),
            }));
        }

        Ok(self.layer.finish()?)
    }
}

/// Blocks in chain order. Ends after the first error.
impl<R: Read> Iterator for BlocksReader<R> {
    type Item = Result<BlockRecord, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }

        let next = self.next_block();
        self.failed = matches!(next, Some(Err(_)));

        next
    }
}

fn check_in_epoch(scope: &EpochScope, slot: u64) -> Result<(), Error> {
    if scope.covers(slot) {
        return Ok(());
    }

    Err(Error::OutsideEpoch {
        slot,
        epoch: scope.epoch,
        start_slot: scope.start_slot,
        end_slot: scope.end_slot,
    })
}
