//! Whether a network's published snapshots are as recent as they should be.
//!
//! Sequence N is the cut at the boundary that opens epoch N, so a snapshot
//! is due once its epoch has been open for the operator's deadline. Placing
//! epochs in time takes only a network's well-known genesis values, so the
//! check reads no store and no Mithril, only the clock and the registry.

use dolos_cardano::networks::ResolvedNetwork;
use dolos_core::config::RootConfig;
use pallas::ledger::traverse::wellknown::GenesisValues;

/// A network whose epochs this module cannot place in time.
#[derive(Debug, thiserror::Error)]
#[error("network `{network}` has no well-known genesis values to place its epochs in time")]
pub struct UnknownEpochs {
    pub network: String,
}

/// A network's epochs, placed in time by its well-known genesis values.
#[derive(Debug, Clone)]
pub struct Epochs(GenesisValues);

/// The newest sequence a deadline says should be published, and when its
/// epoch started, in Unix seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Expected {
    pub sequence: u64,
    pub start: u64,
}

impl Epochs {
    /// The epochs of the configured network. A custom network is refused even
    /// when its magic is a known network's: its genesis may be another one.
    pub fn of(config: &RootConfig) -> Result<Self, UnknownEpochs> {
        let unknown = || UnknownEpochs {
            network: config.chain.network.clone(),
        };
        let network = ResolvedNetwork::resolve(&config.chain).map_err(|_| unknown())?;
        let known = network.known().ok_or_else(unknown)?;
        Self::of_magic(known.magic.into()).ok_or_else(unknown)
    }

    pub fn of_magic(magic: u64) -> Option<Self> {
        GenesisValues::from_magic(magic).map(Self)
    }

    /// When `epoch` started, in Unix seconds.
    pub fn start(&self, epoch: u64) -> u64 {
        let slot = self.0.relative_slot_to_absolute(epoch, 0);
        self.0.slot_to_wallclock(slot)
    }

    /// The epoch open at `time`, Unix seconds, or `None` before the first.
    pub fn at(&self, time: u64) -> Option<u64> {
        let values = &self.0;
        let slot = if time < values.byron_known_time {
            return None;
        } else if time < values.shelley_known_time {
            values.byron_known_slot
                + (time - values.byron_known_time) / u64::from(values.byron_slot_length)
        } else {
            values.shelley_known_slot
                + (time - values.shelley_known_time) / u64::from(values.shelley_slot_length)
        };
        Some(values.absolute_slot_to_relative(slot).0)
    }
}

/// The newest epoch whose start plus `deadline_hours` is at or before `now`,
/// Unix seconds; `None` while even the first epoch's deadline is ahead.
pub fn expected(epochs: &Epochs, deadline_hours: u64, now: u64) -> Option<Expected> {
    let cutoff = now.checked_sub(deadline_hours.checked_mul(3600)?)?;
    let sequence = epochs.at(cutoff)?;
    Some(Expected {
        sequence,
        start: epochs.start(sequence),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3600;

    fn network(name: &str) -> Epochs {
        let config = RootConfig::new(dolos_core::config::ChainConfig::known(name));
        Epochs::of(&config).unwrap()
    }

    /// The expected sequence one second before and at `epoch`'s deadline.
    fn around_deadline(epochs: &Epochs, epoch: u64, hours: u64) -> (u64, u64) {
        let deadline = epochs.start(epoch) + hours * HOUR;
        let before = expected(epochs, hours, deadline - 1).unwrap();
        let at = expected(epochs, hours, deadline).unwrap();
        assert_eq!(at.start, epochs.start(epoch));
        (before.sequence, at.sequence)
    }

    #[test]
    fn mainnet_epochs_fall_due_at_their_deadline() {
        let mainnet = network("mainnet");
        // Byron's first epoch and Shelley's, 2017-09-23 and 2020-07-29 21:44:51 UTC.
        assert_eq!(mainnet.start(0), 1_506_203_091);
        assert_eq!(mainnet.start(208), 1_596_059_091);
        assert_eq!(mainnet.start(209), 1_596_059_091 + 5 * 24 * HOUR);

        assert_eq!(around_deadline(&mainnet, 500, 30), (499, 500));
        assert_eq!(around_deadline(&mainnet, 208, 30), (207, 208));
        assert_eq!(around_deadline(&mainnet, 100, 30), (99, 100));
    }

    #[test]
    fn preprod_epochs_fall_due_at_their_deadline() {
        let preprod = network("preprod");
        // Byron's first epoch and Shelley's, 2022-06-01 and 2022-06-21 UTC.
        assert_eq!(preprod.start(0), 1_654_041_600);
        assert_eq!(preprod.start(4), 1_655_769_600);

        assert_eq!(around_deadline(&preprod, 4, 30), (3, 4));
        assert_eq!(around_deadline(&preprod, 250, 30), (249, 250));
    }

    #[test]
    fn a_preview_deadline_can_fall_inside_the_next_epoch() {
        let preview = network("preview");
        // A one-day epoch from 2022-10-25 UTC, Shelley from the start.
        assert_eq!(preview.start(0), 1_666_656_000);
        assert_eq!(preview.start(101), 1_666_656_000 + 101 * 24 * HOUR);

        // Epoch 100's deadline is six hours into epoch 101.
        let deadline = preview.start(101) + 6 * HOUR;
        assert_eq!(preview.at(deadline - 1), Some(101));
        assert_eq!(expected(&preview, 30, deadline - 1).unwrap().sequence, 99);
        assert_eq!(expected(&preview, 30, deadline).unwrap().sequence, 100);
        assert_eq!(around_deadline(&preview, 100, 30), (99, 100));
    }

    #[test]
    fn nothing_is_due_before_the_first_deadline() {
        let preview = network("preview");
        assert_eq!(expected(&preview, 30, preview.start(1)), None);
        assert_eq!(expected(&preview, 30, 0), None);
        assert_eq!(expected(&preview, u64::MAX, preview.start(1000)), None);
        assert_eq!(preview.at(preview.start(0) - 1), None);
    }

    #[test]
    fn a_network_without_well_known_values_is_refused() {
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.chain.network = dolos_core::config::ChainConfig::CUSTOM.to_owned();
        let error = Epochs::of(&config).unwrap_err().to_string();
        assert!(
            error.contains(dolos_core::config::ChainConfig::CUSTOM),
            "{error}"
        );

        assert!(Epochs::of_magic(42).is_none());
    }
}
