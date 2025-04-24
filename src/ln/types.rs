// This file is Copyright its original authors, visible in version control
// history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE
// or http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your option.
// You may not use this file except in accordance with one or both of these
// licenses.

//! Various wrapper types (most around 32-byte arrays) for use in lightning.

use crate::io;
use crate::ln::msgs::DecodeError;
use crate::util::ser::{Readable, Writeable, Writer};

#[allow(unused_imports)]
use crate::prelude::*;

use bitcoin::hex::display::impl_fmt_traits;

use core::borrow::Borrow;

/// A unique 32-byte identifier for a channel.
/// Depending on how the ID is generated, several varieties are distinguished
/// (but all are stored as 32 bytes):
///   _v1_ and _temporary_.
/// A _v1_ channel ID is generated based on funding tx outpoint (txid & index).
/// A _temporary_ ID is generated randomly.
/// (Later revocation-point-based _v2_ is a possibility.)
/// The variety (context) is not stored, it is relevant only at creation.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChannelId(pub [u8; 32]);

impl ChannelId {
    /// Generic constructor; create a new channel ID from the provided data.
    /// Use a more specific `*_from_*` constructor when possible.
    pub fn from_bytes(data: [u8; 32]) -> Self {
        Self(data)
    }

    /// Create a channel ID consisting of all-zeros data (e.g. when uninitialized or a placeholder).
    pub fn new_zero() -> Self {
        Self([0; 32])
    }

    /// Check whether ID is consisting of all zeros (uninitialized)
    pub fn is_zero(&self) -> bool {
        self.0[..] == [0; 32]
    }
}

impl Writeable for ChannelId {
    fn write<W: Writer>(&self, w: &mut W) -> Result<(), io::Error> {
        self.0.write(w)
    }
}

impl Readable for ChannelId {
    fn read<R: io::Read>(r: &mut R) -> Result<Self, DecodeError> {
        let buf: [u8; 32] = Readable::read(r)?;
        Ok(ChannelId(buf))
    }
}

impl Borrow<[u8]> for ChannelId {
    fn borrow(&self) -> &[u8] {
        &self.0[..]
    }
}

impl_fmt_traits! {
    impl fmt_traits for ChannelId {
        const LENGTH: usize = 32;
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::hashes::{Hash as _, HashEngine as _, sha256::Hash as Sha256};
    use bitcoin::hex::DisplayHex;
    use bitcoin::secp256k1::PublicKey;

    use super::ChannelId;

    use crate::io;
    use crate::ln::channel_keys::RevocationBasepoint;
    use crate::prelude::*;
    use crate::util::ser::{Readable, Writeable};
    use crate::util::test_utils;

    use core::str::FromStr;

    #[test]
    fn test_channel_id_v1_from_funding_txid() {
        let channel_id = ChannelId::v1_from_funding_txid(&[2; 32], 1);
        let expected = "0202020202020202020202020202020202020202020202020202020202020203";
        assert_eq!(channel_id.0.as_hex().to_string(), expected);
    }

    #[test]
    fn test_channel_id_new_from_data() {
        let data: [u8; 32] = [2; 32];
        let channel_id = ChannelId::from_bytes(data.clone());
        assert_eq!(channel_id.0, data);
    }

    #[test]
    fn test_channel_id_equals() {
        let channel_id11 = ChannelId::v1_from_funding_txid(&[2; 32], 2);
        let channel_id12 = ChannelId::v1_from_funding_txid(&[2; 32], 2);
        let channel_id21 = ChannelId::v1_from_funding_txid(&[2; 32], 42);
        assert_eq!(channel_id11, channel_id12);
        assert_ne!(channel_id11, channel_id21);
    }

    #[test]
    fn test_channel_id_write_read() {
        let data: [u8; 32] = [2; 32];
        let channel_id = ChannelId::from_bytes(data.clone());

        let mut w = test_utils::TestVecWriter(Vec::new());
        channel_id.write(&mut w).unwrap();

        let channel_id_2 = ChannelId::read(&mut io::Cursor::new(&w.0)).unwrap();
        assert_eq!(channel_id_2, channel_id);
        assert_eq!(channel_id_2.0, data);
    }

    #[test]
    fn test_channel_id_display() {
        let channel_id = ChannelId::v1_from_funding_txid(&[2; 32], 1);
        let expected = "0202020202020202020202020202020202020202020202020202020202020203";
        assert_eq!(format!("{}", &channel_id), expected);
    }

    #[test]
    fn test_is_v2_channel_id() {
        let our_pk = "0324653eac434488002cc06bbfb7f10fe18991e35f9fe4302dbea6d2353dc0ab1c";
        let ours = RevocationBasepoint(PublicKey::from_str(&our_pk).unwrap());
        let their_pk = "02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f283686619";
        let theirs = RevocationBasepoint(PublicKey::from_str(&their_pk).unwrap());

        let channel_id = ChannelId::v2_from_revocation_basepoints(&ours, &theirs);
        assert!(channel_id.is_v2_channel_id(&ours, &theirs));

        let channel_id = ChannelId::v1_from_funding_txid(&[2; 32], 1);
        assert!(!channel_id.is_v2_channel_id(&ours, &theirs))
    }
}
