use serde::Deserialize;
use sqd_primitives::{BlockNumber, DataMask, ItemIndex};

use crate::types::HexBytes;

mod quantity {
    use serde::{de::Visitor, Deserializer};

    use crate::types::HexBytes;

    struct HexBytesOrIntVisitor;

    impl Visitor<'_> for HexBytesOrIntVisitor {
        type Value = HexBytes;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a hex string or an unsigned integer")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
            Ok(value.to_owned())
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
            Ok(format!("0x{value:x}"))
        }

        fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E> {
            Ok(format!("0x{value:x}"))
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HexBytes, D::Error>
    where
        D: Deserializer<'de>
    {
        deserializer.deserialize_any(HexBytesOrIntVisitor)
    }

    struct OptionalHexBytesOrIntVisitor;

    impl<'de> Visitor<'de> for OptionalHexBytesOrIntVisitor {
        type Value = Option<HexBytes>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a hex string, an unsigned integer, or null")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>
        {
            deserialize(deserializer).map(Some)
        }
    }

    pub fn deserialize_option<'de, D>(deserializer: D) -> Result<Option<HexBytes>, D::Error>
    where
        D: Deserializer<'de>
    {
        deserializer.deserialize_option(OptionalHexBytesOrIntVisitor)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Withdrawal {
    pub address: HexBytes,
    #[serde(deserialize_with = "quantity::deserialize")]
    pub amount: HexBytes,
    #[serde(deserialize_with = "quantity::deserialize")]
    pub index: HexBytes,
    #[serde(deserialize_with = "quantity::deserialize")]
    pub validator_index: HexBytes
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub number: BlockNumber,
    pub hash: HexBytes,
    pub parent_hash: HexBytes,
    pub timestamp: i64,
    pub transactions_root: HexBytes,
    pub receipts_root: HexBytes,
    pub state_root: HexBytes,
    pub logs_bloom: HexBytes,
    pub sha3_uncles: HexBytes,
    pub extra_data: HexBytes,
    pub miner: HexBytes,
    pub nonce: Option<HexBytes>,
    pub mix_hash: Option<HexBytes>,
    pub size: u64,
    #[serde(deserialize_with = "quantity::deserialize")]
    pub gas_limit: HexBytes,
    #[serde(deserialize_with = "quantity::deserialize")]
    pub gas_used: HexBytes,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub difficulty: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub total_difficulty: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub base_fee_per_gas: Option<HexBytes>,
    pub uncles: Option<Vec<HexBytes>>,
    pub withdrawals: Option<Vec<Withdrawal>>,
    pub withdrawals_root: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub blob_gas_used: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub excess_blob_gas: Option<HexBytes>,
    pub parent_beacon_block_root: Option<HexBytes>,
    pub requests_hash: Option<HexBytes>,
    pub l1_block_number: Option<BlockNumber>,
    // Tempo-specific block header fields
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub main_block_general_gas_limit: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub shared_gas_limit: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub timestamp_millis_part: Option<HexBytes>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EIP7702Authorization {
    #[serde(deserialize_with = "quantity::deserialize")]
    pub chain_id: HexBytes,
    pub address: HexBytes,
    #[serde(deserialize_with = "sqd_data_core::serde::decode_string")]
    pub nonce: u64,
    pub y_parity: u8,
    // r/s are fixed 32-byte signature data, not quantities: keep string-only so an
    // integer encoding rejects rather than silently dropping leading zero bytes.
    pub r: HexBytes,
    pub s: HexBytes
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoCall {
    pub to: Option<HexBytes>,
    #[serde(deserialize_with = "quantity::deserialize")]
    pub value: HexBytes,
    pub input: HexBytes
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum TempoPrimitiveSignature {
    #[serde(rename = "secp256k1", rename_all = "camelCase")]
    Secp256k1 {
        r: HexBytes,
        s: HexBytes,
        y_parity: Option<u8>,
        v: Option<u8>
    },
    #[serde(rename = "p256", rename_all = "camelCase")]
    P256 {
        r: HexBytes,
        s: HexBytes,
        pub_key_x: HexBytes,
        pub_key_y: HexBytes,
        pre_hash: bool
    },
    #[serde(rename = "webAuthn", rename_all = "camelCase")]
    WebAuthn {
        r: HexBytes,
        s: HexBytes,
        pub_key_x: HexBytes,
        pub_key_y: HexBytes,
        webauthn_data: HexBytes
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoKeychainSignature {
    pub user_address: HexBytes,
    pub signature: TempoPrimitiveSignature,
    pub version: Option<String>
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum TempoSignature {
    Keychain(TempoKeychainSignature),
    Primitive(TempoPrimitiveSignature)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoSignedAuthorization {
    #[serde(deserialize_with = "quantity::deserialize")]
    pub chain_id: HexBytes,
    pub address: HexBytes,
    pub nonce: u64,
    pub signature: TempoSignature
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoTokenLimit {
    pub token: HexBytes,
    #[serde(deserialize_with = "quantity::deserialize")]
    pub limit: HexBytes
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoSignedKeyAuthorization {
    #[serde(deserialize_with = "quantity::deserialize")]
    pub chain_id: HexBytes,
    pub key_type: String,
    pub key_id: HexBytes,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub expiry: Option<HexBytes>,
    pub limits: Option<Vec<TempoTokenLimit>>,
    pub signature: TempoPrimitiveSignature
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoFeePayerSignature {
    pub v: u8,
    pub r: HexBytes,
    pub s: HexBytes
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessListItem {
    pub address: HexBytes,
    pub storage_keys: Vec<HexBytes>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub transaction_index: ItemIndex,
    pub hash: HexBytes,
    pub nonce: u64,
    pub from: HexBytes,
    pub to: Option<HexBytes>,
    // Optional for Tempo 0x76 transactions which use batched `calls` instead
    pub input: Option<HexBytes>,
    // Optional for Tempo 0x76 transactions which use batched `calls`
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub value: Option<HexBytes>,
    #[serde(rename = "type")]
    pub r#type: Option<u64>,
    #[serde(deserialize_with = "quantity::deserialize")]
    pub gas: HexBytes,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub gas_price: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub max_fee_per_gas: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub max_priority_fee_per_gas: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub v: Option<HexBytes>,
    // r/s are fixed 32-byte signature data, not quantities (see note above).
    pub r: Option<HexBytes>,
    pub s: Option<HexBytes>,
    pub y_parity: Option<u8>,
    pub access_list: Option<Vec<AccessListItem>>,
    pub chain_id: Option<u64>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub max_fee_per_blob_gas: Option<HexBytes>,

    pub blob_versioned_hashes: Option<Vec<HexBytes>>,
    pub authorization_list: Option<Vec<EIP7702Authorization>>,

    // Tempo 0x76 transaction fields
    pub calls: Option<Vec<TempoCall>>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub nonce_key: Option<HexBytes>,
    pub fee_token: Option<HexBytes>,
    pub fee_payer_signature: Option<TempoFeePayerSignature>,
    pub signature: Option<TempoSignature>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub valid_before: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub valid_after: Option<HexBytes>,
    pub aa_authorization_list: Option<Vec<TempoSignedAuthorization>>,
    pub key_authorization: Option<TempoSignedKeyAuthorization>,

    pub contract_address: Option<HexBytes>,
    #[serde(deserialize_with = "quantity::deserialize")]
    pub cumulative_gas_used: HexBytes,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub effective_gas_price: Option<HexBytes>,
    #[serde(deserialize_with = "quantity::deserialize")]
    pub gas_used: HexBytes,
    pub logs_bloom: HexBytes,
    pub status: Option<u8>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub blob_gas_used: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub blob_gas_price: Option<HexBytes>,

    pub l1_base_fee_scalar: Option<u64>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub l1_blob_base_fee: Option<HexBytes>,
    pub l1_blob_base_fee_scalar: Option<u64>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub l1_fee: Option<HexBytes>,
    pub l1_fee_scalar: Option<f64>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub l1_gas_price: Option<HexBytes>,
    #[serde(default, deserialize_with = "quantity::deserialize_option")]
    pub l1_gas_used: Option<HexBytes>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub log_index: ItemIndex,
    pub transaction_index: ItemIndex,
    pub transaction_hash: HexBytes,
    pub address: HexBytes,
    pub data: HexBytes,
    pub topics: Vec<HexBytes>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceActionCreate {
    pub from: HexBytes,
    pub value: Option<HexBytes>,
    pub gas: HexBytes,
    pub init: HexBytes,
    pub creation_method: Option<String>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceActionCall {
    pub from: HexBytes,
    pub to: HexBytes,
    pub value: Option<HexBytes>,
    pub gas: HexBytes,
    pub input: HexBytes,
    pub call_type: String
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceActionReward {
    pub author: HexBytes,
    pub value: HexBytes,
    pub reward_type: String
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceActionSelfDestruct {
    pub address: Option<HexBytes>,
    pub refund_address: HexBytes,
    pub balance: Option<HexBytes>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceResultCreate {
    pub gas_used: HexBytes,
    pub code: Option<HexBytes>,
    pub address: Option<HexBytes>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceResultCall {
    pub gas_used: Option<HexBytes>,
    pub output: Option<HexBytes>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trace {
    pub transaction_index: u32,
    pub trace_address: Vec<u32>,
    pub subtraces: u32,
    pub error: Option<String>,
    pub revert_reason: Option<String>,
    #[serde(flatten)]
    pub op: TraceOp
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum TraceOp {
    #[serde(rename = "create")]
    Create {
        action: TraceActionCreate,
        result: Option<TraceResultCreate>
    },
    #[serde(rename = "call")]
    Call {
        action: TraceActionCall,
        result: Option<TraceResultCall>
    },
    #[serde(rename = "selfdestruct")]
    SelfDestruct { action: TraceActionSelfDestruct },
    #[serde(rename = "reward")]
    Reward { action: TraceActionReward }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateDiff {
    pub transaction_index: ItemIndex,
    pub address: HexBytes,
    pub key: String,
    pub kind: String,
    pub prev: Option<HexBytes>,
    pub next: Option<HexBytes>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub header: BlockHeader,
    // The portal `/stream` response omits `transactions` entirely for zero-tx
    // blocks (omit-empty serialization), so treat a missing key as an empty set
    // rather than a hard `missing field` error that would stall ingest.
    #[serde(default)]
    pub transactions: Vec<Transaction>,
    pub logs: Option<Vec<Log>>,
    pub traces: Option<Vec<Trace>>,
    pub state_diffs: Option<Vec<StateDiff>>
}

impl sqd_primitives::Block for Block {
    fn number(&self) -> BlockNumber {
        self.header.number
    }

    fn hash(&self) -> &str {
        &self.header.hash
    }

    fn parent_number(&self) -> BlockNumber {
        self.header.number.saturating_sub(1)
    }

    fn parent_hash(&self) -> &str {
        &self.header.parent_hash
    }

    fn timestamp(&self) -> Option<i64> {
        // saturate: an absurd source value must not panic (debug) or wrap into a plausible date
        Some(self.header.timestamp.saturating_mul(1000))
    }

    fn data_availability_mask(&self) -> DataMask {
        let mut mask = DataMask::default();
        if self.logs.is_some() {
            mask.set(0)
        }
        if self.traces.is_some() {
            mask.set(1)
        }
        if self.state_diffs.is_some() {
            mask.set(2)
        }
        mask
    }

    fn has_data(mask: DataMask, name: &str) -> bool {
        match name {
            "logs" => mask.get(0),
            "traces" => mask.get(1),
            "statediffs" => mask.get(2),
            _ => true
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::{Block, BlockHeader, Transaction};

    fn quantity(value: u64, as_hex: bool) -> Value {
        if as_hex {
            Value::String(format!("0x{value:x}"))
        } else {
            Value::from(value)
        }
    }

    fn block_header_json(as_hex: bool) -> Value {
        json!({
            "number": 1,
            "hash": "0x01",
            "parentHash": "0x02",
            "timestamp": 3,
            "transactionsRoot": "0x03",
            "receiptsRoot": "0x04",
            "stateRoot": "0x05",
            "logsBloom": "0x06",
            "sha3Uncles": "0x07",
            "extraData": "0x08",
            "miner": "0x09",
            "size": 10,
            "gasLimit": quantity(0, as_hex),
            "gasUsed": quantity(1, as_hex),
            "difficulty": quantity(2, as_hex),
            "totalDifficulty": quantity(3, as_hex),
            "baseFeePerGas": quantity(136_071_899, as_hex),
            "withdrawals": [{
                "address": "0x0a",
                "amount": quantity(4, as_hex),
                "index": quantity(5, as_hex),
                "validatorIndex": quantity(6, as_hex)
            }],
            "blobGasUsed": quantity(7, as_hex),
            "excessBlobGas": quantity(8, as_hex),
            "mainBlockGeneralGasLimit": quantity(9, as_hex),
            "sharedGasLimit": quantity(10, as_hex),
            "timestampMillisPart": quantity(11, as_hex)
        })
    }

    fn transaction_json(as_hex: bool) -> Value {
        json!({
            "transactionIndex": 0,
            "hash": "0x01",
            "nonce": 1,
            "from": "0x02",
            "value": quantity(1, as_hex),
            "gas": quantity(2, as_hex),
            "gasPrice": quantity(3, as_hex),
            "maxFeePerGas": quantity(4, as_hex),
            "maxPriorityFeePerGas": quantity(5, as_hex),
            "v": quantity(6, as_hex),
            "r": "0x7",
            "s": "0x8",
            "maxFeePerBlobGas": quantity(9, as_hex),
            "authorizationList": [{
                "chainId": quantity(10, as_hex),
                "address": "0x03",
                "nonce": "11",
                "yParity": 0,
                "r": "0xc",
                "s": "0xd"
            }],
            "calls": [{
                "to": "0x04",
                "value": quantity(14, as_hex),
                "input": "0x05"
            }],
            "nonceKey": quantity(15, as_hex),
            "validBefore": quantity(16, as_hex),
            "validAfter": quantity(17, as_hex),
            "aaAuthorizationList": [{
                "chainId": quantity(18, as_hex),
                "address": "0x06",
                "nonce": 19,
                "signature": {
                    "type": "secp256k1",
                    "r": "0x07",
                    "s": "0x08"
                }
            }],
            "keyAuthorization": {
                "chainId": quantity(20, as_hex),
                "keyType": "secp256k1",
                "keyId": "0x09",
                "expiry": quantity(21, as_hex),
                "limits": [{
                    "token": "0x0a",
                    "limit": quantity(22, as_hex)
                }],
                "signature": {
                    "type": "secp256k1",
                    "r": "0x0b",
                    "s": "0x0c"
                }
            },
            "cumulativeGasUsed": quantity(23, as_hex),
            "effectiveGasPrice": quantity(24, as_hex),
            "gasUsed": quantity(25, as_hex),
            "logsBloom": "0x0d",
            "blobGasUsed": quantity(26, as_hex),
            "blobGasPrice": quantity(27, as_hex),
            "l1BlobBaseFee": quantity(28, as_hex),
            "l1Fee": quantity(29, as_hex),
            "l1GasPrice": quantity(30, as_hex),
            "l1GasUsed": quantity(31, as_hex)
        })
    }

    #[test]
    fn block_header_quantities_accept_integer_or_hex() {
        let integers: BlockHeader = serde_json::from_value(block_header_json(false)).unwrap();
        let hex: BlockHeader = serde_json::from_value(block_header_json(true)).unwrap();

        assert_eq!(integers.base_fee_per_gas.as_deref(), Some("0x81c4adb"));
        assert_eq!(integers.gas_limit, "0x0");
        assert_eq!(integers.gas_limit, hex.gas_limit);
        assert_eq!(integers.gas_used, hex.gas_used);
        assert_eq!(integers.difficulty, hex.difficulty);
        assert_eq!(integers.total_difficulty, hex.total_difficulty);
        assert_eq!(integers.base_fee_per_gas, hex.base_fee_per_gas);
        assert_eq!(integers.blob_gas_used, hex.blob_gas_used);
        assert_eq!(integers.excess_blob_gas, hex.excess_blob_gas);
        assert_eq!(integers.main_block_general_gas_limit, hex.main_block_general_gas_limit);
        assert_eq!(integers.shared_gas_limit, hex.shared_gas_limit);
        assert_eq!(integers.timestamp_millis_part, hex.timestamp_millis_part);

        let integer_withdrawal = &integers.withdrawals.as_ref().unwrap()[0];
        let hex_withdrawal = &hex.withdrawals.as_ref().unwrap()[0];
        assert_eq!(integer_withdrawal.amount, hex_withdrawal.amount);
        assert_eq!(integer_withdrawal.index, hex_withdrawal.index);
        assert_eq!(integer_withdrawal.validator_index, hex_withdrawal.validator_index);
    }

    #[test]
    fn transaction_quantities_accept_integer_or_hex() {
        let integers: Transaction = serde_json::from_value(transaction_json(false)).unwrap();
        let hex: Transaction = serde_json::from_value(transaction_json(true)).unwrap();

        assert_eq!(integers.value, hex.value);
        assert_eq!(integers.gas, hex.gas);
        assert_eq!(integers.gas_price, hex.gas_price);
        assert_eq!(integers.max_fee_per_gas, hex.max_fee_per_gas);
        assert_eq!(integers.max_priority_fee_per_gas, hex.max_priority_fee_per_gas);
        assert_eq!(integers.v, hex.v);
        assert_eq!(integers.r, hex.r);
        assert_eq!(integers.s, hex.s);
        assert_eq!(integers.max_fee_per_blob_gas, hex.max_fee_per_blob_gas);
        assert_eq!(integers.nonce_key, hex.nonce_key);
        assert_eq!(integers.valid_before, hex.valid_before);
        assert_eq!(integers.valid_after, hex.valid_after);
        assert_eq!(integers.cumulative_gas_used, hex.cumulative_gas_used);
        assert_eq!(integers.effective_gas_price, hex.effective_gas_price);
        assert_eq!(integers.gas_used, hex.gas_used);
        assert_eq!(integers.blob_gas_used, hex.blob_gas_used);
        assert_eq!(integers.blob_gas_price, hex.blob_gas_price);
        assert_eq!(integers.l1_blob_base_fee, hex.l1_blob_base_fee);
        assert_eq!(integers.l1_fee, hex.l1_fee);
        assert_eq!(integers.l1_gas_price, hex.l1_gas_price);
        assert_eq!(integers.l1_gas_used, hex.l1_gas_used);

        let integer_authorization = &integers.authorization_list.as_ref().unwrap()[0];
        let hex_authorization = &hex.authorization_list.as_ref().unwrap()[0];
        assert_eq!(integer_authorization.chain_id, hex_authorization.chain_id);
        assert_eq!(integer_authorization.r, hex_authorization.r);
        assert_eq!(integer_authorization.s, hex_authorization.s);

        let integer_call = &integers.calls.as_ref().unwrap()[0];
        let hex_call = &hex.calls.as_ref().unwrap()[0];
        assert_eq!(integer_call.value, hex_call.value);

        let integer_aa_authorization = &integers.aa_authorization_list.as_ref().unwrap()[0];
        let hex_aa_authorization = &hex.aa_authorization_list.as_ref().unwrap()[0];
        assert_eq!(integer_aa_authorization.chain_id, hex_aa_authorization.chain_id);

        let integer_key_authorization = integers.key_authorization.as_ref().unwrap();
        let hex_key_authorization = hex.key_authorization.as_ref().unwrap();
        assert_eq!(integer_key_authorization.chain_id, hex_key_authorization.chain_id);
        assert_eq!(integer_key_authorization.expiry, hex_key_authorization.expiry);
        assert_eq!(
            integer_key_authorization.limits.as_ref().unwrap()[0].limit,
            hex_key_authorization.limits.as_ref().unwrap()[0].limit
        );
    }

    fn block_json(with_transactions: bool) -> Value {
        let mut block = json!({ "header": block_header_json(false) });
        if with_transactions {
            block["transactions"] = json!([transaction_json(false)]);
        }
        block
    }

    #[test]
    fn block_missing_transactions_field_defaults_to_empty() {
        // The portal `/stream` response omits the `transactions` key entirely for
        // zero-tx blocks. Deserialization must accept that as an empty transaction
        // set rather than failing with `missing field transactions`, which would
        // stall hotblocks ingest on the first empty block.
        let empty: Block = serde_json::from_value(block_json(false)).unwrap();
        assert!(empty.transactions.is_empty());

        let populated: Block = serde_json::from_value(block_json(true)).unwrap();
        assert_eq!(populated.transactions.len(), 1);

        // A missing key and an explicit empty list must be equivalent...
        let mut explicit_empty = block_json(false);
        explicit_empty["transactions"] = json!([]);
        let explicit_empty: Block = serde_json::from_value(explicit_empty).unwrap();
        assert!(explicit_empty.transactions.is_empty());

        // ...while an explicit `null` is still rejected (the portal omits the key,
        // it does not send null — so defaulting must not swallow a malformed null).
        let mut null_txs = block_json(false);
        null_txs["transactions"] = Value::Null;
        assert!(serde_json::from_value::<Block>(null_txs).is_err());
    }

    #[test]
    fn block_header_byte_fields_reject_integer() {
        let mut value = block_header_json(false);
        value["hash"] = Value::from(1);

        assert!(serde_json::from_value::<BlockHeader>(value).is_err());
    }

    #[test]
    fn transaction_signature_fields_reject_integer() {
        // r/s are fixed signature bytes, not quantities: an integer encoding must
        // reject rather than be normalized (which would strip leading zero bytes).
        let mut value = transaction_json(true);
        value["r"] = Value::from(7);

        assert!(serde_json::from_value::<Transaction>(value).is_err());
    }
}
