use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{
    collections::{btree_map::Entry, hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
};

type Address = u32;
type Balance = u64;
type Gas = u64;

const BLOCK_GAS_LIMIT: Gas = 20;
const TX_MINT_GAS: Gas = 5;
const TX_TRANSFER_GAS: Gas = 2;

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct Mint {
    pub to: Address,
    pub amount: Balance,
}

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct Transfer {
    pub from: Address,
    pub to: Address,
    pub amount: Balance,
}

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub enum Transaction {
    Mint(Mint),
    Transfer(Transfer),
}

impl Transaction {
    pub fn serialize(&self) -> Bytes {
        let mut tx = BytesMut::new();
        match self {
            Transaction::Mint(m) => {
                tx.put_u8(0);
                tx.put_u32(m.to);
                tx.put_u64(m.amount);
                tx.resize(13, 0)
            }
            Transaction::Transfer(t) => {
                tx.put_u8(1);
                tx.put_u32(t.from);
                tx.put_u32(t.to);
                tx.put_u64(t.amount);
                tx.resize(17, 0)
            }
        }

        tx.split().freeze()
    }

    pub fn deserialize(data: &mut Bytes) -> Self {
        let ttype = data.get_u8();
        match ttype {
            0 => Transaction::Mint(Mint {
                to: data.get_u32(),
                amount: data.get_u64(),
            }),
            1 => Transaction::Transfer(Transfer {
                from: data.get_u32(),
                to: data.get_u32(),
                amount: data.get_u64(),
            }),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub struct Block {
    pub number: u64,
    transactions: Vec<Transaction>,
    state: State,
    gas_used: Gas,
}

pub enum ExecutionError {
    GasLimitReached,
    InvalidTransaction,
}

impl Block {
    pub fn genesis() -> Self {
        Self {
            number: 0,
            transactions: vec![],
            state: State::new(),
            gas_used: 0,
        }
    }

    pub fn next(&self) -> Self {
        Self {
            number: self.number + 1,
            transactions: Vec::new(),
            state: self.state.clone(),
            gas_used: 0,
        }
    }

    pub fn try_apply_tx(&mut self, tx: &Transaction) -> Result<(), ExecutionError> {
        let gas_cost = match tx {
            Transaction::Mint(_) => TX_MINT_GAS,
            Transaction::Transfer(_) => TX_TRANSFER_GAS,
        };
        if self.gas_used + gas_cost > BLOCK_GAS_LIMIT {
            return Err(ExecutionError::GasLimitReached);
        }

        if self.state.apply_tx(&tx) {
            self.gas_used += gas_cost;
            self.transactions.push(tx.clone());
            return Ok(());
        } else {
            return Err(ExecutionError::InvalidTransaction);
        }
    }

    pub fn root(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.number.hash(&mut hasher);
        self.transactions.hash(&mut hasher);
        self.state.hash(&mut hasher);
        self.gas_used.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Hash, Clone)]
struct State {
    balances: BTreeMap<Address, Balance>,
}

impl State {
    fn new() -> Self {
        Self {
            balances: BTreeMap::new(),
        }
    }

    fn apply_tx(&mut self, tx: &Transaction) -> bool {
        match tx {
            // No 'from' address means minting
            Transaction::Mint(t) => {
                self.balances
                    .entry(t.to.clone())
                    .and_modify(|e| *e += t.amount)
                    .or_insert(t.amount);
                true
            }
            // Transfer
            Transaction::Transfer(t) => {
                match self.balances.entry(t.from.clone()) {
                    Entry::Occupied(mut sender_entry) => {
                        // Sender's balance to small
                        let current_val = *sender_entry.get();
                        if current_val < t.amount {
                            false
                        } else {
                            sender_entry.insert(current_val - t.amount);
                            {
                                self.balances
                                    .entry(t.to.clone())
                                    .and_modify(|e| *e += t.amount)
                                    .or_insert(t.amount);
                            }
                            true
                        }
                    }
                    // Sender has no balance
                    Entry::Vacant(_) => false,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALICE: u32 = 1;
    const BOB: u32 = 2;

    #[test]
    fn state_minting() {
        let mut state = State::new();
        let tx = Transaction::Mint(Mint {
            to: BOB,
            amount: 12345,
        });

        assert_eq!(state.balances.get(&BOB), None);
        assert!(state.apply_tx(&tx));
        assert_eq!(state.balances.get(&BOB), Some(&12345));
    }

    #[test]
    fn state_transfer() {
        let mut state = State::new();

        // Alice has no balance
        let tx = Transaction::Transfer(Transfer {
            from: ALICE,
            to: BOB,
            amount: 12345,
        });
        assert!(!state.apply_tx(&tx));

        // Mint some tokens for Alice
        let tx = Transaction::Mint(Mint {
            to: ALICE,
            amount: 100,
        });
        assert!(state.apply_tx(&tx));
        assert_eq!(state.balances.get(&ALICE), Some(&100));

        // Alice has to little balance
        let tx = Transaction::Transfer(Transfer {
            from: ALICE,
            to: BOB,
            amount: 200,
        });
        assert!(!state.apply_tx(&tx));
        assert_eq!(state.balances.get(&ALICE), Some(&100));

        // Alice can transfer
        let tx = Transaction::Transfer(Transfer {
            from: ALICE,
            to: BOB,
            amount: 99,
        });
        assert!(state.apply_tx(&tx));
        assert_eq!(state.balances.get(&ALICE), Some(&1));
        assert_eq!(state.balances.get(&BOB), Some(&99));
    }

    #[test]
    fn block_creation() {
        let genesis = Block::genesis();

        let m = Transaction::Mint(Mint {
            to: ALICE,
            amount: 100,
        });
        let t1 = Transaction::Transfer(Transfer {
            from: ALICE,
            to: BOB,
            amount: 99,
        });
        let t2 = Transaction::Transfer(Transfer {
            from: BOB,
            to: ALICE,
            amount: 5,
        });
        let t3 = Transaction::Transfer(Transfer {
            from: BOB,
            to: ALICE,
            amount: 5_000,
        });

        let mut new_block = genesis.next();
        let receipt = new_block.try_apply_tx(&m);
        assert!(receipt.is_ok());
        let receipt = new_block.try_apply_tx(&t1);
        assert!(receipt.is_ok());
        let receipt = new_block.try_apply_tx(&t2);
        assert!(receipt.is_ok());
        let receipt = new_block.try_apply_tx(&t3);
        assert!(receipt.is_err());

        assert_eq!(new_block.number, 1);
        assert_eq!(new_block.transactions.len(), 3);
        assert_eq!(new_block.state.balances.get(&ALICE), Some(&6));
        assert_eq!(new_block.state.balances.get(&BOB), Some(&94));

        assert_eq!(new_block.root(), new_block.root());
        assert_ne!(genesis.root(), new_block.root());
    }

    #[test]
    fn serialisation() {
        let mint = Transaction::Mint(Mint {
            to: ALICE,
            amount: 100,
        });
        let mut mint_ser = mint.serialize();
        assert_eq!(
            mint_ser,
            b"\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x64"[..]
        );

        let mint_deser = Transaction::deserialize(&mut mint_ser);
        assert_eq!(mint, mint_deser);

        let transf = Transaction::Transfer(Transfer {
            from: ALICE,
            to: BOB,
            amount: 99,
        });
        let mut transf_ser = transf.serialize();
        assert_eq!(
            transf_ser,
            b"\x01\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x63"[..]
        );

        let transf_deser = Transaction::deserialize(&mut transf_ser);
        assert_eq!(transf, transf_deser);
    }
}
