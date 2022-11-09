use std::{
    collections::{btree_map::Entry, BTreeMap, VecDeque},
    hash::Hash,
};

type Address = String;
type Balance = u64;
type Gas = u64;

const BLOCK_GAS_LIMIT: Gas = 20;
const TX_MINT_GAS: Gas = 5;
const TX_TRANSFER_GAS: Gas = 2;

#[derive(Debug, Hash, Clone)]
pub struct Mint {
    to: Address,
    amount: Balance,
}

#[derive(Debug, Hash, Clone)]
pub struct Transfer {
    from: Address,
    to: Address,
    amount: Balance,
}

#[derive(Debug, Hash, Clone)]
pub enum Transaction {
    Mint(Mint),
    Transfer(Transfer),
}

#[derive(Debug, Hash)]
pub struct Block {
    number: u64,
    transactions: Vec<Transaction>,
    final_state: State,
}

impl Block {
    pub fn genesis() -> Self {
        Self {
            number: 0,
            transactions: vec![],
            final_state: State::new(),
        }
    }

    pub fn create_next_from_txn(
        &self,
        mut txs: VecDeque<Transaction>,
    ) -> (Block, Vec<Transaction>) {
        let mut rejected_txs = vec![];
        let mut accepted_txs = vec![];
        let mut next_state = self.final_state.clone();
        let mut gas_used = 0;

        // FIXME: Add tx ordering here
        loop {
            if let Some(tx) = txs.pop_front() {
                let gas_cost = match tx {
                    Transaction::Mint(_) => TX_MINT_GAS,
                    Transaction::Transfer(_) => TX_TRANSFER_GAS,
                };
                if gas_used + gas_cost > BLOCK_GAS_LIMIT {
                    rejected_txs.push(tx);
                    break;
                }

                if next_state.apply_tx(&tx) {
                    gas_used += gas_cost;
                    accepted_txs.push(tx)
                } else {
                    rejected_txs.push(tx);
                }
            } else {
                break;
            }
        }

        rejected_txs.extend(txs);
        (
            Block {
                number: self.number + 1,
                transactions: accepted_txs,
                final_state: next_state,
            },
            rejected_txs,
        )
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

    const ALICE: &str = "Alice";
    const BOB: &str = "Bob";

    #[test]
    fn state_minting() {
        let mut state = State::new();
        let tx = Transaction::Mint(Mint {
            to: BOB.to_string(),
            amount: 12345,
        });

        assert_eq!(state.balances.get(BOB), None);
        assert!(state.apply_tx(&tx));
        assert_eq!(state.balances.get(BOB), Some(&12345));
    }

    #[test]
    fn state_transfer() {
        let mut state = State::new();

        // Alice has no balance
        let tx = Transaction::Transfer(Transfer {
            from: ALICE.to_string(),
            to: BOB.to_string(),
            amount: 12345,
        });
        assert!(!state.apply_tx(&tx));

        // Mint some tokens for Alice
        let tx = Transaction::Mint(Mint {
            to: ALICE.to_string(),
            amount: 100,
        });
        assert!(state.apply_tx(&tx));
        assert_eq!(state.balances.get(ALICE), Some(&100));

        // Alice has to little balance
        let tx = Transaction::Transfer(Transfer {
            from: ALICE.to_string(),
            to: BOB.to_string(),
            amount: 200,
        });
        assert!(!state.apply_tx(&tx));
        assert_eq!(state.balances.get(ALICE), Some(&100));

        // Alice can transfer
        let tx = Transaction::Transfer(Transfer {
            from: ALICE.to_string(),
            to: BOB.to_string(),
            amount: 99,
        });
        assert!(state.apply_tx(&tx));
        assert_eq!(state.balances.get(ALICE), Some(&1));
        assert_eq!(state.balances.get(BOB), Some(&99));
    }

    #[test]
    fn block_creation() {
        let genesis = Block::genesis();

        let txs = VecDeque::from([
            Transaction::Mint(Mint {
                to: ALICE.to_string(),
                amount: 100,
            }),
            Transaction::Transfer(Transfer {
                from: ALICE.to_string(),
                to: BOB.to_string(),
                amount: 99,
            }),
            Transaction::Transfer(Transfer {
                from: BOB.to_string(),
                to: ALICE.to_string(),
                amount: 5,
            }),
            Transaction::Transfer(Transfer {
                from: BOB.to_string(),
                to: ALICE.to_string(),
                amount: 5_000,
            }),
        ]);

        let (new_block, rejected_txs) = genesis.create_next_from_txn(txs);

        assert_eq!(new_block.number, 1);
        assert_eq!(new_block.final_state.balances.get(ALICE), Some(&6));
        assert_eq!(new_block.final_state.balances.get(BOB), Some(&94));

        assert_eq!(rejected_txs.len(), 1);
    }
}
