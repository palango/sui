use std::{
    collections::{btree_map::Entry, BTreeMap},
    hash::Hash,
};

type Address = String;
type Balance = u64;
type Gas = u64;

const BLOCK_GAS_LIMIT: Gas = 20;
const TX_GAS_PRICE: Gas = 2;

#[derive(Debug, Hash, Clone)]
pub struct Transfer {
    from: Option<Address>,
    to: Address,
    amount: Balance,
}

#[derive(Debug, Hash, Clone)]
pub enum Transaction {
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

    pub fn create_next_from_txn(&self, txs: &mut Vec<Transaction>) -> (Block, Vec<Transaction>) {
        let mut rejected_txs = vec![];
        let mut accepted_txs = vec![];
        let mut next_state = self.final_state.clone();
        let mut gas_used = 0;

        // FIXME: Add tx ordering here
        while gas_used + TX_GAS_PRICE <= BLOCK_GAS_LIMIT {
            if let Some(tx) = txs.first_mut() {
                if next_state.apply_tx(tx) {
                    gas_used += TX_GAS_PRICE;
                    accepted_txs.push(tx.clone())
                } else {
                    rejected_txs.push(tx.clone());
                }
            } else {
                break;
            }
        }

        rejected_txs.append(txs);
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
            Transaction::Transfer(t) if t.from.is_none() => {
                self.balances
                    .entry(t.to.clone())
                    .and_modify(|e| *e += t.amount)
                    .or_insert(t.amount);
                true
            }
            // Transfer
            Transaction::Transfer(t) => {
                let sender = t.from.clone().unwrap();
                match self.balances.entry(sender) {
                    Entry::Occupied(e) => {
                        // Sender's balance to small
                        if *e.get() < t.amount {
                            false
                        } else {
                            self.balances
                                .entry(t.to.clone())
                                .and_modify(|e| *e += t.amount)
                                .or_insert(t.amount);
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
