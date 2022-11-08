use std::collections::{hash_map::Entry, HashMap};

type Address = String;
type Balance = u64;
type Gas = u64;

#[derive(Debug)]
pub struct Transfer {
    from: Option<Address>,
    to: Address,
    amount: Balance,
}

#[derive(Debug)]
pub enum Transaction {
    Transfer(Transfer),
}

#[derive(Debug)]
pub struct Block {
    transactions: Vec<Transaction>,
}

#[derive(Debug)]
struct State {
    balances: HashMap<Address, Balance>,
}

impl State {
    fn new() -> Self {
        Self {
            balances: HashMap::new(),
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
                return true;
            }
            // Transfer
            Transaction::Transfer(t) => {
                let sender = t.from.clone().unwrap();
                match self.balances.entry(sender) {
                    Entry::Occupied(e) => {
                        // Sender's balance to small
                        if *e.get() < t.amount {
                            return false;
                        } else {
                            self.balances
                                .entry(t.to.clone())
                                .and_modify(|e| *e += t.amount)
                                .or_insert(t.amount);
                            return true;
                        }
                    }
                    // Sender has no balance
                    Entry::Vacant(_) => return false,
                }
            }
        }
    }
}
