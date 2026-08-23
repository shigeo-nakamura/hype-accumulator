use chrono::{DateTime, Utc};
#[derive(Clone, Debug, PartialEq)]
pub struct LedgerEntry {
    pub at: DateTime<Utc>,
    pub admitted_deposit_usdc: f64,
    pub deployed_usdc: f64,
}
pub trait Ledger: Send {
    /// Appends one immutable event to the ledger.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific persistence error.
    fn append(&mut self, entry: LedgerEntry) -> Result<(), String>;
}
#[derive(Default)]
pub struct MemoryLedger(pub Vec<LedgerEntry>);
impl Ledger for MemoryLedger {
    fn append(&mut self, entry: LedgerEntry) -> Result<(), String> {
        self.0.push(entry);
        Ok(())
    }
}
