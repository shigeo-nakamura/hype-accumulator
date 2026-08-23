use chrono::{DateTime, Utc};
#[derive(Clone, Debug, PartialEq)]
pub struct SignalInputs {
    pub at: DateTime<Utc>,
    pub reference_price: f64,
    pub validator: String,
}
