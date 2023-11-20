# AACB LimitS

## Objective
The goal of this project is to develop a forecasting tool that calculates and predicts values within the 'limits' column of the daily haircut report provided by ABN Clearing. Our aim is to adjust our financial position in a manner that avoids exceeding the AACB limits, ideally achieving this in real-time.

Key Focus Areas:

1. Estimating Ineligible Financials:
  1.1. Calculation of cash balance.
  1.2. Calculation of payable and receivable balances.
2. Daily Comparison:
  Compare the previous day's (T-1) estimates with the actual AACB figures on day T to identify any discrepancies.
3 FCCMDEF Dashboard Replication:
  Replicate the dashboard found in the FCCMDEF file.
4. Estimate the 'History' row in the daily haircut report at the end of day T-1.

## Implementation Details and Resources:

The foundation of our position calculations is based on GenXs positions.

Daily haircut reports and FCCMDEF files are utilized for backtesting purposes.


