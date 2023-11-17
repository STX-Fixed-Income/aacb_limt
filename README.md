# DNB Common Reporting - Capital and Liquidity Requirements

## Purpose
Phase 1 of this project is to build a tool that calculates the responses
to the DNB Common Report Investment Firm Class 2 Independent report.

The main reporting topics are:

1. Capital Requirements (K-NPR and K-RTF)
2. Liquidity Requirements
3. Daily Traded Flow (K-DTF)

Phase 2 of the project will be ongoing semi real-time (every 5 minutes)
calculations of relevant limits, displays them for Risk and Trading 
review, and to setup monitoring and alerting of the limits and the system.

## Details
GenXs positions are used as the basis of our position. 

Excluding ISINs from calculations is permitted with reasoning.


## Dependencies:
The table used as the basis for bond meta data to be analyzed "sfi_transactions.bloomberg_risk" 
is populated daily at 18:31 by https://github.com/STX-Fixed-Income/bb_tw_importer.

Our prices are populated roughly every half-hour throughout the day 
whenever a new emails comes by https://github.com/STX-Fixed-Income/icbc_reader. 

Currently our last price update is 18:30.

Both of the above item use TW which is part of TOMS. 

GenXs FX rates are kept up to date by [fxrates](https://github.com/STX-Fixed-Income/fxrates) everyday (08:00)
