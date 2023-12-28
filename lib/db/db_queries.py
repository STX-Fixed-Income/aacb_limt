from lib.log import file_log as logging

logger = logging.FileLog.get_logger(name=__name__)


def get_bbg_bond_data_qry(qdate):
    q = f'''
    SELECT 
        "security" AS symbol
        , COALESCE(isin_number, figi, "security") AS isin
        , maturity
        , "s&p_rating"
        , fitch
        , coupon
        , currency
        , bid_price
        , ask_price
        , senior
        , iss_ind
        , cntry_terrtry_of_rsk
        , subordinated
        , guaranty_level
        , perpetual
        , created_at
        , sec_typ_2
        , ind_sector
        , issuer
        , amt_issued
        , ticker
        , figi
        , entity_type
        , moodys_rating
        , dbrs_rating
    FROM public.bloomberg_risk
    WHERE created_at::DATE = '{qdate}'
    GROUP BY
        "security" 
        , COALESCE(isin_number, figi, "security")
        , maturity
        , "s&p_rating"
        , fitch
        , coupon
        , currency
        , bid_price
        , ask_price
        , senior
        , iss_ind
        , cntry_terrtry_of_rsk
        , subordinated
        , guaranty_level
        , perpetual
        , created_at
        , sec_typ_2
        , ind_sector
        , issuer
        , amt_issued
        , ticker
        , figi
        , entity_type
        , moodys_rating
        , dbrs_rating
    '''
    logger.debug(q)

    return q


def get_genxs_postions_qry(qdate):
    q = f'''
    SELECT DISTINCT
        COALESCE(c.isin,i.info2, c.symbol) AS isin,
        SUM(overnight) + SUM(bought) - SUM(sold) AS current_position,
        cc.symbol AS currency,
        c.symbol,
        c.contractsize,
        1/coalesce(fx.Rate, 1.00) AS fx_rate,
        CASE 
            WHEN c.contracttype = 3 THEN 'Bond' 
            WHEN c.contracttype = 2 THEN 'Future' 
            ELSE 'Other'
        END AS contracttype
    FROM tradingsystem.positions p
    LEFT JOIN tradingsystem.instruments i ON p.InstrumentCode=i.Code
    LEFT JOIN tradingsystem.contracts c ON c.Code=i.ContractCode
    LEFT JOIN tradingsystem.contracts cc ON cc.Code=i.CurrencyContractCode
    LEFT JOIN tradingsystem.exchangerateseurobased fx ON fx.ContractCode = i.CurrencyContractCode 
        AND DATE(p.DateTimeSaved) = DATE(fx.`Date`)
    WHERE 
        DATE(CONVERT_TZ(DateTimeSaved, '+00:00', "-02:00")) = '{qdate}'
    GROUP BY	
        COALESCE(c.isin,i.info2, c.symbol),
        cc.Symbol,
        c.symbol,
        coalesce(fx.Rate, 1.00),
        case when c.contracttype = 3 then 'Bond' when c.contracttype = 2 then 'Future' else 'Other' end
    ORDER BY 
        c.contracttype, 
        c.isin
    '''
    logger.debug(q)

    return q


def get_trades_qry(startdate, enddate):
    # Only interested in real trades, exchange or via platform/OTC but not simulated traded (internal rebookings)
    q = f'''
        SELECT
          t.TradeDate as tradedate
        , t.SettleDate as settledate
        , t.TradeID as tradeid
        , coalesce(c.ISIN, i.info2) as isin 
        , c.Symbol as symbol
        , case 
            when side = 1 then 'buy'
            when side = 2 then 'sell'
          end AS side
        , t.size as size
        , t.price as price
        , cc.Symbol as currency
        , case 
            when cc.Symbol = 'EUR' then 1 
            else 1/rate 
          end as fx_rate
        , case 
            when cc.Symbol = 'EUR' then t.price 
            else t.price * 1/rate 
          end as price_in_eur
        , case 
            when cc.Symbol = 'EUR' then t.price * t.size * c.contractsize
            else t.price * fx.rate * t.size * c.contractsize 
          end as total_value_in_eur 
        , c.ContractSize as contractsize 
        , IFNULL(i.MIC, mic.MICMarketId) AS marketid
        , CONCAT(u.FirstName, ' ', u.LastName) AS username
        , case 
            when c.contracttype = 3 then 'Bond' 
            when c.contracttype = 2 then 'Future' 
            else 'Other'
          end as contracttypename
        , 0 as storno
        FROM tradingsystem.tradeshistory t
        LEFT JOIN tradingsystem.instruments i ON i.Code=t.InstrumentCode
        LEFT JOIN tradingsystem.markets m ON m.Code=i.MarketCode
        LEFT JOIN tradingsystem.contracts c ON c.Code=i.ContractCode
        LEFT JOIN tradingsystem.contracts cc ON cc.Code=i.CurrencyContractCode
        LEFT JOIN tradingsystem.micexchangemappings mic ON mic.MarketCode=i.MarketCode AND mic.Flags=1
        LEFT JOIN tradingsystem.users u ON u.Code=t.UserCode 
        LEFT JOIN tradingsystem.exchangerateseurobased fx ON fx.ContractCode = i.CurrencyContractCode 
                AND DATE(t.DateTimeSaved) = DATE(fx.Date)
        WHERE DATE(DateTimeSaved) between '{enddate}' and '{startdate}' 
            and concat(ExchangeTradeID,side,size,price) not in
                (
                    select concat(ExchangeTradeID,side,size,price)
                    from tradingsystem.simulationtradeshistory t
                    where Date between '{enddate}' and '{startdate}'  
                    and storno = 1
                )
            and concat(TradeID,'_cancel') not in
                (
                    select tradeid
                    from tradingsystem.tradeshistory t
                    where t.TradeDate between '{enddate}' and '{startdate}' 
                        and tradeid like '%%_cancel'
                )
        UNION ALL
        SELECT
          t.TradeDate as tradedate
        , t.SettleDate as settledate
        , t.TradeID as tradeid
        , coalesce(c.ISIN, i.info2) as isin 
        , c.Symbol as symbol
        , case 
            when side = 1 then 'buy'
            when side =2 then 'sell'
          end AS side
        , t.size as size
        , t.price as price
        , cc.Symbol as currency
        , case 
            when cc.Symbol = 'EUR' then 1 
            else 1/rate 
          end as fx_rate
        , case 
            when cc.Symbol = 'EUR' then t.price 
            else t.price * 1/rate 
          end as price_in_eur
        , case 
            when cc.Symbol = 'EUR' then t.price * t.size * c.contractsize
            else t.price * fx.rate * t.size * c.contractsize 
          end as total_value_in_eur 
        , c.ContractSize as contractsize 
        , IFNULL(i.MIC, mic.MICMarketId) AS marketid
        , CONCAT(u.FirstName, ' ', u.LastName) AS username
        , case 
            when c.contracttype = 3 then 'Bond' 
            when c.contracttype = 2 then 'Future' 
            else 'Other'
          end as contracttypename
        , 0 as storno
        FROM tradingsystem.trades_merged t
        LEFT JOIN tradingsystem.instruments i ON i.Code=t.InstrumentCode
        LEFT JOIN tradingsystem.markets m ON m.Code=i.MarketCode
        LEFT JOIN tradingsystem.contracts c ON c.Code=i.ContractCode
        LEFT JOIN tradingsystem.contracts cc ON cc.Code=i.CurrencyContractCode
        LEFT JOIN tradingsystem.micexchangemappings mic ON mic.MarketCode=i.MarketCode AND mic.Flags=1
        LEFT JOIN tradingsystem.users u ON u.Code=t.UserCode 
        LEFT JOIN tradingsystem.exchangerateseurobased fx ON fx.ContractCode = i.CurrencyContractCode 
                AND DATE(t.DateTimeSaved) = DATE(fx.Date)
        WHERE DATE(DateTimeSaved) between '{enddate}' and '{startdate}' 
            and concat(ExchangeTradeID,side,size,price) not in
                (
                    select concat(ExchangeTradeID,side,size,price)
                    from tradingsystem.simulationtradeshistory t
                    where Date between '{enddate}' and '{startdate}' 
                    and storno = 1
                )
            and concat(TradeID,'_cancel') not in
                (
                    select tradeid
                    from tradingsystem.tradeshistory t
                    where t.TradeDate between '{enddate}' and '{startdate}' 
                        and tradeid like '%%_cancel'
                )
        ORDER BY tradedate
    '''
    logger.debug(q)

    return q

def get_intratrades_qry(qdate):
    q = f'''
        SELECT 
            date(p.DateTimeSaved) as date,
            COALESCE(c.isin,i.info2, c.symbol) as isin,
            c.symbol,
            c.contractsize,
            sum(Overnight) as Overnight,
            cc.symbol as currency,
            1/coalesce(fx.Rate, 1.00) AS fx_rate,
            1 as constant,
            sum(overnight + bought - sold) as position,
            sum((overnight + bought - sold) * 0.01) as cs_pos,
            abs(sum(overnight)) as abs_over,
            abs(sum((overnight + bought - sold))) as abs_pos,
            abs(sum((overnight + bought - sold))) - abs(sum(overnight)) as diff_exposure,
            
            CASE 
                when abs(sum(overnight + bought - sold) - sum(overnight)) <> 0 then 1
                else 0
            END as filter_col
            
        FROM tradingsystem.positions p
        LEFT JOIN tradingsystem.instruments i ON p.InstrumentCode=i.Code
        LEFT JOIN tradingsystem.contracts c ON c.Code=i.ContractCode
        LEFT JOIN tradingsystem.contracts cc ON cc.Code=i.CurrencyContractCode
        LEFT JOIN tradingsystem.exchangerateseurobased fx ON fx.ContractCode = i.CurrencyContractCode 
                AND DATE(p.DateTimeSaved) = DATE(fx.Date)
        WHERE date(p.DateTimeSaved)  = '{qdate}'
            and c.contracttype in (3)
            -- and overnight + bought - sold <> 0   
        group by 	
            COALESCE(c.isin,i.info2, c.symbol),
            c.symbol,
            coalesce(fx.Rate, 1.00)
        order by c.isin
    '''
    logger.debug(q)

    return q


def get_fccmdef_day_qry(qdate):
    q = f'''
        SELECT
          p.isin,
          coalesce(avg(valuation_price),100) as valuation_price,
          sum(mark_to_market_value) as abn_mtm_value,
          case
            when eligibility = 100 then 'Y'
            else 'N'
          end as eligible
        FROM positions p
        left join fccmdef f on p.isin = f.isin and p.processing_date = f.file_date
        WHERE
          case
            when extract(dow from date '{qdate}') = 1 then processing_date::date = (date '{qdate}' - interval '3 days')::date
            when extract(dow from date '{qdate}') = 0 then processing_date::date = (date '{qdate}' - interval '2 days')::date
            else processing_date::date = (date '{qdate}' - interval '1 day')::date
          end
        group by p.isin, eligibility
    '''
    logger.debug(q)

    return q

def get_fccmdef_all_qry():
    ## this function to get the first occurrence of each ISIN and select the most recent valuation price for each ISIN group.
    q = '''
        WITH recent_valuation AS (
            SELECT 
                p.isin,
                p.valuation_price,  
                CASE 
                    WHEN f.eligibility = 100 THEN 'Y'
                    ELSE 'N'
                END as eligible,
                ROW_NUMBER() OVER (PARTITION BY p.isin ORDER BY f.file_date DESC) as rn
            FROM positions p
            LEFT JOIN fccmdef f ON p.isin = f.isin
        )
        SELECT 
            isin,
            coalesce(avg(valuation_price), 100) as valuation_price,  
            eligible
        FROM recent_valuation
        WHERE rn = 1
        GROUP BY isin, eligible
    '''
    logger.debug(q)
    return q

def get_fccmdef_summary_qry():
    ## this function to get the first occurrence of each ISIN and select the most recent valuation price for each ISIN group.
    q = f'''
        SELECT *
        FROM public.fccmdef_summary
    '''
    logger.debug(q)
    return q

def get_our_prices_qry(qdate):
    q = f'''
        SELECT
            COALESCE(isin_number,figi) AS isin,
            security AS bbg_security, 
            MIN(
             CASE
                WHEN bid_price IS  NOT NULL AND bid_price > 0 THEN bid_price::FLOAT8
                WHEN level_1_bid_price IS NOT NULL AND level_1_bid_price::FLOAT8 > 0 THEN level_1_bid_price::FLOAT8
                ELSE 0.00
             END) AS best_bid,
            MAX(
             CASE
                WHEN ask_price IS NOT NULL AND ask_price > 0 THEN ask_price::FLOAT8
                WHEN level_1_ask_price IS NOT NULL AND level_1_ask_price::FLOAT8 > 0 THEN level_1_ask_price::FLOAT8
                ELSE 0.00
             END) AS best_ask,
            CASE 
                WHEN security LIKE '%% EUX'  THEN SUM(current_net_position) / 1
                WHEN security LIKE 'FV%% CBT' THEN SUM(current_net_position) / 1
                WHEN security LIKE 'TY%% CBT' THEN SUM(current_net_position) / 1
                WHEN security LIKE 'WN%% CBT' THEN SUM(current_net_position) / 1
                WHEN security LIKE 'TU%% CBT' THEN SUM(current_net_position) / 1
                WHEN security LIKE '%% CBT' THEN SUM(current_net_position) / 2 
                WHEN security LIKE 'VGH3 %%' or security LIKE 'VGM3 %%' or security LIKE 'VGU3 %%' or 
                        security LIKE 'VGZ3 %%' THEN SUM(current_net_position) / 10
                ELSE (SUM(current_net_position))::DECIMAL * 1000 
            END AS bbg_position
        FROM bloomberg_position
        WHERE 
            created_at = (
                SELECT created_at FROM bloomberg_position WHERE created_at::DATE = '{qdate}' ORDER BY created_at DESC LIMIT 1
                )
        GROUP BY
            security,
            COALESCE(isin_number,figi)
    '''
    logger.debug(q)

    return q


def get_last_paid_qry(isin):
    q = f'''
        SELECT 
            t.Price, t.TradeDate as TradeDate, 2 as orderby, DateTimeSaved
        FROM tradingsystem.`tradeshistory` t
        LEFT JOIN tradingsystem.instruments i on i.Code=t.InstrumentCode 
        LEFT JOIN tradingsystem.contracts c ON c.Code=i.ContractCode 
        WHERE 
            c.ISIN = '{isin}' or c.symbol = '{isin}' or i.info2 = '{isin}'
        UNION ALL
        SELECT 
            t.Price, t.TradeDate as TradeDate, 1 as orderby, DateTimeSaved
        FROM tradingsystem.`trades_merged` t 
        LEFT JOIN tradingsystem.instruments i on i.Code=t.InstrumentCode 
        LEFT JOIN tradingsystem.contracts c ON c.Code=i.ContractCode 
        WHERE 
            c.ISIN = '{isin}'  or c.symbol = '{isin}' or i.info2 = '{isin}'
        ORDER BY 
            TradeDate DESC, 
            DateTimeSaved DESC, 
            orderby
        LIMIT 1
    '''
    logger.debug(q)

    return q


def get_simtrade_last_paid_qry(isin):
    # raise ValueError(f'Filter needs to be implemented for ISIN: {isin}')
    q = f'''
        SELECT  
            t.Price 
            , t.`Date` as TradeDate
            ,CASE
                WHEN t.Type in (0,6) THEN 1
                ELSE 2
            END AS orderby
        FROM tradingsystem.`simulationtradeshistory` t 
        WHERE t.Storno != 1
        UNION ALL
        SELECT  
            t.Price
            ,t.`Date` as TradeDate 
            ,CASE
                WHEN t.Type in (0,6) THEN 1
                ELSE 2
            END AS orderby
        FROM tradingsystem.`simulationtrades` t 
        WHERE t.Storno != 1
        ORDER BY 
        orderby
        LIMIT 1
    '''
    logger.debug(q)

    return q


def get_dnb_cqs_mapping():
    q = f'''
    SELECT *
    FROM public.dnb_cqs_mapping
    '''
    logger.debug(q)

    return q

def get_abn_breaks_qry():
    q = f'''
    SELECT *
    FROM public.settlement_breaks
    '''
    logger.debug(q)

    return q

def get_aacb_fccmdef_qry():
    q = f'''
    SELECT *
    FROM public.fccmdef
    '''
    logger.debug(q)

    return q


def get_aacb_liqcdef_qry():
    q = f'''
    SELECT *
    FROM public.liqcdef
    '''
    logger.debug(q)

    return q

def get_aacb_cash_balance():
    q = f'''
    SELECT *
    FROM public.interestclientlevel
    '''
    logger.debug(q)

    return q

def get_sfi_txs_fx():
    q = f'''
    SELECT *
    FROM public.fx_rates
    '''
    logger.debug(q)

    return q