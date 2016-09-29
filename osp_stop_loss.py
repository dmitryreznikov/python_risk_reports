import pyodbc
import argparse
import datetime
import logging
import pandas
import dateutil
import dateutil.rrule as dt_rr

STOP_LOSS_LEVEL = -0.0050


str_sql_citco_pl_coresecurity = '''
SELECT
	'{}' AS AS_OF_DATE
	, c.SECURITY_ASSET_NAME
	, c.SECURITY_DESCRIPTION
	, c.SECURITY_ASSET_CLASS
	, c.SECURITY_EDM_ID
	, c.CITCO_SECURITY_ID 
	, c.ISIN
	, C.BLOOMBERG_ID
	, s.ALADDIN_SECURITY_ID
	, sc.BARCLAYS_LVL_3_5 INDUSTRY

	, c.ISSUE_CURRENCY
	, c.LONG_SHORT
	, MAX(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) AS FX_RATE
	, SUM(c.CURRENT_FACE * CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) AS CURRENT_FACE_USD
	
	, case when s.UNDERLYING_ID IS NULL AND s.NB_SECURITY_TYPE_1 <> 'TRS' then 
			UPPER(i.ULTIMATE_PARENT_NAME)
			
			when s.UNDERLYING_ID IS NOT NULL then  (
				SELECT UPPER(iu.NAME) 
					FROM   core.vSECURITY u 
						LEFT OUTER JOIN core.vISSUER i  ON s.UNDERLYING_ID = u.NB_ID AND i.NB_ID = u.ISSUER_NB_ID
						INNER JOIN      core.vISSUER iu ON i.ULTIMATE_PARENT_NB_ID = iu.NB_ID) 
						
				when  NB_SECURITY_TYPE_1 = 'TRS' then (
					SELECT UPPER(iu2.NAME) 
						FROM core.vSECURITY u2 
							LEFT OUTER JOIN core.vISSUER i2  ON s.ASSET_BENCHMARK = u2.ALADDIN_SECURITY_ID and i2.NB_ID = u2.ISSUER_NB_ID
							INNER JOIN      core.vISSUER iu2 ON i2.ULTIMATE_PARENT_NB_ID = iu2.NB_ID)
							
	  end Ultimate_Issuer_Name
	  
	  , case when s.UNDERLYING_ID IS NULL AND s.NB_SECURITY_TYPE_1 <> 'TRS' then 
			i.ULTIMATE_PARENT_NB_ID
			
			when s.UNDERLYING_ID IS NOT NULL then  (
				SELECT iu.NB_ID
					FROM   core.vSECURITY u 
						LEFT OUTER JOIN core.vISSUER i  ON s.UNDERLYING_ID = u.NB_ID AND i.NB_ID = u.ISSUER_NB_ID
						INNER JOIN      core.vISSUER iu ON i.ULTIMATE_PARENT_NB_ID = iu.NB_ID) 
						
				when  NB_SECURITY_TYPE_1 = 'TRS' then (
					SELECT iu2.NB_ID
						FROM core.vSECURITY u2 
							LEFT OUTER JOIN core.vISSUER i2  ON s.ASSET_BENCHMARK = u2.ALADDIN_SECURITY_ID and i2.NB_ID = u2.ISSUER_NB_ID
							INNER JOIN      core.vISSUER iu2 ON i2.ULTIMATE_PARENT_NB_ID = iu2.NB_ID)
							
	  end Ultimate_Issuer_ID

	--BASE NAV AND MARKET VALUE --
    , case when SECURITY_ASSET_NAME in ('ACCOUNTING CASH') then sum(isnull(c.MARKET_VALUE,0)) else sum(isnull(c.BASE_NAV_CONTRIBUTION,0)) end  BASE_NAV_CONTRIBUTION
	, SUM(case when SECURITY_ASSET_NAME in ('ACCOUNTING CASH') and isnull(c.MARKET_VALUE,0) <0 then 0 else isnull(c.MARKET_VALUE,0)+isnull(c.BASE_END_LOAN_AMOUNT,0)  end) MARKET_VALUE
	, SUM(case when SECURITY_ASSET_NAME in ('ACCOUNTING CASH') and isnull(c.MARKET_VALUE,0) <0 then 0 else isnull(c.MARKET_VALUE,0) + isnull(BASE_BOND_ACCRUED_INTEREST,0) end) TOTAL_MARKET_VALUE
	, SUM (case when SECURITY_ASSET_NAME in ('ACCOUNTING CASH') then 
			(case when isnull(c.MARKET_VALUE,0) >0 then 0 else isnull(c.MARKET_VALUE,0)  END) 
		else isnull(c.BASE_END_LOAN_AMOUNT,0)+ ISNULL( BASE_REPO_ACCRUED_INTEREST,0) end) TOTAL_LOAN_AMOUNT
	
	--ISSUE NAV AND MARKET VALUE--
	, SUM(case when SECURITY_ASSET_NAME in ('ACCOUNTING CASH') then isnull(c.MARKET_VALUE,0)/(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) else isnull(c.ISSUE_NAV_CONTRIBUTION,0) 
				- case when c.ISSUE_CURRENCY = 'USD' and LONG_SHORT not in ('Repo', 'Rever', 'Reverse Repo') then ISNULL(ISSUE_TRADE_ACQUISITION_INTEREST,0)
				       when LONG_SHORT in ('Short') then ISNULL(ISSUE_TRADE_ACQUISITION_INTEREST,0)  else 0 end															-- compensate for error in file
		  end)  ISSUE_NAV_CONTRIBUTION
		  
	, SUM(case when LONG_SHORT in ('Repo', 'Rever', 'Reverse Repo') then isnull(c.ISSUE_END_LOAN_AMOUNT,0) else isnull(c.ISSUE_MARKET_VALUE,0) end) ISSUE_MARKET_VALUE
	, SUM(case when LONG_SHORT in ('Repo', 'Rever', 'Reverse Repo') then isnull(c.ISSUE_END_LOAN_AMOUNT,0)+ISNULL( ISSUE_REPO_ACCRUED_INTEREST,0) else isnull(c.ISSUE_MARKET_VALUE,0)+ isnull(ISSUE_BOND_ACCRUED_INTEREST,0) end )  ISSUE_TOTAL_MARKET_VALUE
	
	-- ISSUE PL DAILY --
	, SUM( isnull(REPORT_DATE_ISSUE_UNREALIZED_PL,0) + isnull(REPORT_DATE_ISSUE_REALIZED_PL,0) 
		+(ISNULL(ISSUE_TOTAL_BOND_INTEREST,0))
		+(ISNULL(ISSUE_TOTAL_REPO_INTEREST,0))*0 ) DAILY_ISSUE_PL

	-- BASE PL DAILY --
	, SUM(isnull(c.DAILY_BASE_PL,0)  
			+ (ISNULL(BASE_TOTAL_REPO_INTEREST,0)) 
			+  (ISNULL(BASE_TOTAL_BOND_INTEREST,0) ) 
			+ isnull(DIVIDEND_INCOME_LOSS, 0) ) DAILY_BASE_PL
			
	, SUM(ISNULL( REPORT_DATE_BASE_REALIZED_PL,0)) DAILY_BASE_REALIZED_PL
	, SUM(ISNULL( REPORT_DATE_BASE_UNREALIZED_PL,0)) DAILY_BASE_UNREALIZED_PL	
	
	, SUM(((ISNULL( REPORT_DATE_ISSUE_REALIZED_PL,0) + ISNULL( REPORT_DATE_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) + isnull(REPORT_DATE_BASE_COMMISSIONS,0)+ isnull(REPORT_DATE_BASE_SEC_FEES,0)) * (case when SECURITY_ASSET_CLASS = 'Currency' then 0 else 1 end)) DAILY_PRICE_PL
	, SUM ( ISNULL( BASE_TOTAL_BOND_INTEREST,0)+ ISNULL(BASE_TOTAL_REPO_INTEREST,0) + isnull(DIVIDEND_INCOME_LOSS, 0) ) [DAILY_ACCRUALS]
	, SUM( case when SECURITY_ASSET_NAME = 'Cross Rate' then (ISNULL( REPORT_DATE_ISSUE_REALIZED_PL,0) + ISNULL( REPORT_DATE_ISSUE_UNREALIZED_PL,0)) else 
			 isnull(c.DAILY_BASE_PL,0) - (ISNULL( REPORT_DATE_ISSUE_REALIZED_PL,0) + ISNULL( REPORT_DATE_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) -( isnull(REPORT_DATE_BASE_COMMISSIONS,0)+ isnull(REPORT_DATE_BASE_SEC_FEES,0) ) end ) DAILY_FX_PL
	
	-- ISSUE PL MONTHLY--
	, SUM( isnull(MTD_ISSUE_UNREALIZED_PL,0) + isnull(MTD_ISSUE_REALIZED_PL,0) 
		+(ISNULL(MTD_ISSUE_TOTAL_BOND_INTEREST,0)) 
		+(ISNULL(MTD_ISSUE_TOTAL_REPO_INTEREST,0)) ) MTD_ISSUE_PL

	-- BASE PL MONTHLY --
	, SUM(isnull(c.MONTHLY_BASE_PL,0)
			+ (ISNULL(MTD_BASE_TOTAL_REPO_INTEREST,0)) 
			+  (ISNULL(MTD_BASE_TOTAL_BOND_INTEREST,0) )
			+ isnull(MTD_DIVIDEND_INCOME_LOSS, 0)  ) MTD_BASE_PL
			
	, SUM(ISNULL( MTD_BASE_REALIZED_PL,0)) MTD_BASE_REALIZED_PL
	, SUM(ISNULL( MTD_BASE_UNREALIZED_PL,0)) MTD_BASE_UNREALIZED_PL	
	
	, SUM(((ISNULL( MTD_ISSUE_REALIZED_PL,0) + ISNULL( MTD_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) + isnull(MTD_BASE_COMMISSIONS,0)+ isnull(MTD_BASE_SEC_FEES,0)) * (case when SECURITY_ASSET_CLASS = 'Currency' then 0 else 1 end)) MTD_PRICE_PL
	, SUM ( ISNULL(MTD_BASE_TOTAL_BOND_INTEREST,0) + ISNULL(MTD_BASE_TOTAL_REPO_INTEREST,0) + isnull(MTD_DIVIDEND_INCOME_LOSS, 0) ) [MTD_ACCRUALS]
	
	, SUM( case when SECURITY_ASSET_NAME = 'Cross Rate' then  (ISNULL( MTD_BASE_REALIZED_PL,0) + ISNULL( MTD_BASE_UNREALIZED_PL,0))
				when SECURITY_ASSET_NAME = 'ACCOUNTING CASH' then (ISNULL( MTD_BASE_REALIZED_PL,0) + ISNULL( MTD_BASE_UNREALIZED_PL,0))
			else isnull(c.MONTHLY_BASE_PL,0) - (ISNULL( MTD_ISSUE_REALIZED_PL,0) + ISNULL( MTD_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) -( isnull(MTD_BASE_COMMISSIONS,0)+ isnull(MTD_BASE_SEC_FEES,0) ) end ) MTD_FX_PL

	-- ISSUE PL YEARLY--
	, SUM( isnull(YTD_ISSUE_UNREALIZED_PL,0) + isnull(YTD_ISSUE_REALIZED_PL,0) 
		+(ISNULL( YTD_ISSUE_TOTAL_BOND_INTEREST,0)) 
		+(ISNULL( YTD_ISSUE_TOTAL_REPO_INTEREST,0))
		+ ISNULL(ISSUE_YTD_DIVIDEND_INCOME_LOSS,0) ) YTD_ISSUE_PL

	-- BASE PL YEARLY --
	, SUM(isnull(c.YEARLY_BASE_PL,0)
			+ (ISNULL(YTD_BASE_TOTAL_REPO_INTEREST,0)) 
			+  (ISNULL( YTD_BASE_TOTAL_BOND_INTEREST,0) )
			+ isnull(YTD_DIVIDEND_INCOME_LOSS, 0)  ) YTD_BASE_PL
			
	, SUM(ISNULL( YTD_BASE_REALIZED_PL,0)) YTD_BASE_REALIZED_PL
	, SUM(ISNULL( YTD_BASE_UNREALIZED_PL,0)) YTD_BASE_UNREALIZED_PL	
	
	, SUM(((ISNULL( YTD_ISSUE_REALIZED_PL,0) + ISNULL( YTD_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) + isnull(YTD_BASE_COMMISSIONS,0)+ isnull(YTD_BASE_SEC_FEES,0)) * (case when SECURITY_ASSET_CLASS = 'Currency' then 0 else 1 end)) YTD_PRICE_PL
	, SUM ( ISNULL( YTD_BASE_TOTAL_BOND_INTEREST,0)+ ISNULL( YTD_BASE_TOTAL_REPO_INTEREST,0) + isnull(YTD_DIVIDEND_INCOME_LOSS, 0) ) [YTD_ACCRUALS]
	
	, SUM( case when SECURITY_ASSET_NAME = 'Cross Rate' then  (ISNULL( YTD_BASE_REALIZED_PL,0) + ISNULL( YTD_BASE_UNREALIZED_PL,0))
			when SECURITY_ASSET_NAME = 'ACCOUNTING CASH' then (ISNULL( YTD_BASE_REALIZED_PL,0) + ISNULL( YTD_BASE_UNREALIZED_PL,0))
		else isnull(c.YEARLY_BASE_PL,0) - (ISNULL( YTD_ISSUE_REALIZED_PL,0) + ISNULL( YTD_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) -( isnull(YTD_BASE_COMMISSIONS,0)+ isnull(YTD_BASE_SEC_FEES,0) ) end ) YTD_FX_PL
	
	, SUM( case when SECURITY_ASSET_NAME = 'Cross Rate' then  (ISNULL( MTD_BASE_REALIZED_PL,0))else 0 end) FX_FWD_REALIZED_MTD
	, SUM( case when SECURITY_ASSET_NAME = 'Cross Rate' then  (ISNULL( YTD_BASE_REALIZED_PL,0)) else 0 end ) FX_FWD_REALIZED_YTD
	
	, MAX(ISSUE_AVERAGE_UNIT_COST) ISSUE_AVERAGE_UNIT_COST
	, SUM(COST) COST
	, MAX([ISSUE_PRICE]) ISSUE_PRICE
	, MAX([EFFECTIVE_COUPON_RATE]) Coupon

FROM
FACT_POSITION_CITCO c 
	LEFT OUTER JOIN core.vSECURITY s                         ON s.NB_ID = c.SECURITY_EDM_ID 
	LEFT OUTER JOIN core.vSECURITY_CLASSIFICATION_ALADDIN_CURRENT sc ON s.NB_ID=sc.SECURITY_NB_ID
    LEFT OUTER JOIN core.vISSUER i                           ON i.NB_ID = s.ISSUER_NB_ID
WHERE 
c.POSITION_SET_ID = (SELECT cc.POSITION_SET_ID FROM DIM_POSITION_SET_CURRENT cc WHERE cc.PROVIDER = 'Private' AND cc.SOURCE = 'Citco' AND cc.AS_OF_DATE='{}')

GROUP BY
c.SECURITY_EDM_ID
, c.SECURITY_ASSET_NAME	
, c.SECURITY_DESCRIPTION	
, c.SECURITY_ASSET_CLASS	
, c.CITCO_SECURITY_ID
, c.LONG_SHORT
, c.ISSUE_CURRENCY
, c.ISIN
, C.BLOOMBERG_ID 
, s.ALADDIN_SECURITY_ID
, s.UNDERLYING_ID
, s.NB_SECURITY_TYPE_1
, i.ULTIMATE_PARENT_NAME
, i.ULTIMATE_PARENT_NB_ID
, s.ASSET_BENCHMARK
, sc.BARCLAYS_LVL_3_5
	
ORDER BY Ultimate_Issuer_Name, c.SECURITY_ASSET_CLASS, c.SECURITY_ASSET_NAME
'''

str_sql_citco_pl = '''
SELECT
	'{}' AS AS_OF_DATE
	, SECURITY_ASSET_NAME
	, SECURITY_DESCRIPTION
	, SECURITY_ASSET_CLASS
	, c.SECURITY_EDM_ID
	, c.CITCO_SECURITY_ID 
	, c.ISIN
	, C.BLOOMBERG_ID
	, s.ALADDIN_SECURITY_ID
	, sc.BARCLAYS_LVL_3_5 INDUSTRY
	, c.ISSUE_CURRENCY
	, c.LONG_SHORT
	, MAX(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) FX_RATE
	, sum(c.CURRENT_FACE ) CURRENT_FACE
	
	, case when s.UNDERLYING_ID IS NULL AND s.NB_SECURITY_TYPE_1 <> 'TRS' then 
			UPPER(s.ISSUER_ULTIMATE_PARENT_NAME)
			
			when s.UNDERLYING_ID Is not null then  (
				SELECT UPPER(iu.NAME) 
					FROM   vDIM_SECURITY_ALL u 
						LEFT OUTER JOIN DIM_ISSUER i  ON s.UNDERLYING_ID = u.EDM_ID AND i.EDM_ID = u.ISSUER_EDM_ID
						INNER JOIN      DIM_ISSUER iu ON i.ULTIMATE_PARENT_EDM_ID = iu.EDM_ID) 
						
				when  NB_SECURITY_TYPE_1 = 'TRS' then (
					SELECT UPPER(iu2.NAME) 
						FROM vDIM_SECURITY_ALL u2 
							LEFT OUTER JOIN DIM_ISSUER i2  ON s.ASSET_BENCHMARK = u2.ALADDIN_SECURITY_ID and i2.EDM_ID = u2.ISSUER_EDM_ID
							INNER JOIN      DIM_ISSUER iu2 ON i2.ULTIMATE_PARENT_EDM_ID = iu2.EDM_ID)
							
	  end Ultimate_Issuer_Name
	  
	, case when s.UNDERLYING_ID IS NULL AND s.NB_SECURITY_TYPE_1 <> 'TRS' then 
			s.ISSUER_ULTIMATE_PARENT_EDM_ID
			
			when s.UNDERLYING_ID Is not null then  (
				SELECT iu.EDM_ID
					FROM   vDIM_SECURITY_ALL u 
						LEFT OUTER JOIN DIM_ISSUER i  ON s.UNDERLYING_ID = u.EDM_ID AND i.EDM_ID = u.ISSUER_EDM_ID
						INNER JOIN      DIM_ISSUER iu ON i.ULTIMATE_PARENT_EDM_ID = iu.EDM_ID) 
						
				when  NB_SECURITY_TYPE_1 = 'TRS' then (
					SELECT iu2.EDM_ID
						FROM vDIM_SECURITY_ALL u2 
							LEFT OUTER JOIN DIM_ISSUER i2  ON s.ASSET_BENCHMARK = u2.ALADDIN_SECURITY_ID and i2.EDM_ID = u2.ISSUER_EDM_ID
							INNER JOIN      DIM_ISSUER iu2 ON i2.ULTIMATE_PARENT_EDM_ID = iu2.EDM_ID)
							
	  end Ultimate_Issuer_ID

	--BASE NAV AND MARKET VALUE --
    , case when SECURITY_ASSET_NAME in ('ACCOUNTING CASH') then sum(isnull(c.MARKET_VALUE,0)) else sum(isnull(c.BASE_NAV_CONTRIBUTION,0)) end  BASE_NAV_CONTRIBUTION
	, sum(case when SECURITY_ASSET_NAME in ('ACCOUNTING CASH') and isnull(c.MARKET_VALUE,0) <0 then 0 else isnull(c.MARKET_VALUE,0)+isnull(c.BASE_END_LOAN_AMOUNT,0)  end) MARKET_VALUE
	, sum(case when SECURITY_ASSET_NAME in ('ACCOUNTING CASH') and isnull(c.MARKET_VALUE,0) <0 then 0 else isnull(c.MARKET_VALUE,0) + isnull(BASE_BOND_ACCRUED_INTEREST,0) end) TOTAL_MARKET_VALUE
	, SUM (case when SECURITY_ASSET_NAME in ('ACCOUNTING CASH') then 
			(case when isnull(c.MARKET_VALUE,0) >0 then 0 else isnull(c.MARKET_VALUE,0)  END) 
		else isnull(c.BASE_END_LOAN_AMOUNT,0)+ ISNULL( BASE_REPO_ACCRUED_INTEREST,0) end) TOTAL_LOAN_AMOUNT
	
	--ISSUE NAV AND MARKET VALUE--
	, sum(case when SECURITY_ASSET_NAME in ('ACCOUNTING CASH') then isnull(c.MARKET_VALUE,0)/(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) else isnull(c.ISSUE_NAV_CONTRIBUTION,0) 
				- case when c.ISSUE_CURRENCY = 'USD' and LONG_SHORT not in ('Repo', 'Rever', 'Reverse Repo') then ISNULL(ISSUE_TRADE_ACQUISITION_INTEREST,0)
				       when LONG_SHORT in ('Short') then ISNULL(ISSUE_TRADE_ACQUISITION_INTEREST,0)  else 0 end															-- compensate for error in file
		  end)  ISSUE_NAV_CONTRIBUTION
		  
	, sum(case when LONG_SHORT in ('Repo', 'Rever', 'Reverse Repo') then isnull(c.ISSUE_END_LOAN_AMOUNT,0) else isnull(c.ISSUE_MARKET_VALUE,0) end) ISSUE_MARKET_VALUE
	, sum(case when LONG_SHORT in ('Repo', 'Rever', 'Reverse Repo') then isnull(c.ISSUE_END_LOAN_AMOUNT,0)+ISNULL( ISSUE_REPO_ACCRUED_INTEREST,0) else isnull(c.ISSUE_MARKET_VALUE,0)+ isnull(ISSUE_BOND_ACCRUED_INTEREST,0) end )  ISSUE_TOTAL_MARKET_VALUE
	
	-- ISSUE PL DAILY --
	, SUM( isnull(REPORT_DATE_ISSUE_UNREALIZED_PL,0) + isnull(REPORT_DATE_ISSUE_REALIZED_PL,0) 
		+(ISNULL(ISSUE_TOTAL_BOND_INTEREST,0))
		+(ISNULL(ISSUE_TOTAL_REPO_INTEREST,0))*0 ) DAILY_ISSUE_PL

	-- BASE PL DAILY --
	, SUM(isnull(c.DAILY_BASE_PL,0)  
			+ (ISNULL(BASE_TOTAL_REPO_INTEREST,0)) 
			+  (ISNULL(BASE_TOTAL_BOND_INTEREST,0) ) 
			+ isnull(DIVIDEND_INCOME_LOSS, 0) ) DAILY_BASE_PL
			
	, SUM(ISNULL( REPORT_DATE_BASE_REALIZED_PL,0)) DAILY_BASE_REALIZED_PL
	, SUM(ISNULL( REPORT_DATE_BASE_UNREALIZED_PL,0)) DAILY_BASE_UNREALIZED_PL	
	
	, SUM(((ISNULL( REPORT_DATE_ISSUE_REALIZED_PL,0) + ISNULL( REPORT_DATE_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) + isnull(REPORT_DATE_BASE_COMMISSIONS,0)+ isnull(REPORT_DATE_BASE_SEC_FEES,0)) * (case when SECURITY_ASSET_CLASS = 'Currency' then 0 else 1 end)) DAILY_PRICE_PL
	, SUM ( ISNULL( BASE_TOTAL_BOND_INTEREST,0)+ ISNULL(BASE_TOTAL_REPO_INTEREST,0) + isnull(DIVIDEND_INCOME_LOSS, 0) ) [DAILY_ACCRUALS]
	, SUM( case when SECURITY_ASSET_NAME = 'Cross Rate' then (ISNULL( REPORT_DATE_ISSUE_REALIZED_PL,0) + ISNULL( REPORT_DATE_ISSUE_UNREALIZED_PL,0)) else 
			 isnull(c.DAILY_BASE_PL,0) - (ISNULL( REPORT_DATE_ISSUE_REALIZED_PL,0) + ISNULL( REPORT_DATE_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) -( isnull(REPORT_DATE_BASE_COMMISSIONS,0)+ isnull(REPORT_DATE_BASE_SEC_FEES,0) ) end ) DAILY_FX_PL
	
	-- ISSUE PL MONTHLY--
	, SUM( isnull(MTD_ISSUE_UNREALIZED_PL,0) + isnull(MTD_ISSUE_REALIZED_PL,0) 
		+(ISNULL(MTD_ISSUE_TOTAL_BOND_INTEREST,0)) 
		+(ISNULL(MTD_ISSUE_TOTAL_REPO_INTEREST,0)) ) MTD_ISSUE_PL

	-- BASE PL MONTHLY --
	, SUM(isnull(c.MONTHLY_BASE_PL,0)
			+ (ISNULL(MTD_BASE_TOTAL_REPO_INTEREST,0)) 
			+  (ISNULL(MTD_BASE_TOTAL_BOND_INTEREST,0) )
			+ isnull(MTD_DIVIDEND_INCOME_LOSS, 0)  ) MTD_BASE_PL
			
	, SUM(ISNULL( MTD_BASE_REALIZED_PL,0)) MTD_BASE_REALIZED_PL
	, SUM(ISNULL( MTD_BASE_UNREALIZED_PL,0)) MTD_BASE_UNREALIZED_PL	
	
	, SUM(((ISNULL( MTD_ISSUE_REALIZED_PL,0) + ISNULL( MTD_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) + isnull(MTD_BASE_COMMISSIONS,0)+ isnull(MTD_BASE_SEC_FEES,0)) * (case when SECURITY_ASSET_CLASS = 'Currency' then 0 else 1 end)) MTD_PRICE_PL
	, SUM ( ISNULL(MTD_BASE_TOTAL_BOND_INTEREST,0) + ISNULL(MTD_BASE_TOTAL_REPO_INTEREST,0) + isnull(MTD_DIVIDEND_INCOME_LOSS, 0) ) [MTD_ACCRUALS]
	
	, SUM( case when SECURITY_ASSET_NAME = 'Cross Rate' then  (ISNULL( MTD_BASE_REALIZED_PL,0) + ISNULL( MTD_BASE_UNREALIZED_PL,0))
				when SECURITY_ASSET_NAME = 'ACCOUNTING CASH' then (ISNULL( MTD_BASE_REALIZED_PL,0) + ISNULL( MTD_BASE_UNREALIZED_PL,0))
			else isnull(c.MONTHLY_BASE_PL,0) - (ISNULL( MTD_ISSUE_REALIZED_PL,0) + ISNULL( MTD_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) -( isnull(MTD_BASE_COMMISSIONS,0)+ isnull(MTD_BASE_SEC_FEES,0) ) end ) MTD_FX_PL

	-- ISSUE PL YEARLY--
	, SUM( isnull(YTD_ISSUE_UNREALIZED_PL,0) + isnull(YTD_ISSUE_REALIZED_PL,0) 
		+(ISNULL( YTD_ISSUE_TOTAL_BOND_INTEREST,0)) 
		+(ISNULL( YTD_ISSUE_TOTAL_REPO_INTEREST,0))
		+ ISNULL(ISSUE_YTD_DIVIDEND_INCOME_LOSS,0) ) YTD_ISSUE_PL

	-- BASE PL YEARLY --
	, SUM(isnull(c.YEARLY_BASE_PL,0)
			+ (ISNULL(YTD_BASE_TOTAL_REPO_INTEREST,0)) 
			+  (ISNULL( YTD_BASE_TOTAL_BOND_INTEREST,0) )
			+ isnull(YTD_DIVIDEND_INCOME_LOSS, 0)  ) YTD_BASE_PL
			
	, SUM(ISNULL( YTD_BASE_REALIZED_PL,0)) YTD_BASE_REALIZED_PL
	, SUM(ISNULL( YTD_BASE_UNREALIZED_PL,0)) YTD_BASE_UNREALIZED_PL	
	
	, SUM(((ISNULL( YTD_ISSUE_REALIZED_PL,0) + ISNULL( YTD_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) + isnull(YTD_BASE_COMMISSIONS,0)+ isnull(YTD_BASE_SEC_FEES,0)) * (case when SECURITY_ASSET_CLASS = 'Currency' then 0 else 1 end)) YTD_PRICE_PL
	, SUM ( ISNULL( YTD_BASE_TOTAL_BOND_INTEREST,0)+ ISNULL( YTD_BASE_TOTAL_REPO_INTEREST,0) + isnull(YTD_DIVIDEND_INCOME_LOSS, 0) ) [YTD_ACCRUALS]
	
	, SUM( case when SECURITY_ASSET_NAME = 'Cross Rate' then  (ISNULL( YTD_BASE_REALIZED_PL,0) + ISNULL( YTD_BASE_UNREALIZED_PL,0))
			when SECURITY_ASSET_NAME = 'ACCOUNTING CASH' then (ISNULL( YTD_BASE_REALIZED_PL,0) + ISNULL( YTD_BASE_UNREALIZED_PL,0))
		else isnull(c.YEARLY_BASE_PL,0) - (ISNULL( YTD_ISSUE_REALIZED_PL,0) + ISNULL( YTD_ISSUE_UNREALIZED_PL,0))*(CASE WHEN c.ISSUE_CURRENCY IN ('GBP', 'AUD','EUR', 'NZD') THEN FX_RATE ELSE 1/FX_RATE END) -( isnull(YTD_BASE_COMMISSIONS,0)+ isnull(YTD_BASE_SEC_FEES,0) ) end ) YTD_FX_PL
	
	, SUM( case when SECURITY_ASSET_NAME = 'Cross Rate' then  (ISNULL( MTD_BASE_REALIZED_PL,0))else 0 end) FX_FWD_REALIZED_MTD
	, SUM( case when SECURITY_ASSET_NAME = 'Cross Rate' then  (ISNULL( YTD_BASE_REALIZED_PL,0)) else 0 end ) FX_FWD_REALIZED_YTD
	
	
	,MAX(ISSUE_AVERAGE_UNIT_COST) ISSUE_AVERAGE_UNIT_COST
	,SUM(COST) COST
	,MAX([ISSUE_PRICE]) ISSUE_PRICE
	,MAX([EFFECTIVE_COUPON_RATE]) Coupon
      ,MAX([TOTAL_FUND_CAPITAL]) AUM
FROM
FACT_POSITION_CITCO c 
	LEFT OUTER JOIN VDIM_SECURITY_ALL s                    ON s.EDM_ID = c.SECURITY_EDM_ID 
	LEFT OUTER JOIN DIM_SECURITY_CLASSIFICATION_ALADDIN sc ON (s.EDM_ID=sc.SECURITY_EDM_ID)
	
WHERE 
c.POSITION_SET_ID = (SELECT cc.POSITION_SET_ID FROM DIM_POSITION_SET_CURRENT cc WHERE cc.PROVIDER = 'Private' AND cc.SOURCE = 'Citco' AND cc.AS_OF_DATE='{}')
AND (sc.RECORD_INCEPTION  <=  '{}' or sc.RECORD_INCEPTION is null) AND (sc.RECORD_EXPIRATION  >=  '{}' or sc.RECORD_EXPIRATION is null)
--AND c.LONG_SHORT IN ('Long', 'Short')
GROUP BY

c.SECURITY_EDM_ID
, SECURITY_ASSET_NAME	
, SECURITY_DESCRIPTION	
, SECURITY_ASSET_CLASS	
, c.CITCO_SECURITY_ID
, c.LONG_SHORT
, c.ISSUE_CURRENCY
, c.ISIN
, C.BLOOMBERG_ID 
, s.ALADDIN_SECURITY_ID
, s.UNDERLYING_ID
, s.NB_SECURITY_TYPE_1
, s.ISSUER_ULTIMATE_PARENT_NAME
, s.ISSUER_ULTIMATE_PARENT_EDM_ID
, s.ASSET_BENCHMARK
, sc.BARCLAYS_LVL_3_5
	
ORDER BY Ultimate_Issuer_Name, SECURITY_ASSET_CLASS, SECURITY_ASSET_NAME

'''


str_sql_dates = '''
        SELECT cc.as_of_date FROM DIM_POSITION_SET_CURRENT cc 
        WHERE cc.PROVIDER = 'Private' 
            AND cc.SOURCE = 'Citco' 
        ORDER BY cc.AS_OF_DATE DESC
    '''
    
if __name__ == '__main__':

        
    engine = pyodbc.connect(Trusted_Connection='yes',
                            driver='{SQL Server}',
                            server='PIPWSQL023B\PFRM802',
                            database='DM_Operations')

    df_dates = pandas.read_sql_query(str_sql_dates, engine)
        
    latest_date = df_dates.as_of_date[0].date()
    
    arg_parser = argparse.ArgumentParser(description='Run OSP Stop Loss report')
    
    def parse_date(d):
        return dateutil.parser.parse(d).date()
        
    arg_parser.add_argument('--report_date', default=latest_date, help='Report P&L as of date', type=parse_date)
    arg_parser.add_argument('--loglevel', default="INFO", choices={"WARN","INFO","DEBUG"}, help='Logging level verbosity')
    arg_parser.add_argument('--outdir', default="//nb/corp/groups/NY/Institutional/SPA/Dmitry/osp_stop-loss", help='Report Output Directory')
    
    args = arg_parser.parse_args()
    
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: {}'.format(args.loglevel))

    logging.basicConfig(filename="stop_loss_osp_{}.log".format(args.report_date if latest_date>args.report_date else latest_date),
                        filemode="w",
                        level=numeric_level,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        format="%(asctime)s:%(levelname)s:%(message)s")

    if latest_date < args.report_date:
        logging.warn ("Data for {} is not yet available. Using latest available data as of {}".format(args.report_date, latest_date))
        args.report_date = latest_date
        
    #    
    #date_list = [p.date() for p in pandas.date_range(end=args.report_date, periods=12, freq='M')]
    #date_list = [datetime.date(2016,1,29) if p==datetime.date(2016,1,31) else p for p in date_list]

    #
    # generate list of month-end dates that fall on weekdays only (CITCO does not generate month-end data for weekends)
    #
    date_list = list(dt_rr.rrule(dt_rr.MONTHLY, 
                                 bysetpos=-1,
                                 byweekday=[dt_rr.MO, dt_rr.TU, dt_rr.WE, dt_rr.TH, dt_rr.FR],
                                 dtstart=args.report_date - datetime.timedelta(days=365), 
                                 until=latest_date, 
                                 count=12))
    
    if args.report_date not in date_list:
        date_list.append(args.report_date)
    
    logging.info("Report dates {}".format(date_list))
    ####
    ## read in some config data from excel template file
    ####
    with pandas.ExcelFile("{0}/OSP_SIZE_STOPLOSS_PARAMETERS.xlsx".format(args.outdir)) as xls:
        
        df_basket_dict = pandas.read_excel(io=xls, sheetname="BASKETS", parse_cols="B:E")
        df_rec_size    = pandas.read_excel(io=xls, sheetname="Position Sizing", parse_cols="A,E")
        osp_navs       = pandas.read_excel(io=xls, sheetname="AUM", index_col=0)
   
    dataframe_all_list= []
    
    for i, month in enumerate(date_list):       
       
        logging.debug(str_sql_dates)
        current_nav = osp_navs.loc[datetime.date(month.year, month.month, 1)].AUM

        data_frame = pandas.read_sql_query(str_sql_citco_pl_coresecurity.format(month, month), engine, parse_dates=['AS_OF_DATE'])
        
        logging.info("Month end {} of {} date {}, number of records {}, NAV={}".format(i+1, len(date_list), month, len(data_frame), current_nav))
    
        data_frame['MTD_RET']=data_frame['MTD_BASE_PL'] / current_nav
        
        data_frame['EXP_PCT_NAV'] = data_frame['TOTAL_MARKET_VALUE'] / current_nav
        
        swaps_index_vector = (data_frame['SECURITY_ASSET_NAME']=='Credit Default Swap') | (data_frame['SECURITY_ASSET_CLASS']=='Swap')
        
        # data_frame.loc[swaps_index_vector, 'EXP_PCT_NAV'] = data_frame[swaps_index_vector]['CURRENT_FACE'] / current_nav
        data_frame.loc[swaps_index_vector, 'EXP_PCT_NAV'] = data_frame[swaps_index_vector]['CURRENT_FACE_USD'] / current_nav

        dataframe_all_list.append(data_frame)


    dataframe_all_stack = pandas.concat(dataframe_all_list, ignore_index=True)
    
    dataframe_all_stack_nofx = dataframe_all_stack[(dataframe_all_stack['SECURITY_ASSET_CLASS']!='CURRENCY')&(dataframe_all_stack['SECURITY_ASSET_CLASS']!='Currency')&(dataframe_all_stack['LONG_SHORT']!="Repo")&(dataframe_all_stack['LONG_SHORT']!="Reverse Repo")]

    #fix up some derivatives with dummy issuer codes
    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Bond")  &(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Credit Default Swap")&(dataframe_all_stack_nofx["Ultimate_Issuer_Name"].isnull()),      "Ultimate_Issuer_Name"] = "Credit Index Swap"
    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Bond")  &(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Credit Default Swap")&(dataframe_all_stack_nofx["Ultimate_Issuer_Name"]=="CDS REF OB"), "Ultimate_Issuer_Name"] = "Credit Index Swap"

    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Future")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Commodity Future"),        "Ultimate_Issuer_Name"] = "Commodity"
    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Option")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Commodity Future Option"), "Ultimate_Issuer_Name"] = "Commodity"

    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Future")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Index Future"),        "Ultimate_Issuer_Name"] = "Broad Market Index"
    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Option")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Index Option"),        "Ultimate_Issuer_Name"] = "Broad Market Index"
    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Option")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Index Future Option"), "Ultimate_Issuer_Name"] = "Broad Market Index"
    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Swap")  &(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Index Swap"),          "Ultimate_Issuer_Name"] = "Broad Market Index"
       
    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Future")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="IR Future"),                     "Ultimate_Issuer_Name"] = "IR Future"
    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Future")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="IR Future - Short Term"),        "Ultimate_Issuer_Name"] = "IR Future"
    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Option")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="IR Future Option"),              "Ultimate_Issuer_Name"] = "IR Future"
    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Option")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="IR Future Option - Short Term"), "Ultimate_Issuer_Name"] = "IR Future"
    
    no_issr = dataframe_all_stack_nofx[dataframe_all_stack_nofx['Ultimate_Issuer_Name'].isnull()][['SECURITY_ASSET_NAME','SECURITY_DESCRIPTION','SECURITY_ASSET_CLASS',
                                                                                                   'SECURITY_EDM_ID','CITCO_SECURITY_ID','ALADDIN_SECURITY_ID']].groupby(['SECURITY_ASSET_NAME','SECURITY_DESCRIPTION','SECURITY_ASSET_CLASS','SECURITY_EDM_ID','CITCO_SECURITY_ID','ALADDIN_SECURITY_ID'], as_index=False).aggregate(max)


    current_df = dataframe_all_stack_nofx[dataframe_all_stack_nofx['AS_OF_DATE']==date_list[-1]]
       
    logging.debug("Issuer P&L for date {}".format(date_list[-1]))
    result_last = current_df[['Ultimate_Issuer_Name', 'CURRENT_FACE_USD','MTD_RET','MTD_BASE_PL']].groupby('Ultimate_Issuer_Name', as_index=False).aggregate(sum)
    #result_last = current_df[(current_df['MTD_BASE_PL']!=0)|(current_df['CURRENT_FACE_USD']!=0)][['Ultimate_Issuer_Name', 'CURRENT_FACE_USD','MTD_RET','MTD_BASE_PL']].groupby('Ultimate_Issuer_Name', as_index=False).aggregate(sum)
    df1 = result_last.sort_values(by='MTD_RET',ascending=True).fillna(0).rename(columns={"MTD_RET":"RETURN", "MTD_BASE_PL":"BASE_PL"})
    logging.debug(df1)
    
    year_end_date = datetime.date(args.report_date.year-1,12,31)
    logging.debug("Issuer P&L for date {}".format(year_end_date))
    #result_ytd = dataframe_all_stack_nofx[(dataframe_all_stack_nofx['AS_OF_DATE']>year_end_date)&((dataframe_all_stack_nofx['MTD_BASE_PL']!=0) | (dataframe_all_stack_nofx['CURRENT_FACE_USD']!=0))][['AS_OF_DATE','Ultimate_Issuer_Name','MTD_RET','MTD_BASE_PL']].groupby('Ultimate_Issuer_Name', as_index=False).aggregate(sum)
    result_ytd = dataframe_all_stack_nofx[(dataframe_all_stack_nofx['AS_OF_DATE']>year_end_date)][['AS_OF_DATE','Ultimate_Issuer_Name','MTD_RET','MTD_BASE_PL']].groupby('Ultimate_Issuer_Name', as_index=False).aggregate(sum)
    df2 = pandas.merge( result_last[['Ultimate_Issuer_Name', 'CURRENT_FACE_USD']], result_ytd, how='outer', on='Ultimate_Issuer_Name').sort_values(by='MTD_RET', ascending=True).fillna(0).rename(columns={"MTD_RET":"RETURN", "MTD_BASE_PL":"BASE_PL"})

    quarter_end_date=pandas.date_range(end=latest_date, freq='Q', periods=1)[0].date()
    logging.debug("Issuer P&L for date {}".format(quarter_end_date))
    #result_qtd = dataframe_all_stack_nofx[(dataframe_all_stack_nofx['AS_OF_DATE']>quarter_end_date)&((dataframe_all_stack_nofx['MTD_BASE_PL']!=0) | (dataframe_all_stack_nofx['CURRENT_FACE_USD']!=0))][['AS_OF_DATE','Ultimate_Issuer_Name','MTD_RET','MTD_BASE_PL']].groupby('Ultimate_Issuer_Name', as_index=False).aggregate(sum)
    result_qtd = dataframe_all_stack_nofx[(dataframe_all_stack_nofx['AS_OF_DATE']>quarter_end_date)][['AS_OF_DATE','Ultimate_Issuer_Name','MTD_RET','MTD_BASE_PL']].groupby('Ultimate_Issuer_Name', as_index=False).aggregate(sum)
    df4 = pandas.merge( result_last[['Ultimate_Issuer_Name', 'CURRENT_FACE_USD']], result_qtd, how='outer', on='Ultimate_Issuer_Name').sort_values(by='MTD_RET', ascending=True).fillna(0).rename(columns={"MTD_RET":"RETURN", "MTD_BASE_PL":"BASE_PL"})

    trailing_12M_date = datetime.date(2016,2,29) if args.report_date<datetime.date(2017,2,28) else date_list[0]
    logging.debug("Issuer P&L for date {}".format(trailing_12M_date))
    #result_12m = dataframe_all_stack_nofx[(dataframe_all_stack_nofx['AS_OF_DATE']>trailing_12M_date)&((dataframe_all_stack_nofx['MTD_BASE_PL']!=0) | (dataframe_all_stack_nofx['CURRENT_FACE_USD']!=0))][['AS_OF_DATE','Ultimate_Issuer_Name','MTD_RET','MTD_BASE_PL']].groupby('Ultimate_Issuer_Name', as_index=False).aggregate(sum)
    result_12m = dataframe_all_stack_nofx[(dataframe_all_stack_nofx['AS_OF_DATE']>trailing_12M_date)][['AS_OF_DATE','Ultimate_Issuer_Name','MTD_RET','MTD_BASE_PL']].groupby('Ultimate_Issuer_Name', as_index=False).aggregate(sum)
    df3 = pandas.merge( result_last[['Ultimate_Issuer_Name', 'CURRENT_FACE_USD']], result_12m, how='outer', on='Ultimate_Issuer_Name').sort_values(by='MTD_RET', ascending=True).fillna(0).rename(columns={"MTD_RET":"RETURN", "MTD_BASE_PL":"BASE_PL"})
    
    # check for securities outside of the baskets that exceed the stop loss threshold
    # violations = df3[df3['RETURN']<=STOP_LOSS_LEVEL]
    
    df_non_basket = pandas.merge(df_basket_dict.groupby("ISSUER_NAME", as_index=False).aggregate(sum), 
                                 df3, 
                                 left_on="ISSUER_NAME", 
                                 right_on="Ultimate_Issuer_Name", 
                                 how='right')
    
    violations    = df_non_basket[(df_non_basket.ISSUER_NAME.isnull()) & (df_non_basket.RETURN<=STOP_LOSS_LEVEL)]
    
    #################################
    ## Stop-Loss report
    #################################
    with pandas.ExcelWriter("{}/OSP stop loss {}.xlsx".format(args.outdir, date_list[-1].strftime("%Y-%m-%d")), date_format='YYYY-MM-DD', datetime_format='YYYY-MM-DD') as writer:
        
        format_par = writer.book.add_format({'num_format':'#,##0_);[Red](#,##0)'})
        format_pct = writer.book.add_format({'num_format':'0.00%'})
        format_usd = writer.book.add_format({'num_format':'$ #,##0_);[Red]($ #,##0)'})
        
        red_bg     = writer.book.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        orange_bg  = writer.book.add_format({'bg_color': '#FFC000', 'font_color': '#982D02'})
        green_bg   = writer.book.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})

        for basket in sorted(set(df_basket_dict['BASKET'])):
            
            df_basket = pandas.merge(df_basket_dict[df_basket_dict['BASKET']==basket], df3, how='inner', left_on='ISSUER_NAME', right_on='Ultimate_Issuer_Name')

            df_basket.CURRENT_FACE_USD *= df_basket.WEIGHT
            df_basket.RETURN       *= df_basket.WEIGHT
            df_basket.BASE_PL      *= df_basket.WEIGHT
            
            basket_total_line      = pandas.DataFrame(data=df_basket[['RETURN','BASE_PL']].sum()).T.reindex(columns=df_basket.columns)
            df_basket_with_total   = df_basket.append(basket_total_line, ignore_index=True)

            if basket_total_line.RETURN[0] <= STOP_LOSS_LEVEL:
                violations.append(df_basket_with_total)

            df_basket_with_total.to_excel(writer, index=False, sheet_name=basket, columns=['Ultimate_Issuer_Name', 'CURRENT_FACE_USD', 'RETURN', 'BASE_PL'])
            

                
        violations.to_excel(writer, index=False, sheet_name='VIOLATIONS', columns=['Ultimate_Issuer_Name', 'CURRENT_FACE_USD', 'RETURN', 'BASE_PL'])        
        logging.warn(violations)

        df3.to_excel(writer, index=False, sheet_name='12M {} to {}'.format(trailing_12M_date.strftime("%Y-%m-%d"), args.report_date))
        df1.to_excel(writer, index=False, sheet_name='MTD {} to {}'.format(date_list[-2].strftime("%Y-%m-%d")    , args.report_date))
        df4.to_excel(writer, index=False, sheet_name='QTD {} to {}'.format(quarter_end_date.strftime("%Y-%m-%d") , args.report_date))
        df2.to_excel(writer, index=False, sheet_name='YTD {} to {}'.format(year_end_date.strftime("%Y-%m-%d")    , args.report_date))

        no_issr.sort_values(by=['SECURITY_ASSET_CLASS','SECURITY_ASSET_NAME'], ascending=True).to_excel(writer, 
                                                                                                        index=False, 
                                                                                                        columns=['SECURITY_ASSET_CLASS','SECURITY_ASSET_NAME','SECURITY_DESCRIPTION','SECURITY_EDM_ID','CITCO_SECURITY_ID', 'ALADDIN_SECURITY_ID','ISIN','ISSUE_CURRENCY','LONG_SHORT'],  
                                                                                                        sheet_name='Issuer Not Assigned')
        current_df.to_excel(writer, index=False, sheet_name='Current Positions')
        
        for worksheet in writer.book.worksheets():
            
            if worksheet.name=='Issuer Not Assigned':
                worksheet.set_column('A:A', width=22)
                worksheet.set_column('B:B', width=30)
                worksheet.set_column('C:C', width=55)
                worksheet.set_column('D:F', width=30) 
                worksheet.autofilter('A1:F1000')                                        
                
            else:
                worksheet.set_column('A:A', width=50)
                                         
                worksheet.set_column('B:B', width=15, cell_format=format_par)                                     
                worksheet.set_column('C:C', width=15, cell_format=format_pct)                                     
                worksheet.set_column('D:D', width=15, cell_format=format_usd)                                     
    
                # Write a conditional format over a range.
                worksheet.conditional_format('C2:C200', {'type': 'cell',
                                                         'criteria': '<=',
                                                         'value': STOP_LOSS_LEVEL,
                                                         'format': red_bg})
                                                             
                worksheet.autofilter('A1:D1000')       

    
    
    ##############################################
    #INDUSTRY REPORT
    ##############################################

    #Industry not available
#    dataframe_all_stack_nofx_1 = dataframe_all_stack_nofx
#    dataframe_all_stack_nofx = dataframe_all_stack
#
#    dataframe_all_stack_nofx.loc[dataframe_all_stack_nofx['INDUSTRY'].isnull(), "INDUSTRY"] = "Not Assigned"
#
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Bond")  &(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Credit Default Swap")&(dataframe_all_stack_nofx["Ultimate_Issuer_Name"].isnull()),      "INDUSTRY"] = "Credit Index Swap"
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Bond")  &(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Credit Default Swap")&(dataframe_all_stack_nofx["Ultimate_Issuer_Name"]=="CDS REF OB"), "INDUSTRY"] = "Credit Index Swap"
#
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Future")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Commodity Future"),        "INDUSTRY"] = "Commodity"
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Option")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Commodity Future Option"), "INDUSTRY"] = "Commodity"
#
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Future")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Index Future"),        "INDUSTRY"] = "Broad Market Index"
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Option")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Index Option"),        "INDUSTRY"] = "Broad Market Index"
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Option")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Index Future Option"), "INDUSTRY"] = "Broad Market Index"
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Swap")  &(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="Index Swap"),          "INDUSTRY"] = "Broad Market Index"
#       
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Future")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="IR Future"),                     "INDUSTRY"] = "IR Future"
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Future")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="IR Future - Short Term"),        "INDUSTRY"] = "IR Future"
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Option")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="IR Future Option"),              "INDUSTRY"] = "IR Future"
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx["SECURITY_ASSET_CLASS"]=="Option")&(dataframe_all_stack_nofx["SECURITY_ASSET_NAME"]=="IR Future Option - Short Term"), "INDUSTRY"] = "IR Future"
#    
#    dataframe_all_stack_nofx.loc[(dataframe_all_stack_nofx['LONG_SHORT']=='Repo') | (dataframe_all_stack_nofx['LONG_SHORT']=='Reverse Repo'), "INDUSTRY"] = "Repo Financing"
#
# 
#    df_ytd_industry  = dataframe_all_stack_nofx[dataframe_all_stack_nofx['AS_OF_DATE']>year_end_date   ][['AS_OF_DATE','INDUSTRY','MTD_RET']].groupby(['AS_OF_DATE','INDUSTRY'], as_index=False).aggregate(sum).pivot(index='INDUSTRY', columns='AS_OF_DATE', values='MTD_RET').fillna(0)
#    df_qtd_industry  = dataframe_all_stack_nofx[dataframe_all_stack_nofx['AS_OF_DATE']>quarter_end_date][['AS_OF_DATE','INDUSTRY','MTD_RET']].groupby(['AS_OF_DATE','INDUSTRY'], as_index=False).aggregate(sum).pivot(index='INDUSTRY', columns='AS_OF_DATE', values='MTD_RET').fillna(0)
#    
#    with pandas.ExcelWriter("{}/OSP industry returns {}.xlsx".format(args.outdir, date_list[-1].strftime("%Y-%m-%d")), date_format='YYYY-MM-DD', datetime_format='YYYY-MM-DD') as writer:
# 
#        format_pct = writer.book.add_format({'num_format':'0.00%'})
#        red_bg     = writer.book.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
#        dk_red_bg  = writer.book.add_format({'bg_color': '#FF3747', 'font_color': '#640005'})
#        green_bg   = writer.book.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
#        dk_green_bg= writer.book.add_format({'bg_color': '#00B050', 'font_color': '#005000'})
#
#        pandas.concat([df_ytd_industry,  df_qtd_industry.compound(axis=1).rename('QTD'),  df_ytd_industry.compound(axis=1).rename('YTD')], axis=1).to_excel(writer, index=True, sheet_name='Industry Return {}'.format(args.report_date))
#        
#        dataframe_all_stack_nofx[(dataframe_all_stack_nofx['INDUSTRY']=='Not Assigned')&(dataframe_all_stack_nofx['INDUSTRY']=='Not Assigned')].sort_values(by=['SECURITY_ASSET_CLASS','SECURITY_ASSET_NAME','SECURITY_DESCRIPTION', 'AS_OF_DATE'], ascending=True).to_excel(writer, index=False, sheet_name='Industry Not Assigned')
#        
#        dataframe_all_stack_nofx.sort_values(by=['AS_OF_DATE', 'SECURITY_ASSET_CLASS','SECURITY_ASSET_NAME','SECURITY_DESCRIPTION'], ascending=True).to_excel(writer, index=False, sheet_name='Positions')
#
#        for worksheet in writer.book.worksheets():
#            if worksheet.name=='Industry Not Assigned' or worksheet.name=='Positions':
#                worksheet.set_column('A:A', width=15)
#                worksheet.set_column('B:B', width=25)
#                worksheet.set_column('C:C', width=50)
#                worksheet.set_column('D:D', width=25)
#                worksheet.set_column('E:AY',width=10)
#                
#                worksheet.autofilter('A1:AY1000')       
#
#            else:
#                worksheet.set_column('A:A', width=30)
#                worksheet.set_column('B:Z', width=15, cell_format=format_pct)       
#                              
#                worksheet.conditional_format('B2:Z200', {'type': 'cell', 'criteria': '<=', 'value': 2*STOP_LOSS_LEVEL, 'format': dk_red_bg})
#                worksheet.conditional_format('B2:Z200', {'type': 'cell', 'criteria': '<=', 'value':   STOP_LOSS_LEVEL, 'format': red_bg})
#                worksheet.conditional_format('B2:Z200', {'type': 'cell', 'criteria': '>=', 'value':-2*STOP_LOSS_LEVEL, 'format': dk_green_bg})
#                worksheet.conditional_format('B2:Z200', {'type': 'cell', 'criteria': '>=', 'value':  -STOP_LOSS_LEVEL, 'format': green_bg})
#    
#    
    ##############################################
    # Position Sizing Report 
    ##############################################
#    result_current_position = current_df[((current_df["SECURITY_ASSET_CLASS"]=="Bond") | (current_df["SECURITY_ASSET_CLASS"]=="Equity") | (current_df["SECURITY_ASSET_NAME"]=="Total Return Swap"))  & (current_df['CURRENT_FACE_USD']!=0) & ~(current_df['CURRENT_FACE_USD'].isnull())]
#    
#    df_pos_size = pandas.merge(result_current_position[['Ultimate_Issuer_Name', 'CURRENT_FACE_USD', 'MARKET_VALUE', 'EXP_PCT_NAV']].groupby('Ultimate_Issuer_Name', as_index=False).aggregate(sum), df_rec_size,  how='left')   
#    
#    with pandas.ExcelWriter("{}/OSP position size {}.xlsx".format(args.outdir, date_list[-1].strftime("%Y-%m-%d")), date_format='YYYY-MM-DD', datetime_format='YYYY-MM-DD') as writer:
#    
#        format_pct = writer.book.add_format({'num_format':'0.00%'})   
#        format_usd = writer.book.add_format({'num_format':'$ #,##0_);[Red]($ #,##0)'})
#        format_par = writer.book.add_format({'num_format':'#,##0_);[Red](#,##0)'})
#        
#        red_bg     = writer.book.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
#        dk_red_bg  = writer.book.add_format({'bg_color': '#FF3747', 'font_color': '#640005'})
#        green_bg   = writer.book.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
#        dk_green_bg= writer.book.add_format({'bg_color': '#00B050', 'font_color': '#005000'})
#
#        df_pos_size.sort_values(by='Ultimate_Issuer_Name', ascending=True).to_excel(writer, index=False, columns=['Ultimate_Issuer_Name', 'CURRENT_FACE_USD', 'MARKET_VALUE', 'EXP_PCT_NAV','OSP Position Size'], sheet_name='Position Size {}'.format(args.report_date))
#        result_current_position.sort_values(by=["Ultimate_Issuer_Name","SECURITY_ASSET_CLASS", "SECURITY_ASSET_NAME", 'SECURITY_DESCRIPTION'], ascending=True).to_excel(writer, index=False, columns=["SECURITY_ASSET_CLASS", "SECURITY_ASSET_NAME", "Ultimate_Issuer_Name",'SECURITY_DESCRIPTION', 'CURRENT_FACE_USD', 'MARKET_VALUE', 'EXP_PCT_NAV','SECURITY_EDM_ID','CITCO_SECURITY_ID', 'ALADDIN_SECURITY_ID','ISIN','ISSUE_CURRENCY','LONG_SHORT'], sheet_name='details')
#        
#        for worksheet in writer.book.worksheets():
#            if worksheet.name=='details':
#                worksheet.set_column('A:B', width=25)
#                worksheet.set_column('C:D', width=50)
#                worksheet.set_column('E:E', width=15, cell_format=format_par)
#                worksheet.set_column('F:F', width=15, cell_format=format_usd)
#                worksheet.set_column('G:G', width=15, cell_format=format_pct)
#                worksheet.set_column('H:M', width=15)
#                worksheet.autofilter('A1:M1000')    
#            else:
#                worksheet.set_column('A:A', width=50)
#                worksheet.set_column('B:B', width=20, cell_format=format_par)
#                worksheet.set_column('C:C', width=20, cell_format=format_usd)
#                worksheet.set_column('D:E', width=15, cell_format=format_pct)
#                    
#                worksheet.conditional_format('D2:D{}'.format(len(df_pos_size)+1), {'type': 'formula', 'criteria': '=ABS(D2)>=ABS(E2)',        'format': dk_red_bg})
#                worksheet.conditional_format('D2:D{}'.format(len(df_pos_size)+1), {'type': 'formula', 'criteria': '=ABS(D2)+0.0025>=ABS(E2)', 'format': red_bg})
        