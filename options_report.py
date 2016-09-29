# -*- coding: utf-8 -*-
"""
Created on Tue Jul 26 12:03:21 2016

@author: drezniko
"""
import pyodbc
import argparse
import datetime
import logging
import pandas
import numpy
import re

import dateutil
import dateutil.rrule as du_rr
import dateutil.relativedelta as dr

import matplotlib.pyplot as plt
import matplotlib.finance as plt_fin
import matplotlib.dates as mdates
#import matplotlib.ticker as mtick
from matplotlib.backends.backend_pdf import PdfPages
#from mpl_toolkits.mplot3d import Axes3D

import vollib.black_scholes_merton.implied_volatility as bsm_vol
import vollib.black_scholes_merton.greeks.analytical as bsm_greeks

import bbg_api.bbg_api_wrap as bbg

p=re.compile(r'[\s\s]+')

bbg_mnemonic_list = ["PX_OPEN","PX_LAST",'PX_HIGH','PX_LOW']    

OWT_ALL_ACCOUNTS = [
'59880094', #NBG LLC GLOBAL OTM PUT WRITE
'77705590', #NBGAF GLOBAL STRANGLE
'77706017',  #GETTY TRUST ACCOUNT GLOBAL ATM PUT-WRITE
'77706018',  #U OF PITTSBURGH ACCOUNT SPX STRANGLE
'77706305', # EMPLOYEES RETIREMENT SYSTEM OF THE STATE OF HAWAII

'77706408', #UPMC BASIC RETIREMENT PLAN
'77706460', #NB MAC INCOME FUND OPTIONS STRATEGY SLEEVE
'77706498',  #US EQ PUT-WRITE FUND
'77706012','77706009', #COOK FOUNDATION SPX PUT-WRITE OTM, #COOK FOUNDATION RUSSEL STRANGLE, 
#'88816009', #W I COOK FOUNDATION INC COMBINED
'77706014','77706015','77706016',#DESERET MUT BEN GLOBAL ATM PUT-WRITE
'88816014', # DESERET MUTUAL BENEFIT ADMINISTRATORS COMBINED
]

TICKER_VOL_MAP = [('SPX','VIX'), ('SPY','VIX'),('RTY','RVX'),('RUT','RVX')]

TICKER_SUFFIX_MAP = dict( [('SPX', 'Index'),
                           ('RTY', 'Index'), 
                           ('RUY', 'Index'), 
                           ('RUT', 'Index'), 
                            ('SPY', 'Equity'), 
                            ('EEM', "Equity"), 
                            ('EFA', 'Equity'),
                            ('XSP', 'Index'),
                            ])

str_sql_pmr_allaccounts = '''
SELECT 
a.NAA_ACCOUNT_ID, a.ACCOUNT_TITLE, a.OPEN_DATE, a.MANAGER_CODE, a.MANAGER_NAME,a.IS_MANAGED_ACCOUNT,
a.CUSTODIAN_ACCT_NUMBER,a.CUSTODIAN_NAME, a.NAO_CLIENT_TYPE, 
a.CLIENT_TYPE_DESCRIPTION, a.CLASSIFICATION_DESCRIPTION, 
a.NAO_CLIENT_TYPE_CATEGORY,a.VEHICLE,a.TEAM_ROLL_UP, 
a.INVESTMENT_TEAM, a.TARGET_NAME, a.CLIENT_COVERAGE_AREA, a.NFS_ACCOUNT_ID 
FROM dbo.vDIM_ACCOUNT  a
WHERE  
--TEAM_ROLL_UP='QMAC'
INVESTMENT_TEAM='Options'
--MANAGER_CODE LIKE 'OWT%'
AND a.NAO_CLIENT_TYPE_CATEGORY NOT IN ('INDIV', 'TRUST')
ORDER BY a.ACCOUNT_TITLE
'''

str_sql_pmr_transaction = '''
SELECT 
t.TRANSACTION_NUMBER, t.TRANSACTION_SET_ID,t.AS_OF_DATE,t.ENTRY_DATE_CLOSE_PRICE, t.TRANSACTION_CODE, t.ABBREVIATED_TRANSACTION_DESCRIPTION,
t.SECURITY_TICKER_SYMBOL, t.SECURITY_DESCRIPTION_LINE_ONE, t.SECURITY_DESCRIPTION_LINE_TWO, t.SECURITY_DESCRIPTION_LINE_THREE,
t.QUANTITY, t.ORIGINAL_FACE, t.UNIT_COST, t.TOTAL_COST, t.ORIGINAL_COST, t.PRINCIPAL_AMOUNT, t.TRADE_DATE, t.SETTLEMENT_DATE, t.EFFECTIVE_DATE, t.ENTRY_DATE,
t.LINKED_TRANSACTION_NUMBER, t.REVERSING_TRANSACTION_NUMBER, t.REVERSING_TRANSACTION_TYPE, t.CURRENCY_ISO_CODE, t.TRAN_CODE
FROM dbo.vFACT_TRANSACTION_PMR t 
INNER JOIN dbo.DIM_ACCOUNT a ON a.EDM_ID=t.ACCOUNT_EDM_ID
INNER JOIN core.vSECURITY s ON s.NB_ID=t.SECURITY_EDM_ID
WHERE 
t.TRADE_DATE BETWEEN '{0}' AND '{1}' AND
a.NAA_ACCOUNT_ID='{2}'
ORDER BY t.TRADE_DATE

'''

str_sql_pmr_position = '''
SELECT
p.TRADE_DATE, p.AS_OF_DATE, p.UNITS, p.MARKET_VALUE, p.UNIT_COST, p.TOTAL_COST, p.ORIGINAL_COST, 
p.SECURITY_GAIN_LOSS, p.CURRENCY_GAIN_LOSS, p.ACCRUED_INTEREST,
p.TRADING_UNIT, p.SECURITY_NUMBER, p.MARKET_VALUE / p.UNITS / p.TRADING_UNIT AS PRICE,

a.ACCOUNT_TITLE,a.NAA_ACCOUNT_ID,a.MANAGER_CODE, a.MANAGER_NAME,

s.SECURITY_DESCRIPTION, s.DESCRIPTION, s.CALL_PUT, s.EXPIRATION_DATE, s.STRIKE_PRICE,
s.MATURITY_DATE, s.BB_GLOBAL_ID,  s.PRIMARY_SECURITY_ID_TYPE, s.PRIMARY_SECURITY_ID, s.PMR_SECURITY_ID,
s.TICKER, s.NB_SECURITY_GROUP,  s.NB_SECURITY_TYPE_1, s.NB_SECURITY_TYPE_2 ,

ss.UNDERLYING_SEC_NO, ss.DERIVATIVE_ID,ss.OPTION_TYPE, 

ssu.SECURITY_EDM_ID AS UNDERLYING_EDM_ID, ssu.DESCRIPTION AS UNDER_SEC_DESC, ssu.SEC_SYMBOL AS UNDER_TICKER, 
ssu.CUSIP AS UNDER_CUSIP,ssu.ISIN AS UNDER_ISIN, ssu.SEDOL AS UNDER_SEDOL, 
ssu.NB_ASSET_CLASS AS UNDER_NB_ASSET_CLASS, ssu.NB_SECURITY_GROUP AS UNDER_NB_SECURITY_GROUP, ssu.NB_SECURITY_TYPE_1 AS UNDER_NB_SECURITY_TYPE_1,

s1.SECURITY_DESCRIPTION AS UNDER_SECURITY_DESCRIPTION, s1.DESCRIPTION AS UNDER_DESCRIPTION, s1.BB_GLOBAL_ID AS UNDER_BB_GLOBAL_ID, 
s1.TICKER AS UNDER_TICKER_1,  s1.NB_ASSET_CLASS AS UNDER_NB_ASSET_CLASS_S1, 
s1.NB_SECURITY_GROUP AS UNDER_NB_SECURITY_GROUP_S1,  s1.NB_SECURITY_TYPE_1 AS UNDER_NB_SECURITY_TYPE_1_S1

FROM 
dbo.vFACT_POSITION_PMR p
INNER JOIN dbo.DIM_POSITION_SET_CURRENT ps ON ps.POSITION_SET_ID=p.POSITION_SET_ID
INNER JOIN dbo.DIM_ACCOUNT a on a.EDM_ID=p.ACCOUNT_EDM_ID
LEFT OUTER JOIN core.vSECURITY s ON s.NB_ID=p.SECURITY_EDM_ID
LEFT OUTER JOIN core.vSECURITY s1 ON s1.NB_ID=s.UNDERLYING_ID
LEFT OUTER JOIN dbo.vDIM_SECURITY_PMR ss ON ss.SEC_NO=p.SECURITY_NUMBER
LEFT OUTER JOIN dbo.vDIM_SECURITY_PMR ssu ON ssu.SEC_NO=ss.UNDERLYING_SEC_NO
WHERE 
ps.AS_OF_DATE BETWEEN '{0}' AND '{1}' 
AND ps.PROVIDER='PMR' AND ps.SOURCE='PMR'
AND a.NAA_ACCOUNT_ID='{2}'
ORDER BY p.AS_OF_DATE
'''


str_sql_dates = '''
    SELECT ps.AS_OF_DATE FROM dbo.DIM_POSITION_SET_CURRENT ps WHERE ps.PROVIDER='PMR' AND ps.SOURCE='PMR' ORDER BY AS_OF_DATE DESC
'''

#str_sql_pmr_NAV_cash = '''
#--AUM and non-investible cash
#SELECT 
#d.AsOf_Date,d.Portfolio_ID, d.Current_AUM,d.Cash, d.Return_Daily,d.Return_YTD
#FROM alt_core_02_portfolio_daily d
#WHERE d.AsOf_Date BETWEEN '{0}' AND '{1}'
#AND d.Portfolio_ID='{2}'
#'''
#    
    
str_sql_pmr_cash = '''
SELECT 
cs.AS_OF_DATE, SUM( c.MKT_VAL) AS CASH
FROM DM_Operations.dbo.VFACT_CASH_BALANCE_PMR c
INNER JOIN dbo.vDIM_CASH_SET_CURRENT cs ON cs.ID=c.CASH_SET_ID
--INNER JOIN core.vSECURITY s ON s.NB_ID = c.SECURITY_EDM_ID
INNER JOIN dbo.DIM_ACCOUNT a ON a.EDM_ID = c.ACCOUNT_EDM_ID
WHERE 
cs.AS_OF_DATE BETWEEN '{0}' AND '{1}'
AND cs.PROVIDER='PMR'
AND cs.SOD_OR_EOD='EOD'
AND a.NAA_ACCOUNT_ID='{2}'
GROUP BY cs.AS_OF_DATE
'''


str_sql_pmr_ge_gainloss = '''
SELECT 
g.PeriodType, g.From_Date,g.To_Date, g.Sec_No, g.Sec_Description,g.Long_Short, 
g.Gain_Loss, g.Beginning_Market_Value,g.End_Market_Value
FROM dbo.alt_core_09_gain_loss g
WHERE 
g.Portfolio_ID='{2}'
AND g.To_Date BETWEEN '{0}' AND '{1}'
AND g.Sec_No <>'TOTAL'
ORDER BY g.PeriodType, g.Sec_Description
'''

DAYS_IN_YEAR = 365.25


############################################
# PMR BUG FIX FOR END OF MONTH FALLS ON WEEKEND 
############################################
def fix_weekend_EOM_date(df_business_days, df, df_date_field_name):
    
    for dd in sorted(df[df_date_field_name].unique()):
        
        if dd not in df_business_days:
            
            prev_business_day = [d  for d in df_business_days if d<dd][-1]
            
            logging.info("{0} month-end falls on weekend {1} is the last business day of the month".format( dd, prev_business_day))
            
            df = df[df[df_date_field_name] != dd]
            
            df.loc[df[df_date_field_name]==dd, "AS_OF_DATE"] = prev_business_day



if __name__ == '__main__':

        
    engineDW = pyodbc.connect(Trusted_Connection='yes',
                            driver='{SQL Server}',
                            server='PIPWSQL023B\PFRM802',
                            database='DM_Operations')

    enginePI = pyodbc.connect(Trusted_Connection='yes',
                            driver='{SQL Server}',
                            server='PIPWSFRM401\PFRM401',
                            database='pi')

    df_dates = pandas.read_sql_query(str_sql_dates, engineDW)
        
    latest_date = df_dates.AS_OF_DATE.max().date()
    
    arg_parser = argparse.ArgumentParser(description='Run Options Writing Team transaction analysis report')
    
    def parse_date(d):
        return dateutil.parser.parse(d).date()
        
    arg_parser.add_argument('--end_date',   default=latest_date, help='Report P&L as of date', type=parse_date)
    arg_parser.add_argument('--start_date', help='Report P&L as of date', type=parse_date)
    arg_parser.add_argument('--pdf',        default=False, action='store_true', help='Generate PDF document with all charts')
    arg_parser.add_argument('--loglevel',   default="INFO", choices={"WARN","INFO","DEBUG"}, help='Logging level verbosity')
    arg_parser.add_argument('--outdir',     default="//nb/corp/groups/NY/Institutional/SPA/Dmitry/options_team", help='Report Output Directory')
    arg_parser.add_argument('--account',    help='Comma-delimited list of all accounts for which to run reports')
    arg_parser.add_argument('--all',        default=False, action='store_true', help='Run for all known accounts')
    
    args = arg_parser.parse_args()
    
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: {}'.format(args.loglevel))

    if latest_date < args.end_date:
        args.end_date = latest_date

    if args.end_date is not None and args.start_date is None:
        args.start_date = args.end_date + dr.relativedelta(weeks=-6)
        
    if args.all:
        df_accounts_all = pandas.read_sql_query(str_sql_pmr_allaccounts, engineDW, parse_dates=['OPEN_DATE'])
        pmr_account_list = df_accounts_all.NAA_ACCOUNT_ID
        
    elif args.account is not None:
        pmr_account_list = args.account.split(',')
    
    else:
        raise Exception ("Specify --account PMR_ACCOUNT or --all options")        
        
    logging.basicConfig(filename="{0}/options_team_report_{1}_{2}.log".format(args.outdir, args.start_date, args.end_date),
                        filemode="w",
                        level=numeric_level,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        format="%(asctime)s:%(levelname)s:%(message)s")

    bh = bbg.bbg_helper()
    bh.bbg_init()
    
    colors = ['red','orange','yellow','green','blue','violet']
    markers = ['x','o','^','.']

    business_days = pandas.bdate_range(start=args.start_date, end=args.end_date)

    for account in pmr_account_list:
        ############################################
        #   GRAB PMR POSITION DATA
        ############################################
        #df_transact = pandas.read_sql_query(str_sql_pmr_transaction.format(args.start_date, args.end_date, account), engine, parse_dates=['AS_OF_DATE', 'TRADE_DATE', 'SETTLEMENT_DATE','EFFECTIVE_DATE','ENTRY_DATE'])
        df_position = pandas.read_sql_query(str_sql_pmr_position.format(args.start_date, args.end_date, account), 
                                            engineDW, 
                                            parse_dates=['AS_OF_DATE', 'TRADE_DATE', 'MATURITY_DATE', 'EXPIRATION_DATE'])

        if df_position.size==0:
            logging.error("Account {0} has no positions".format(account))
            continue
        
        ###########################
        # CLEAN UP tickers
        ###########################
        df_position.UNDER_TICKER = df_position.UNDER_TICKER.apply(lambda x: x.rstrip(".99") if x is not None and x[-3:]==".99" else x)
        df_position.UNDER_TICKER = df_position.apply(func=lambda x: x.UNDER_TICKER_1 if x.UNDER_TICKER is None else x.UNDER_TICKER, axis=1)

        df_position['DIRTY_MV']  = df_position.MARKET_VALUE + df_position.ACCRUED_INTEREST
        
        fix_weekend_EOM_date(business_days, df_position, "AS_OF_DATE")
                
        ########################################################
        # PMR positions in Treasuries, ETFs, moneymarkets and others used for collateral purpose
        ########################################################
        df_collateral = df_position[((df_position.NB_SECURITY_GROUP=='Bond') & (df_position.NB_SECURITY_TYPE_1=='Treasury')) | 
                                    ((df_position.NB_SECURITY_GROUP=='Bond') & (df_position.NB_SECURITY_TYPE_1=='Bond')) | 
                                    ((df_position.NB_SECURITY_GROUP=='Equity') & (df_position.NB_SECURITY_TYPE_1=='Fund'))][["AS_OF_DATE","MARKET_VALUE","ACCRUED_INTEREST"]].groupby(["AS_OF_DATE"], as_index=False).aggregate(sum)


        ########################################################
        # PMR non-asset cash held away and fund NAV
        ########################################################
        #df_cash = pandas.read_sql_query(str_sql_pmr_NAV_cash.format(args.start_date, args.end_date, account), enginePI, parse_dates=['AsOf_Date'])
        df_cash = pandas.read_sql_query(str_sql_pmr_cash.format(args.start_date, args.end_date, account), engineDW, parse_dates=['AS_OF_DATE'])
        
        #fix_weekend_EOM_date(business_days, df_cash, "AsOf_Date")
        
        df_collateral = pandas.merge(left=df_collateral, 
                                     right=df_cash[['AS_OF_DATE','CASH']], 
                                     left_on=['AS_OF_DATE'], 
                                     right_on=['AS_OF_DATE'], 
                                     how='outer',
                                     copy=False).fillna(0)
                                     
        df_collateral['TOTAL_COLLATERAL'] = df_collateral['MARKET_VALUE'] + df_collateral['CASH'] + df_collateral['ACCRUED_INTEREST'] 
        df_collateral                     = df_collateral[['AS_OF_DATE', 'MARKET_VALUE', 'CASH', 'ACCRUED_INTEREST','TOTAL_COLLATERAL']]
        
        df_collateral.rename(columns={'MARKET_VALUE':'COLLATERAL', 'Cash':'CASH'}, inplace=True)
        
        df_collateral                     = pandas.merge(left=df_collateral,
                                                         right=df_position[['AS_OF_DATE','MARKET_VALUE']].groupby('AS_OF_DATE', as_index=False).aggregate('sum'), 
                                                         how='outer', 
                                                         left_on='AS_OF_DATE', 
                                                         right_on='AS_OF_DATE').fillna(0).sort_values(by='AS_OF_DATE')
        
        df_collateral['NAV']              = df_collateral.MARKET_VALUE + df_collateral.ACCRUED_INTEREST + df_collateral.CASH
        df_collateral.MARKET_VALUE        = df_collateral.MARKET_VALUE - df_collateral.COLLATERAL
        
        df_collateral.rename(columns={'MARKET_VALUE':'ASSET_VALUE'}, inplace=True)
        

        df_options  = pandas.merge(left=df_position[df_position.NB_SECURITY_TYPE_1=='Option'],
                                 right=df_collateral[['AS_OF_DATE','NAV']],
                                 how='left',
                                 left_on='AS_OF_DATE',
                                 right_on='AS_OF_DATE')
        
        df_options.rename(columns={'TICKER':'OPTION_TICKER'}, inplace=True)

        ############################################
        #   UNDERLYING ASSET PRICING FROM BLOOMBERG
        ############################################
        underlying_list = [(t, TICKER_SUFFIX_MAP[t]) for t in df_options.UNDER_TICKER.unique()]
        
        (res, exc) = bh.bbg_get_hist_data(cusip_list = [v[0] for v in underlying_list],
                                         start_date = args.start_date,
                                         end_date = args.end_date,
                                         yellow_key = [v[1] for v in underlying_list],
                                         bbg_mnemonic=bbg_mnemonic_list,
                                         freq="DAILY",
                                         days="ACTUAL",
                                         override_prc=False)
    
        df_bbg_list=[]
        
        for results in res:
            for bbg_secid, v in results.iteritems():
                
                t = pandas.DataFrame(zip([bbg_secid]*len(v), 
                                                        [vv[1] for vv in v if vv[0]=='date'], 
                                                        [vv[1] for vv in v if vv[0]==bbg_mnemonic_list[0]],
                                                        [vv[1] for vv in v if vv[0]==bbg_mnemonic_list[1]],
                                                        [vv[1] for vv in v if vv[0]==bbg_mnemonic_list[2]],
                                                        [vv[1] for vv in v if vv[0]==bbg_mnemonic_list[3]]),
                                                    columns=["BBG_ID","DATE"] + bbg_mnemonic_list )
                t.sort_values(by='DATE')                                    
                t["PCT_RETURN"] = t.PX_LAST.pct_change()
                df_bbg_list.append(t)
        
        df_bbg_prices                 = pandas.concat(df_bbg_list, ignore_index=True)
        df_bbg_prices['MP_DATE']      = mdates.date2num(df_bbg_prices.DATE.astype(datetime.date))    
        df_bbg_prices["UNDER_TICKER"] = df_bbg_prices.BBG_ID.apply(lambda x:  x[:x.find(' ')])
        df_bbg_prices["DATE"]         = pandas.to_datetime(df_bbg_prices.DATE).values

        bbg_mnemonic_divyld_list = ['IDX_EST_DVD_YLD','EQY_DVD_YLD_IND']
        (res, exc) = bh.bbg_get_ref_data(cusip_list = [v[0] for v in underlying_list],
                                         bbg_mnemonic = bbg_mnemonic_divyld_list,
                                         yellow_key = [v[1] for v in underlying_list])
        
        
        ##########################################
        # GET DIVIDEND YIELD INFO
        ##########################################
        df_bbg_list=[]
        for results in res:
            for bbg_secid, v in results.iteritems():
                
                df_bbg_list.append(pandas.DataFrame(zip([bbg_secid]*len(v), 
                                                        [vv[1]/100.0 for vv in v if vv[0] in bbg_mnemonic_divyld_list]),
                                                    columns=["BBG_ID","DIV_YLD"]))

        df_bbg_divyld                 = pandas.concat(df_bbg_list, ignore_index=True)
        df_bbg_divyld['UNDER_TICKER'] = df_bbg_divyld.BBG_ID.apply(lambda x:  x[:x.find(' ')])
        
        
        ##########################################
        # USE 1M USD LIBOR For Risk Free Rate
        ##########################################
        df_bbg_list=[]
        rf_bbg_ticker = ['US0001M']
        (res, exc) = bh.bbg_get_hist_data(cusip_list = rf_bbg_ticker,
                                         start_date = args.start_date,
                                         end_date = args.end_date,
                                         yellow_key = ['Index'],
                                         bbg_mnemonic=[bbg_mnemonic_list[1]],
                                         freq="DAILY",
                                         days="ACTUAL")
    
        df_bbg_list=[]
        for results in res:
            for bbg_secid, v in results.iteritems():
                
                df_bbg_list.append(pandas.DataFrame(zip([bbg_secid]*len(v), 
                                                        [vv[1] for vv in v if vv[0]=='date'], 
                                                        #[pow(1.0+vv[1]/100.0, 12) - 1.0 for vv in v if vv[0]==bbg_mnemonic_list[1]]),
                                                        [vv[1]/100.0 for vv in v if vv[0]==bbg_mnemonic_list[1]]),
                                                    columns=["BBG_ID","DATE", 'US0001M'] ))
        
        
        df_bbg_libor         = pandas.concat(df_bbg_list, ignore_index=True)
        df_bbg_libor["DATE"] = pandas.to_datetime(df_bbg_libor.DATE).values
        
        
        
        
        df_options_analytics = pandas.merge(left=df_options, 
                                            right=df_bbg_libor[['DATE','US0001M']], 
                                            how='left', 
                                            left_on='AS_OF_DATE',
                                            right_on='DATE')
                                            
        ## USE last known value to fill gaps on days when LIBOR is not fixed                                    
        df_options_analytics.US0001M = df_options_analytics.US0001M.fillna(method='ffill')                            

        df_options_analytics = pandas.merge(left=df_options_analytics, 
                                            right=df_bbg_divyld[['UNDER_TICKER','DIV_YLD']], 
                                            how='left', 
                                            left_on='UNDER_TICKER',
                                            right_on='UNDER_TICKER',
                                            )
        
        df_options_analytics = pandas.merge(left=df_options_analytics, 
                                            right=df_bbg_prices, 
                                            how='left', 
                                            left_on=['AS_OF_DATE','UNDER_TICKER'],
                                            right_on=['DATE','UNDER_TICKER'],
                                            suffixes = ("_OPTION", "_UNDERLY")).sort_values(by=['AS_OF_DATE','UNDER_TICKER','EXPIRATION_DATE','STRIKE_PRICE'])
        
       
        df_options_analytics.loc[df_options_analytics.CALL_PUT.str.lower()=='put',  'CALL_PUT'] = 'P'
        df_options_analytics.loc[df_options_analytics.CALL_PUT.str.lower()=='call', 'CALL_PUT'] = 'C'

        df_options_analytics['YRS_TO_EXPIRY']  = (df_options_analytics.EXPIRATION_DATE - df_options_analytics.AS_OF_DATE) / datetime.timedelta(days=DAYS_IN_YEAR)
        df_options_analytics['DAYS_TO_EXPIRY'] = (df_options_analytics.EXPIRATION_DATE - df_options_analytics.AS_OF_DATE) / datetime.timedelta(days=1)
        df_options_analytics["EXPOSURE_SIGN"]  = df_options_analytics.apply(func=lambda x: -1.0 if x.CALL_PUT.lower()=='p'else 1.0, axis=1)
        df_options_analytics['MONEYNESS']      = df_options_analytics.EXPOSURE_SIGN * ( 1.0 - df_options_analytics.STRIKE_PRICE/df_options_analytics.PX_LAST )
        
        # clean up: delete options expiring on as of date                                                            
        #df_options_analytics = df_options_analytics[ df_options_analytics.EXPIRATION_DATE > df_options_analytics.AS_OF_DATE]
        #no_strikes_per_maturity = df_options.groupby(['AS_OF_DATE','CALL_PUT','UNDER_TICKER','EXPIRATION_DATE'], as_index=False).aggregate('count').SECURITY_NUMBER
        
        ###################################################
        # COMPUTE ALL THE GREEKS 
        ###################################################
        df_options_analytics['IVOL'] = df_options_analytics.apply(func=lambda x: bsm_vol.implied_volatility(price=x.PRICE,
                                                                                                        S=x.PX_LAST,
                                                                                                        K=x.STRIKE_PRICE, 
                                                                                                        t=x.YRS_TO_EXPIRY,
                                                                                                        r=x.US0001M,
                                                                                                        q=x.DIV_YLD,
                                                                                                        flag=x.CALL_PUT.lower()), 
                                                                axis=1)
        # clean up:  reset options implied vol to infinite
        df_options_analytics.loc[abs(df_options_analytics.IVOL)>10, 'IVOL'] = numpy.inf        
        
        #option price change per 1 point change in underlying price
        df_options_analytics['DELTA'] =  df_options_analytics.apply(func=lambda x: bsm_greeks.delta(S=x.PX_LAST,
                                                                                        K=x.STRIKE_PRICE, 
                                                                                        sigma=x.IVOL,
                                                                                        t=x.YRS_TO_EXPIRY,
                                                                                        r=x.US0001M,
                                                                                        q=x.DIV_YLD,                                                                                        
                                                                                        flag=x.CALL_PUT.lower()),
                                                             axis=1) 
                                                             
        # option price change per 1 point change in underlying price                                                     
        df_options_analytics['GAMMA'] = df_options_analytics.apply(func=lambda x: bsm_greeks.gamma(S=x.PX_LAST,
                                                                                        K=x.STRIKE_PRICE, 
                                                                                        sigma=x.IVOL,
                                                                                        t=x.YRS_TO_EXPIRY,
                                                                                        r=x.US0001M,
                                                                                        q=x.DIV_YLD,                                                                                        
                                                                                        flag=x.CALL_PUT.lower()),
                                                             axis=1) 
                                                             
        # option price change 1 calendar day decrease in time to maturity        
        df_options_analytics['THETA'] = df_options_analytics.apply(func=lambda x: bsm_greeks.theta(S=x.PX_LAST,
                                                                                        K=x.STRIKE_PRICE, 
                                                                                        sigma=x.IVOL,
                                                                                        t=x.YRS_TO_EXPIRY,
                                                                                        r=x.US0001M,
                                                                                        q=x.DIV_YLD,                                                                                        
                                                                                        flag=x.CALL_PUT.lower()),
                                                             axis=1)
        #option price change per 1 point rise in volatility         
        df_options_analytics['VEGA'] = df_options_analytics.apply(func=lambda x: bsm_greeks.vega(S=x.PX_LAST,
                                                                                        K=x.STRIKE_PRICE, 
                                                                                        sigma=x.IVOL,
                                                                                        t=x.YRS_TO_EXPIRY,
                                                                                        r=x.US0001M,
                                                                                        q=x.DIV_YLD,                                                                                        
                                                                                        flag=x.CALL_PUT.lower()),
                                                             axis=1)
                                                             
        #option price change per 1 point rise in risk-free rate                                                     
        df_options_analytics['RHO'] = df_options_analytics.apply(func=lambda x: bsm_greeks.rho(S=x.PX_LAST,
                                                                                        K=x.STRIKE_PRICE, 
                                                                                        sigma=x.IVOL,
                                                                                        t=x.YRS_TO_EXPIRY,
                                                                                        r=x.US0001M,
                                                                                        q=x.DIV_YLD,                                                                                        
                                                                                        flag=x.CALL_PUT.lower()),
                                                             axis=1) 


        df_options_analytics['NOTIONAL_EXP']           = df_options_analytics.UNITS * df_options_analytics.TRADING_UNIT * df_options_analytics.PX_LAST

        df_options_analytics['DELTA_ADJ_NOTIONAL_EXP'] = df_options_analytics.NOTIONAL_EXP * df_options_analytics.DELTA * df_options_analytics.EXPOSURE_SIGN
        
        df_options_analytics['GAMMA_ADJ_NOTIONAL_EXP'] = df_options_analytics.NOTIONAL_EXP * df_options_analytics.GAMMA * df_options_analytics.EXPOSURE_SIGN

        df_options_analytics['VEGA_ADJ_NOTIONAL_EXP']  = df_options_analytics.NOTIONAL_EXP * df_options_analytics.VEGA * df_options_analytics.EXPOSURE_SIGN

        df_options_analytics['THETA_ADJ_NOTIONAL_EXP'] = df_options_analytics.NOTIONAL_EXP * df_options_analytics.THETA * df_options_analytics.EXPOSURE_SIGN

        df_options_analytics['MONEYNESS_NOTIONAL_EXP'] = df_options_analytics.NOTIONAL_EXP * df_options_analytics.MONEYNESS
        
        df_options_analytics['INTRINSIC_VALUE']        = (df_options_analytics.PX_LAST - df_options_analytics.STRIKE_PRICE) * df_options_analytics.EXPOSURE_SIGN 
        
        df_options_analytics.loc[df_options_analytics.INTRINSIC_VALUE < 0, 'INTRINSIC_VALUE'] = 0
        
        df_options_analytics.INTRINSIC_VALUE           = df_options_analytics.INTRINSIC_VALUE * df_options_analytics.UNITS * df_options_analytics.TRADING_UNIT 

        df_options_analytics['TIME_VALUE']             = df_options_analytics.MARKET_VALUE -  df_options_analytics.INTRINSIC_VALUE
        
        
        fund_notional_bydate = df_options_analytics[["AS_OF_DATE","NOTIONAL_EXP", "DELTA_ADJ_NOTIONAL_EXP", "MARKET_VALUE", 'INTRINSIC_VALUE', 'TIME_VALUE', 'TOTAL_COST']].groupby(["AS_OF_DATE"], as_index=False).aggregate(sum)
        

        
        account_title = "{0}_{1}".format(account, p.sub(' ', df_options_analytics.ACCOUNT_TITLE.unique()[0]))
        if (args.pdf):
            pp = PdfPages('{3}/options_writing_team_{0}-{1}_{2}.pdf'.format(args.start_date, args.end_date, account_title, args.outdir))


        #########################################
        # EXCEL
        #########################################
        writer = pandas.ExcelWriter('{3}/options_writing_team_{0}-{1}_{2}.xlsx'.format(args.start_date, args.end_date, account_title, args.outdir), date_format='YYYY-MM-DD', datetime_format='YYYY-MM-DD')
 
        format_par = writer.book.add_format({'num_format':'#,##0_);[Red](#,##0)'})
        format_num = writer.book.add_format({'num_format':'#,##0.00;[Red](#,##0.00)'})
        format_pct = writer.book.add_format({'num_format':'0.00%'})
        format_usd = writer.book.add_format({'num_format':'$ #,##0_);[Red]($ #,##0)'})

        red_bg     = writer.book.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        dk_red_bg  = writer.book.add_format({'bg_color': '#FF3747', 'font_color': '#640005'})
        green_bg   = writer.book.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        dk_green_bg= writer.book.add_format({'bg_color': '#00B050', 'font_color': '#005000'})

        ########################################################
        # PMR Gain Loss (DAILY AND MTD)
        ########################################################
        df_gainloss = pandas.read_sql_query(str_sql_pmr_ge_gainloss.format(args.start_date, args.end_date, account), enginePI, parse_dates=['From_Date','To_Date'])
        #fix_weekend_EOM_date(df_business_days, df_gainloss, "To_Date")
                                            

        if df_gainloss.size>0:
        
            df_gainloss.rename(columns={'To_Date':'AS_OF_DATE'}, inplace=True)

            df_gainloss = pandas.merge(how='left', 
                                       left=df_gainloss,
                                       right=df_position[['SECURITY_NUMBER','NB_SECURITY_GROUP','NB_SECURITY_TYPE_1','UNDER_TICKER']].drop_duplicates(), 
                                       left_on='Sec_No',
                                       right_on='SECURITY_NUMBER')
                                       
            ########################################################
            ## PI on maturing bonds writes down all of the market value into a loss
            ## PI newly bought bonds might appear with P&L of the market value into a gain
            ########################################################
            df_gainloss.loc[(df_gainloss.End_Market_Value==0)      &(df_gainloss.Gain_Loss==-df_gainloss.Beginning_Market_Value)&(df_gainloss.NB_SECURITY_GROUP=='Bond'), 'Gain_Loss']=0
            df_gainloss.loc[(df_gainloss.Beginning_Market_Value==0)&(df_gainloss.Gain_Loss==df_gainloss.End_Market_Value)       &(df_gainloss.NB_SECURITY_GROUP=='Bond'), 'Gain_Loss']=0
    
            
            df_fund_gainloss  = pandas.merge(left=df_gainloss[df_gainloss.PeriodType=='P&L DAILY'][['AS_OF_DATE','Gain_Loss']].groupby('AS_OF_DATE', as_index=False).aggregate('sum'),
                                             right=df_collateral[['AS_OF_DATE','NAV']],
                                             how='left',
                                             left_on='AS_OF_DATE',
                                             right_on='AS_OF_DATE')
                                             
                                             
            df_position_gain_loss = pandas.merge(left=df_options_analytics, 
                                                 right=df_gainloss[df_gainloss.PeriodType=='P&L DAILY'][['AS_OF_DATE','Gain_Loss','SECURITY_NUMBER']], 
                                                 left_on=['AS_OF_DATE','SECURITY_NUMBER'],
                                                 right_on=['AS_OF_DATE','SECURITY_NUMBER'],
                                                 how='outer')
            
            
        else:
            
            ###: FINISH THIS WHEN NO PL IN PI IS AVAILABLE
            logging.warn("Fund {0} has no P&L information, using market value change day over day,  new trades and closed out positions will be missing from P&L".format(account))
            df_position.sort_values(['SECURITY_NUMBER', 'AS_OF_DATE'])
            
            for secid in df_position.SECURITY_NUMBER.unique():
                df_position.loc[df_position.SECURITY_NUMBER==secid, 'Gain_Loss'] = df_position.loc[df_position.SECURITY_NUMBER==secid, 'DIRTY_MV'] - df_position.loc[df_position.SECURITY_NUMBER==secid, 'DIRTY_MV'].shift(periods=1) 
                
            df_fund_gainloss  = pandas.merge(left=df_position[['AS_OF_DATE','Gain_Loss']].groupby('AS_OF_DATE', as_index=False).aggregate('sum'),
                                             right=df_collateral[['AS_OF_DATE','NAV']],
                                             how='left',
                                             left_on='AS_OF_DATE',
                                             right_on='AS_OF_DATE')
            
        
            df_position_gain_loss = pandas.merge(left=df_options_analytics, 
                                                 right=df_position[['AS_OF_DATE','Gain_Loss','SECURITY_NUMBER']], 
                                                 left_on=['AS_OF_DATE','SECURITY_NUMBER'],
                                                 right_on=['AS_OF_DATE','SECURITY_NUMBER'],
                                                 how='left')

        df_fund_gainloss['PCT_RETURN'] = df_fund_gainloss.Gain_Loss / df_fund_gainloss.NAV
        df_fund_gainloss = df_fund_gainloss[['AS_OF_DATE','Gain_Loss','NAV','PCT_RETURN']]
        
            
        
        df_gainloss_by_ticker_date = df_position_gain_loss[['AS_OF_DATE','UNDER_TICKER','Gain_Loss','MARKET_VALUE','NOTIONAL_EXP']].groupby(['AS_OF_DATE','UNDER_TICKER'], as_index=False).aggregate('sum')
        
        df_gainloss_by_ticker_date = pandas.merge(left=df_gainloss_by_ticker_date,
                                                 right=df_collateral[['AS_OF_DATE','TOTAL_COLLATERAL']],
                                                 how='left',
                                                 left_on='AS_OF_DATE',
                                                 right_on='AS_OF_DATE')
                                                 
        df_gainloss_by_ticker_date = pandas.merge(left=df_gainloss_by_ticker_date,
                                                 right=fund_notional_bydate[['AS_OF_DATE','NOTIONAL_EXP']],
                                                 how='left',
                                                 left_on='AS_OF_DATE',
                                                 right_on='AS_OF_DATE',
                                                 suffixes=['','_FUND'])
                                                 
        df_gainloss_by_ticker_date['COLLATERAL'] = df_gainloss_by_ticker_date.TOTAL_COLLATERAL * df_gainloss_by_ticker_date.NOTIONAL_EXP / df_gainloss_by_ticker_date.NOTIONAL_EXP_FUND
        
        df_gainloss_by_ticker_date = df_gainloss_by_ticker_date[['AS_OF_DATE','UNDER_TICKER','Gain_Loss','MARKET_VALUE','NOTIONAL_EXP','COLLATERAL']]        
        
        #########################################
        # PDF CHARTS
        #########################################
        fig_fund, (plt_notional, plt_fund_gainloss, plt_iv) = plt.subplots(nrows=3, ncols=1, sharex=False, subplot_kw=None, gridspec_kw={'height_ratios':[10,10,10]})

        fig_fund.set_figheight(22)
        fig_fund.set_figwidth(17)

        plt_notional.plot(fund_notional_bydate.AS_OF_DATE, 
                 fund_notional_bydate.NOTIONAL_EXP.abs(),
                 color='orange', 
                 marker='x', 
                 linestyle='-', 
                 label='Notional Exposure')            
       

        if df_collateral.size > 0:
            plt_notional.plot(df_collateral.AS_OF_DATE, 
                     df_collateral.CASH,
                     color='red', 
                     marker='x', 
                     linestyle='-', 
                     label='Cash')            
                 

            plt_notional.plot(df_collateral.AS_OF_DATE, 
                     df_collateral.COLLATERAL + df_collateral.ACCRUED_INTEREST,
                     color='green', 
                     marker='x', 
                     linestyle='-', 
                     label='Collateral')            
                     
            plt_notional.plot(df_collateral.AS_OF_DATE, 
                     (df_collateral.TOTAL_COLLATERAL ),
                     color='blue', 
                     marker='x', 
                     linestyle='-', 
                     label='Collateral + Cash')            

        plt_notional.set_title('{} Exposure & Collateral'.format(account_title))
        plt_notional.axhline(0,color='black')
        plt_notional.grid(b=True, which='major', color='black', linestyle='--')
        plt_notional.xaxis.set_major_locator(mdates.WeekdayLocator())
        plt_notional.xaxis.set_major_formatter(mdates.DateFormatter('%b %d %Y'))
        plt_notional.set_xlim([args.start_date, args.end_date])
        plt_notional.set_yticklabels(["{:,}".format(x/pow(10,6)) for x in plt_notional.get_yticks()])
        plt_notional.set_ylabel("USD Millions")
        
        plt_notional.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'medium'}, framealpha=0.3)             



        plt_iv.plot(fund_notional_bydate.AS_OF_DATE, 
                 fund_notional_bydate.MARKET_VALUE,
                 color='blue', 
                 marker='o', 
                 linestyle='-', 
                 label='Market Value')            
       

        plt_iv.plot(fund_notional_bydate.AS_OF_DATE, 
                 fund_notional_bydate.INTRINSIC_VALUE,
                 color='orange', 
                 marker='x', 
                 linestyle='-', 
                 label='Intrinsic Value')            
                 

        plt_iv.plot(fund_notional_bydate.AS_OF_DATE, 
                 fund_notional_bydate.TIME_VALUE,
                 color='green', 
                 marker='x', 
                 linestyle='-', 
                 label='Time Value')            
                 

        plt_iv.plot(fund_notional_bydate.AS_OF_DATE, 
                 fund_notional_bydate.TOTAL_COST,
                 color='red', 
                 marker='o', 
                 linestyle='-', 
                 label='Cost')            
                 
        plt_iv.set_title('{} Option Value'.format(account_title))
        plt_iv.axhline(0,color='black')
        plt_iv.grid(b=True, which='major', color='black', linestyle='--')
        plt_iv.xaxis.set_major_locator(mdates.WeekdayLocator())
        plt_iv.xaxis.set_major_formatter(mdates.DateFormatter('%b %d %Y'))
        plt_iv.set_xlim([args.start_date, args.end_date])
        plt_iv.set_yticklabels(["{:,}".format(x/pow(10,5)) for x in plt_iv.get_yticks()])
        plt_iv.set_ylabel("USD Hundred Thousands")
        
        plt_iv.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'medium'}, framealpha=0.3)      
        
        if df_fund_gainloss.size>0:
            width = 0.35
            plt_fund_gainloss.bar(df_bbg_prices.MP_DATE.unique(), 
                                  df_bbg_prices[['DATE','UNDER_TICKER','PCT_RETURN']].groupby('DATE').aggregate('mean').PCT_RETURN, 
                                    color='green', 
                                    label='Avg Index', 
                                    alpha=0.5, 
                                    width=width,
                                    align='center')

            plt_fund_gainloss.bar(mdates.date2num(df_fund_gainloss.AS_OF_DATE.astype(datetime.date)), 
                                  df_fund_gainloss.PCT_RETURN, 
                                  color='blue', 
                                  label='Fund', 
                                  width=width,
                                  alpha=0.5, 
                                  align='center')
            

            plt_fund_gainloss.set_title('{} Daily Return'.format(account_title))
            plt_fund_gainloss.axhline(0,color='black')
            plt_fund_gainloss.grid(b=True, which='major', color='black', linestyle='--')
            plt_fund_gainloss.xaxis.set_major_locator(mdates.WeekdayLocator())
            plt_fund_gainloss.xaxis.set_major_formatter(mdates.DateFormatter('%b %d %Y'))
            plt_fund_gainloss.set_xlim([args.start_date, args.end_date])
            plt_fund_gainloss.set_yticklabels(["{:,}%".format(x*100) for x in plt_fund_gainloss.get_yticks()])
            plt_fund_gainloss.set_ylabel("USD Millions")
            
            plt_fund_gainloss.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'medium'}, framealpha=0.3)      

        if args.pdf:
            pp.savefig(fig_fund)
        
        for ticker in df_options_analytics.UNDER_TICKER.unique():
            
            bbg_suffix = TICKER_SUFFIX_MAP[ticker]
            
            df_options_for_ticker = df_options_analytics[df_options_analytics.UNDER_TICKER==ticker]
            under_desc =  df_options_for_ticker.UNDER_SEC_DESC.unique()[0]

            df_bbg_prices_for_ticker = df_bbg_prices[df_bbg_prices.BBG_ID=="{} {}".format(ticker, bbg_suffix)]

            df_gainloss_for_ticker = df_gainloss_by_ticker_date[df_gainloss_by_ticker_date.UNDER_TICKER==ticker] 
            
            df_gainloss_for_ticker['PCT_RETURN'] = df_gainloss_for_ticker.Gain_Loss / (df_gainloss_for_ticker.MARKET_VALUE.shift(1) + df_gainloss_for_ticker.COLLATERAL.shift(1))

    
            #collateral_factor = df_gainloss_by_ticker_date.loc[df_gainloss_by_ticker_date.UNDER_TICKER==ticker, 'NOTIONAL_EXP'].shift(1) / df_gainloss_by_ticker_date.NOTIONAL_EXP.shift(1)
            
            #df_gainloss_by_ticker_date.loc[df_gainloss_by_ticker_date.UNDER_TICKER==ticker, 'PCT_RETURN'] = (df_gainloss_by_ticker_date.loc[df_gainloss_by_ticker_date.UNDER_TICKER==ticker, 'Gain_Loss'] 
            #                                                            * collateral_factor 
            #                                                            * df_gainloss_by_ticker_date.TOTAL_COLLATERAL)

            ####################################################
            # Paint PRETTY CHARTS
            ####################################################
            fig, (plt_und_price, plt_ticker_gainloss, plt_otp_iv, plt_greeks, plt_moneyness) = plt.subplots(nrows=5, ncols=1, sharex=False, subplot_kw=None, gridspec_kw={'height_ratios':[10,10,10,10,5]})
#            fig = plt.figure()
#            plt_und_price = fig.add_subplot(311)
#            ax3 = fig.add_subplot(312)
#            ax2 = fig.add_subplot(313)
#            ax2 = fig.add_subplot(313, projection='3d')
            
            fig.set_figheight(22)
            fig.set_figwidth(17)
    
            df_options_notional_by_date = df_options_for_ticker[['AS_OF_DATE','NOTIONAL_EXP','DELTA_ADJ_NOTIONAL_EXP','GAMMA_ADJ_NOTIONAL_EXP',
                                                                 'VEGA_ADJ_NOTIONAL_EXP','THETA_ADJ_NOTIONAL_EXP', 'MONEYNESS_NOTIONAL_EXP',
                                                                 'MARKET_VALUE','TOTAL_COST', 'INTRINSIC_VALUE', 'TIME_VALUE']].groupby('AS_OF_DATE', as_index=False).aggregate('sum')
            
            #########################################
            ## plot the underlying ticker price history between start and end dates
            #########################################
            plt_fin.candlestick_ochl( plt_und_price, df_bbg_prices_for_ticker[['MP_DATE'] + bbg_mnemonic_list].values)

            plt_und_price.set_title("{0} ({1}) Market Price".format( under_desc, ticker))
            plt_und_price.xaxis.set_major_locator(mdates.WeekdayLocator())
            plt_und_price.xaxis.set_major_formatter(mdates.DateFormatter('%b %d %Y'))
            plt_und_price.grid(b=True, which='major', color='gray', linestyle='--')
            plt_und_price.set_ylabel("Open/Close/High/Low Price")
            plt_und_price.set_xlim([args.start_date, args.end_date])

            
            if df_gainloss_for_ticker.size>0:
                
                #plt_ticker_gainloss.bar(df_gainloss_for_ticker.index, df_gainloss_for_ticker.Gain_Loss, color='blue', label='P&L', alpha=0.5, align='center')
                width = 0.35
                plt_ticker_gainloss.bar(df_bbg_prices_for_ticker.MP_DATE.unique(), 
                                        df_bbg_prices_for_ticker.PCT_RETURN,
                                        color='green', 
                                        label='Index', 
                                        width=width,
                                        alpha=0.5, 
                                        align='center')
                
                plt_ticker_gainloss.bar(mdates.date2num(df_gainloss_for_ticker.AS_OF_DATE.astype(datetime.date)), 
                                      df_gainloss_for_ticker.PCT_RETURN, 
                                      color='blue', 
                                      label='Options', 
                                      width=width,
                                      alpha=0.5, 
                                      align='center')
                

                plt_ticker_gainloss.set_title('{0} {1} P&L'.format(under_desc, ticker))
                plt_ticker_gainloss.axhline(0,color='black')
                plt_ticker_gainloss.grid(b=True, which='major', color='black', linestyle='--')
                plt_ticker_gainloss.xaxis.set_major_locator(mdates.WeekdayLocator())
                plt_ticker_gainloss.xaxis.set_major_formatter(mdates.DateFormatter('%b %d %Y'))
                plt_ticker_gainloss.set_xlim([args.start_date, args.end_date])
                plt_ticker_gainloss.set_yticklabels(["{:,}%".format(x*100) for x in plt_ticker_gainloss.get_yticks()])
                plt_ticker_gainloss.set_ylabel("USD Millions")
                
                plt_ticker_gainloss.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'medium'}, framealpha=0.3)      

            plt_moneyness.plot(df_options_notional_by_date['AS_OF_DATE'], 
                     df_options_notional_by_date.MONEYNESS_NOTIONAL_EXP / df_options_notional_by_date.NOTIONAL_EXP ,
                     color='blue', 
                     marker='^', 
                     linestyle='-', 
                     label='Moneyness')            
                                          
            plt_moneyness.set_title('{0} ({1}) Moneyness'.format(under_desc, ticker))
            plt_moneyness.axhline(0,color='black')
            plt_moneyness.grid(b=True, which='major', color='black', linestyle=':')
            plt_moneyness.xaxis.set_major_locator(mdates.WeekdayLocator())
            plt_moneyness.xaxis.set_major_formatter(mdates.DateFormatter('%b %d %Y'))
            plt_moneyness.set_xlim([args.start_date, args.end_date])
            #plt_moneyness.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f%%'))
            plt_moneyness.set_yticklabels(["{}%".format(x*100) for x in plt_moneyness.get_yticks()])
            
            plt_moneyness.legend(loc="best", ncol=1, fancybox=True, shadow=True, prop={'size':'medium'}, framealpha=0.3)             

            plt_greeks.plot(df_options_notional_by_date['AS_OF_DATE'], 
                     df_options_notional_by_date.DELTA_ADJ_NOTIONAL_EXP / df_options_notional_by_date.NOTIONAL_EXP ,
                     color='blue', 
                     marker='.', 
                     linestyle='-',
                     label='Delta')            
                     
            plt_greeks.plot(df_options_notional_by_date['AS_OF_DATE'], 
                     df_options_notional_by_date.GAMMA_ADJ_NOTIONAL_EXP / df_options_notional_by_date.NOTIONAL_EXP ,
                     color='red', 
                     marker='x', 
                     linestyle='-', 
                     label='Gamma')            
                     

            plt_greeks.plot(df_options_notional_by_date['AS_OF_DATE'], 
                     df_options_notional_by_date.VEGA_ADJ_NOTIONAL_EXP / df_options_notional_by_date.NOTIONAL_EXP ,
                     color='green', 
                     marker='x', 
                     linestyle='-', 
                     label='Vega')            
                     
            plt_greeks.plot(df_options_notional_by_date['AS_OF_DATE'], 
                     df_options_notional_by_date.THETA_ADJ_NOTIONAL_EXP / df_options_notional_by_date.NOTIONAL_EXP ,
                     color='orange', 
                     marker='^', 
                     linestyle='-', 
                     label='Theta')            

            
            plt_greeks.set_title('{0} ({1}) Greeks'.format(under_desc, ticker))
            plt_greeks.axhline(0,color='black')
            plt_greeks.grid(b=True, which='major', color='black', linestyle='--')
            plt_greeks.xaxis.set_major_locator(mdates.WeekdayLocator())
            plt_greeks.xaxis.set_major_formatter(mdates.DateFormatter('%b %d %Y'))
            plt_greeks.set_xlim([args.start_date, args.end_date])
            
            plt_greeks.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'medium'}, framealpha=0.3)             
            
            plt_otp_iv.plot(df_options_notional_by_date.AS_OF_DATE, 
                     df_options_notional_by_date.MARKET_VALUE,
                     color='blue', 
                     marker='o', 
                     linestyle='-', 
                     label='Market Value')            
           
    
            plt_otp_iv.plot(df_options_notional_by_date.AS_OF_DATE, 
                     df_options_notional_by_date.INTRINSIC_VALUE,
                     color='orange', 
                     marker='x', 
                     linestyle='-', 
                     label='Intrinsic Value')            
                     
    
            plt_otp_iv.plot(df_options_notional_by_date.AS_OF_DATE, 
                     df_options_notional_by_date.TIME_VALUE,
                     color='green', 
                     marker='x', 
                     linestyle='-', 
                     label='Time Value')            
                     
    
            plt_otp_iv.plot(df_options_notional_by_date.AS_OF_DATE, 
                      df_options_notional_by_date.TOTAL_COST,
                      color='red', 
                      marker='o', 
                      linestyle='-', 
                      label='Cost')            

            plt_otp_iv.set_title('{0} ({1}) Option Market & Intrinsic Value'.format(under_desc, ticker))
            plt_otp_iv.axhline(0,color='black')
            plt_otp_iv.grid(b=True, which='major', color='black', linestyle='--')
            plt_otp_iv.xaxis.set_major_locator(mdates.WeekdayLocator())
            plt_otp_iv.xaxis.set_major_formatter(mdates.DateFormatter('%b %d %Y'))
            plt_otp_iv.set_xlim([args.start_date, args.end_date])
            plt_otp_iv.set_yticklabels(["{:,}".format(x/pow(10,5)) for x in plt_otp_iv.get_yticks()])
            plt_otp_iv.set_ylabel("USD Hundred Thousands")
            
            plt_otp_iv.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'medium'}, framealpha=0.3)      

               
            plot_dates = set(du_rr.rrule(du_rr.WEEKLY, 
                                         dtstart=df_options_analytics.AS_OF_DATE.max() - datetime.timedelta(days=6*7), 
                                         until=df_options_analytics.AS_OF_DATE.max(), 
                                         interval=2))
                                         
            plot_dates.add(df_options_analytics.AS_OF_DATE.max())
            plot_dates = sorted(map(lambda x: x.date(), plot_dates))
            
            df_options_for_ticker_plt_dates = pandas.merge(left=df_options_for_ticker[df_options_for_ticker.AS_OF_DATE.isin(plot_dates)], 
                                                           right=fund_notional_bydate, 
                                                           left_on='AS_OF_DATE', 
                                                           right_on='AS_OF_DATE',
                                                           how='left',
                                                           suffixes=['','_FUND'])
                                                           
            df_options_for_ticker_plt_dates['NOTIONAL_EXP_PCT_FUND'] = df_options_for_ticker_plt_dates.NOTIONAL_EXP / df_options_for_ticker_plt_dates.NOTIONAL_EXP_FUND
            
            
            pivot_delta_ts = pandas.pivot_table(df_options_for_ticker_plt_dates,
                                                 values=['DELTA'], 
                                                 index=['AS_OF_DATE','STRIKE_PRICE'],
                                                 columns=['CALL_PUT','DAYS_TO_EXPIRY'],
                                                 aggfunc=numpy.average, 
                                                 margins=False)
                                                 
            pivot_delta_ts.to_excel(writer, index=True, startrow = 0, startcol = 0, sheet_name='Delta {}'.format(ticker))
                                                 
                                                 
            pivot_notional_ts = pandas.pivot_table(df_options_for_ticker_plt_dates,
                                                 values=['NOTIONAL_EXP_PCT_FUND'], 
                                                 index=['AS_OF_DATE','STRIKE_PRICE'],
                                                 columns=['CALL_PUT','DAYS_TO_EXPIRY'],
                                                 aggfunc=numpy.sum, 
                                                 margins=False)
                                                 
            pivot_notional_ts.to_excel(writer, index=True, startrow = 0, startcol = 0, sheet_name='Notional {}'.format(ticker))
                                                 
#            pivot_money_ts = pandas.pivot_table(df_options_for_ticker_plt_dates,
#                                                 values=['MONEYNESS'], 
#                                                 index=['AS_OF_DATE','STRIKE_PRICE'],
#                                                 columns=['CALL_PUT','DAYS_TO_EXPIRY'],
#                                                 aggfunc=numpy.average, 
#                                                 margins=False)
#                                                 
#            pivot_money_ts.to_excel(writer, index=True, startrow = 0, startcol = 0, sheet_name='Moneyness {}'.format(ticker))
                                                 
            #radius = 10.0**2 * numpy.pi
            
            #gamma_multiplier = 100 if  df_options_analytics[df_options_analytics.UNDER_TICKER==ticker].GAMMA.apply(lambda x: abs(x)).median()<0.01 else 10
            #fig2,(plt_tbl_notional, ax5, ax6) = plt.subplots(nrows=3, ncols=1, sharex=False, subplot_kw=None, gridspec_kw={'height_ratios':[10, 10, 10]})
               
            #fig2.set_figheight(22)
            #fig2.set_figwidth(17)
#            row = 0
#            spaces = 2
#
#            for j, run_date in enumerate(plot_dates):
#                
#                df_options_for_ticker_asofdate = df_options_for_ticker[df_options_for_ticker.AS_OF_DATE==run_date]
#
#
#
#                pivot_opt_notional = pandas.pivot_table(df_options_for_ticker_asofdate,
#                                                         values=['NOTIONAL_EXP'], 
#                                                         index=['STRIKE_PRICE'],
#                                                         columns=['CALL_PUT','EXPIRATION_DATE'],
#                                                         aggfunc=numpy.sum, 
#                                                         margins=False) / df_options_notional_by_date[df_options_notional_by_date.AS_OF_DATE==run_date].NOTIONAL_EXP.max()  
#
#
#                pivot_opt_count = pandas.pivot_table(df_options_for_ticker_asofdate,
#                                                         values=['UNITS'], 
#                                                         index=['STRIKE_PRICE'],
#                                                         columns=['CALL_PUT','EXPIRATION_DATE'],
#                                                         aggfunc=numpy.sum, 
#                                                         margins=False)
#
#
#                pivot_opt_delta = pandas.pivot_table(df_options_for_ticker_asofdate,
#                                                         values=['DELTA'], 
#                                                         index=['STRIKE_PRICE'],
#                                                         columns=['CALL_PUT','EXPIRATION_DATE'],
#                                                         aggfunc=numpy.average, 
#                                                         margins=False)
#
#                pivot_opt_delta.to_excel(writer, index=True, startrow = row, startcol = 0, sheet_name='Delta {}'.format(ticker))
#                pivot_opt_notional.to_excel(writer, index=True, startrow = row, startcol = 0, sheet_name='Notional {}'.format(ticker))
#                pivot_opt_count.to_excel(writer, index=True, startrow = row, startcol = 0, sheet_name='Contracts {} '.format(ticker))
#                
#                row = row + len(pivot_opt_count.index) + spaces + 1

                #pandas.tools.plotting.table(plt_tbl_notional, numpy.round(pivot_opt_notional.describe(), 2))


#                x_axis_label, y_axis_label = numpy.meshgrid(sorted(df_options_for_ticker_asofdate.DAYS_TO_EXPIRY.unique()),
#                                                            sorted(df_options_for_ticker_asofdate.STRIKE_PRICE.unique()))
#    
#                x_axis_step, y_axis_step = (1,1)
                
#                h = ax4.scatter(x = df_options_for_ticker_asofdate.YRS_TO_EXPIRY * DAYS_IN_YEAR, 
#                                y = df_options_for_ticker_asofdate.STRIKE_PRICE,
#                                s = abs(df_options_for_ticker_asofdate.DELTA) * radius,
#                                color=colors[j],
#                                alpha=0.3,
#                                edgecolors='none',
#                                label = "")
#
#                ax4.axhline(df_options_for_ticker_asofdate.PX_LAST.max(), color=colors[j], label="")
#                ax4.scatter(x=None, y=None, s=radius, color=colors[j], alpha=0.3, edgecolor='none', label="{0}".format(run_date.strftime("%b %d")))

                                
#                h = ax5.scatter(x = df_options_for_ticker_asofdate.YRS_TO_EXPIRY * DAYS_IN_YEAR, 
#                                y = df_options_for_ticker_asofdate.STRIKE_PRICE,
#                                s = abs(df_options_for_ticker_asofdate.VEGA) * radius,
#                                color=colors[j],
#                                alpha=0.3,
#                                edgecolors='none',
#                                label = "")
#
#                ax5.axhline(df_options_for_ticker_asofdate.PX_LAST.max(), color=colors[j], label="")
#                ax5.scatter(x=None,y=None , s = radius, color=colors[j], alpha=0.3, edgecolor='none', label="{0}".format(run_date.strftime("%b %d")))
#
#
#                h = ax6.scatter(x = df_options_for_ticker_asofdate.YRS_TO_EXPIRY * DAYS_IN_YEAR, 
#                                y = df_options_for_ticker_asofdate.STRIKE_PRICE,
#                                s = abs(df_options_for_ticker_asofdate.THETA) * radius,
#                                color=colors[j],
#                                alpha=0.3,
#                                edgecolors='none',
#                                label="{0}".format(run_date.strftime("%b %d")))
#
#                h = ax6.axhline(df_options_for_ticker_asofdate.PX_LAST.max(),color=colors[j], label="")                
#

                                   
            ## END for j, run_date in enumerate( [args.start_date, args.end_date]):
                    
#            ax4.set_title("{0} Close Price And Options Delta".format( ticker))
#            ax4.set_ylabel("Strike Price")
#            ax4.set_xlabel("Days To Maturity")
#            ax4.grid(b=True, which='major', color='black', linestyle='--')
#            #ax4.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'medium'}) 
#            ax4.legend(loc="best", fancybox=True, shadow=True, scatterpoints=1, markerscale=1.0, framealpha=0.3) 

#            ax5.set_title("{0} Close Price And Options Vega".format( ticker))
#            ax5.set_ylabel("Strike Price")
#            ax5.set_xlabel("Days To Maturity")
#            ax5.grid(b=True, which='major', color='black', linestyle='--')
#            ax5.legend(loc="best", fancybox=True, shadow=True, scatterpoints=1, markerscale=1.0, framealpha=0.3) 
#
#            ax6.set_title("{0} Close Price And Options Theta".format( ticker))
#            ax6.set_ylabel("Strike Price")
#            ax6.set_xlabel("Days To Maturity")
#            ax6.grid(b=True, which='major', color='black', linestyle='--')
#            ax6.legend(loc="best", fancybox=True, shadow=True, scatterpoints=1, markerscale=1.0, framealpha=0.3) 

            if args.pdf:
                pp.savefig(fig)
                #pp.savefig(fig2)
                
            else:
                plt.show()

            plt.close()

            pdf_dict = pp.infodict()
            pdf_dict['Title'] = 'Options Writing Team Strategy {} report'.format(account)
            pdf_dict['Author'] = u'Dmitry Reznikov'
            pdf_dict['Subject'] = 'Options Writing Team Strategy {} report'.format(account)
            pdf_dict['CreationDate'] = datetime.datetime.today()
            pdf_dict['ModDate'] = datetime.datetime.today()

        ## END for ticker, bbg_suffix in underlying_list:

        if (args.pdf):
            pp.close()

        for worksheet in writer.book.worksheets():
            worksheet.set_column('A:A', width=15)
            
            if worksheet.name.startswith('Delta'):
                worksheet.set_column('B:Z', width=10, cell_format=format_num)
                
            elif worksheet.name.startswith('Notional'):
                worksheet.set_column('B:Z', width=10, cell_format=format_pct)
            
            elif worksheet.name.startswith('Contracts'):
                worksheet.set_column('B:Z', width=10, cell_format=format_par)

            elif worksheet.name.startswith('Moneyness'):
                worksheet.set_column('B:Z', width=10, cell_format=format_pct)

        writer.book.worksheets    
        writer.save()
        writer.close()

    #END for account in pmr_account_list:
            
    bh.bbg_shutdown()
        
#            plt.legend((lo, ll, l, a, h, hh, ho),
#                       ('Low Outlier', 'LoLo', 'Lo', 'Average', 'Hi', 'HiHi', 'High Outlier'),
#                       scatterpoints=1,
#                       loc='lower left',
#                       ncol=3,
#                       fontsize=8)
#
            #ax2.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'small'})   



            ##PLOT ivol fo all options
#            df_options_for_ticker=df_options_analytics[df_options_analytics.UNDER_TICKER==ticker]    
#
#            for i, secid in enumerate(set(df_options_for_ticker.PRIMARY_SECURITY_ID)):
#                color = cmap(float(i)/10)
#
#                ivol = df_options_for_ticker[df_options_for_ticker.PRIMARY_SECURITY_ID == secid][["AS_OF_DATE","IVOL","DESCRIPTION"]].fillna(0)
#                
#                name_str = "{0}".format(list(ivol['DESCRIPTION'])[0])
#
#                h = ax2.plot(ivol['AS_OF_DATE'], ivol['IVOL'], color=color, marker='', linestyle='-', label=name_str)          
#    
#                
#                #ax2.set_yticklabels(["{:,}".format(x/pow(10,5)) for x in ax3.get_yticks()])
#                ax2.axhline(0,color='black')
#                ax2.set_ylabel("Option Implied Volatility")
#                ax2.grid(b=True, which='major', color='black', linestyle='--')
#                ax2.xaxis.set_major_locator(mdates.WeekdayLocator())
#                ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %d %Y'))
#                plt.xlim(args.start_date, args.end_date)
#                
#                ax2.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'small'})   
            #plt.tight_layout()

            #fig.autofmt_xdate()







#            ###############################################
#            # CONSTRUCT SURFACES USING BLOOMBERG OPTIONS PRICING
#            ###############################################
#            if False:
#                strike_min, strike_max = df_options_for_ticker.STRIKE_PRICE.min(), df_options_for_ticker.STRIKE_PRICE.max()
#                strike_step = numpy.diff(df_options_for_ticker.STRIKE_PRICE.sort_values().unique()).min()
#                
#                bbg_option_ticker_list = ["{0} {1} {2}{3}".format(ticker, d.strftime('%m/%d/%y'), t, s) 
#                                            for d in  pandas.to_datetime(df_options_for_ticker.EXPIRATION_DATE.sort_values().unique()) 
#                                            for s in numpy.arange(strike_min, strike_max, strike_step)
#                                            for t in ['P']]
#                                                
#                                                
#                                                
#                                                
#                (bbg_options_prices, bbg_options_exceptions) = bh.bbg_get_hist_data(cusip_list=bbg_option_ticker_list,
#                                                                                     start_date=args.start_date,
#                                                                                     end_date=args.end_date,
#                                                                                     yellow_key=bbg_suffix,
#                                                                                     bbg_mnemonic=bbg_mnemonic_list,
#                                                                                     freq="DAILY",
#                                                                                     days="ACTUAL",
#                                                                                     override_prc=False)
#                
#                
#                df_bbg_options_list=[]
#                for results in bbg_options_prices:
#                    for bbg_secid, v in results.iteritems():
#                        
#                        df_bbg_options_list.append(pandas.DataFrame(zip([bbg_secid]*len(v), 
#                                                                    [vv[1] for vv in v if vv[0]=='date'], 
#                                                                    [vv[1] for vv in v if vv[0]==bbg_mnemonic_list[0]],
#                                                                    [vv[1] for vv in v if vv[0]==bbg_mnemonic_list[1]],
#                                                                    [vv[1] for vv in v if vv[0]==bbg_mnemonic_list[2]],
#                                                                    [vv[1] for vv in v if vv[0]==bbg_mnemonic_list[3]]),
#                                                                columns=["BBG_ID","DATE"] + bbg_mnemonic_list ))
#                
#                df_bbg_options_prices = pandas.concat(df_bbg_options_list, ignore_index=True)
#                
#                df_bbg_options_prices['MP_DATE'] = mdates.date2num(df_bbg_options_prices['DATE'].astype(datetime.date))    
#                df_bbg_options_prices["DATE"]    = pandas.to_datetime(df_bbg_options_prices.DATE).values
#                
#                df_bbg_ticker_dirty                 = pandas.DataFrame(df_bbg_options_prices.BBG_ID.apply(lambda x: pandas.Series(x.split(" "))))
#                df_bbg_ticker_dirty.columns         = ['TICKER','EXPIRATION_DATE','STRIKE','BBG_SUFFIX']
#                df_bbg_ticker_dirty.EXPIRATION_DATE = pandas.to_datetime(df_bbg_ticker_dirty.EXPIRATION_DATE)
#                df_bbg_ticker_dirty['CALL_PUT']     = df_bbg_ticker_dirty.STRIKE.str[:1]
#                df_bbg_ticker_dirty.STRIKE          = df_bbg_ticker_dirty.STRIKE.str[1:].astype(float)
#                df_bbg_ticker_dirty["BBG_ID"]       = df_bbg_options_prices.BBG_ID
#                df_bbg_ticker_dirty = df_bbg_ticker_dirty.drop_duplicates().sort_values("BBG_ID")
#                
#                df_bbg_options_analytics = pandas.merge(left=df_bbg_options_prices, 
#                                                        right=df_bbg_ticker_dirty[['BBG_ID','EXPIRATION_DATE','STRIKE','CALL_PUT']], 
#                                                        left_on='BBG_ID', 
#                                                        right_on='BBG_ID', 
#                                                        how='left')
#                                                        
#                df_bbg_options_analytics = pandas.merge(left=df_bbg_options_analytics, 
#                                                        right=df_bbg_prices[df_bbg_prices.TICKER==ticker], 
#                                                        left_on='DATE',
#                                                        right_on='DATE', 
#                                                        suffixes=['_OPTION','_UNDER'],
#                                                        how='left')
#                
#                df_bbg_options_analytics['YRS_TO_EXPIRY']  = (df_bbg_options_analytics.EXPIRATION_DATE - df_bbg_options_analytics.DATE) / datetime.timedelta(days=DAYS_IN_YEAR)
#                df_bbg_options_analytics['DAYS_TO_EXPIRY'] = (df_bbg_options_analytics.EXPIRATION_DATE - df_bbg_options_analytics.DATE) / datetime.timedelta(days=1)
#    
#                df_bbg_options_analytics["EXPOSURE_SIGN"]  = df_bbg_options_analytics.apply(func=lambda x: -1.0 if x.CALL_PUT.lower()=='p'else 1.0, axis=1)
#                df_bbg_options_analytics['MONEYNESS']      = df_bbg_options_analytics.EXPOSURE_SIGN * ( 1.0 - df_bbg_options_analytics.STRIKE / df_bbg_options_analytics.PX_LAST_UNDER )
#            
#                # clean up: delete options expiring on as of date                                                            
#                #df_options_analytics = df_options_analytics[abs(df_options_analytics.YRS_TO_EXPIRY)>0]
#                #no_strikes_per_maturity = df_options.groupby(['AS_OF_DATE','CALL_PUT','UNDER_TICKER','EXPIRATION_DATE'], as_index=False).aggregate('count').SECURITY_NUMBER
#                
#                df_bbg_options_analytics['IVOL'] = df_bbg_options_analytics.apply(func=lambda x: bs_vol.implied_volatility(price=x.PX_LAST_OPTION,
#                                                                                                                        S=x.PX_LAST_UNDER,
#                                                                                                                        K=x.STRIKE, 
#                                                                                                                        t=x.YRS_TO_EXPIRY,
#                                                                                                                        r=0.,
#                                                                                                                        flag=x.CALL_PUT.lower()), 
#                                                                        axis=1)
#                # clean up:  reset options implied vol to infinite
#                df_bbg_options_analytics.loc[abs(df_bbg_options_analytics.IVOL)>10, 'IVOL'] = numpy.inf        
#                
#                #option price change per 1 point change in underlying price
#                df_bbg_options_analytics['DELTA'] =  df_bbg_options_analytics.apply(func=lambda x: bs_greeks.delta(S=x.PX_LAST_UNDER,
#                                                                                                K=x.STRIKE, 
#                                                                                                sigma=x.IVOL,
#                                                                                                t=x.YRS_TO_EXPIRY,
#                                                                                                r=0.,
#                                                                                                flag=x.CALL_PUT.lower()),
#                                                                     axis=1) * df_bbg_options_analytics.EXPOSURE_SIGN 
#                                                                     
#                # option price change per 1 point change in underlying price                                                     
#                df_bbg_options_analytics['GAMMA'] = df_bbg_options_analytics.apply(func=lambda x: bs_greeks.gamma(S=x.PX_LAST_UNDER,
#                                                                                                K=x.STRIKE, 
#                                                                                                sigma=x.IVOL,
#                                                                                                t=x.YRS_TO_EXPIRY,
#                                                                                                r=0.,
#                                                                                                flag=x.CALL_PUT.lower()),
#                                                                     axis=1) * df_bbg_options_analytics.EXPOSURE_SIGN  ##* (df_options_analytics.PX_LAST /100.0)
#                                                                     
#                # option price change 1 calendar day decrease in time to maturity        
#                df_bbg_options_analytics['THETA'] = df_bbg_options_analytics.apply(func=lambda x: bs_greeks.theta(S=x.PX_LAST_UNDER,
#                                                                                                K=x.STRIKE, 
#                                                                                                sigma=x.IVOL,
#                                                                                                t=x.YRS_TO_EXPIRY,
#                                                                                                r=0.,
#                                                                                                flag=x.CALL_PUT.lower()),
#                                                                     axis=1) * df_bbg_options_analytics.EXPOSURE_SIGN 
#                #option price change per 1 point rise in volatility         
#                df_bbg_options_analytics['VEGA'] = df_bbg_options_analytics.apply(func=lambda x: bs_greeks.vega(S=x.PX_LAST_UNDER,
#                                                                                                K=x.STRIKE, 
#                                                                                                sigma=x.IVOL,
#                                                                                                t=x.YRS_TO_EXPIRY,
#                                                                                                r=0.,
#                                                                                                flag=x.CALL_PUT.lower()),
#                                                                     axis=1) * df_bbg_options_analytics.EXPOSURE_SIGN 
#                                                                     
#                #option price change per 1 point rise in risk-free rate                                                     
#                df_bbg_options_analytics['RHO'] = df_bbg_options_analytics.apply(func=lambda x: bs_greeks.rho(S=x.PX_LAST_UNDER,
#                                                                                                K=x.STRIKE, 
#                                                                                                sigma=x.IVOL,
#                                                                                                t=x.YRS_TO_EXPIRY,
#                                                                                                r=0.,
#                                                                                                flag=x.CALL_PUT.lower()),
#                                                                     axis=1) * df_bbg_options_analytics.EXPOSURE_SIGN
#    
#            #END if False:
#            ###############################################
#            # CONSTRUCT SURFACES USING BLOOMBERG OPTIONS PRICING
#            ###############################################




            #####################################
            # TEST OUT IVOL SURFACE ON BBG OPTIONS MESHGRID
            #####################################
                                    
#            df_options_for_ticker_asofdate = df_options_for_ticker[df_options_for_ticker.DATE==df_options_for_ticker.AS_OF_DATE.max()]
#            
#            x_axis_label, y_axis_label = numpy.meshgrid(sorted(df_options_for_ticker_asofdate.DAYS_TO_EXPIRY.unique()),
#                                                        sorted(df_options_for_ticker_asofdate.STRIKE_PRICE.unique()))
#            
#            x_axis_step, y_axis_step = (1,max(1,int(strike_step)))
#            
#            h=ax12.plot_wireframe(X = x_axis_label,
#                                  Y = y_axis_label,
#                                  Z = df_options_for_ticker_asofdate.pivot(index='STRIKE_PRICE', columns='DAYS_TO_EXPIRY', values='MONEYNESS'),
#                                  color='r',
#                                  rstride=x_axis_step,
#                                  cstride=y_axis_step,
#                                  label="{0}".format(df_options_for_ticker.AS_OF_DATE.max().strftime("%b %d")))
#            ax12.set_title("{0} Options Moneyness".format( ticker))
#            ax12.set_zlabel("{0} Options Moneyness".format( ticker))
#            ax12.set_ylabel("Strike Price")
#            ax12.set_xlabel("Days To Maturity")
#            ax12.zaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.1%}'.format(y)))
#            ax12.legend(loc="best", fancybox=True, shadow=True, framealpha=0.3) 
#            
#            logging.info(df_options_for_ticker_asofdate.pivot(index='STRIKE_PRICE', columns='DAYS_TO_EXPIRY', values='MONEYNESS'))
#            
#            h = ax2.bar(left = df_options_for_ticker_asofdate.DAYS_TO_EXPIRY,
#                        height = abs(df_options_for_ticker_asofdate.NOTIONAL_EXP / df_options_for_ticker_asofdate.NOTIONAL_EXP.sum()),
#                        zs = df_options_for_ticker_asofdate.STRIKE_PRICE,
#                        zdir = 'y',
#                        color='r',
#                        alpha=0.5,
#                        edgecolor='none')
#                    
#            #3D bar plot legend does not work, requires a manual override called proxy artist
#            artist_proxy_list= []
#            legend_list = []
#            artist_proxy_list.append(plt.Rectangle((0,0), 1,1, fc='r'))
#            legend_list.append("{0}".format(df_options_for_ticker.AS_OF_DATE.max().strftime("%b %d")))
#
#            ax2.set_title("{0} Options Percent Notional Exposure".format( ticker))
#            ax2.set_zlabel("Percent")
#            ax2.set_ylabel("Strike Price")
#            ax2.set_xlabel("Days To Maturity")
#            ax2.zaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#            ax2.legend(artist_proxy_list, legend_list, loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'medium'}, framealpha=0.3)
#            
#            



#                h=ax7.plot_wireframe(X = x_axis_label,
#                                       Y = y_axis_label,
#                                       Z = df_options_for_ticker_asofdate.pivot(index='STRIKE_PRICE', columns='DAYS_TO_EXPIRY', values='IVOL'),
#                                       color=colors[j],
#                                       rstride=x_axis_step,
#                                       cstride=y_axis_step,
#                                       label="{0}".format(run_date.strftime("%b %d")))
#                
#                h=ax8.plot_wireframe(X = x_axis_label,
#                                   Y = y_axis_label,
#                                   Z = df_options_for_ticker_asofdate.pivot(index='STRIKE_PRICE', columns='DAYS_TO_EXPIRY', values='VEGA'),
#                                   color=colors[j],
#                                       rstride=x_axis_step,
#                                       cstride=y_axis_step,
#                                   label="{0}".format(run_date.strftime("%b %d")))
#
#                h=ax9.plot_wireframe(X = x_axis_label,
#                                   Y = y_axis_label,
#                                   Z = df_options_for_ticker_asofdate.pivot(index='STRIKE_PRICE', columns='DAYS_TO_EXPIRY', values='DELTA'),
#                                   color=colors[j],
#                                       rstride=x_axis_step,
#                                       cstride=y_axis_step,
#                                   label="{0}".format(run_date.strftime("%b %d")))
#
#                h=ax10.plot_wireframe(X = x_axis_label,
#                                   Y = y_axis_label,
#                                   Z = df_options_for_ticker_asofdate.pivot(index='STRIKE_PRICE', columns='DAYS_TO_EXPIRY', values='THETA'),
#                                   color=colors[j],
#                                       rstride=x_axis_step,
#                                       cstride=y_axis_step,
#                                   label="{0}".format(run_date.strftime("%b %d")))
#                                   
#                h=ax11.plot_wireframe(X = x_axis_label,
#                                       Y = y_axis_label,
#                                       Z = df_options_for_ticker_asofdate.pivot(index='STRIKE_PRICE', columns='DAYS_TO_EXPIRY', values='GAMMA'),
#                                       color=colors[j],
#                                       rstride=x_axis_step,
#                                       cstride=y_axis_step,
#                                       label="{0}".format(run_date.strftime("%b %d")))
#                                       
                                        


#                x_axis_step = int(numpy.diff(df_options_for_ticker_asofdate.DAYS_TO_EXPIRY.sort_values().unique()).min())
#                y_axis_step = int(numpy.diff(df_options_for_ticker_asofdate.STRIKE_PRICE.sort_values().unique()).min())
                
#                h = ax2.scatter(x = df_options_for_ticker_asofdate.YRS_TO_EXPIRY * DAYS_IN_YEAR, 
#                                y = df_options_for_ticker_asofdate.STRIKE_PRICE,
#                                s = radius * 100 * abs(df_options_for_ticker_asofdate.NOTIONAL_EXP / df_options_for_ticker_asofdate.NOTIONAL_EXP.sum()),
#                                color=colors[j],
#                                alpha=0.3,
#                                edgecolors='none',
#                                label = "")
#
#                ax2.axhline(df_options_for_ticker_asofdate.PX_LAST.max(), color=colors[j], label="")
#                ax2.scatter(x=None,y=None ,s=radius, color=colors[j], alpha=0.3, edgecolor='none', label="{0}".format(run_date.strftime("%b %d")))                                    
                
#                h = ax2.bar(left = df_options_for_ticker_asofdate.DAYS_TO_EXPIRY,
#                            height = abs(df_options_for_ticker_asofdate.NOTIONAL_EXP / df_options_for_ticker_asofdate.NOTIONAL_EXP.sum()),
#                            zs = df_options_for_ticker_asofdate.STRIKE_PRICE,
#                            zdir = 'y',
#                            color=colors[j],
#                            alpha=0.3,
#                            edgecolor='none')
#                            
#                #3D bar plot legend does not work, requires a manual override called proxy artist
#                artist_proxy_list.append(plt.Rectangle((0,0), 1,1, fc=colors[j]))
#                legend_list.append("{0}".format(run_date.strftime("%b %d")))
#                
            #ax2.set_xlim([0, x_axis_max*1.05])
            #ax2.set_ylim([0.95*y_axis_min, 1.05*y_axis_max])

#            ax2.set_title("{0} Close Price And Options Notional As Percent Of Total".format( ticker))
#            ax2.set_ylabel("Strike Price")
#            ax2.set_xlabel("Days To Maturity")
#            ax2.grid(b=True, which='major', color='black', linestyle='--')
#            #ax2.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'medium'}) 
#            ax2.legend(loc="best", fancybox=True, shadow=True) 
        


                #ax6.scatter(x=None,y=None , s = radius, color=colors[j], alpha=0.3, edgecolor='none', label="{0}".format(run_date.strftime("%b %d")))
#                h = ax5.scatter(x = df_options_for_ticker_asofdate.YRS_TO_EXPIRY * DAYS_IN_YEAR, 
#                                y = df_options_for_ticker_asofdate.STRIKE_PRICE,
#                                s = abs(df_options_for_ticker_asofdate.GAMMA) * radius * gamma_multiplier,
#                                color=colors[j],
#                                alpha=0.3,
#                                edgecolors='none',
#                                label = "")
#                h=ax8.scatter(xs=df_options_for_ticker_asofdate.DAYS_TO_EXPIRY,
#                            ys=df_options_for_ticker_asofdate.STRIKE_PRICE,
#                            zs=df_options_for_ticker_asofdate.THETA,
#                            c=colors[j],
#                            marker=markers[j],
#                            alpha=0.3,
#                            edgecolor='none', 
#                            label="{0}".format(run_date.strftime("%b %d")))
