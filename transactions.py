import pyodbc
import argparse
import datetime
import logging
import pandas
import dateutil
import matplotlib.pyplot as plt
import matplotlib.dates as dates
import numpy as np

import bbg_api.bbg_api_wrap as bbg
from matplotlib.backends.backend_pdf import PdfPages

str_sql_citco_transact = '''
SELECT 
c.AS_OF_DATE, c.SETTLE_DATE, c.AEXEO_ORDER_ID, c.AEXEO_TRADE_ID, c.BASE_CURRENCY,c.BASE_PRICE , c.BASE_UNIT_COST,c.COST, c.EXECUTION_BROKER,c.FUND,c.ISSUE_CURRENCY,c.ISSUE_MARKET_VALUE, c.ISSUE_TOTAL_COST,c.ISSUE_TOTAL_COST,c.LONG_SHORT,c.MARKET_VALUE,c.MODIFIED_DATE, c.NET_SETTLEMENT_AMOUNT,
c.ORIGINAL_FACE,c.RED, c.REPO,c.SIDE,c.STATUS, c.STRATEGY,c.TRADE_CURRENCY,c.LAST_UPDATED,c.PERIOD_END_DATE, c.KNOWLEDGE_TIMESTAMP, c.PRIME_BROKER_CLEARING_BROKER_LONG_NAME, c.MNG_CLEARING_BROKER,
s.SECURITY_DESCRIPTION,s.SECURITY_DESCRIPTION2,s.SECURITY_NAME,s.NB_ASSET_CLASS,s.NB_SECURITY_GROUP, s.NB_SECURITY_TYPE_1, s.NB_SECURITY_TYPE_2,s.BB_SECURITY_TYPE,s.BB_SECURITY_TYPE_2,s.TICKER,s.CUSIP,s.ISIN,
s.MATURITY_DATE, s.FIRST_SETTLEMENT_DATE,s.AMOUNT_ISSUED, s.COLLATERAL_TYPE,
s.BB_GLOBAL_ID, s.ALADDIN_SECURITY_ID, s.UNDERLYING_ID, s.ASSET_BENCHMARK, s.INDUSTRY_GROUP, s.INDUSTRY_SECTOR, s.INDUSTRY_SUBGROUP,
i.NAME, i.PARENT_NB_ID, i.ULTIMATE_PARENT_NAME, i.ULTIMATE_PARENT_NB_ID
FROM FACT_TRANSACTION_CITCO c 
INNER JOIN (
	SELECT MAX(AEXEO_TRADE_ID) AS AEXEO_TRADE_ID 
	FROM FACT_TRANSACTION_CITCO cc
	WHERE cc.AS_OF_DATE BETWEEN '{0}' AND '{1}'
	AND cc.PROVIDER='PRIVATE'
	GROUP BY cc.AEXEO_ORDER_ID
) AS cc_last_trade ON c.AEXEO_TRADE_ID=cc_last_trade.AEXEO_TRADE_ID
LEFT OUTER JOIN core.vACCOUNT_OSP a ON a.NB_ID=c.ACCOUNT_EDM_ID
LEFT OUTER JOIN core.vSECURITY s ON s.NB_ID=c.SECURITY_EDM_ID
LEFT OUTER JOIN core.vISSUER i on i.NB_ID=s.ISSUER_NB_ID
WHERE c.AS_OF_DATE BETWEEN '{0}' AND '{1}'
AND c.PROVIDER='PRIVATE'
AND c.STATUS <> 'Canceled'
AND s.NB_ASSET_CLASS='Fixed Income'
ORDER BY c.AEXEO_ORDER_ID, c.MODIFIED_DATE
'''

str_sql_citco_pl = '''
SELECT
ps.AS_OF_DATE, s.NB_ID, MAX(s.ISIN) AS ISIN, MAX(s.NAME) as SEC_NAME, MAX(s.SECURITY_DESCRIPTION) AS SEC_DESCRIPTION, 
SUM(c.CURRENT_FACE) CURRENT_FACE,
SUM(c.YEARLY_BASE_PL) YEARLY_BASE_PL
FROM FACT_POSITION_CITCO c
LEFT OUTER JOIN core.vSECURITY s ON s.NB_ID=c.SECURITY_EDM_ID
INNER JOIN DIM_POSITION_SET_CURRENT ps ON ps.POSITION_SET_ID=c.POSITION_SET_ID AND ps.SOURCE='CITCO' AND ps.PROVIDER='PRIVATE'
WHERE ps.AS_OF_DATE BETWEEN '{0}' AND '{1}'
AND s.NB_ASSET_CLASS='Fixed Income' AND s.NB_SECURITY_GROUP <> 'M Market'
GROUP BY ps.AS_OF_DATE, s.NB_ID
ORDER BY ps.AS_OF_DATE, s.NB_ID
'''


str_sql_aladdin_transact = '''
SELECT 
al.AS_OF_DATE,al.TRD_ORIG_ENTRY_DATE,al.EXECUTION_TIME, al.TRD_TRADE_DATE, al.TRD_CURRENCY,al.TRD_PRICE, al.TRD_ORIG_FACE,
al.TRD_DIRTY_PRICE,al.TRD_STATUS,al.CPN_TYPE,al.DESC_INSTMT,al.CUSIP,al.ISIN,al.SEDOL, al.SECURITY_EDM_ID,al.ACCOUNT_EDM_ID,
al.DESK_TYPE,al.MATURITY,al.MTG_SUBTYPE,al.PORTFOLIOS_PORTFOLIO_NAME,al.SM_SEC_GROUP,al.SM_SEC_TYPE,al.TD_NUM,al.TICKER,al.TOUCH_COUNT,
al.TRAN_TYPE,al.TRAN_TYPE1,al.TRD_COUNTERPARTY,al.TRD_EXCHANGE_RATE,al.UNITS,al.ID,

s.SECURITY_DESCRIPTION,s.SECURITY_DESCRIPTION2,s.SECURITY_NAME,s.NB_ASSET_CLASS,s.NB_SECURITY_GROUP, s.NB_SECURITY_TYPE_1, s.NB_SECURITY_TYPE_2,
s.BB_SECURITY_TYPE,s.BB_SECURITY_TYPE_2,s.COUPON,
s.MATURITY_DATE, s.FIRST_SETTLEMENT_DATE,s.AMOUNT_ISSUED, s.COLLATERAL_TYPE,
s.BB_GLOBAL_ID, s.ALADDIN_SECURITY_ID, s.UNDERLYING_ID, s.ASSET_BENCHMARK, s.INDUSTRY_GROUP, s.INDUSTRY_SECTOR, s.INDUSTRY_SUBGROUP,

i.NAME, i.PARENT_NB_ID, i.ULTIMATE_PARENT_NAME, i.ULTIMATE_PARENT_NB_ID
FROM FACT_TRANSACTION_ALADDIN al
RIGHT OUTER JOIN core.vACCOUNT_OSP a ON a.NB_ID=al.ACCOUNT_EDM_ID 
LEFT OUTER JOIN core.vSECURITY s ON s.NB_ID=al.SECURITY_EDM_ID
LEFT OUTER JOIN core.vISSUER i ON i.NB_ID=s.ISSUER_NB_ID
WHERE 
al.TRD_TRADE_DATE BETWEEN '{0}' and '{1}'AND 
a.ALADDIN_ACCOUNT_ID LIKE 'OSP-%'
AND al.SM_SEC_GROUP IN ('BND')
AND al.TRAN_TYPE iN ('BUY','SELL', 'BUYCLOSE','SELLSHORT')
AND al.TRD_COUNTERPARTY NOT IN ('NONE', 'TEST', 'TST', 'ASSGN')
ORDER BY al.TRD_TRADE_DATE, al.DESC_INSTMT
'''

str_sql_aladdin_security_analytics = '''
SELECT ac.PUBLISHED_DATE, aa.SECURITY_EDM_ID, s.ISIN, s.SECURITY_DESCRIPTION, aa.OAS, aa.SPREAD_DUR, aa.YIELD_TO_WORST 
FROM FACT_SECURITY_ANALYTIC_ALADDIN aa
INNER JOIN DIM_ANALYTIC_SET_CURRENT ac ON ac.ANALYTIC_SET_ID=aa.ANALYTIC_SET_ID AND ac.PROVIDER='OSP' AND ac.SOURCE='Aladdin' AND ac.TYPE='Portfolio'
LEFT OUTER JOIN core.vSECURITY s ON s.NB_ID=aa.SECURITY_EDM_ID
WHERE ac.PUBLISHED_DATE BETWEEN '{0}' AND '{1}'
AND s.NB_ASSET_CLASS='Fixed Income' AND s.NB_SECURITY_GROUP <> 'M Market'
ORDER BY ac.PUBLISHED_DATE, s.SECURITY_DESCRIPTION
'''
str_sql_aladdin_position_analytics='''
SELECT ac.AS_OF_DATE, s.NB_ID, SUM(aa.SPD_DV01) AS SDV01, SUM(aa.DOLLAR_DURATION) AS DV01
FROM vDIM_POSITION aa
INNER JOIN DIM_POSITION_SET_CURRENT ac ON ac.POSITION_SET_ID=aa.POSITION_SET_ID AND ac.PROVIDER='OSP' AND ac.SOURCE='Aladdin' AND ac.TYPE='Portfolio'
LEFT OUTER JOIN core.vSECURITY s ON s.NB_ID=aa.SECURITY_EDM_ID
WHERE ac.AS_OF_DATE BETWEEN '{0}' AND '{1}'
AND s.NB_ASSET_CLASS='Fixed Income' AND s.NB_SECURITY_GROUP <> 'M Market'
GROUP BY ac.AS_OF_DATE, s.NB_ID
ORDER BY ac.AS_OF_DATE, s.NB_ID
'''

str_sql_dates = '''
        SELECT cc.as_of_date FROM DIM_POSITION_SET_CURRENT cc 
        WHERE cc.PROVIDER = 'Private' 
            AND cc.SOURCE = 'Citco' 
        ORDER BY cc.AS_OF_DATE DESC
    '''
    
def date_to_int(d):
    if isinstance(d, datetime.date):
        return d.year*10000+d.month*100+d.day
    else:
        return d
    
if __name__ == '__main__':

        
    engine = pyodbc.connect(Trusted_Connection='yes',
                            driver='{SQL Server}',
                            server='PIPWSQL023B\PFRM802',
                            database='DM_Operations')

    df_dates = pandas.read_sql_query(str_sql_dates, engine)
        
    latest_date = df_dates.as_of_date[0].date()
    
    arg_parser = argparse.ArgumentParser(description='Run OSP transaction analysis report')
    
    def parse_date(d):
        return dateutil.parser.parse(d).date()
        
    arg_parser.add_argument('--end_date',   default=latest_date, help='End of Reporting Period, default last business day', type=parse_date)
    arg_parser.add_argument('--start_date', default=datetime.date(2014,10,24), help='Start of Reporting Period, default 10/24/2014', type=parse_date)
    arg_parser.add_argument('--pdf',        default=False, action='store_true', help='Generate PDF document with all charts')
    arg_parser.add_argument('--loglevel',   default="INFO", choices={"WARN","INFO","DEBUG"}, help='Logging level verbosity')
    arg_parser.add_argument('--outdir',     default="./", help='Report Output Directory')
    #arg_parser.add_argument('--issuer',     default="PETROBRAS INTERNATIONAL FINANCE", help='Process single issuer')
    arg_parser.add_argument('--issuer',     help='Process single issuer')
    
    
    args = arg_parser.parse_args()
    
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: {}'.format(args.loglevel))

    if latest_date < args.end_date:
        args.end_date = latest_date

    logging.basicConfig(filename="{1}/transact_osp_{0}_{2}_{3}.log".format(args.issuer.replace('/','-') if args.issuer is not None else "ALL", args.outdir, args.start_date, args.end_date),
                        filemode="w",
                        level=numeric_level,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        format="%(asctime)s:%(levelname)s:%(message)s")

    #logging.info("Run-time parameters:{}", args)
    
    df_transact = pandas.read_sql_query(str_sql_aladdin_transact.format(args.start_date, args.end_date), engine, parse_dates=['AS_OF_DATE'])
     
    df_position_pl = pandas.read_sql_query(str_sql_citco_pl.format(args.start_date, args.end_date), engine, parse_dates=['AS_OF_DATE'])

    df_position_risk1 = pandas.read_sql_query(str_sql_aladdin_security_analytics.format(date_to_int(args.start_date), date_to_int(args.end_date)), engine)
    df_position_risk1["AS_OF_DATE"]= pandas.to_datetime(df_position_risk1.PUBLISHED_DATE, format="%Y%m%d")

    df_position_risk2 = pandas.read_sql_query(str_sql_aladdin_position_analytics.format(date_to_int(args.start_date), date_to_int(args.end_date)), engine, parse_dates=['AS_OF_DATE'])
    ##filter out outlier SDV01 values
    df_position_risk2 = df_position_risk2[np.abs(df_position_risk2.SDV01)<20000]
    
    df_position_risk = pandas.merge(left=df_position_risk1, right=df_position_risk2, how='outer', left_on=['AS_OF_DATE','SECURITY_EDM_ID'], right_on=['AS_OF_DATE','NB_ID'])
    
    bh = bbg.bbg_helper()
    bh.bbg_init()
    
        
#        (res, exc) = bh.bbg_get_hist_data(["TFCIX", "AAPL"],
#                                        datetime.date(2013,12,31),
#                                        datetime.date(2014,1,31),
#                                        "Equity",
#                                        ["PX_LAST","BID"],
#                                        "WEEKLY",
#                                        "ACTUAL")
        
    #df_transact = df_transact[ df_transact.TRD_PRICE<60 ]
    
    if args.issuer is not None:                                    
        
        df_transact = df_transact[df_transact.ULTIMATE_PARENT_NAME==args.issuer]
        
        if len(df_transact)==0:
            raise Exception("Issuer '{0}' was not found".format(args.issuer))
    
    secid_type_list = ['SECURITY_EDM_ID','BB_GLOBAL_ID','TICKER','CUSIP','ISIN','SEDOL']

    all_id_map = df_transact[secid_type_list].drop_duplicates()
    all_id_map["BBG_ID"]= all_id_map.apply(func=lambda x: x.ISIN if x.ISIN is not None else x.CUSIP if x.CUSIP is not None else x.SEDOL if x.SEDOL is not None else x.BB_GLOBAL_ID if x.BB_GLOBAL_ID is not None else None, axis=1)
    
    bbg_to_edm_id_map=all_id_map[["SECURITY_EDM_ID",'BBG_ID']].drop_duplicates()
    
    (res, exc) = bh.bbg_get_hist_data(bbg_to_edm_id_map.BBG_ID,
                                      args.start_date,
                                      args.end_date,
                                      "Corp",
                                      ["PX_LAST"],
                                      "DAILY",
                                      "ACTUAL",
                                      True)

    df_bbg_list=[]
    for results in res:
        for bbg_secid, v in results.iteritems():
            df_bbg_list.append(pandas.DataFrame(zip([bbg_secid]*len(v), 
                                                    [vv[1] for vv in v if vv[0]=='date'], 
                                                    [vv[1] for vv in v if vv[0]=='PX_LAST']), columns=["BBG_ID","DATE", "PX_CLOSE"]))
    
    df_bbg_prices = pandas.concat(df_bbg_list, ignore_index=True)
    
    if (args.pdf):
        pp = PdfPages('osp_bond_transactions_by_issue_{2}_{0}_{1}.pdf'.format(args.start_date, args.end_date, args.issuer.replace('/','-') if args.issuer is not None else "ALL"))

    for issuer in sorted(df_transact.ULTIMATE_PARENT_NAME.unique()):

        transact_for_issuer = df_transact[df_transact.ULTIMATE_PARENT_NAME == issuer ]
        
        #for secid in set(transact_for_issuer[~transact_for_issuer["ISIN"].isnull()].ISIN):
        for secid in transact_for_issuer.SECURITY_EDM_ID.unique():
           
            trades = transact_for_issuer[transact_for_issuer.SECURITY_EDM_ID==secid]
            
            positions = df_position_pl[df_position_pl.NB_ID==secid].fillna(0)
            
            analytics = df_position_risk[df_position_risk.SECURITY_EDM_ID==secid].fillna(0)
            
            ## TODO Fix PL adjustment for any start date
            ## TODO remove hard-coded end of year adjustement and 
            ##replace with automatic adjustment for every year-end in 
            ## the span of start_date -> end_date
            ## Aladdin positions and analytics start 10/24/14
            ## citco PL starts 1/1/15
            ##
            ##roll aggregate 2015 P&L forward to 2016
            pl_2015_YTD = positions.loc[positions['AS_OF_DATE']==datetime.date(2015,12,31),"YEARLY_BASE_PL"].values
            if len(pl_2015_YTD)>0:
                positions.loc[positions['AS_OF_DATE']>datetime.date(2015,12,31), "YEARLY_BASE_PL"] += pl_2015_YTD
                
            
            fig, (plt_price, plt_trd_par, plt_pl, plt_sdv01, plt_ytw, plt_oas) = plt.subplots(nrows=6, ncols=1,sharex=True, subplot_kw=None, gridspec_kw={'height_ratios':[5,3,3,3,3,3]})
           
            fig.set_figheight(22)
            fig.set_figwidth(17)
                
            bbg_id = bbg_to_edm_id_map[bbg_to_edm_id_map.SECURITY_EDM_ID==secid].BBG_ID
            
            if bbg_id.size>0:
                bbg_id=bbg_id.values[0]
                plt_price.plot(df_bbg_prices[df_bbg_prices.BBG_ID=="{0} Corp".format(bbg_id)].DATE, 
                               df_bbg_prices[df_bbg_prices.BBG_ID=="{0} Corp".format(bbg_id)].PX_CLOSE, color='b', marker='', linestyle='-')
            else:
                logging.error("Did not find BBG_ID for Security EDM_ID={}".format(secid))
                
            
            plt_price.plot(trades.TRD_TRADE_DATE, trades.TRD_PRICE, color='r', marker='o', linestyle='')   
            
            plt_price.set_title("{0}:{1}:{2} {3} {4}".format( issuer, bbg_id, trades.TICKER.unique()[0], trades.COUPON.max(), trades.MATURITY_DATE.max().strftime("%Y/%m/%d")))
            plt_price.xaxis_date()
            plt_price.grid(b=True, which='major', color='black', linestyle='--')
            plt_price.set_ylabel("Closing Price")
            plt_price.set_yticklabels(['{:3.2f}'.format(x) for x in plt_price.get_yticks()])
            plt_price.xaxis.set_major_locator(dates.MonthLocator())
            #plt.xlim(args.start_date, args.end_date)
            
            plt_trd_par.stem(trades['TRD_TRADE_DATE'], trades['TRD_ORIG_FACE'])
            
            plt_trd_par.plot(positions['AS_OF_DATE'], positions['CURRENT_FACE'], color='g', marker='', linestyle='-')            
            plt_trd_par.xaxis_date()
            plt_trd_par.axhline(0,color='black')
            plt_trd_par.set_ylabel("Total Face Amount \nFace Amount Traded \n(Millions)")
            plt_trd_par.set_yticklabels(["{:,}".format(x/pow(10,6)) for x in plt_trd_par.get_yticks()])
            plt_trd_par.grid(b=True, which='major', color='black', linestyle='--')
            plt_trd_par.xaxis.set_major_locator(dates.MonthLocator())
            #plt.xlim(args.start_date, args.end_date)
            
            plt_pl.plot(positions['AS_OF_DATE'], positions['YEARLY_BASE_PL'], color='b', marker='o', linestyle='-')     
            
            plt_pl.set_yticklabels(["{:,}".format(x/pow(10,5)) for x in plt_pl.get_yticks()])
            plt_pl.axhline(0,color='black')
            plt_pl.set_ylabel("Total P&L\n(USD Hundred Thousand)")
            plt_pl.grid(b=True, which='major', color='black', linestyle='--')
            plt_pl.xaxis.set_major_locator(dates.MonthLocator())
            plt_pl.xaxis.set_major_formatter(dates.DateFormatter('%b %d %Y'))
            #plt.xlim(args.start_date, args.end_date)

            plt_sdv01.plot(analytics['AS_OF_DATE'], analytics['SDV01'], color='g', marker='.', linestyle='-')
            plt_sdv01.set_yticklabels(["{:3.1f}".format(x/1000) for x in plt_sdv01.get_yticks()])
            plt_sdv01.axhline(0,color='black')
            plt_sdv01.set_ylabel("SDV01\n(Thousands)")
            plt_sdv01.grid(b=True, which='major', color='black', linestyle='--')
            plt_sdv01.xaxis.set_major_locator(dates.MonthLocator())
            plt_sdv01.xaxis.set_major_formatter(dates.DateFormatter('%b %d %Y'))
            #plt.xlim(args.start_date, args.end_date)

            plt_ytw.plot(analytics['AS_OF_DATE'], analytics['YIELD_TO_WORST'], color='g', marker='.', linestyle='-')
            plt_ytw.set_yticklabels(["{:3.1f}%".format(x) for x in plt_ytw.get_yticks()])
            plt_ytw.axhline(0,color='black')
            plt_ytw.set_ylabel("YTW")
            plt_ytw.grid(b=True, which='major', color='black', linestyle='--')
            plt_ytw.xaxis.set_major_locator(dates.MonthLocator())
            plt_ytw.xaxis.set_major_formatter(dates.DateFormatter('%b %d %Y'))
            #plt.xlim(args.start_date, args.end_date)

            plt_oas.plot(analytics['AS_OF_DATE'], analytics['OAS'], color='g', marker='.', linestyle='-')
            plt_oas.set_yticklabels(["{:,}".format(x) for x in plt_oas.get_yticks()])
            plt_oas.axhline(0,color='black')
            plt_oas.set_ylabel("OAS")
            plt_oas.grid(b=True, which='major', color='black', linestyle='--')
            plt_oas.xaxis.set_major_locator(dates.MonthLocator())
            plt_oas.xaxis.set_major_formatter(dates.DateFormatter('%b %d %Y'))
            

            #plt.tight_layout()

            plt.xlim(args.start_date, args.end_date)
            fig.autofmt_xdate()

            if (args.pdf):
            
                pp.savefig()
            
                plt.close()
                
            else:
               
                plt.show()

    if args.pdf:
        pp.close()


    #plot all issues on the same page
    if args.pdf:
        pp = PdfPages('osp_bond_transactions_by_issuer_{2}_{0}_{1}.pdf'.format(args.start_date, args.end_date, args.issuer.replace('/','-') if args.issuer is not None else "ALL"))

    for issuer in sorted(set(df_transact.ULTIMATE_PARENT_NAME)):

        transact_for_issuer = df_transact[df_transact.ULTIMATE_PARENT_NAME == issuer ]
        
        fig, (ax1, ax2, ax3, ax4) = plt.subplots(nrows=4, ncols=1,sharex=True, subplot_kw=None, gridspec_kw={'height_ratios':[5,3,3,3]})
       
        fig.set_figheight(22)
        fig.set_figwidth(17)
        cmap = plt.get_cmap('jet_r')
        
        ax_handle_lst=[]
        ax_label_lst=[]
        df_position_lst=[]
        df_analytics_lst=[]
        
        for i, secid in enumerate(transact_for_issuer.SECURITY_EDM_ID.unique()):
            
            trades = transact_for_issuer[transact_for_issuer.SECURITY_EDM_ID==secid]
            
            positions = df_position_pl[df_position_pl.NB_ID==secid].fillna(0)
            
            analytics = df_position_risk[df_position_risk.SECURITY_EDM_ID==secid].fillna(0)

            #trades = transact_for_issuer[transact_for_issuer['ISIN']==secid]
            #positions = df_position_pl[df_position_pl['ISIN']==secid].fillna(0)
            #analytics = df_position_risk[df_position_risk['ISIN']==secid].fillna(0)
            
            df_position_lst.append(positions)
            
            df_analytics_lst.append(analytics)
            
            color = cmap(float(i)/10)
            
            ##roll aggregate 2015 P&L forward to 2016
            pl_2015_YTD = positions.loc[positions.AS_OF_DATE==datetime.date(2015,12,31),"YEARLY_BASE_PL"].values
            if len(pl_2015_YTD)>0:
                positions.loc[positions.AS_OF_DATE>datetime.date(2015,12,31), "YEARLY_BASE_PL"] += pl_2015_YTD
                
            name_str = "{0} {1} {2}".format(list(trades['TICKER'])[0],list(trades['COUPON'])[0], list(trades['MATURITY_DATE'])[0].strftime("%Y/%m/%d"))
            
            bbg_id = bbg_to_edm_id_map[bbg_to_edm_id_map.SECURITY_EDM_ID==secid].BBG_ID
            
            if bbg_id.size>0:
                bbg_id=bbg_id.values[0]
                h = ax1.plot(df_bbg_prices[df_bbg_prices["BBG_ID"]=="{0} Corp".format(bbg_id)].DATE, 
                            df_bbg_prices[df_bbg_prices["BBG_ID"]=="{0} Corp".format(bbg_id)].PX_CLOSE, 
                            marker='', 
                            linestyle='-', 
                            color=color,
                            label = name_str)
            else:
                logging.error("Did not find BBG_ID for Security EDM_ID={}".format(secid))
            
            ax1.plot(trades['TRD_TRADE_DATE'], trades['TRD_PRICE'], marker='o', linestyle='', color=color, label="")  

            ax_handle_lst.append(h)
            ax_label_lst.append(name_str)

            ax2.stem(trades['TRD_TRADE_DATE'], trades['TRD_ORIG_FACE'], color=color)
            
            ax2.plot(positions['AS_OF_DATE'], positions['CURRENT_FACE'], color=color, label = name_str)
            
            ax4.plot(analytics['AS_OF_DATE'], analytics['SDV01'], color=color, label = name_str)
            
        ax1.set_title("{0}".format( issuer))
        ax1.xaxis_date()
        ax1.grid(b=True, which='major', color='black', linestyle='--')
        ax1.set_ylabel("Closing Price")
        ax1.set_yticklabels(['{:3.2f}'.format(x) for x in ax1.get_yticks()])
        ax1.xaxis.set_major_locator(dates.MonthLocator())
        plt.xlim(args.start_date, args.end_date)
        ax1.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'small'})       

        df_all_postions = pandas.concat(df_position_lst, ignore_index=True)[['AS_OF_DATE','CURRENT_FACE','YEARLY_BASE_PL']].groupby(['AS_OF_DATE'], as_index=False).aggregate(sum)
        
        ax2.plot(df_all_postions['AS_OF_DATE'], df_all_postions['CURRENT_FACE'], color='blue', marker='', linestyle='-', label='Total Face')            
        ax2.xaxis_date()
        ax2.axhline(0,color='black')
        ax2.set_ylabel("Total Face Amount \nFace Amount Traded \n(Millions)")
        ax2.set_yticklabels(["{:,}".format(x/pow(10,6)) for x in ax2.get_yticks()])
        ax2.grid(b=True, which='major', color='black', linestyle='--')
        ax2.xaxis.set_major_locator(dates.MonthLocator())
        plt.xlim(args.start_date, args.end_date)
        ax2.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'small'})             
        
        ax3.plot(df_all_postions['AS_OF_DATE'], df_all_postions['YEARLY_BASE_PL'], color='blue', marker='o', linestyle='-', label='Total P&L')            

        ax3.set_yticklabels(["{:,}".format(x/pow(10,5)) for x in ax3.get_yticks()])
        ax3.axhline(0,color='black')
        ax3.set_ylabel("Total P&L\n(USD Hundred Thousand)")
        ax3.grid(b=True, which='major', color='black', linestyle='--')
        ax3.xaxis.set_major_locator(dates.MonthLocator())
        ax3.xaxis.set_major_formatter(dates.DateFormatter('%b %d %Y'))
        plt.xlim(args.start_date, args.end_date)
        ax3.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'small'})             

        df_all_analytics = pandas.concat(df_analytics_lst, ignore_index=True)[['AS_OF_DATE','SDV01']].groupby(['AS_OF_DATE'], as_index=False).aggregate(sum)
        
        ax4.plot(df_all_analytics['AS_OF_DATE'], df_all_analytics['SDV01'], color='blue', marker='', linestyle='-', label='Total SDV01')
        ax4.set_yticklabels(["{:,}".format(x/1000) for x in ax4.get_yticks()])
        ax4.axhline(0,color='black')
        ax4.set_ylabel("SDV01\n(Thousands)")
        ax4.grid(b=True, which='major', color='black', linestyle='--')
        ax4.xaxis.set_major_locator(dates.MonthLocator())
        ax4.xaxis.set_major_formatter(dates.DateFormatter('%b %d %Y'))
        plt.xlim(args.start_date, args.end_date)
        ax4.legend(loc="best", ncol=2, fancybox=True, shadow=True, prop={'size':'small'})             
 
       # Put a legend below current axis
        #fig.legend(ax_handle_lst, ax_label_lst, loc='lower center', bbox_to_anchor=(0.5, -0.05), fancybox=True, shadow=True, ncol=5)
        fig.autofmt_xdate()

        if (args.pdf):
        
            pp.savefig()
        
            plt.close()
        else:
           
            plt.show()
            
    if args.pdf:
        pp.close()

#    fund_pl = df_position_pl[["AS_OF_DATE",'YEARLY_BASE_PL']].groupby(by="AS_OF_DATE",as_index=False).aggregate(sum)
#    pl_2015_fund_YTD = fund_pl.loc[fund_pl['AS_OF_DATE']==datetime.date(2015,12,31),"YEARLY_BASE_PL"].values
#    if len(pl_2015_fund_YTD)>0:
#        fund_pl.loc[fund_pl['AS_OF_DATE']>datetime.date(2015,12,31), "YEARLY_BASE_PL"] += pl_2015_fund_YTD
#    
#    plt.plot(fund_pl['AS_OF_DATE'], fund_pl['YEARLY_BASE_PL'])
#    
#    logging.warn(exc)
                
    bh.bbg_shutdown()

 