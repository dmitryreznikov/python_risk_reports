# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 11:51:53 2016

@author: drezniko
"""
import pyodbc
import argparse
import datetime
import logging
import pandas
import dateutil
#import dateutil.rrule as dt_rr

POS_SIZE_TOLERANCE=0.25

str_sql_positions =  '''
--Aladdin all accounts small
SELECT 

--'POSITION',
psc.AS_OF_DATE, p.CURRENT_FACE, p.DOLLAR_DURATION,p.MARKET_VALUE_PERCENT, p.MARKET_VALUE, p.NOTIONAL_MARKET_VALUE, 
p.FX_RATE, p.SPD_DV01, p.OSP_NET_EXP, p.OSP_GROSS_EXP, p.OSP_LONG_EXP, p.OSP_SHORT_EXP,

ah.CHILD_ALADDIN_ACCOUNT_ID, ah.PARENT_ALADDIN_ACCOUNT_ID, 

--'SECREF',
s.NB_ID,s.SECURITY_DESCRIPTION, s.SECURITY_CURRENCY, s.ALADDIN_SECURITY_GROUP,s.ALADDIN_SECURITY_TYPE,
s.NB_ASSET_CLASS,s.NB_SECURITY_GROUP,s.NB_SECURITY_TYPE_1, s.NB_SECURITY_TYPE_2, s.TICKER,s.CUSIP,s.ISIN,s.SEDOL,s.ALADDIN_SECURITY_ID,

i.NAME AS ISSUER, i.TICKER AS ISSUER_TICKER, iparent.NAME AS ULTIMATE_ISSUER,iparent.TICKER AS ULTIMATE_ISSUER_TICKER,

--'CDS/CDX/OPTIONS underlying  security',
sunder.SECURITY_DESCRIPTION AS UNDER_SECURITY_DESCRIPTION, sunder.ALADDIN_SECURITY_GROUP AS UNDER_ALADDIN_SECURITY_GROUP, sunder.ALADDIN_SECURITY_TYPE AS UNDER_ALADDIN_SECURITY_TYPE,
sunder.NB_ASSET_CLASS AS UNDER_NB_ASSET_CLASS, sunder.NB_SECURITY_GROUP AS UNDER_NB_SECURITY_GROUP, sunder.NB_SECURITY_TYPE_1 AS UNDER_NB_SECURITY_TYPE_1, 
sunder.NB_SECURITY_TYPE_2 AS UNDER_NB_SECURITY_TYPE_2, sunder.TICKER AS UNDER_TICKER, sunder.CUSIP,sunder.ISIN AS UNDER_ISIN,sunder.SEDOL,sunder.ALADDIN_SECURITY_ID,

iunder.NAME AS UNDER_ISSUER, iunder.TICKER AS UNDER_ISSUER_TICKER, iunderparent.NAME AS UNDER_ULTIMATE_ISSUER,iunderparent.TICKER AS UNDER_ULTIMATE_ISSUER_TICKER ,

--'TRS underlying  security',
sswap.SECURITY_DESCRIPTION AS SWAP_UNDER_SECURITY_DESCRIPTION, sswap.ALADDIN_SECURITY_GROUP AS SWAP_UNDER_ALADDIN_SECURITY_GROUP, sswap.ALADDIN_SECURITY_TYPE AS SWAP_UNDER_ALADDIN_SECURITY_TYPE,
sswap.NB_ASSET_CLASS AS SWAP_UNDER_NB_ASSET_CLASS, sswap.NB_SECURITY_GROUP AS SWAP_UNDER_NB_SECURITY_GROUP, sswap.NB_SECURITY_TYPE_1 AS SWAP_UNDER_NB_SECURITY_TYPE_1, 
sswap.NB_SECURITY_TYPE_2 AS SWAP_UNDER_NB_SECURITY_TYPE_2, sswap.TICKER AS SWAP_UNDER_TICKER, sswap.CUSIP,sswap.ISIN AS SWAP_UNDER_ISIN,sswap.SEDOL,sswap.ALADDIN_SECURITY_ID,

iswap.NAME AS SWAP_UNDER_ISSUER, iswap.TICKER AS SWAP_UNDER_ISSUER_TICKER, iswapparent.NAME AS SWAP_UNDER_ULTIMATE_ISSUER, iswapparent.TICKER AS SWAP_UNDER_ULTIMATE_ISSUER_TICKER

FROM DIM_POSITION p 
INNER JOIN DIM_POSITION_SET ps ON ps.ID=p.POSITION_SET_ID
INNER JOIN DIM_POSITION_SET_CURRENT psc ON psc.POSITION_SET_ID=ps.ID
INNER JOIN common.[vDIM_ACCOUNT_HIERARCHY_CURRENT] ah on ah.CHILD_ACCOUNT_NB_ID=p.ACCOUNT_EDM_ID
INNER JOIN core.vSECURITY s ON s.NB_ID=p.SECURITY_EDM_ID
LEFT OUTER JOIN core.vSECURITY sunder ON sunder.NB_ID=s.UNDERLYING_ID
LEFT OUTER JOIN core.vSECURITY sswap ON sswap.ALADDIN_SECURITY_ID=s.ASSET_BENCHMARK
LEFT OUTER JOIN DIM_ISSUER i ON i.EDM_ID=s.ISSUER_NB_ID
LEFT OUTER JOIN DIM_ISSUER iparent ON iparent.EDM_ID=i.ULTIMATE_PARENT_EDM_ID
LEFT OUTER JOIN DIM_ISSUER iunder ON iunder.EDM_ID=sunder.ISSUER_NB_ID
LEFT OUTER JOIN DIM_ISSUER iunderparent ON iunderparent.EDM_ID=iunder.ULTIMATE_PARENT_EDM_ID
LEFT OUTER JOIN DIM_ISSUER iswap ON iswap.EDM_ID=sswap.ISSUER_NB_ID
LEFT OUTER JOIN DIM_ISSUER iswapparent ON iswapparent.EDM_ID=iswap.ULTIMATE_PARENT_EDM_ID
WHERE 
psc.SOURCE='Aladdin' AND psc.TYPE='PORTFOLIO' AND psc.PROVIDER='OSP' AND psc.AS_OF_DATE='{}'
AND ah.PARENT_ALADDIN_ACCOUNT_ID  IN ('OSP', 'LSCU', 'LSCMF')

'''

str_sql_dates = '''
        SELECT cc.as_of_date FROM DIM_POSITION_SET_CURRENT cc 
        WHERE cc.PROVIDER = 'OSP' AND cc.SOURCE = 'Aladdin' 
        ORDER BY cc.AS_OF_DATE DESC
    '''
    
   
str_sql_navs = '''

SELECT 'OSP' AS PARENT_ALADDIN_ACCOUNT_ID, '{0}' AS AS_OF_DATE,
MAX(p.TOTAL_FUND_CAPITAL) AS AUM
FROM vFACT_POSITION_CITCO p 
INNER JOIN DIM_POSITION_SET ps ON ps.ID=p.POSITION_SET_ID
INNER JOIN DIM_POSITION_SET_CURRENT psc ON psc.POSITION_SET_ID=ps.ID
INNER JOIN common.[vDIM_ACCOUNT_HIERARCHY_CURRENT] ah on ah.CHILD_ACCOUNT_NB_ID=p.ACCOUNT_EDM_ID
WHERE 
psc.SOURCE='Citco' AND psc.TYPE='PORTFOLIO' AND psc.PROVIDER='PRIVATE' AND psc.AS_OF_DATE='{0}'
AND ah.PARENT_ALADDIN_ACCOUNT_ID ='OSP'

UNION

SELECT 'LSCMF' AS PARENT_ALADDIN_ACCOUNT_ID, '{0}' AS AS_OF_DATE, s.[TNA_PRICE] AS AUM
FROM 
common.[vDIM_ACCOUNT_HIERARCHY_CURRENT] ah 
INNER JOIN DIM_ACCOUNT a ON a.EDM_ID=ah.CHILD_ACCOUNT_NB_ID
INNER JOIN vFACT_NAV_SSB s ON s.SSB_NUMBER=a.CUSTODIAN_ACCT_NUMBER
WHERE 
s.SSB_FLAG =0 
AND s.AS_OF_DATE='{0}'
AND ah.PARENT_ALADDIN_ACCOUNT_ID ='LSCMF'


UNION

SELECT 'LSCU_' AS PARENT_ALADDIN_ACCOUNT_ID, '{0}' AS AS_OF_DATE, MAX(p.TOTAL_BASE_MARKET_VALUE) AS AUM
FROM vFACT_POSITION_BBH p 
INNER JOIN DIM_POSITION_SET ps ON ps.ID=p.POSITION_SET_ID
INNER JOIN DIM_POSITION_SET_CURRENT psc ON psc.POSITION_SET_ID=ps.ID
INNER JOIN common.[vDIM_ACCOUNT_HIERARCHY_CURRENT] ah on ah.CHILD_ACCOUNT_NB_ID=p.ACCOUNT_EDM_ID
WHERE 
psc.SOURCE='BBH' AND psc.TYPE='Portfolio' AND psc.PROVIDER='BBH' AND psc.AS_OF_DATE='{0}'
AND ah.PARENT_ALADDIN_ACCOUNT_ID ='LSCU'

UNION

SELECT 'LSCU' AS PARENT_ALADDIN_ACCOUNT_ID, 
    '{0}' AS AS_OF_DATE, 
        SUM(CASE WHEN CURRENCY in ('USD') THEN  (B.SHARE_CLASS_AUM_LOCAL_CURRENCY)
                WHEN CURRENCY <> 'USD' THEN (B.SHARE_CLASS_AUM_LOCAL_CURRENCY * F.SPOT_PRICE) 
                ELSE 0 
           END) AS AUM
FROM 
vFACT_NAV_BBH B 
INNER JOIN FACT_FX_RATE F ON F.source_currency=B.CURRENCY AND F.TARGET_CURRENCY='USD' AND F.CLOSE_INDICATOR='LDN'
INNER JOIN vDIM_NAV_SET_CURRENT s ON s.ID=B.NAV_SET_ID AND s.SOURCE='BBH' AND s.TYPE='FUND' AND s.AS_OF_DATE=CONVERT(VARCHAR(10), '{0}', 112)
WHERE
F.PRICE_CHANGE_DATE='{0}'

AND ROUND(B.FUND_AUM_USD,0)  = (

	SELECT ROUND (SUM(B.ENDING_GL_BALANCE),0)
	FROM vFACT_PERFORMANCE_ACCOUNT_BBH B
	INNER JOIN vDIM_PERFORMANCE_SET_CURRENT ps ON ps.ID = B.PERFORMANCE_SET_ID
	WHERE 
	ps.SOURCE = 'BBH' AND ps.TYPE = 'Account' AND ps.AS_OF_DATE = CONVERT(VARCHAR(10), '{0}', 112)
	AND B.GL_ACCOUNT_CATEGORY IN ('Assets','Liabilities')
	AND ENTITY_NUMBER = 9985
	)
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

    logging.basicConfig(filename="position_by_issuer_{}.log".format(args.report_date if latest_date>args.report_date else latest_date),
                        filemode="w",
                        level=numeric_level,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        format="%(asctime)s:%(levelname)s:%(message)s")
                        
    if latest_date < args.report_date:
        logging.warn ("Data for {} is not yet available. Using latest available data as of {}".format(args.report_date, latest_date))
        args.report_date = latest_date
        
    ####################################################
    ## read in some config data from excel template file
    ####################################################
    with pandas.ExcelFile("{0}/OSP_SIZE_STOPLOSS_PARAMETERS.xlsx".format(args.outdir)) as xls:
        df_rec_size    = pandas.read_excel(io=xls, sheetname="Position Sizing", parse_cols="A:E", index_col='Ultimate_Issuer_Name')[['LSCMF Position Size', 'LSCU Position Size', 'OSP Position Size']]
   
        df_rec_size.columns=[('SIZE_LIMIT', 'LSCMF'), ('SIZE_LIMIT','LSCU'), ('SIZE_LIMIT','OSP')]
    
        osp_navs       = pandas.read_excel(io=xls, sheetname="AUM", index_col=0)


    df_positions_all = pandas.read_sql_query(str_sql_positions.format(args.report_date), engine, parse_dates=['AS_OF_DATE'])
    
    data_frame_navs = pandas.read_sql_query(str_sql_navs.format(args.report_date.strftime('%Y%m%d')), engine, parse_dates=['AS_OF_DATE'])

    ####################################################
    ## fix the nav for the private fund, use the spreadsheet value
    ####################################################   
    current_osp_nav = osp_navs.loc[datetime.date(args.report_date.year, args.report_date.month, 1)].AUM 
    if current_osp_nav is None:
        current_osp_nav = osp_navs.AUM[-1]
        logging.warn("OSP LP is missing NAV estimate for report date {} using previous month-end", args.report_date)
    else:
        logging.info("Using OSP LP NAV {0} dated {1} from the setting file".format(current_osp_nav, args.report_date))
        
    data_frame_navs.loc[data_frame_navs["PARENT_ALADDIN_ACCOUNT_ID"]=='OSP', 'AUM'] = current_osp_nav

    work_frame = df_positions_all[['AS_OF_DATE','PARENT_ALADDIN_ACCOUNT_ID', 'CURRENT_FACE','OSP_NET_EXP', 'MARKET_VALUE','SPD_DV01','DOLLAR_DURATION',
    'SECURITY_DESCRIPTION', 'ULTIMATE_ISSUER', 'ULTIMATE_ISSUER_TICKER', 'UNDER_ULTIMATE_ISSUER','UNDER_ULTIMATE_ISSUER_TICKER', 
    'SWAP_UNDER_ULTIMATE_ISSUER','SWAP_UNDER_ULTIMATE_ISSUER_TICKER','ISIN','UNDER_ISIN','SWAP_UNDER_ISIN',
    'NB_ASSET_CLASS','NB_SECURITY_GROUP', 'NB_SECURITY_TYPE_1','NB_SECURITY_TYPE_2','ALADDIN_SECURITY_GROUP','ALADDIN_SECURITY_TYPE',
    'UNDER_NB_ASSET_CLASS','UNDER_NB_SECURITY_GROUP','UNDER_NB_SECURITY_TYPE_1','UNDER_NB_SECURITY_TYPE_2']]
    
    work_frame = work_frame[
      (work_frame['ALADDIN_SECURITY_GROUP']!='FUTURE')
    & (work_frame['ALADDIN_SECURITY_GROUP']!='OPTION')
    & (work_frame['ALADDIN_SECURITY_TYPE']!='FWRD')
    & (work_frame['ALADDIN_SECURITY_TYPE']!='STIF')
    & (work_frame['NB_SECURITY_GROUP']!='M Market')]
    
    #align the issuer columns for SWAPS
    work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='CDS', 'ULTIMATE_ISSUER'] = work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='CDS','UNDER_ULTIMATE_ISSUER']
    work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='TRS', 'ULTIMATE_ISSUER'] = work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='TRS', 'SWAP_UNDER_ULTIMATE_ISSUER']
    
    work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='CDS', 'ULTIMATE_ISSUER_TICKER'] = work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='CDS', 'UNDER_ULTIMATE_ISSUER_TICKER']
    work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='TRS', 'ULTIMATE_ISSUER_TICKER'] = work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='TRS', 'SWAP_UNDER_ULTIMATE_ISSUER_TICKER']

    work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='CDS', 'ISIN'] = work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='CDS', 'UNDER_ISIN']
    work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='TRS', 'ISIN'] = work_frame.loc[work_frame['NB_SECURITY_TYPE_1']=='TRS', 'SWAP_UNDER_ISIN']

    work_frame = pandas.merge(left=work_frame, right=data_frame_navs, how='left')
    
    work_frame['OSP_NET_EXP_PCT'] = work_frame['OSP_NET_EXP'] / work_frame['AUM']
    
    #issuer_sbs_exp_frame = work_frame.groupby(['AS_OF_DATE','PARENT_ALADDIN_ACCOUNT_ID','ULTIMATE_ISSUER'], as_index=False).aggregate(sum).pivot(index='ULTIMATE_ISSUER', columns='PARENT_ALADDIN_ACCOUNT_ID', values='OSP_NET_EXP').fillna(0)
    #issuer_sbs_exp_pct_frame = work_frame.groupby(['AS_OF_DATE','PARENT_ALADDIN_ACCOUNT_ID','ULTIMATE_ISSUER'], as_index=False).aggregate(sum).pivot(index='ULTIMATE_ISSUER', columns='PARENT_ALADDIN_ACCOUNT_ID', values='OSP_NET_EXP_PCT').fillna(0)

    issuer_sbs_exp_frame = pandas.pivot_table(work_frame,
                                             values=['OSP_NET_EXP','OSP_NET_EXP_PCT'], 
                                             index=['ULTIMATE_ISSUER_TICKER', 'ULTIMATE_ISSUER'],
                                             columns=['PARENT_ALADDIN_ACCOUNT_ID'],
                                             aggfunc=sum, 
                                             margins=False)
                                             
    issue_sbs_exp_frame = pandas.pivot_table(work_frame, 
                                             values=['OSP_NET_EXP','OSP_NET_EXP_PCT'], 
                                             index=['ULTIMATE_ISSUER_TICKER', 'ULTIMATE_ISSUER','SECURITY_DESCRIPTION','ISIN','NB_SECURITY_TYPE_1'],
                                             columns=['PARENT_ALADDIN_ACCOUNT_ID'],
                                             aggfunc=sum, 
                                             margins=False)
                                             
    issue_sbs_duration_frame = pandas.pivot_table(work_frame, 
                                             values=['SPD_DV01','DOLLAR_DURATION'], 
                                             index=['ULTIMATE_ISSUER_TICKER', 'ULTIMATE_ISSUER','SECURITY_DESCRIPTION','ISIN','NB_SECURITY_TYPE_1'],
                                             columns=['PARENT_ALADDIN_ACCOUNT_ID'],
                                             aggfunc=sum, 
                                             margins=False)

    issuer_sbs_exp_frame[('SIZE_DIFF_FROM_OSP','LSCMF')] = (issuer_sbs_exp_frame[('OSP_NET_EXP_PCT','LSCMF')]  / issuer_sbs_exp_frame[('OSP_NET_EXP_PCT','OSP')] - 1.0).fillna(-1.0)
    issuer_sbs_exp_frame[('SIZE_DIFF_FROM_OSP','LSCU')]  = (issuer_sbs_exp_frame[('OSP_NET_EXP_PCT','LSCU')]   / issuer_sbs_exp_frame[('OSP_NET_EXP_PCT','OSP')] - 1.0).fillna(-1.0)

    issuer_sbs_exp_frame=issuer_sbs_exp_frame.sort_values(by=('OSP_NET_EXP_PCT','OSP'), ascending=False)
    
    issuer_size_limit_frame= pandas.merge(pandas.pivot_table(work_frame,
                                                             values=['OSP_NET_EXP_PCT'], 
                                                             index=['ULTIMATE_ISSUER'],
                                                             columns=['PARENT_ALADDIN_ACCOUNT_ID'],
                                                             aggfunc=sum, 
                                                             margins=False), 
                                         df_rec_size, 
                                         left_index=True,
                                         right_index=True,
                                         how='left')

    with pandas.ExcelWriter("{}/Credit LS fund side-by-side {}.xlsx".format(args.outdir, args.report_date.strftime("%Y-%m-%d")), date_format='YYYY-MM-DD', datetime_format='YYYY-MM-DD') as writer:
 
        format_par = writer.book.add_format({'num_format':'#,##0_);[Red](#,##0)'})
        format_pct = writer.book.add_format({'num_format':'0.00%'})
        format_usd = writer.book.add_format({'num_format':'$ #,##0_);[Red]($ #,##0)'})

        red_bg     = writer.book.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        dk_red_bg  = writer.book.add_format({'bg_color': '#FF3747', 'font_color': '#640005'})
        green_bg   = writer.book.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        dk_green_bg= writer.book.add_format({'bg_color': '#00B050', 'font_color': '#005000'})


        issuer_size_limit_frame.to_excel(writer, index=True, sheet_name='Issuer Size Limit')
        
        issuer_sbs_exp_frame.to_excel(writer, index=True, sheet_name='Issuer Compare')
        
        issue_sbs_duration_frame.to_excel(writer, index=True, sheet_name='Issuer Duration')

        issue_sbs_exp_frame.to_excel(writer, index=True, sheet_name='Issuer Detail')

#        work_frame[['AS_OF_DATE','PARENT_ALADDIN_ACCOUNT_ID','CURRENT_FACE','OSP_NET_EXP','MARKET_VALUE',
#        'SECURITY_DESCRIPTION','ULTIMATE_ISSUER','ULTIMATE_ISSUER_TICKER','ISIN','NB_ASSET_CLASS','NB_SECURITY_GROUP',
#        'NB_SECURITY_TYPE_1','NB_SECURITY_TYPE_2','OSP_NET_EXP_PCT']].to_excel(writer, index=True, sheet_name='Issue Details')

        df_positions_all.to_excel(writer, index=True, sheet_name='ALL')       
        
        for worksheet in writer.book.worksheets():
            if worksheet.name=='Issuer Compare':
                worksheet.set_column('A:A', width=15)
                worksheet.set_column('B:B', width=50)
                worksheet.set_column('C:E', width=15, cell_format=format_par)
                worksheet.set_column('F:J', width=15, cell_format=format_pct)

                worksheet.conditional_format('I4:J{0}'.format(len(issuer_sbs_exp_frame)+3), {'type': 'formula', 'criteria': '=ABS(I4)>={0}'.format(POS_SIZE_TOLERANCE), 'format': red_bg})

            elif worksheet.name=='Issuer Size Limit':
                worksheet.set_column('A:A', width=50)
                worksheet.set_column('B:G', width=15, cell_format=format_pct)

                worksheet.conditional_format('B4:D{0}'.format(len(issuer_size_limit_frame)+3), {'type': 'formula', 'criteria': '=ABS(B4)>E4', 'format': dk_red_bg})
                
            elif worksheet.name=='Issuer Detail':
                worksheet.set_column('A:A', width=15)
                worksheet.set_column('B:C', width=40)
                worksheet.set_column('D:D', width=15)
                worksheet.set_column('E:E', width=20)
                worksheet.set_column('F:H', width=15, cell_format=format_par)
                worksheet.set_column('I:K', width=15, cell_format=format_pct)
                worksheet.autofilter('A3:E{0}'.format(len(issue_sbs_exp_frame)+3))

            elif worksheet.name=='Issuer Duration':
                worksheet.set_column('A:A', width=15)
                worksheet.set_column('B:C', width=40)
                worksheet.set_column('D:D', width=15)
                worksheet.set_column('E:E', width=20)
                worksheet.set_column('F:H', width=15, cell_format=format_par)
                worksheet.set_column('I:K', width=15, cell_format=format_par)
                worksheet.autofilter('A3:E{0}'.format(len(issue_sbs_duration_frame)+3))

            else:
                #worksheet.set_column('G:H', width=50)
                #worksheet.set_column('A:F', width=15)
                #worksheet.set_column('I:Z', width=15)
                
                worksheet.autofilter('A1:AY1000')
