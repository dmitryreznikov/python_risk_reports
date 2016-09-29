import operator
import os
import sys
import argparse
import datetime
import math

from dateutil.rrule import *
from dateutil.relativedelta import *
import dateutil.parser

import numpy as np

import matplotlib.colors as colors
import matplotlib.finance as finance
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager
import matplotlib.cbook as cbook

from decimal import *

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "NBRIS.settings")
sys.path.append("C:/Users/drezniko/src/NBRIS")

import nbris_riskhub.models as nbris
import activist_fund.models as af

import bbg_api_wrap as bbg
import logging

class portfolio_constructor():
    def __init__(self, strat, rebalance, freq, max_names, port):
        self.bh        = bbg.bbg_helper()
        self.portfolio = port
        self.strategy  = strat
        self.rebalance = rebalance
        self.frequency = freq.upper()
        self.max_names = max_names


    def equalweight_mv(self, holding_date, sec_rank_wgt_sorted, rejects):


        equal_weight = Decimal(1.0/self.max_names) # Decimal(1.0 / len(sec_rank_wgt_sorted)))

        all_position = self.portfolio.security.holding.filter(date = holding_date)

        portfolio_mv = sum([p.mv for p in all_position])

        portfolio_cash = all_position.get(security__sec_type__sec_type = "CASH", security__name="USD Cash")

        old_security_set  = set ([p.security for p in all_position])

        logging.info("{0:<12}{1:<12}{2:<30}{3:>15}{4:>15}{5:>8}{6:>8}".format("DATE","ACTION","SEC_NAME","SHARES","MV USD","MV %%","PRICE"))

        sec_to_keep = []

        for i,sec in enumerate(sec_rank_wgt_sorted):

            if len(sec_to_keep) < self.max_names:

                #############################################
                # buy new positions that appeared in filings
                #############################################
                if sec not in old_security_set:

                    price = Decimal(0)

                    (db_price, excepts) = self.bh.bbg_create_price([sec],
                                                                   "PX_LAST",
                                                                   holding_date + relativedelta(months=-1),
                                                                   holding_date,
                                                                   "DAILY",
                                                                   "CALENDAR")
                    
                    if len(db_price)>0:
                        price    = db_price[-1].price
                        quantity = Decimal(equal_weight * portfolio_mv // price)
                        mv       = Decimal(quantity * price)

                        new_position = nbris.Position.objects.create(date             = holding_date,
                                                                     holding_security = self.portfolio.security,
                                                                     currency         = sec.currency,
                                                                     security         = sec,
                                                                     quantity         = quantity,
                                                                     mv               = mv)
                        portfolio_cash.mv       -= mv
                        portfolio_cash.quantity -= mv

                        logging.info("{0:12}{1:12}{2:30}{3:15,}{4:>15.2f}{5:>8.2f}{6:>8.2f}".format(
                            str(holding_date),
                            "BUY",
                            sec.name,
                            quantity,
                            mv,
                            100 * mv / portfolio_mv,
                            price))

                        sec_to_keep.append(sec)

                    else:
                        rejects[sec] = "{} has no BBG price from {} to {}:{}".format(sec, 
                                                                                     holding_date + relativedelta(months=-1),
                                                                                     holding_date,
                                                                                     excepts)

                else:
                    #############################################
                    # Rebalance existing positions 
                    #############################################

                    curr_pos = all_position.get(security = sec)

                    old_quantity = curr_pos.quantity

                    try:

                        db_price  = sec.price_set.get(date=holding_date, ds__code="BBG")

                    except nbris.Price.DoesNotExist:
                        db_price = sec.price_set.filter(date__lte=holding_date, ds__code="BBG").order_by("-date").first()
                        rejects[sec] = "{} has no price in DB onn {} REBALANCE at stale price {} {}".format(sec,
                                                                                                            str(holding_date),db_price.date, db_price.price)
                    new_quantity      = Decimal(equal_weight * portfolio_mv // db_price.price)
                    curr_pos.quantity = new_quantity
                    curr_pos.mv       = new_quantity * db_price.price
                    curr_pos.save()

                    cash_adjust              = db_price.price * ( new_quantity-old_quantity )
                    portfolio_cash.mv       -= cash_adjust
                    portfolio_cash.quantity -= cash_adjust

                    logging.info("{0:12}{1:12}{2:30}{3:15,}{4:>15.2f}{5:>8.2f}{6:>8.2f}".format(
                        str(holding_date),
                        "REBAL",
                        sec.name,
                        new_quantity-old_quantity,
                        cash_adjust,
                        100 * cash_adjust / portfolio_mv,
                        db_price.price))

                    sec_to_keep.append(sec)

            else:
                break
        #for i,sec in enumerate(sec_rank_wgt_sorted):

        #######################
        # Sell all positions that disappeared from current filings
        #######################
        for sec in old_security_set:

            if sec.sec_type.sec_type != "CASH" and sec not in sec_to_keep:
                
                curr_pos = all_position.get(security = sec)

                try:
                    curr_price = sec.price_set.get(date=holding_date, ds__code="BBG").price

                except nbris.Price.DoesNotExist:
                    db_price = sec.price_set.filter(date__lte=holding_date, ds__code="BBG").order_by("-date").first()
                    curr_price = db_price.price
                    rejects[sec] = "{} has no price in DB onn {} SOLD at stale price {} {}".format(sec, str(holding_date),db_price.date, db_price.price)

                portfolio_cash.mv       += curr_pos.mv
                portfolio_cash.quantity += curr_pos.mv

                logging.info("{0:12}{1:12}{2:30}{3:15,}{4:>15.2f}{5:>8.2f}{6:>8.2f}".format(
                    str(holding_date),
                    "SELL",
                    curr_pos.security.name,
                    curr_pos.quantity,
                    curr_pos.mv,
                    100 * curr_pos.mv / portfolio_mv,
                    curr_price))

                curr_pos.delete()
            
        portfolio_cash.save()



    def construct(self, filings, repricing_date_set, total_seed_mv):

        nbris.Position.objects.filter(holding_security = self.portfolio.security).delete()
        logging.debug("Deleted all positions for portfolio {}".format(self.portfolio))

        db_usd_cash  = nbris.SecurityMaster.objects.get(name="USD Cash", sec_type__sec_type="CASH")
        db_usd_fx    = nbris.Currency.objects.get(code="USD")
        sec_form_13F = af.SecFilingType.objects.get(code="13F-HR")

        af.SecFilingSecurityRank.objects.filter(form_type = sec_form_13F,
                                                date__range=(min(repricing_date_set),max(repricing_date_set))).delete()
                                                
        seed_date  =  min(repricing_date_set) + dateutil.relativedelta.relativedelta(days=-1)

        seed_position = nbris.Position.objects.create(date             = seed_date,
                                                      security         = db_usd_cash,
                                                      holding_security = self.portfolio.security,
                                                      currency         = db_usd_fx,
                                                      quantity         = total_seed_mv,
                                                      mv               = total_seed_mv)

        logging.info("Seed portfolio {0} on {1} with USD {2:12,}".format(self.portfolio, seed_date, total_seed_mv))

        previous_date = seed_date

        rebal_date_to_filing_dict = {}
        #13F always have report_period, 13D are filed at most 10 days after the event date, but no report_date is available
        #assume rebalancing is monthly, so force report_date to be 
        report_date_set = set([d.report_period for d in filings if d.report_period is not None])

        ## 13F filings date logic
        for rd in sorted(report_date_set):
        
            max_filed_date = max([d.filed_date for d in filings.filter(report_period=rd)])

            next_bus_day = max_filed_date + relativedelta(days=+1)

            if next_bus_day.weekday() >= 5:
                next_bus_day  += relativedelta(weekday=MO)

            rebal_date_to_filing_dict[next_bus_day] = (max_filed_date,
                                                       filings.filter(filed_date__lte=max_filed_date, 
                                                                      report_period__gte=rd).order_by('filer_name'))


        all_dates_set = repricing_date_set.union(rebal_date_to_filing_dict.keys())
        filing_date_set = set([f.filed_date for f in filings if f.filed_date is not None])

        logging.info("{0:<5}{1:<15}{2:5}{3:10}{4:10}{5:10}{6:10}".format("ROW","DATE","DAY","ASOF","RETURN","FILED","REBALANCE"))

        ################
        # report_date_set is the unique date set for which filings 13F are reporting on, quarterly month end
        # repricing_date_set is the set of ordered dates on which portfolio will be valued: user-generated daily, monthly, quarterly
        # filing_date_set is the set of ordered dates on which the filing was made, for 13F 5-6 weeks after report_date, for 13D 10days after the event
        # rebal_date_to_filing_dict maps the next business day after a date in filing_date_set to the last filing date in the reporting period and all the filings that fall in that period

        for i, dt in enumerate(sorted(all_dates_set.union(filing_date_set).union(report_date_set))):
            logging.info("{0:<5}{1:<15}{2:5}{3:10}{4:10}{5:10}{6:10}".format(i+1, 
                                                                             str(dt),
                                                                             ["MO","TU","WE","TH","FR","SA","SU"][dt.weekday()],
                                                                             "x" if dt in report_date_set else ".",
                                                                             "x" if dt in repricing_date_set else ".",
                                                                             "x" if dt in filing_date_set else ".",
                                                                             "x" if rebal_date_to_filing_dict.has_key(dt) else ".",
                                                                         ))
        for current_date in sorted(all_dates_set):

            rejected_security = {}

            logging.info("LOADING DATA FOR {}".format(current_date))

            prev_position = nbris.Position.objects.filter(date             = previous_date,
                                                          holding_security = self.portfolio.security)

            ##########################################
            # roll positions forward
            # update price and mv
            ############################################
            for sec_cnt, old_pos in enumerate(prev_position):

                (db_price, excepts) = self.bh.bbg_create_price([old_pos.security],
                                                               "PX_LAST",
                                                               current_date + relativedelta(months=-1),
                                                               current_date,
                                                               "DAILY",
                                                               "CALENDAR")
                    
                if len(db_price)>0:
                    price  = db_price[-1].price
                    new_mv = old_pos.quantity * price

                    if db_price[-1].date != current_date:
                        logging.debug("BLOOMBERG has no price for {0} on {1} using stale price {2}={3:0.4f}".format(old_pos.security,
                                                                                                                    current_date,
                                                                                                                    db_price[-1].date,
                                                                                                                    price))
                        
                    new_pos = nbris.Position.objects.create(date             = current_date,
                                                            holding_security = old_pos.holding_security,
                                                            security         = old_pos.security,
                                                            currency         = old_pos.currency,
                                                            quantity         = old_pos.quantity,
                                                            mv               = new_mv)

                    logging.info("ROLLED POSITION {0:<4} {1:12} {2:10} {3:30} {4:12,} {5:12.2f} {6:12.2f}".format(
                        sec_cnt+1, 
                        str(current_date),
                        old_pos.security.sec_type.sec_type,
                        old_pos.security.name, 
                        old_pos.quantity, 
                        new_mv,
                        price))

                else:
                    ## todo this is wrong: cash balance has to be updated when security is removed
                    rejected_security[old_pos.security] = "DROPPED {} BLOOMBERG has no prices between {} and {}".format(old_pos.security,
                                                                                                                        current_date + relativedelta(months=-1),
                                                                                                                        current_date)
                    # imperror,created = af.SecFilingImportError.objects.get_or_create(filing_detail=filing_detail,
                    #                                                                  error = af.ImportError.objects.get(name="Missing Price Data"))
                    
            ##for sec_cnt, old_pos in enumerate(prev_position):

            ######################
            # rebalance portfolio
            ######################
            if rebal_date_to_filing_dict.has_key(current_date):

                filing_security_list = []

                ####################
                ##process each filing manager info
                ####################
                sec_rank_wgt_all_dict = {}
                for i,filing in enumerate(rebal_date_to_filing_dict[current_date][1]):

                    sec_rank_mv_dict = {}
                    logging.debug("Process filing {}".format(filing))

                    #######################################################
                    # process filing detail make sure all securities are in SecurityMaster
                    #######################################################
                    for detail in filing.secfilingdetail_set.all():
                        logging.debug("Process filing detail {}".format(detail))

                        if detail.cusip is not None and len(detail.cusip) in [8,9]:
                            subject_security=None

                            # try:
                            #     subject_security = nbris.SecurityMaster.objects.get(securityreference__sec_id_value = detail.cusip,
                            #                                                         securityreference__sec_ref_type__sec_ref_type="CUSIP")


                            # except nbris.SecurityMaster.MultipleObjectsReturned:
                            #     sec_list = nbris.SecurityMaster.objects.filter(securityreference__sec_id_value = detail.cusip,
                            #                                                    securityreference__sec_ref_type__sec_ref_type="CUSIP")

                            #     err_str = "Found Multiple Securities {} for CUSIP {}".format(":".join([s.name for s in sec_list]), detail.cusip)
                            #     raise Exception(err_str)

                            # except nbris.SecurityMaster.DoesNotExist:
                                        
                            try:
                                subject_security, warning = self.bh.bbg_create_security(secid=detail.cusip,
                                                                                        secid_type=nbris.SecurityReferenceType.objects.get(sec_ref_type="CUSIP"))

                                if type(warning) is bbg.bbgTickerChangeWarning or type(warning) is bbg.bbgAcquisitiondWarning:

                                    # try:
                                    logging.warn("Security {}: old ticker:{} new ticker:{} reason:{}".format(subject_security,
                                                                                                             warning.old_ticker,
                                                                                                             warning.new_ticker,
                                                                                                             warning.status))

                                    status_change,created = nbris.SecurityStatusChange.objects.get_or_create(security        = subject_security,
                                                                                                             change_status   = warning.status)

                                    try:
                                        parent_security,warning2 = self.bh.bbg_create_security(secid=warning.new_ticker, 
                                                                                               secid_type=nbris.SecurityReferenceType.objects.get(sec_ref_type="TICKER"),
                                                                                               yellow_key="Equity")
                                        
                                        status_change.parent_security = parent_security
                                        status_change.save()

                                    except bbg.bbgCreateSecurityFailedException as e:
                                        rejected_security[warning.new_ticker] = str(e)
                                        
                                    except bbg.bbgException as e:
                                        rejected_security[warning.new_ticker] = str(e)

                                elif type(warning) is bbg.bbgExchangeStatusChangeWarning:

                                    logging.warn("Security {}: ticker:{} exchange market change:{}".format(subject_security,
                                                                                                           warning.old_ticker,
                                                                                                           warning.status))

                                    status_change,created = nbris.SecurityStatusChange.objects.get_or_create(security        = subject_security,
                                                                                                             change_status   = warning.status)


                            except bbg.bbgCreateSecurityFailedException as e:
                                rejected_security[detail.subject_name] = str(e)

                                # imperror,created = af.SecFilingImportError.objects.get_or_create(filing_detail=filing_detail,
                                #                                                                  error = af.ImportError.objects.get(name="Security Master Error"),
                                #                                                                  defaults={'description':str(e)} )
                                # if not created:
                                #     imperror.defaults = str(e)
                                #     imperror.save()



                            if ( subject_security is not None and
                                 subject_security.sec_type.sec_type not in ("HedgeFund", "MLP", "REIT","ETF","ETP")):
                                
                                if detail.mv is not None:
                                    sec_rank_mv_dict[subject_security] = sec_rank_mv_dict.get(subject_security, Decimal(0)) + detail.mv

                                else:
                                    (bbg_price, exc) = self.bh.bbg_create_price([subject_security],
                                                                                "PX_LAST",
                                                                                filing.report_period,
                                                                                current_date,
                                                                                self.frequency)
                                        
                                    if len(bbg_price)>0:
                                        sec_rank_mv_dict[subject_security] = sec_rank_mv_dict.get(subject_security, Decimal(0)) + (detail.no_shares * bbg_price[-1].price)

                                    else:
                                        errstr = "UNABLE TO ASSIGN MV WEIGHT RANK FOR SECURITY {} MV IS Null BBG has no price from {} to {} REASON:{} ".format(subject_security, filing.report_period, current_date, exc)

                                        rejected_security[detail.subject_name] = errstr
                                        # imperror,created = af.SecFilingImportError.objects.get_or_create(filing_detail=filing_detail,
                                        #                                                                  error = af.ImportError.objects.get(name="Missing Price Error"),
                                        #                                                                  defaults={'description':errstr} )
                                        # if not created:
                                        #     imperror.defaults = errstr
                                        #     imperror.save()


                        else:
                            errstr = "SECURITY FROM FILING NOT LOADED INVALID CUSIP:{}".format(str(detail.cusip))
                            rejected_security[detail.subject_name] = errstr

                            # imperror,created = af.SecFilingImportError.objects.get_or_create(filing_detail=filing_detail,
                            #                                                                  error = af.ImportError.objects.get(name="Invalid Cusip"),
                            #                                                                  defaults={'description':errstr} )
                            # if not created:
                            #     imperror.defaults = errstr
                            #     imperror.save()

                    # for detail in filing.secfilingdetail_set.all():

                    # for (sec_i,(s,w)) in enumerate(sec_rank_mv_dict.iteritems()):
                    #     print "{1:5} {4:<40} {0:10} {2:30} {3:12,}".format(str(current_date), sec_i+1, s, w, filing.filer_name)
                

                    sec_filing_mv = sum([fd.mv for fd in filing.secfilingdetail_set.all() if fd.mv is not None])
                        
                    for k,v in sec_rank_mv_dict.iteritems():

                        sec_rank_wgt_all_dict[k] = sec_rank_wgt_all_dict.get(k, Decimal(0)) + v / sec_filing_mv

                    # for k,v in sec_rank_mv_dict.iteritems():
                # for i,filing in enumerate(rs):

                sec_rank_wgt_sorted = [x[0] for x in sorted(sec_rank_wgt_all_dict.iteritems(), 
                                                            key=operator.itemgetter(1),
                                                            reverse=True)]

                logging.info("{0:5} {1:40} {2}".format("RANK","NAME","FILING MV WGT"))

                for i,sec in enumerate( sec_rank_wgt_sorted):
                    logging.info("{0:<5} {1:<40} {2:0.4f}".format(i+1, sec, sec_rank_wgt_all_dict[sec]))

                    rank_db = af.SecFilingSecurityRank.objects.create(date=current_date,
                                                                      security=sec,
                                                                      form_type = sec_form_13F,
                                                                      rank_wgt = sec_rank_wgt_all_dict[sec],
                                                                      rank=i)                                                                                      
                getattr(self, self.rebalance)(current_date,
                                              sec_rank_wgt_sorted,
                                              rejected_security)


            #     for holding_date in sorted(filing_date_set)[0:1]:
            # if current_date in filing_date_set:

            for (i,(k,v)) in enumerate(rejected_security.iteritems()):
                logging.error("{0:<5}ERROR {1:<30} {2:40}".format(i+1, k, v))


            previous_date = current_date

            #if current_date in filing_date_set:
        #for current_date in all_dates:



    def compute_weights(self, date):
        positions = self.portfolio.security.holding.filter(date=date)

        port_mv = sum([p.mv for p in positions])

        return [(p.security, float(p.mv/port_mv)) for p in positions ]


    def compute_backtest_portfolio_value(self, date, lookback):
        positions      = self.portfolio.security.holding.filter(date=date)
        start_date     = date + relativedelta(months=-lookback)
        lookback_dates = sorted([d.date() for d in rrule(MONTHLY, dtstart=start_date, until=date, bymonthday=-1)])

        result = []
        rejects = []
        for (i, vd) in enumerate(lookback_dates):

            result_sec = []

            for (sec, shares) in [(p.security, p.quantity) for p in positions]:
                sec_price = Decimal(0)
                try:
                    sec_price =  sec.price_set.get(date=vd, ds__code="BBG").price

                except nbris.Price.DoesNotExist:
                    (bbg_price, exc) = self.bh.bbg_create_price([sec],
                                                                "PX_LAST",
                                                                vd,
                                                                None,
                                                                self.frequency)
                
                    if len(bbg_price)>0:
                        sec_price = bbg_price[0].price
                    
                    else:
                        rejects.append((vd, sec, "MISSING PRICE:{}".format(exc[0])))

                result_sec.append(shares * sec_price)

            port_mv = sum(result_sec)

            port_ret = None
            if i>0:
                port_ret = float(port_mv / result[i-1][1] - 1) if result[i-1]!=0 else None
                
            result.append((vd, port_mv, port_ret))

        return (result, rejects)


    def compute_annual_return_stats(self, date, lookback_months = None):
        
        (rs,rejects) = self.compute_backtest_portfolio_value(date, lookback_months)

        ann_vol = np.std([float(r[2]) for r in rs[1:]]) * np.sqrt(12)
    
        tot_ret = rs[-1][1] / rs[0][1] - 1
        
        ann_ret = (math.pow(tot_ret+1, 1.0/lookback_months)-1) * 12.0

        return (float(tot_ret), ann_ret, ann_vol)

    
    def compute_monthly_var(self, date, rejects, lookback_months = 120, confidence=0.95):

        cutoff_low = int(math.floor((1-confidence) * lookback_months))
        cutoff_hi  = int(math.ceil((1-confidence) * lookback_months))

        (rs,rj) = self.compute_backtest_portfolio_value(date, lookback_months)

        #first month has no return
        ranked_rs = sorted(rs[1:], key=operator.itemgetter(2))
        rejects.append(rj)

        var =  np.average( [ranked_rs[cutoff_low][2], ranked_rs[cutoff_hi][2]] )

        cvar = np.average( [r[2] for r in ranked_rs[0:cutoff_hi] ])

        return (var if var < 0 else 0., cvar if cvar<0 else 0.)

            
            
    def compute_actual_portfolio_value(self, repricing_date_set, security = None):

        if security is None:
            positions = self.portfolio.security.holding.filter(date__in=repricing_date_set)

        else:
            positions = security.holding.filter(date__in=repricing_date_set)

        result = []

        for i,a in enumerate(sorted(repricing_date_set)):

            filter_positions = positions.filter(date=a)

            tot_mv =  sum( [ p.mv for p in filter_positions ])

            ret = Decimal(0)
            if i>0 and result[i-1][2] != 0:
                ret = tot_mv / result[i-1][2] - 1
            else:
                ret = None

            result.append((a, filter_positions.count(), tot_mv, ret))

            # print "{0:12}{1:12}{2:12.2f}{3:6.4f}".format(str(a), filter_positions.count(), tot_mv, ret)

        return result


    # def plot_performance(self, portfolio, benchmark):
    #     from matplotlib import * as plt

    #     plt.plot([p[0] for p in port_13f_mv], [p[1]/1000000 for p in port_13f_mv],"b",[p[0] for p in spx_mv],[p[1]/spx_mv[0][1] for p in spx_mv],"r")
    #     plt.show()



    def plot(self, plot_dates, navs):

        years    = mdates.YearLocator()   # every year
        months   = mdates.MonthLocator()  # every month
        yearsFmt = mdates.DateFormatter('%Y')

        fig, ax = plt.subplots()
        ax.plot(plot_dates, navs)

        # format the ticks
        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.xaxis.set_minor_locator(months)

        datemin = min(plot_dates)
        datemax = max(plot_dates)
        ax.set_xlim(datemin, datemax)

        # format the coords message box
        # def price(x): return '$%1.2f'%x
        ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
        # ax.format_ydata = price
        ax.grid(True)

        # rotates and right aligns the x labels, and moves the bottom of the
        # axes up to make room for them
        fig.autofmt_xdate()

        plt.show()



if __name__ == '__main__':
    
    arg_parser = argparse.ArgumentParser(description='Search for 13D filings and scrape contents')

    arg_parser.add_argument('--strat',     choices={"13F","13D","BOTH"}, help='What filings should be used in the model portfolio')
    arg_parser.add_argument('--rebalance', choices={"equalweight_mv" }, help='What is the rebalancing strategy')
    arg_parser.add_argument('--start',     default=datetime.date(2008,12,31).strftime("%Y%m%d"), help="First holdings date")
    arg_parser.add_argument('--end',       default=datetime.date.today().strftime("%Y%m%d"), help = "Last holdings date")
    arg_parser.add_argument('--seed',      default=Decimal(1000000), help="What is the initial amount to be seeded on day 1")
    arg_parser.add_argument('--max_names', default=50, type=int, help="Max number of positions in the portfolio")
    arg_parser.add_argument('--freq',      default='monthly', choices={"daily","monthly","quarterly"},  help="How often should portfolio holdings be regenerated and re-priced")
    arg_parser.add_argument('--loglevel', default="INFO", choices={"WARN","INFO","DEBUG"}, help='Logging level')

    args = arg_parser.parse_args()
    
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)

    logging.basicConfig(filename="construct_model_portfolio_{}_{}_{}.log".format(args.strat, args.rebalance, args.freq),
                        filemode="w",
                        level=numeric_level,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    port_name  = "{0} {1} TOP-{2} MODEL ACTIVIST FUND".format(args.strat.upper(), args.rebalance.upper(),args.max_names)

    start_date = dateutil.parser.parse(args.start).date()
    end_date   = dateutil.parser.parse(args.end).date()
    funds      = af.SecFilingStrategyFundMember.objects.filter(strategy__code="{}_ONLY".format(args.strat))
    cik_list   = [nbris.SecurityReference.objects.get(sec_ref_type__sec_ref_type="CIK", security=a.fund.security).sec_id_value for a in funds]
    portfolio  = nbris.Portfolio.objects.get(security__name=port_name)

    if args.strat=="13F":
        
        filings = af.SecFiling.objects.filter(form_type__code = "13F-HR",
                                              filer_cik__in   = cik_list,
                                              filed_date__range=(start_date,end_date)).order_by('report_period','filer_name')


    elif args.strat=="13D":

        filings = af.SecFiling.objects.filter(form_type__code__in = ["SC 13D", "SC 13D/A"],
                                              filer_cik__in       = cik_list,
                                              filed_date__range   = (start_date,end_date)).order_by('filed_date','filer_name')

    elif args.strat=="BOTH":
        filings = af.SecFiling.objects.filter(form_type__code__in = ["SC 13D", "SC 13D/A","13F-HR"],
                                              filer_cik__in       = cik_list,
                                              filed_date__range   = (start_date,end_date)).order_by('filed_date','filer_name')

    else:
        pass
        
    if args.freq=="daily":
        repricing_date_set = set([d.date() for d in rrule(DAILY, byweekday=(MO,TU,WE,TH,FR), dtstart=start_date, until=end_date)])

    elif args.freq=="monthly":
        repricing_date_set = set([d.date() for d in rrule(MONTHLY, dtstart=start_date, until=end_date, bymonthday=-1)])
        
    elif args.freq=="quarterly":
        repricing_date_set = set([d.date() for d in rrule(MONTHLY, dtstart=start_date, until=end_date, bymonthday=-1, interval=3)])

    pc = portfolio_constructor(args.strat,
                               args.rebalance,
                               args.freq,
                               args.max_names,
                               portfolio)

    pc.construct(filings, repricing_date_set, Decimal(args.seed))


    ##don't compute return for first leading months before filing
    rs = pc.compute_actual_portfolio_value(sorted(repricing_date_set)[1:])
    with open("{}.csv".format(port_name),"w") as f:
        for p in rs:
            f.write("{}|{}|{}|{}\n".format(*p))

    nprs = np.array([r[2] for r in rs], dtype = np.float64)

    stdev = np.std(nprs) * np.sqrt(12)

    plot_dates = sorted(repricing_date_set)[1:]

    pc.plot(plot_dates, nprs)

    # cik_managers_13f = set(['0001063296',
    #                         '0001325256',
    #                         '0000921669',
    #                         '0001277742',
    #                         '0001336528',
    #                         '0001047644',
    #                         '0001517137',
    #                         '0001345471',
    #                         '0001418814'])
    # cik_managers_13d = set(['0001063296',
    #                         '0001325256',
    #                         '0000921669',
    #                         '0001277742',
    #                         '0001336528',
    #                         '0001047644',
    #                         '0001517137',
    #                         '0001345471',
    #                         '0001418812',
    #                         '0001535472',
    #                         '0001079114',
    #                         '0001079563',
    #                         '0001159159',
    #                         '0001541996',
    #                         '0001029160',
    #                         '0001040273'])


def queries():

    ##list all ICAHN 13F filings history for TRANSOCEAN
    for i,p in enumerate(af.SecFilingDetail.objects.filter(sec_filing__filer_cik="0000921669",
                                                           sec_filing__form_type__code__in=["13F-HR"],
                                                           subject_name__startswith="TRANSOCEAN").order_by("-sec_filing__filed_date")):

        print "{0:<3} {1:14} {2:8} {3:12} {4:10} {5:40} {6:12} {7:16} {8:>0.4f}".format(i,
                                                                                          p.sec_filing.a_number,
                                                                                          p.sec_filing.form_type,
                                                                                          p.sec_filing.filed_date.__str__(),
                                                                                          p.cusip, 
                                                                                          p.subject_name,
                                                                                          p.no_shares, 
                                                                                          p.mv,
                                                                                          p.pct_shares if p.pct_shares is not None else 0)

        

    ##dump all filings to csv file

    with open("SEC_filings_db.csv","w") as f :
        for i,p in enumerate(af.SecFilingDetail.objects.all().order_by("-sec_filing__filed_date")):


            f.write("{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}\n".format(i+1,
                                                                                 p.sec_filing.a_number,
                                                                                 p.sec_filing.form_type,
                                                                                 p.sec_filing.filed_date.__str__(),
                                                                                 p.sec_filing.filer_cik,
                                                                                 p.sec_filing.filer_name,
                                                                                 p.cusip,
                                                                                 p.subject_name,
                                                                                p.no_shares, 
                                                                                 p.pct_shares,
                                                                                 p.mv,
                                                                                 p.pct_mv))

