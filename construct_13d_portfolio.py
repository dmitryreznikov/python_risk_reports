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
sys.path.append("//nb/corp/PST/NY1/drezniko/src/NBRIS")

import nbris_riskhub.models as nbris
import activist_fund.models as af
from django.db.models import Sum, Max, Min

import bbg_api_wrap as bbg
import get_bbg_price 
import logging

class portfolio_constructor():
    def __init__(self, strat, rebalance, freq, max_names, useBBG,port):
        self.bh        = bbg.bbg_helper()
        self.portfolio = port
        self.strategy  = strat
        self.rebalance = rebalance
        self.frequency = freq.upper()
        self.max_names = max_names
        self.useBBG    = useBBG
        



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
                    # (bbg_price, exc) = self.bh.bbg_create_price([sec],
                    #                                             "PX_LAST",
                    #                                             vd,
                    #                                             None,
                    #                                             self.frequency)
                
                    # if len(bbg_price)>0:
                    #     sec_price = bbg_price[0].price
                    
                    # else:
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

            tot_mv = filter_positions.aggregate(Sum('mv')).get('mv__sum')
            
            ret = Decimal(0)

            if i>0 and result[i-1][2] != 0 and tot_mv is not None:
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


    def fill_missing_prices_over_horizon(self, subject_security, current_date, horizon):

        price_reload_start_date = current_date+relativedelta(months = -horizon)
        price_reload_end_date   = current_date+relativedelta(months = +horizon)

        latest_db_price_before_current = subject_security.price_set.filter(date__lte=current_date,
                                                                           date__gte=price_reload_start_date).aggregate(Max('date')).get('date__max')


        
        if latest_db_price_before_current is None:
            (bbg_price, exc) = get_bbg_price.bbg_create_price(self.bh,
                                                        [subject_security],
                                                        "PX_LAST",
                                                        price_reload_start_date,
                                                        current_date,
                                                        "DAILY",
                                                        "CALENDAR")

            logging.info("Refreshed prices  for {}  from {} to {}: count {} exceptions {}".format(
                subject_security, 
                price_reload_start_date,
                current_date, 
                len(bbg_price), 
                exc[0]))

        elif latest_db_price_before_current < current_date:

            (bbg_price, exc) = get_bbg_price.bbg_create_price(self.bh,
                                                        [subject_security],
                                                        "PX_LAST",
                                                        latest_db_price_before_current,
                                                        current_date,
                                                        "DAILY",
                                                        "CALENDAR")

            logging.info("Refreshed prices  for {}  from {} to {}: count {} exceptions {}".format(
                subject_security, 
                latest_db_price_before_current,
                current_date, 
                len(bbg_price), 
                exc[0]))

        db_price_forward = subject_security.price_set.filter(date__gte=current_date,
                                                             date__lte=price_reload_end_date).aggregate(Min('date'),Max('date'))

        earliest_db_price_after_current = db_price_forward.get('date__min')
        latest_db_price_before_next_rebal = db_price_forward.get('date__max')

        if earliest_db_price_after_current is None:

            (bbg_price, exc) = get_bbg_price.bbg_create_price(self.bh,
                                                        [subject_security],
                                                        "PX_LAST",
                                                        current_date,
                                                        price_reload_end_date,
                                                        "DAILY",
                                                        "CALENDAR")

            logging.info("Refreshed prices  for {}  from {} to {}: count {} exceptions {}".format(
                subject_security,
                current_date,
                price_reload_end_date,
                len(bbg_price),
                exc[0]))

        elif earliest_db_price_after_current > current_date:

            (bbg_price, exc) = get_bbg_price.bbg_create_price(self.bh,
                                                        [subject_security],
                                                        "PX_LAST",
                                                        current_date,
                                                        earliest_db_price_after_current,
                                                        "DAILY",
                                                        "CALENDAR")

            logging.info("Refreshed prices  for {}  from {} to {}: count {} exceptions {}".format(
                subject_security,
                current_date,
                earliest_db_price_after_current,
                len(bbg_price),
                exc[0]))

        if latest_db_price_before_next_rebal is None:
            (bbg_price, exc) = get_bbg_price.bbg_create_price(self.bh,
                                                        [subject_security],
                                                        "PX_LAST",
                                                        current_date,
                                                        latest_db_price_before_next_rebal,
                                                        "DAILY",
                                                        "CALENDAR")

            logging.info("Refreshed prices  for {}  from {} to {}: count {} exceptions {}".format(
                subject_security,
                current_date,
                latest_db_price_before_next_rebal,
                len(bbg_price),
                exc[0]))


        elif latest_db_price_before_next_rebal < price_reload_end_date:
            (bbg_price, exc) = get_bbg_price.bbg_create_price(self.bh,
                                                        [subject_security],
                                                        "PX_LAST",
                                                        latest_db_price_before_next_rebal,
                                                        price_reload_end_date,
                                                        "DAILY",
                                                        "CALENDAR")

            logging.info("Refreshed prices  for {}  from {} to {}: count {} exceptions {}".format(
                subject_security,
                latest_db_price_before_next_rebal,
                price_reload_end_date,
                len(bbg_price),
                exc[0]))



    def create_security_with_cusip_from_detail(self, detail, rejected_security):

        sec_type_cusip  = nbris.SecurityReferenceType.objects.get(sec_ref_type="CUSIP")

        subject_security = get_bbg_price.bbg_create_security(self.bh, 
                                                             secid="{}".format(detail.cusip),
                                                             secid_type=sec_type_cusip)

        (market_status, trade_status, last_trade_dt, warning) = get_bbg_price.bbg_security_market_status(self.bh, subject_security)

        if type(warning) is bbg.bbgTickerChangeWarning or type(warning) is bbg.bbgAcquisitiondWarning:

            logging.warn("Security {}: old ticker:{} new ticker:{} last traded {} reason: {}".format(subject_security,
                                                                                                     warning.old_ticker,
                                                                                                     warning.new_ticker,
                                                                                                     last_trade_dt,
                                                                                                     warning.status))

            status_change,created = nbris.SecurityStatusChange.objects.get_or_create(security      = subject_security,
                                                                                     change_status = warning.status,
                                                                                     defaults = {'effective_date':last_trade_dt})
            if not created and status_change.effective_date != last_trade_dt:
                status_change.effective_date = last_trade_dt
                status_change.save()


            try:
                sec_type_ticker = nbris.SecurityReferenceType.objects.get(sec_ref_type="TICKER")

                parent_security = get_bbg_price.bbg_create_security(bbg_helper=self.bh, 
                                                                    secid=warning.new_ticker, 
                                                              secid_type=sec_type_ticker,
                                                              yellow_key="Equity")

                # (market_status, trade_status, last_trade_dt, warning2) = self.bh.bbg_security_market_status(parent_security)

                status_change.parent_security = parent_security
                status_change.save()

            except (bbg.bbgCreateSecurityFailedException, bbg.bbgException) as e:
                rejected_security[warning.new_ticker] = str(e)

        elif type(warning) is bbg.bbgExchangeStatusChangeWarning:

            logging.warn("Security {}: ticker:{} exchange market change:{}".format(subject_security,
                                                                                   warning.old_ticker,
                                                                                   warning.status))

            status_change,created = nbris.SecurityStatusChange.objects.get_or_create(security      = subject_security,
                                                                                     change_status = warning.status)

        return subject_security




    def equalweight_mv_fully_invested(self, holding_date, roll_from_date, sec_rank_wgt_sorted, rejects):

        self.equalweight_mv(holding_date, roll_from_date, sec_rank_wgt_sorted, rejects)

        all_position = self.portfolio.security.holding.filter(date = holding_date)
        portfolio_mv = all_position.aggregate(Sum('mv')).get('mv__sum')
        portfolio_cash = all_position.get(security__sec_type__sec_type = "CASH", security__name="USD Cash")

        all_position = all_position.exclude(security__sec_type__sec_type="CASH")

        if all_position.count()>0:
            equal_weight = Decimal(1.0) / all_position.count()

            ##REBALANCE all positions to be fully invested and equally MV weighted
            for curr_pos in all_position:
                old_quantity = curr_pos.quantity

                db_price = curr_pos.security.price_set.filter(date__lte=holding_date,
                                                              date__gte  = holding_date + relativedelta(months=-1), 
                                                              ds__code="BBG").order_by("-date").first()

                if db_price is not None:
                    new_quantity      = Decimal(equal_weight * portfolio_mv // db_price.price)
                    curr_pos.quantity = new_quantity
                    curr_pos.mv       = new_quantity * db_price.price
                    curr_pos.save()

                    cash_adjust              = db_price.price * ( new_quantity-old_quantity )
                    portfolio_cash.mv       -= cash_adjust
                    portfolio_cash.quantity -= cash_adjust

                    logging.info("{0:12}{1:12}{2:30}{3:15,}{4:>15,.2f}{5:>8,.2f}{6:>8,.2f}".format(
                        str(holding_date),
                        "REBFI",
                        curr_pos.security.name,
                        new_quantity-old_quantity,
                        cash_adjust,
                        100 * cash_adjust / portfolio_mv,
                        db_price.price))


                    if db_price.date + relativedelta(days=+3) < holding_date:
                        rejects[curr_pos.security] = "REBALANCE {} at stale price of {}:{}".format(curr_pos.security, db_price.date, db_price.price)

                else:
                    rejects[curr_pos.security] = "CAN'T REBALANCE SECURITY {} has no BBG prices prior to {}".format(curr_pos.security, holding_date) 


            portfolio_cash.save()




        
    def equalweight_mv(self, holding_date, roll_from_date, sec_rank_wgt_sorted, rejected_security):

        if roll_from_date is not None:
            prev_position = self.portfolio.security.holding.filter(date = roll_from_date).order_by('id')
            portfolio_old_mv = prev_position.aggregate(Sum('mv')).get('mv__sum')

            logging.info("{0:<12}{1:<12}{2:<30}{3:>15}{4:>15}{5:>8}{6:>8}".format("DATE","ACTION","SEC_NAME","SHARES","MV USD","MV %%","PRICE"))

            ##########################################
            # roll positions forward
            # update price and mv
            ############################################
            for sec_cnt, old_pos in enumerate(prev_position):

                db_price = old_pos.security.price_set.filter(date__lte = holding_date, 
                                                             date__gte  = holding_date + relativedelta(months=-1), 
                                                             ds__code  = "BBG").order_by("-date").first()

                if db_price is not None:
                    new_mv = old_pos.quantity * db_price.price


                    new_pos = nbris.Position.objects.create(date             = holding_date,
                                                            holding_security = old_pos.holding_security,
                                                            security         = old_pos.security,
                                                            currency         = old_pos.currency,
                                                            quantity         = old_pos.quantity,
                                                            mv               = new_mv)

                    logging.info("{0:12}{1:12}{2:30}{3:15,}{4:>15,.2f}{5:>8,.2f}{6:>8,.2f}".format(
                        str(holding_date),
                        "ROLL",
                        old_pos.security.name, 
                        old_pos.quantity, 
                        new_mv,
                        100 * new_mv / portfolio_old_mv,
                        db_price.price))

                    if db_price.date + relativedelta(days=+3) < holding_date:
                        rejected_security[old_pos.security] ="ROLLED {0} to {1} using stale price {2}={3:0.4f}".format(old_pos.security,
                                                                                                                       holding_date,
                                                                                                                       db_price.date,
                                                                                                                       db_price.price)

                else:
                    rejected_security[old_pos.security] = "DROPPED {} BLOOMBERG has no prices 1 month prior to {}".format(old_pos.security, holding_date)

                    ########assumes the cash is rolled first to the new date##############
                    portfolio_cash = self.portfolio.security.holding.get(date = holding_date,
                                                                         security__sec_type__sec_type = "CASH",
                                                                         security__name="USD Cash")
                    portfolio_cash.mv       += old_pos.mv
                    portfolio_cash.quantity += old_pos.mv
                    portfolio_cash.save()

            ##for sec_cnt, old_pos in enumerate(prev_position):

        # if roll_from_date is not None:

        equal_weight = Decimal(1.0/self.max_names) # Decimal(1.0 / len(sec_rank_wgt_sorted)))

        all_position = self.portfolio.security.holding.filter(date = holding_date)
        portfolio_mv = all_position.aggregate(Sum('mv')).get('mv__sum')

        portfolio_cash   = all_position.get(security__sec_type__sec_type = "CASH", security__name="USD Cash")
        old_security_set = set ([p.security for p in all_position if p.security.sec_type.sec_type != "CASH"])


        sec_to_keep = []

        for i,sec in enumerate(sec_rank_wgt_sorted):
            
            db_price = sec.price_set.filter(date__lte=holding_date, 
                                            date__gte=holding_date + relativedelta(months=-1),
                                            ds__code="BBG").order_by("-date").first()

            if len(sec_to_keep) < self.max_names:

                #############################################
                # buy new positions that appeared in filings
                #############################################
                if sec not in old_security_set:

                    if db_price is not None:
                        quantity = Decimal(equal_weight * portfolio_mv // db_price.price)
                        mv       = Decimal(quantity * db_price.price)

                        new_position = nbris.Position.objects.create(date             = holding_date,
                                                                     holding_security = self.portfolio.security,
                                                                     currency         = sec.currency,
                                                                     security         = sec,
                                                                     quantity         = quantity,
                                                                     mv               = mv)
                        portfolio_cash.mv       -= mv
                        portfolio_cash.quantity -= mv

                        logging.info("{0:12}{1:12}{2:30}{3:15,}{4:>15,.2f}{5:>8,.2f}{6:>8,.2f}".format(
                            str(holding_date),
                            "BUY",
                            sec.name,
                            quantity,
                            mv,
                            100 * mv / portfolio_mv,
                            db_price.price))

                        sec_to_keep.append(sec)

                        if db_price.date + relativedelta(days=+3) < holding_date:
                            rejected_security[sec] = "BUY {} at stale price of {}:{}".format(sec, db_price.date, db_price.price)

                    else:
                        rejected_security[sec] = "CAN'T BUY NEWLY FILED SECURITY {} has no BBG prices 1 month prior to {}".format(sec, holding_date) 


                else:
                    #############################################
                    # Rebalance existing positions 
                    #############################################

                    curr_pos = all_position.get(security = sec)

                    old_quantity = curr_pos.quantity

                    if db_price is not None:
                        new_quantity      = Decimal(equal_weight * portfolio_mv // db_price.price)
                        curr_pos.quantity = new_quantity
                        curr_pos.mv       = new_quantity * db_price.price
                        curr_pos.save()

                        cash_adjust              = db_price.price * ( new_quantity-old_quantity )
                        portfolio_cash.mv       -= cash_adjust
                        portfolio_cash.quantity -= cash_adjust

                        logging.info("{0:12}{1:12}{2:30}{3:15,}{4:>15,.2f}{5:>8,.2f}{6:>8,.2f}".format(
                            str(holding_date),
                            "REBAL",
                            sec.name,
                            new_quantity-old_quantity,
                            cash_adjust,
                            100 * cash_adjust / portfolio_mv,
                            db_price.price))

                        sec_to_keep.append(sec)


                        if db_price.date + relativedelta(days=+3) < holding_date:
                            rejected_security[sec] = "REBALANCE {} at stale price of {}:{}".format(sec, db_price.date, db_price.price)

                    else:
                        rejected_security[sec] = "CAN'T REBALANCE NEWLY FILED SECURITY {} has no BBG prices 1 month prior to {}".format(sec, holding_date) 
            else:
                break

        #######################
        # Sell all positions that disappeared from current filings
        #######################
        for sec in old_security_set:

            if sec not in sec_to_keep:
                
                curr_pos = all_position.get(security = sec)

                portfolio_cash.mv       += curr_pos.mv
                portfolio_cash.quantity += curr_pos.mv

                db_price = sec.price_set.filter(date__lte=holding_date, ds__code="BBG").order_by("-date").first()

                logging.info("{0:12}{1:12}{2:30}{3:15,}{4:>15,.2f}{5:>8,.2f}{6:>8,.2f}".format(
                    str(holding_date),
                    "SELL",
                    curr_pos.security.name,
                    curr_pos.quantity,
                    curr_pos.mv,
                    100 * curr_pos.mv / portfolio_mv,
                    db_price.price if db_price is not None else 0.))

                curr_pos.delete()

                if db_price is not None:
                    if db_price.date + relativedelta(days=+2) < holding_date:
                        rejected_security[sec] = "SOLD {} at stale price of {}:{}".format(sec, db_price.date, db_price.price)

                else:
                    rejected_security[sec] = "SOLD {} at 0.0 price".format(sec)

        portfolio_cash.save()





    def extract_security_mv_from_sec_filing(self, filing, current_date, rejected_security, new_security_cusip_set):
        sec_filing_total_mv = 0
        sec_rank_mv_dict = {}

        logging.info("Process filing {}".format(filing))

        # ##PROCESS AMENDED 13F/A forms
        # ##that were added at the end of 13F filings list
        # ## if sec is in 13FA but not most recent 13F for the same manager, should we ignore it?
        # ## if the sec is in both 13F/A and most recent 13F for the same manager, how do we avoid inflating the rankings?

        #     most_recent_13F = af.SecFiling.objects.filter(filer_cik=use_filer_cik,
        #                                                   filed_date__lte=current_date,
        #                                                   form_type=sec_form_13F).order_by('-filed_date').first()
        #     if most_recent_13F is not None:
        #         try:
        #             sec_filing_total_mv = af.SecFilingMarketValue.objects.get(sec_filing=most_recent_13F).mv

        #         except af.SecFilingMarketValue.DoesNotExist:
        #             sec_filing_total_mv = sum([fd.mv for fd in most_recent_13F.secfilingdetail_set.all() if fd.mv is not None])

        #         logging.info("Use most recent 13F form {0} market value {1:12,.0f} to rank contents of {2}".format(most_recent_13F,sec_filing_total_mv,filing))

        #     # else:
        #         # for fd in filing.secfilingdetail_set.all():
        #         #     err_str = "Unable to find any 13F-HR form to reference MV for ranking of {}".format(fd)
        #         #     rejected_security[filing] =  err_str

        #######################################################
        # process filing detail make sure all securities are in SecurityMaster
        #######################################################
        for detail in filing.secfilingdetail_set.all():
            logging.debug("Process filing detail {}".format(detail))

            if detail.cusip is None:
                replacement_cusip = set(f.cusip for f in  af.SecFilingDetail.objects.filter(subject_name          = detail.subject_name,
                                                                                            sec_filing__filer_cik = filing.filer_cik,
                                                                                            cusip__isnull         = False))
                if len(replacement_cusip)>0:
                    logging.info("{} CUSIP field is NULL. Alternative CUSIPs used by the same manager for this name are: '{}'".format(detail, replacement_cusip))
                    detail.cusip = replacement_cusip.pop()

                else:
                    rejected_security[detail.subject_name] = "{} CUSIP field is NULL and no similar name exists in filings of this manager".format(detail)

            if detail.cusip is not None:
                subject_security=None
                                                                                                    
                if self.useBBG and detail.cusip not in new_security_cusip_set:

                    try:
                        subject_security = self.create_security_with_cusip_from_detail(detail, rejected_security)

                    except bbg.bbgCreateSecurityFailedException as e:
                        rejected_security[detail.subject_name] = str(e)

                else:

                    try:
                        subject_security = nbris.SecurityMaster.objects.get(securityreference__sec_id_value=detail.cusip,
                                                                            securityreference__sec_ref_type__sec_ref_type="CUSIP")

                    except  nbris.SecurityMaster.DoesNotExist as e:
                        rejected_security[detail.subject_name] = "Security with CUSIP {} does not exist in SecurityMaster".format(detail.cusip)

                    except nbris.SecurityMaster.MultipleObjectsReturned as e:
                        rejected_security[detail.subject_name] = "CUSIP {} mapped to more than one security, fix SecurityReference".format(detail.cusip)

                if subject_security is not None:
                    if subject_security.sec_type.sec_type not in ("HedgeFund", "MLP", "REIT","ETF","ETP"):

                        security_after_status_change = subject_security.security_before_change_status.filter(change_status__reason="Ticker Change").first()
                        if security_after_status_change is not None:

                            logging.warn("Security {} changed to {} through corporate action. New security ticker will be used".format(subject_security, security_after_status_change.parent_security))

                            subject_security = security_after_status_change.parent_security

                        if self.useBBG:
                            self.fill_missing_prices_over_horizon(subject_security, current_date, 4)

                        if detail.mv is not None:
                            sec_rank_mv_dict[subject_security] = sec_rank_mv_dict.get(subject_security, Decimal(0)) + detail.mv

                        else:

                            if detail.no_shares is not None:
                                db_price = subject_security.price_set.filter(date__lte=filing.filed_date, ds__code="BBG").order_by("-date").first()

                                if db_price is not None:
                                    sec_rank_mv_dict[subject_security] = sec_rank_mv_dict.get(subject_security, Decimal(0)) + (detail.no_shares * db_price.price)
                                    sec_filing_total_mv += (detail.no_shares * db_price.price)


                                else:
                                    errstr = "Filing {} has no price on BBG on or before {} -- cannot compute filing MV".format(
                                    detail, current_date)

                                    rejected_security[detail.subject_name] = errstr
                            else:
                                if filing.form_type.code in ["13F-HR","13F-HR/A"]:
                                    errstr = "Filing {} contains null number of shares -- assigning 0 rank".format(detail)
                                    sec_rank_mv_dict[subject_security] = sec_rank_mv_dict.get(subject_security, Decimal(0)) + Decimal(0)
                                    
                                else:
                                    errstr = "Cannot rank filing {} -- contains null number of shares".format(detail)

                                rejected_security[detail.subject_name] = errstr


                    else:
                        logging.warn("Skip filing {} security type is {}".format(detail, subject_security.sec_type.sec_type))
                else:
                    logging.warn("Skip filing {} security was not found".format(detail))
                    
                # Null cusips are errors from parsing the filings file
                # Just ignore
                #
                # errstr = "SECURITY FROM FILING NOT LOADED INVALID CUSIP:{}".format(str(detail.cusip))
                # rejected_security[detail.subject_name] = errstr

                # imperror,created = af.SecFilingImportError.objects.get_or_create(filing_detail=filing_detail,
                #                                                                  error = af.ImportError.objects.get(name="Invalid Cusip"),
                #                                                                  defaults={'description':errstr} )
                # if not created:
                #     imperror.defaults = errstr
                #     imperror.save()

                new_security_cusip_set.add(detail.cusip)
                
            else:
                pass
                ## cusip field is NULL, can't do much here except update the total filing MV
                ## and move on to the next CUSIP
                
            ##take the filing total MV counter out of inner loop. We want total MV to be accurate as possible regadless
            ##of whether bloomberg recognized security and has pricing for this particular security
            if detail.mv is not None:
                sec_filing_total_mv += detail.mv

            else:
                pass
                ## filing has null value for market value; if bbg has pricing for this CUSIP, then the inner loop
                ## will try to update the total MV counter by using no shares field

            # if detail.cusip is not None:
        # for detail in filing.secfilingdetail_set.all():

        sfmv = af.SecFilingMarketValue.objects.create(sec_filing = filing, mv =sec_filing_total_mv)

        return (sec_rank_mv_dict, sec_filing_total_mv)




    def rank_security_from_13F(self, filings_13f):
        new_security_cusip_set = set()
        rebal_date_to_filing_dict = {}

        report_date_set = set([d.report_period for d in filings_13f if d.report_period is not None])
        filing_date_set = set()

        for rd in sorted(report_date_set):

            reported_filings = list(filings_13f.filter(report_period=rd).order_by('filer_name'))

            max_filed_date = max([d.filed_date for d in reported_filings])

            next_bus_day = max_filed_date + relativedelta(days=+1)

            if next_bus_day.weekday() >= 5:
                next_bus_day  += relativedelta(weekday=MO)

            # Add amended 13F/A filings that were filed since the last rebalance date
            # Only consider amendments for the last 2 quarters 
            # discregard any that amend for 3 quarters or more back in time
            # filings_13fa = filings_13f.filter(form_type=sec_form_13FA,
            #                                  filed_date__range=(last_rebal_date, max_filed_date)).order_by('filer_name')

            # for f in filings_13fa:
            #    if f.filed_date <= f.report_period + relativedelta(months=+6):
            #        reported_filings.append(f)

            reported_filings = sorted(reported_filings, key=operator.attrgetter('filed_date'))

            rebal_date_to_filing_dict[next_bus_day] = reported_filings

            for d in reported_filings:
                filing_date_set.add(d.filed_date)

        # all_dates_set = repricing_date_set.union(rebal_date_to_filing_dict.keys())

        ################################################################
        # report_date_set is the unique date set for which filings 13F are reporting on, quarterly month end
        # repricing_date_set is the set of ordered dates on which portfolio will be valued: user-generated daily, monthly, quarterly
        # filing_date_set is the set of ordered dates on which the filing was made, for 13F 5-6 weeks after report_date, for 13D 10days after the event
        # rebal_date_to_filing_dict maps the next business day after a date in filing_date_set to the last filing date in the reporting period and all the filings that fall in that period
        

        logging.info("{0:<5}{1:<15}{2:5}{3:10}{4:10}{5:10}{6:10}".format("ROW","DATE","DAY","ASOF","FILED","REBALANCE","FORM_TYPE"))
                                 
        for i, dt in enumerate(sorted(set(rebal_date_to_filing_dict.keys()).union(filing_date_set).union(report_date_set))):
            filed_on_date = rebal_date_to_filing_dict.get(dt)

            logging.info("{0:<5}{1:<15}{2:5}{3:10}{4:10}{5:10}{6:10}".format(i+1, 
                                                                             str(dt),
                                                                             ["MO","TU","WE","TH","FR","SA","SU"][dt.weekday()],
                                                                             "x" if dt in report_date_set else ".",
                                                                             "x" if dt in filing_date_set else ".",
                                                                             "x" if rebal_date_to_filing_dict.has_key(dt) else ".",
                                                                             "." if filed_on_date is None else ":".join(sorted([str(s) for s in set(
                                                                                 [f.form_type.code for f in filed_on_date if f is not None])]))))
            
        for current_date in sorted(rebal_date_to_filing_dict.keys()):

            rejected_security = {}

            ########################################
            ##process each filing manager info
            ########################################
            sec_rank_wgt_all_dict = {}
            for i,filing in enumerate(rebal_date_to_filing_dict[current_date]):

                logging.info("Creating security rankings from filing {} rebalanced on {}".format(filing, current_date))

                (sec_rank_mv_dict, sec_filing_total_mv) = self.extract_security_mv_from_sec_filing(filing,
                                                                                                   current_date,
                                                                                                   rejected_security,
                                                                                                   new_security_cusip_set)

                # TODO: the 13F amended form might only have additions so the form weight is overstated
                # need somehow to dig up the filing for that manager for that quarter and inject 
                # logging.info("{0:5} {1:40} {2}".format("RANK","NAME","FILING MV WGT"))

                manager = nbris.SecurityReference.objects.get(sec_ref_type__sec_ref_type = "CIK",
                                                              sec_id_value = filing.filer_cik).security.portfolio

                rank_wgt = 0

                for (sec, mv) in sec_rank_mv_dict.iteritems():

                    if sec_filing_total_mv>0:
                        rank_wgt = sec_rank_wgt_all_dict.get(sec, Decimal(0)) + mv / sec_filing_total_mv

                    else:
                        rank_wgt = sec_rank_wgt_all_dict.get(sec, Decimal(0))

                    rank_db = af.SecFilingSecurityRank.objects.create(date     = current_date,
                                                                      security = sec,
                                                                      fund     = manager,
                                                                      filing   = filing,
                                                                      rank_wgt = rank_wgt)

                    logging.info("Created new security ranking {}".format(rank_db))

            # for i,filing in enumerate(rebal_date_to_filing_dict[current_date]):


            for (i,(k,v)) in enumerate(rejected_security.iteritems()):
                logging.error("{0:<5}ERROR {1:<30} {2:40}".format(i+1, k, v))


        #for current_date in
        



        
    def rank_security_from_13D(self, filings_13f):
        sec_form_13F    = af.SecFilingType.objects.get(code="13F-HR")
        
        new_security_cusip_set = set()
        rebal_date_to_filing_dict = {}

        filing_date_set = set([d.filed_date for d in filings_13d if d.filed_date is not None])

        for rd in sorted(filing_date_set):

            reported_filings = list(filings_13d.filter(filed_date=rd).order_by('filer_name'))

            next_bus_day = rd + relativedelta(days=+1)

            if next_bus_day.weekday() >= 5:
                next_bus_day  += relativedelta(weekday=MO)

            rebal_date_to_filing_dict[next_bus_day] = reported_filings

        ################
        # report_date_set is the unique date set for which filings 13F are reporting on, quarterly month end
        # repricing_date_set is the set of ordered dates on which portfolio will be valued: user-generated daily, monthly, quarterly
        # filing_date_set is the set of ordered dates on which the filing was made, for 13F 5-6 weeks after report_date, for 13D 10days after the event
        # rebal_date_to_filing_dict maps the next business day after a date in filing_date_set 
        # to the last filing date in the reporting period and all the filings that fall in that period

        logging.info("{0:<5}{1:<15}{2:5}{3:10}{4:10}{5:10}".format("ROW","DATE","DAY","FILED","REBALANCE","FORM_TYPE"))

        for i, dt in enumerate(sorted(set(rebal_date_to_filing_dict.keys()).union(filing_date_set))):
            filed_on_date = rebal_date_to_filing_dict.get(dt)

            logging.info("{0:<5}{1:<15}{2:5}{3:10}{4:10}{5:10}".format(i+1, 
                                                                       str(dt),
                                                                       ["MO","TU","WE","TH","FR","SA","SU"][dt.weekday()],
                                                                       "x" if dt in filing_date_set else ".",
                                                                       "x" if rebal_date_to_filing_dict.has_key(dt) else ".",
                                                                       "." if filed_on_date is None else ":".join(sorted([str(s) for s in set(
                                                                           [f.form_type.code for f in filed_on_date if f is not None])])),
                                                                   ))
            
        for current_date in sorted(rebal_date_to_filing_dict.keys()):

            rejected_security = {}

            #########################################
            ##process each filing manager info
            #########################################
            sec_rank_wgt_all_dict = {}

            for i,filing in enumerate(rebal_date_to_filing_dict[current_date]):

                logging.info("Creating security rankings from filing {} rebalanced on {}".format(filing, current_date))

                (sec_rank_mv_dict, sec_filing_total_mv) = self.extract_security_mv_from_sec_filing(filing,
                                                                                                   current_date,
                                                                                                   rejected_security,
                                                                                                   new_security_cusip_set)

                
                manager = nbris.SecurityReference.objects.get(sec_ref_type__sec_ref_type = "CIK",
                                                              sec_id_value = filing.filer_cik).security.portfolio

                rank_wgt = 0

                ## use most recent 13F as the base market value
                # to assign weight to this 13d

                if filing.filer_cik == '0001418812':
                    use_filer_cik = '0001418814'

                else:
                    use_filer_cik = filing.filer_cik

                most_recent_13F = af.SecFiling.objects.filter(filer_cik       = use_filer_cik,
                                                              filed_date__lte = current_date,
                                                              form_type       = sec_form_13F).order_by('-filed_date').first()

                if most_recent_13F is not None:
                    try:
                        sec_filing_total_mv = af.SecFilingMarketValue.objects.get(sec_filing=most_recent_13F).mv

                    except af.SecFilingMarketValue.DoesNotExist:

                        # sec_filing_total_mv = sum([fd.mv for fd in most_recent_13F.secfilingdetail_set.all() if fd.mv is not None])
                        (sec_rank_mv_dict_13f, sec_filing_total_mv) = self.extract_security_mv_from_sec_filing(most_recent_13F,
                                                                                                               current_date,
                                                                                                               rejected_security,
                                                                                                               new_security_cusip_set)
                        
                    logging.info("Use most recent 13F form {0} market value {1:12,.0f} to rank contents of {2}".format(most_recent_13F,sec_filing_total_mv,filing))


                    for (sec, mv) in sec_rank_mv_dict.iteritems():

                        for detail in filing.secfilingdetail_set.all():

                            if detail.no_shares is not None and detail.no_shares == 0:

                                rank_wgt = Decimal(0.0)
                                logging.info("13D filing has 0 shares, security ranking to indicate sell {}".format(detail))

                            else:

                                if sec_filing_total_mv>0:
                                    rank_wgt = sec_rank_wgt_all_dict.get(sec, Decimal(0)) + mv / sec_filing_total_mv

                                else:
                                    rank_wgt = sec_rank_wgt_all_dict.get(sec, Decimal(0))

                            rank_db = af.SecFilingSecurityRank.objects.create(date     = current_date,
                                                                              security = sec,
                                                                              fund     = manager,
                                                                              filing   = filing,
                                                                              rank_wgt = rank_wgt)

                            logging.info("Created new security ranking {}".format(rank_db))


                    # for (sec_i,(s,w)) in enumerate(sec_rank_mv_dict.iteritems()):

                else:
                    logging.warn("Could not find any 13F filings for manager {0} unable to assign ranking to {1}".format(manager, filing))

            # For i,filing in enumerate(rebal_date_to_filing_dict[current_date]):


            for (i,(k,v)) in enumerate(rejected_security.iteritems()):
                logging.error("{0:<5}ERROR {1:<30} {2:40}".format(i+1, k, v))


        #for current_date in
        





    def construct_ranked_security_universe_13F_13D(self, repricing_date_set):

        sec_form_13D  = af.SecFilingType.objects.get(code="SC 13D")
        sec_form_13DA = af.SecFilingType.objects.get(code="SC 13D/A")

        sec_form_13F  = af.SecFilingType.objects.get(code="13F-HR")
        # sec_type_cusip = nbris.SecurityReferenceType.objects.get(sec_ref_type="CUSIP")
        
        all_dates = sorted(repricing_date_set.union(set(d.date for d in af.SecFilingSecurityRank.objects.filter(date__range=(min(repricing_date_set),
                                                                                                                             max(repricing_date_set))))))
        previous_date         = None
        security_universe_13d = dict()
        security_universe_13f = dict()

        for current_date in all_dates:

            logging.info("Creating security universe for {}".format(current_date))

            all_ranks_on_date     = af.SecFilingSecurityRank.objects.filter(date=current_date)
            all_13f_ranks_on_date = all_ranks_on_date.filter(filing__form_type=sec_form_13F)
            all_13d_ranks_on_date = all_ranks_on_date.filter(filing__form_type__in=[sec_form_13D, sec_form_13DA])
            
            #####################################################
            # CREATE INVESTIBLE UNIVERSE OUT OF 13F FILINGS
            #####################################################

            ## only replace security_universe_13f with a new security set
            ## if new 13f rankings exist, i.e. the new 13 was filed for all managers

            if len(all_13f_ranks_on_date)>0:
                security_universe_13f = dict()

                for rank13f in all_13f_ranks_on_date:
                    
                    ##see if manager filed 13d with 0 shares after reported_period of this 13F
                    sold_security_rank_for_manager = af.SecFilingSecurityRank.objects.filter(fund                      = rank13f.fund,
                                                                                             security                  = rank13f.security,
                                                                                             rank_wgt                  = Decimal(0),
                                                                                             filing__form_type__in     = [sec_form_13D, sec_form_13DA],
                                                                                             filing__filed_date__range = (rank13f.filing.report_period, current_date))

                    if sold_security_rank_for_manager.count()>0:
                        logging.info("Ignore {} -- security was sold according to {}".format(rank13f, sold_security_rank_for_manager))
 
                    else:
                        security_universe_13f[rank13f.security] = security_universe_13f.get(rank13f.security, Decimal(0)) + rank13f.rank_wgt

                    total_security_13d_rank = security_universe_13d.get(rank13f.security, None)
                    
                    # looks for duplicates in previously filed 13d's and 0's those out to avoid duped ratings
                    if total_security_13d_rank is not None:
                        most_recent_13d_manager_for_security = af.SecFilingSecurityRank.objects.filter(fund                      = rank13f.fund,
                                                                                                       security                  = rank13f.security,
                                                                                                       filing__form_type__in     = [sec_form_13D, sec_form_13DA],
                                                                                                       filing__filed_date__range = (min(all_dates), current_date)).order_by('-date').first()

                        if most_recent_13d_manager_for_security is not None:

                            # compare the date of the latest 13d filing and reported_period for this 13f
                            if most_recent_13d_manager_for_security.filing.filed_date < rank13f.filing.report_period:


                                if most_recent_13d_manager_for_security.rank_wgt>0:

                                    if total_security_13d_rank - most_recent_13d_manager_for_security.rank_wgt > 0:
                                        security_universe_13d[rank13f.security] = total_security_13d_rank - most_recent_13d_manager_for_security.rank_wgt

                                        logging.info("Reduced current total 13D security ranking of {} by  {} because 13F filing for {} is more up to date".format(
                                            total_security_13d_rank,
                                            most_recent_13d_manager_for_security.rank_wgt,
                                            rank13f))

                                    else:
                                        removed_wgt = security_universe_13d.pop(rank13f.security, None)

                                        logging.info("Removed current total 13D security ranking {} because 13F filing {} is more up to date".format(
                                            removed_wgt,
                                            rank13f))
                                else:
                                    pass
                                    # last ranking 13d is 0 shares, ignore

                            else:
                                #13d filed after 13f reported period, so remove out of date 13f filing 
                                removed_wgt = security_universe_13f.pop(rank13f.security,None)
                                logging.info("Ignored 13F ranking {} because 13D filing {} is more up to date".format(
                                    rank13f,
                                    most_recent_13d_manager_for_security))
                        else:
                            pass
                            # the ranking is not from this manager, ignore

                    else:
                        pass
                        ## this security has no prior 13D filing from this manager, nothing to do


                for mgr in set(f.fund for f in all_13f_ranks_on_date):
                    ## Compare to the security universe from previous 13f filing date

                    ## for each manager, compare securities in prev date filing and current_date filing
                    ## for all missing securities, check the date of the last 13D filing, if any
                    ## if 13D filing date precedes reported period of the last 13f, adjust 13D ranking universe (decrease or pop)
                    ## this should not be necessary if all managers filed 13d with 0 shares consistently (and there were no parse errors on import)
                    all_13f_ranks_prev_date= af.SecFilingSecurityRank.objects.filter(date__lt          = current_date, 
                                                                                     filing__form_type = sec_form_13F, 
                                                                                     fund              = mgr).order_by('-date').first()

                    if all_13f_ranks_prev_date is not None:
                        all_13f_ranks_on_date_prior = af.SecFilingSecurityRank.objects.filter(date=all_13f_ranks_prev_date.date, filing__form_type=sec_form_13F, fund=mgr)

                        logging.debug("Comparing 13F ranking universe changes from date {} to date {} for manager {}".format(all_13f_ranks_prev_date.date, current_date, mgr))


                        prev_sec_set = set(s.security for s in all_13f_ranks_on_date_prior.filter(fund = mgr))
                        curr_sec_set = set(s.security for s in all_13f_ranks_on_date.filter(fund = mgr))

                        for sec_to_check in prev_sec_set.difference(curr_sec_set):
                            logging.info("13-F indicates manager {} sold {}".format(mgr, sec_to_check))

                            total_13d_security_rank = security_universe_13d.get(sec_to_check, None)

                            logging.debug("Checking {} for stale 13d filing rankings. Total current rank {}".format(sec_to_check, total_13d_security_rank))

                            if total_13d_security_rank is not None:
                                ##fails to work properly when manager files multiple filings on the same security on the same date (Pershing)
                                most_recent_13d_to_check = af.SecFilingSecurityRank.objects.filter(fund                    = mgr,
                                                                                                   security                = sec_to_check,
                                                                                                   filing__form_type__in   = [sec_form_13D, sec_form_13DA],
                                                                                                   date__range             = (min(all_dates), current_date)).order_by('-date').first()

                                if most_recent_13d_to_check is not None:

                                    # compare the date of the latest 13d filing and reported_period for this 13f
                                    if most_recent_13d_to_check.filing.filed_date < rank13f.filing.report_period:


                                        ### add a 0 ranking, so that if security ever re-appears through 13-D we can assign it a full ranking
                                        ## HACK ALERT! Tthis ranking is artificial as it is not directly sourced from 13-D form data
                                        ## but deduced from the change in 13-F's
                                        rank_db = af.SecFilingSecurityRank.objects.create(date     = current_date,
                                                                                          security = sec_to_check,
                                                                                          fund     = mgr,
                                                                                          filing   = most_recent_13d_to_check.filing,
                                                                                          rank_wgt = 0)

                                        if total_13d_security_rank - most_recent_13d_to_check.rank_wgt > 0:
                                            security_universe_13d[sec_to_check] = total_13d_security_rank - most_recent_13d_to_check.rank_wgt

                                            logging.info("Reduced current total 13D security ranking from {} to  {} because 13F filing for {} no longer has {} and 13D filed date precedes 13F reported period".format(
                                                total_13d_security_rank,
                                                most_recent_13d_to_check.rank_wgt,
                                                mgr,
                                                sec_to_check))

                                        else:
                                            removed_wgt = security_universe_13d.pop(sec_to_check, None)
                                            logging.info("Removed current total 13D security ranking {} because 13F filing for {} no longer has {} and 13D filed date precedes 13F reported period".format(
                                                removed_wgt,
                                                mgr,
                                                sec_to_check))

                                    else:
                                        #13d filed after 13f reported period, do nothing
                                        pass

                                else:
                                    pass
                                    #mgr did not file 13d for this security in the time-period 

                            else:
                                pass
                                ## current 13d rank universe has no such security, don't bother looking further

                        # for sec_to_check in prev_sec_set.difference(curr_sec_set):

                    else:
                        pass
                        ## all_13f_ranks_prev_date is null, no previous 13f-s filed for this manager
                            
                                

            #####################################################
            # CREATE INVESTIBLE UNIVERSE OUT OF 13D FILINGS
            #####################################################
            for rank13d in all_13d_ranks_on_date:
                
                assert rank13d.rank_wgt is not None, "Rank {} was created with NULL weight".format(rank13d)

                ## avoid Duplicate 13f/13d from the same manager by
                ## FIXME: how to zero out 13d ranking that came before 13F?
                is_security_in_recent_13F = False

                if rank13d.security in security_universe_13f and rank13d.rank_wgt>0:
                    manager_most_recent_13f_date = af.SecFilingSecurityRank.objects.filter(date__lte         = current_date,
                                                                                           filing__form_type = sec_form_13F,
                                                                                           fund              = rank13d.fund).order_by('-date').first()
                
                    if manager_most_recent_13f_date is not None:
                        is_security_in_recent_13F = af.SecFilingSecurityRank.objects.filter(date              = manager_most_recent_13f_date.date,
                                                                                            filing__form_type = sec_form_13F, 
                                                                                            fund              = rank13d.fund,
                                                                                            security          = rank13d.security).count() > 0
                        if is_security_in_recent_13F:
                            logging.warn("{} security already ranked in recent manager 13-F from {}".format(rank13d, manager_most_recent_13f_date.date))

                        
                most_recent_filing_13d_for_security = af.SecFilingSecurityRank.objects.filter(date__lt              = current_date, 
                                                                                              security              = rank13d.security, 
                                                                                              fund                  = rank13d.fund,
                                                                                              filing__form_type__in = [sec_form_13D, sec_form_13DA]).filter(date__gte = min(all_dates)).order_by('-date').first()
                ## need to !replace! rank_wgt for this manager if it is in the set already
                if most_recent_filing_13d_for_security is not None:

                    most_recent_13d_total_security_rank = security_universe_13d.get(rank13d.security, None)

                    if rank13d.rank_wgt == 0.0:

                        if most_recent_13d_total_security_rank is not None:

                            if most_recent_13d_total_security_rank - most_recent_filing_13d_for_security.rank_wgt>0:
                                
                                #total and individual ranks are not the same, so reduce the total by individual contribution
                                security_universe_13d[rank13d.security] = most_recent_13d_total_security_rank - most_recent_filing_13d_for_security.rank_wgt

                                logging.info("Reduced total 13D security ranking by {} because {} shows 0 shares".format(
                                    most_recent_filing_13d_for_security.rank_wgt,
                                    rank13d))

                            else:                                
                                # total and individual ranks are the same, this was the only manager with 13d for this security,
                                # remove security from 13d ranking universe. Don't keep 0-rankied securities around

                                removed_wgt = security_universe_13d.pop(rank13d.security, None)
                                logging.info("Removed {} from current 13D rankings because the only filing manager {} shows 0 shares".format(removed_wgt, rank13d))

                        else:
                            pass
                            # this security is not in current 13d ranking universe, ignore filing

                    else:

                        if not is_security_in_recent_13F:

                            # 13D filing with non-zero shares, adjust the existing ranking to the new rank
                            security_universe_13d[rank13d.security] = (security_universe_13d.get(rank13d.security, Decimal(0)) +
                                                                       (rank13d.rank_wgt - most_recent_filing_13d_for_security.rank_wgt))

                            if security_universe_13d[rank13d.security] <=0:
                                removed_wgt = security_universe_13d.pop(rank13d.security, None)


                            logging.info("Replaced 13D ranking of {} by {}".format(
                                most_recent_filing_13d_for_security,
                                rank13d))
                        else:

                            if most_recent_13d_total_security_rank is not None:

                                if most_recent_13d_total_security_rank - most_recent_filing_13d_for_security.rank_wgt>0:

                                    #total and individual ranks are not the same, so reduce the total by individual contribution
                                    security_universe_13d[rank13d.security] = most_recent_13d_total_security_rank - most_recent_filing_13d_for_security.rank_wgt

                                    logging.info("Reduced total 13D security ranking by {} because {} is a duplicate of 13-F".format(
                                        most_recent_filing_13d_for_security.rank_wgt,
                                        rank13d))

                                else:                                
                                    # total and individual ranks are the same, this was the only manager with 13d for this security,
                                    # remove security from 13d ranking universe. Don't keep 0-rankied securities around

                                    removed_wgt = security_universe_13d.pop(rank13d.security, None)
                                    logging.info("Removed {} from current 13D rankings because it is a duplicate 13-F of the only filing manager {}".format(removed_wgt, rank13d))

                            else:
                                pass
                                # this security is not in current 13d ranking universe, ignore filing
                        

                ## need to ! add ! rank_wgt for this manager if not in the set
                else:
                    if rank13d.rank_wgt == 0.0:
                        logging.info("Ignoring 13D rank for security {} with {}".format(rank13d.security, rank13d.rank_wgt))

                    else:
                        if not is_security_in_recent_13F:
                            security_universe_13d[rank13d.security] = security_universe_13d.get(rank13d.security, Decimal(0)) + rank13d.rank_wgt
                            logging.info("Adding new security with 13D rank {}".format(rank13d))

                        else:
                            pass
                            #skip duplicate rating

                ####################################################################################################
                ## adjust 13f rankings of this manager###############
                ####################################################################################################
                if rank13d.rank_wgt == 0:
                    #1 get the last 13f ranking date for the security from manager within last quarter
                    # what if query retursn last two quarter filings? only need the most recent
                    most_recent_13f_date = af.SecFilingSecurityRank.objects.filter(date__range       = (max(min(all_dates), current_date + relativedelta(months=-3)),
                                                                                                        current_date),
                                                                                   fund              = rank13d.fund,
                                                                                   filing__form_type = sec_form_13F).order_by('-date').first()
                    if most_recent_13f_date is not None:

                        #2 get the latest 13f security ranking contribution from this manager 
                        try:
                            most_recent_13f_security_rank_from_manager = af.SecFilingSecurityRank.objects.get(date              = most_recent_13f_date.date,
                                                                                                              security          = rank13d.security,
                                                                                                              fund              = rank13d.fund,
                                                                                                              filing__form_type = sec_form_13F).rank_wgt

                            most_recent_13f_total_security_rank = security_universe_13f.get(rank13d.security, None)

                            #3 decrease the total of 13f current_Date security ranking by #2
                            if most_recent_13f_total_security_rank is not None:

                                if most_recent_13f_total_security_rank - most_recent_13f_security_rank_from_manager>0:
                                    
                                    #total and individual ranks are not the same, so reduce the total by individual contribution

                                    security_universe_13f[rank13d.security] = most_recent_13f_total_security_rank - most_recent_13f_security_rank_from_manager
                                    logging.info("Reduced current total 13F security ranking by {} because {} shows 0 shares".format(most_recent_13f_security_rank_from_manager,
                                                                                                                                     rank13d))
                                else:

                                    # total and individual ranks are the same, this was the only manager with 13F for this security,
                                    # remove security from 13F ranking universe. Don't keep 0-rankied securities around

                                    removed_wgt = security_universe_13f.pop(rank13d.security, None)
                                    logging.info("Removed {} from current 13F rankings because {} shows 0 shares".format(removed_wgt, rank13d))

                            else:
                                pass
                                # 13f security ranking universe does not have this security

                        except af.SecFilingSecurityRank.DoesNotExist:
                            pass
                            # latest 13f does not contain this security, nothing to do

                    else:
                        pass
                        # no 13f filed for this manager in the last quarter/since repricing start date, whichever is latest

            # for rank13d in all_13d_ranks_on_date:


            ### JOIN TWO SETS OF FILING RATINGS, 
            
            sec_rank_wgt_join_dict=dict()

            for s in set(security_universe_13f.keys()).union(set(security_universe_13d.keys())):
                
                ## TODO: IS THIS THE RIGHT PLACE TO CHECK FOR ACQUISITION DATE?
                status_change = s.security_before_change_status.filter(change_status__reason="Acquired").first()
                
                if status_change is not None and status_change.effective_date is not None and status_change.effective_date < current_date:

                    logging.warn("Removed {} from ranked universe, acquired by {} last trading day was {}".format(
                        s, status_change.parent_security, status_change.effective_date))

                else:
                    sec_rank_wgt_join_dict[s] = security_universe_13f.get(s, Decimal(0.0)) + security_universe_13d.get(s, Decimal(0.0))

            sec_rank_wgt_sorted = [x[0] for x in sorted(sec_rank_wgt_join_dict.iteritems(), 
                                                        key=operator.itemgetter(1),
                                                        reverse=True)]
            
            logging.info("{0:<5} {1:<15} {2:<40} {3:<10} {4:<10} {5:<10}".format("RANK","DATE", "NAME","TOT RANK", "13F RANK", "13D RANK"))
            
            for (i,sec) in enumerate( sec_rank_wgt_sorted):
                                 
                logging.info("{0:<5} {1:<15} {2:<40} {3:<10} {4:<10} {5:<10}".format(i+1, 
                                                                                     str(current_date),
                                                                                     sec, 
                                                                                     sec_rank_wgt_join_dict[sec],
                                                                                     security_universe_13f.get(sec),
                                                                                     security_universe_13d.get(sec)))

            ######################
            # rebalance portfolio
            ######################
            rejected_security = {}

            getattr(self, self.rebalance)(current_date,
                                          previous_date,
                                          sec_rank_wgt_sorted,
                                          rejected_security)

            for (i,(k,v)) in enumerate(rejected_security.iteritems()):
                logging.error("{0:<5}ERROR {1:<30} {2:40}".format(i+1, k, v))


            previous_date = current_date





    def construct_ranked_security_universe_13F(self, repricing_date_set):

        sec_form_13F  = af.SecFilingType.objects.get(code="13F-HR")
        
        all_rankings = af.SecFilingSecurityRank.objects.filter(date__range=(min(repricing_date_set),
                                                                            max(repricing_date_set)),
                                                               filing__form_type=sec_form_13F)

        previous_date = None

        security_universe_13f = dict()

        for current_date in sorted(repricing_date_set.union(set(d.date for d in all_rankings))):

            logging.info("Creating security universe for {}".format(current_date))

            all_13f_ranks_on_date = all_rankings.filter(date=current_date, filing__form_type=sec_form_13F)
            
            
            #####################################################
            # CREATE INVESTIBLE UNIVERSE OUT OF 13F FILINGS
            #####################################################

            ## only replace security_universe_13f with a new security set
            ## if new 13f rankings exist, i.e. the new 13 was filed for all managers

            if len(all_13f_ranks_on_date)>0:
                security_universe_13f = dict()

                for rank13f in all_13f_ranks_on_date:
                    
                    security_universe_13f[rank13f.security] = security_universe_13f.get(rank13f.security, Decimal(0)) + rank13f.rank_wgt

            sec_rank_wgt_sorted = [x[0] for x in sorted(security_universe_13f.iteritems(), 
                                                        key=operator.itemgetter(1),
                                                        reverse=True)]
            
            logging.info("{0:<5} {1:<15} {2:<40} {3:<10} {4:<10}".format("RANK","DATE", "NAME","TOT RANK", "13F RANK"))
            
            for (i,sec) in enumerate( sec_rank_wgt_sorted):
                                 
                logging.info("{0:<5} {1:<15} {2:<40} {3:<10} {4:<10}".format(i+1, 
                                                                             str(current_date),
                                                                             sec, 
                                                                             security_universe_13f.get(sec),
                                                                             security_universe_13f.get(sec)))

            ######################
            # rebalance portfolio
            ######################
            rejected_security = {}

            getattr(self, self.rebalance)(current_date,
                                          previous_date,
                                          sec_rank_wgt_sorted,
                                          rejected_security)

            for (i,(k,v)) in enumerate(rejected_security.iteritems()):
                logging.error("{0:<5}ERROR {1:<30} {2:40}".format(i+1, k, v))


            previous_date = current_date





if __name__ == '__main__':
    
    arg_parser = argparse.ArgumentParser(description='Search for 13D filings and scrape contents')

    arg_parser.add_argument('--strat',     choices = {"13F","13D"}, help='What filings should be used in the model portfolio')
    arg_parser.add_argument('--rebalance', choices = {"equalweight_mv","equalweight_mv_fully_invested" }, help='What is the rebalancing strategy')
    arg_parser.add_argument('--start',     default = datetime.date(2003,12,31).strftime("%Y%m%d"), help="First holdings date")
    arg_parser.add_argument('--end',       default = datetime.date.today().strftime("%Y%m%d"), help = "Last holdings date")
    arg_parser.add_argument('--seed',      default = Decimal(1000000), help="What is the initial amount to be seeded on day 1")
    arg_parser.add_argument('--max_names', default = 50, type=int, help="Max number of positions in the portfolio")
    arg_parser.add_argument('--freq',      default = 'monthly', choices={"daily","monthly"},  help="How often should portfolio holdings be regenerated and re-priced")
    arg_parser.add_argument('--loglevel',  default = "INFO", choices={"WARN","INFO","DEBUG"}, help='Logging level')
    arg_parser.add_argument('--useBBG',    default = False, action='store_true', help="Find SecRef data on BBG and create new securities; fill missing prices")
    arg_parser.add_argument('--refreshRank',default = False, action='store_true', help="Refresh Security Rankings from sec filing tables")

    args = arg_parser.parse_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)

    port_name  = "{0} {1} TOP-{2} MODEL ACTIVIST FUND".format(args.strat.upper(), args.rebalance.upper(),args.max_names)

    logging.basicConfig(filename="{}_{}_{}.log".format(port_name,args.start,args.end),
                        filemode="w",
                        level=numeric_level,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        format="%(asctime)s:%(levelname)s:%(message)s")

    portfolio  = nbris.Portfolio.objects.get(security__name=port_name)
    start_date = dateutil.parser.parse(args.start).date()
    end_date   = dateutil.parser.parse(args.end).date()

    if args.freq=="daily":
        repricing_date_set = set([d.date() for d in rrule(DAILY, byweekday=(MO,TU,WE,TH,FR), dtstart=start_date, until=end_date)])

    elif args.freq=="monthly":
        repricing_date_set = set([d.date() for d in rrule(MONTHLY, dtstart=start_date, until=end_date, bymonthday=-1)])
        
    elif args.freq=="quarterly":
        repricing_date_set = set([d.date() for d in rrule(MONTHLY, dtstart=start_date, until=end_date, bymonthday=-1,interval=3)])

    pc = portfolio_constructor(args.strat,
                               args.rebalance,
                               args.freq,
                               args.max_names,
                               args.useBBG,
                               portfolio)

    if args.useBBG:
        cash_usd = nbris.SecurityMaster.objects.get(name='USD Cash')
        pc.fill_missing_prices_over_horizon(cash_usd, end_date, 4)

        
    if args.refreshRank:
        cik_list   = [f.fund.security.securityreference_set.get(sec_ref_type__sec_ref_type='CIK').sec_id_value for f 
                      in af.SecFilingStrategyFundMember.objects.filter(strategy__code="13F_ONLY")]
    
        filings_13f = af.SecFiling.objects.filter(form_type__code__in = ["13F-HR"],
                                                  filer_cik__in       = cik_list,
                                                  filed_date__range   = (start_date,end_date)).order_by('report_period','filer_name')

        for f in filings_13f:
            f.secfilingsecurityrank_set.all().delete()
            f.secfilingmarketvalue_set.all().delete()

        pc.rank_security_from_13F(filings_13f)


        filings_13d = []

        if args.strat=="13D":
            cik_list   = [f.fund.security.securityreference_set.get(sec_ref_type__sec_ref_type='CIK').sec_id_value for f 
                          in af.SecFilingStrategyFundMember.objects.filter(strategy__code="13D_ONLY")]

            filings_13d = af.SecFiling.objects.filter(form_type__code__in = ["SC 13D", "SC 13D/A"],
                                                      filer_cik__in        = cik_list,
                                                      filed_date__range    = (start_date, end_date)).order_by('filed_date','filer_name')

            for f in filings_13d:
                f.secfilingsecurityrank_set.all().delete()
                f.secfilingmarketvalue_set.all().delete()

            pc.rank_security_from_13D(filings_13d)

        
    nbris.Position.objects.filter(holding_security = portfolio.security).delete()
    logging.debug("Deleted all positions for portfolio {}".format(portfolio))

    if args.seed is not None:
        db_usd_fx     = nbris.Currency.objects.get(code="USD")
        db_usd_cash   = nbris.SecurityMaster.objects.get(name="USD Cash", sec_type__sec_type="CASH")
        total_seed_mv = Decimal(args.seed)
        
        seed_position = nbris.Position.objects.create(date             = start_date,
                                                      security         = db_usd_cash,
                                                      holding_security = portfolio.security,
                                                      currency         = db_usd_fx,
                                                      quantity         = total_seed_mv,
                                                      mv               = total_seed_mv)

    logging.info("Seed portfolio {0} on {1} with USD {2:12,.0f}".format(portfolio, start_date, total_seed_mv))

    if args.strat=='13F':
        pc.construct_ranked_security_universe_13F(repricing_date_set)

    elif args.strat == '13D':
        pc.construct_ranked_security_universe_13F_13D(repricing_date_set)

    else:
        pass
    
    #####################
    # sys.exit(0)
    #####################

    ##don't compute return for first leading months before filing
    rs = pc.compute_actual_portfolio_value(sorted(repricing_date_set)[1:])
    with open("{}.csv".format(port_name),"w") as f:
        for p in rs:
            f.write("{},{},{},{}\n".format(*p))

    # nprs = np.array([r[2] for r in rs], dtype = np.float64)

    # stdev = np.std(nprs) * np.sqrt(12)

    # plot_dates = sorted(repricing_date_set)[1:]

    # pc.plot(plot_dates, nprs)


    



















def queries():
    ## list all 
    for i,f in enumerate(af.SecFilingDetail.objects.filter(subject_name__startswith="herbalife").order_by('sec_filing__filed_date')):
        print "{0:3} {6:10} {7:10} {8:30} {1:20} {2:10} {3:10} {4:40} {5:12}".format(i, f.sec_filing.a_number,f.sec_filing.form_type.code, f.cusip, f.subject_name, f.no_shares, str(f.sec_filing.filed_date), f.sec_filing.filer_cik,f.sec_filing.filer_name)

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


    cusips_13d_2004=set([f.cusip for f in af.SecFilingDetail.objects.filter(sec_filing__filed_date__range=(datetime.date(2003,12,31),datetime.date(2014,7,31)), sec_filing__form_type__code__startswith="SC 13")])

    cusips_13f_2004=set([f.cusip for f in af.SecFilingDetail.objects.filter(sec_filing__report_period__range=(datetime.date(2003,12,31),datetime.date(2014,7,31)), sec_filing__form_type__code__startswith="13F")])

    len(cusips_13d_2004)
    # Out[101]: 260

    len(cusips_13f_2004)
    # Out[102]: 2755

    len(cusips_13d_2004.difference(cusips_13f_2004))
    # Out[103]: 41


                # sec_rank_mv_dict=0
                # sec_filing_total_mv=0
                # # logging.info("Process filing {}".format(filing))

                # #######################################################
                # # process filing detail make sure all securities are in SecurityMaster
                # #######################################################
                # for detail in filing.secfilingdetail_set.all():
                #     logging.info("Process filing detail {}".format(detail))

                #     if detail.cusip is not None:
                #         subject_security=None

                #         if self.useBBG and detail.cusip not in new_security_cusip_set:

                #             try:
                #                 subject_security = self.create_security_with_cusip_from_detail(detail, rejected_security)

                #             except bbg.bbgCreateSecurityFailedException as e:
                #                 rejected_security[detail.subject_name] = str(e)

                #         else:

                #             try:
                #                 subject_security = nbris.SecurityMaster.objects.get(securityreference__sec_id_value=detail.cusip,
                #                                                                     securityreference__sec_ref_type=sec_type_cusip)

                #             except  nbris.SecurityMaster.DoesNotExist as e:
                #                 rejected_security[detail.subject_name] = "Security with CUSIP {} does not exist".format(detail.cusip)

                #             except nbris.SecurityMaster.MultipleObjectsReturned as e:
                #                 rejected_security[detail.subject_name] = "CUSIP {} mapped to more than one security, fix SecurityReference".format(detail.cusip)

                #         if ( subject_security is not None and subject_security.sec_type.sec_type not in ("HedgeFund", "MLP", "REIT","ETF","ETP")):

                #             security_after_status_change = subject_security.security_before_change_status.filter(change_status__reason="Ticker Change").first()
                #             if security_after_status_change is not None:

                #                 logging.warn("Security {} changed ticker to {} and will appear in investable set under new ticker".format(subject_security, security_after_status_change.parent_security))

                #                 subject_security = security_after_status_change.parent_security

                #             if self.useBBG:
                #                 self.fill_missing_prices_over_horizon(subject_security, current_date, 4)

                #             if detail.mv is not None:
                #                 sec_rank_mv_dict[subject_security] = sec_rank_mv_dict.get(subject_security, Decimal(0)) + detail.mv
                #                 sec_filing_total_mv += detail.mv

                #             else:
                #                 db_price = subject_security.price_set.filter(date__lte=filing.filed_date, ds__code="BBG").order_by("-date").first()

                #                 if db_price is not None:
                #                     if detail.no_shares is not None:
                #                         sec_rank_mv_dict[subject_security] = sec_rank_mv_dict.get(subject_security, Decimal(0)) + (detail.no_shares * db_price.price)
                #                         sec_filing_total_mv += (detail.no_shares * db_price.price)

                #                     else:
                #                         errstr = "Cannot rank filing {} -- contains null number of shares".format(detail)
                #                         rejected_security[detail.subject_name] = errstr

                #                         # sec_rank_mv_dict[subject_security] = sec_rank_mv_dict.get(subject_security, Decimal(0)) + Decimal(0)

                #                 else:
                #                     errstr = "Cannot rank filing {} security {} has no price on BBG on or before {} -- cannot compute filing MV".format(
                #                         detail, subject_security, current_date)

                #                     rejected_security[detail.subject_name] = errstr

                #                     # imperror,created = af.SecFilingImportError.objects.get_or_create(filing_detail=filing_detail,
                #                     #                                                                  error = af.ImportError.objects.get(name="Missing Price Error"),
                #                     #                                                                  defaults={'description':errstr} )
                #                     # if not created:
                #                     #     imperror.defaults = errstr
                #                     #     imperror.save()
                #         else:
                #             pass

                #         # Null cusips are errors from parsing the filings file
                #         # Just ignore
                #         #
                #         # errstr = "SECURITY FROM FILING NOT LOADED INVALID CUSIP:{}".format(str(detail.cusip))
                #         # rejected_security[detail.subject_name] = errstr

                #         # imperror,created = af.SecFilingImportError.objects.get_or_create(filing_detail=filing_detail,
                #         #                                                                  error = af.ImportError.objects.get(name="Invalid Cusip"),
                #         #                                                                  defaults={'description':errstr} )
                #         # if not created:
                #         #     imperror.defaults = errstr
                #         #     imperror.save()

                #     new_security_cusip_set.add(detail.cusip)

                # # for detail in filing.secfilingdetail_set.all():

                # sfmv,created = af.SecFilingMarketValue.objects.get_or_create(sec_filing = filing, defaults={'mv':sec_filing_total_mv})

                # if not created:
                #     sfmv.mv = sec_filing_total_mv
                #     sfmv.save()


