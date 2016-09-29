import blpapi
import itertools
from decimal import *
import logging
import datetime

class bbgException(BaseException):
    pass


class bbgExchangeStatusChangeWarning(bbgException):
    def __init__(self, old_ticker, status):
        self.status = status
        self.old_ticker = old_ticker
        super(bbgException, self).__init__()

class bbgTickerChangeWarning(bbgExchangeStatusChangeWarning):
    def __init__(self, new_ticker, old_ticker, status):
        self.new_ticker = new_ticker
        self.old_ticker = old_ticker
        self.status = status
        super(bbgException, self).__init__()

class bbgAcquisitiondWarning(bbgExchangeStatusChangeWarning):
    def __init__(self, new_ticker, old_ticker, status):
        self.new_ticker = new_ticker
        self.old_ticker = old_ticker
        self.status = status
        super(bbgException, self).__init__()


class bbgCreateSecurityFailedException(bbgException):
    pass

class bbgMnemonicException(bbgException):
    pass

class bbg_helper():
    def __init__(self):
        self.bbg_svc_name = "//blp/refdata"
        self.session = None
        self.refDataService = None

    def bbg_init(self):
        if self.session is None:
            sessionOptions = blpapi.SessionOptions()
            sessionOptions.setServerHost("localhost")
            sessionOptions.setServerPort(8194)

            self.session = blpapi.Session(sessionOptions)
            if self.session.start():
                # Open service to get historical data from
                if self.session.openService(self.bbg_svc_name):

                    self.refDataService = self.session.getService(self.bbg_svc_name)

                else:
                    raise bbgException("Failed to open {}".format(self.bbg_svc_name))

            else:
                raise bbgException("Failed to start session.")


    def bbg_shutdown(self):
        if self.session is not None:
            self.session.stop()
            self.session=None
            self.refDataService = None

# ReferenceDataResponse = {
#     securityData[] = {
#         securityData = {
#             security = "SX5E Equity"
#             eidData[] = {
#             }
#             fieldExceptions[] = {
#                 fieldExceptions = {
#                     fieldId = "ISIN"
#                     errorInfo = {
#                         source = "215::bbdbd9"
#                         code = 9
#                         category = "BAD_FLD"
#                         message = "Field not valid"
#                         subcategory = "INVALID_FIELD"
#                     }
#                 }
#             }
#             sequenceNumber = 0
#             fieldData = {
#                 TICKER = "SX5E"
#                 NAME = "Euro Stoxx 50 Pr"
#                 SHORT_NAME = "Euro Stoxx 50 Pr"
#             }
#         }
#     }
# }


#             fieldExceptions[] = {
#                 fieldExceptions = {
#                     fieldId = "ID_ISIN"
#                     errorInfo = {
#                         source = "215::bbdbl9"
#                         code = 9
#                         category = "BAD_FLD"
#                         message = "Field not applicable to security"
#                         subcategory = "NOT_APPLICABLE_TO_REF_DATA"
#                     }
#                 }
#             }

    def parse_exception(self, secdata):
        excepts = []
        
        if secdata.hasElement("fieldExceptions"):
            for exc in secdata.getElement("fieldExceptions").values():
                if exc.hasElement("errorInfo"):
                    err = exc.getElement("errorInfo")
                    err_str = "BBG ERROR '{}':{}:{}:{}".format(exc.getElementAsString("fieldId"),
                                                               err.getElementAsString("category"),
                                                               err.getElementAsString("subcategory"),
                                                               err.getElementAsString("message"))
                    excepts.append(err_str)
                
                if secdata.hasElement("securityError"):
                    err = secdata.getElement("securityError")
                    err_str = "BBG ERROR '{}':{}:{}:{}".format(secdata.getElementAsString("security"),
                                                               err.getElementAsString("category"),
                                                               err.getElementAsString("subcategory"),
                                                               err.getElementAsString("message"))
                    excepts.append(err_str)

        #     for exc in secdata.getElement("fieldExceptions").values():
        # if secdata.hasElement("fieldExceptions"):
        return excepts


    def parse_field_data_segment(self, fd):
        
        dt = fd.datatype()
        
        if dt == blpapi.DataType.BOOL:
            value = fd.getValueAsBool()
            
        elif dt == blpapi.DataType.DECIMAL:
            value = fd.getValueAsDecimal()
            
        elif dt in (blpapi.DataType.DATE, blpapi.DataType.DATETIME):
            value = fd.getValueAsDatetime()
            
        elif dt in (blpapi.DataType.INT32, blpapi.DataType.INT64, blpapi.DataType):
            value = fd.getValueAsInteger()
            
        elif dt in (blpapi.DataType.FLOAT32, blpapi.DataType.FLOAT64):
            value = fd.getValueAsFloat()
        
        else:
            value =fd.getValueAsString()

        return (str(fd.name()), value)


    def parse_sec_data_segment(self, secdata, bbg_mnemonic):
        # print secdata

        seq_no  = secdata.getElementAsInteger("sequenceNumber")
        bbg_id  = secdata.getElementAsString("security")
        eid     = secdata.getElement("eidData").values()
        excepts = self.parse_exception(secdata)

        fd = secdata.getElement("fieldData")

        result = []
        results = {}
        if fd.isArray():
            for x in fd.values() :
                for y in x.elements():
                    rs = self.parse_field_data_segment(y)
                    result.append(rs)
        else:
            for y in fd.elements():
                rs = self.parse_field_data_segment(y)
                result.append(rs)

        results[bbg_id] = result
        return (results, excepts)
            

    def bbg_parse_response_msg(self, msg, bbg_mnemonic):
        secdata = msg.getElement("securityData")

        results = []
        excepts = []

        if secdata is not None:
            if secdata.isArray():
                for sd in secdata.values():
                    rs = self.parse_sec_data_segment( sd, bbg_mnemonic)
                    results.append( rs[0] )
                    excepts.append( rs[1] )
            else:
                rs = self.parse_sec_data_segment(secdata, bbg_mnemonic)
                results.append( rs[0] )
                excepts.append( rs[1] )

        return (results, excepts)


 # The possible types of event:
 # |      ADMIN                 Admin event
 # |      SESSION_STATUS        2 Status updates for a session
 # |      SUBSCRIPTION_STATUS   Status updates for a subscription
 # |      REQUEST_STATUS        Status updates for a request
 # |      RESPONSE              5 The final (possibly only) response to a request
 # |      PARTIAL_RESPONSE      6 A partial response to a request
 # |      SUBSCRIPTION_DATA     Data updates resulting from a subscription
 # |      SERVICE_STATUS        Status updates for a service
 # |      TIMEOUT               9 An Event returned from nextEvent() if it
 # |                            timed out
 # |      AUTHORIZATION_STATUS  Status updates for user authorization
 # |      RESOLUTION_STATUS     Status updates for a resolution operation
 # |      TOPIC_STATUS          Status updates about topics for service providers
 # |      ROKEN_STATUS          Status updates for a generate token request
 # |      REQUEST               Request event
 # |      UNKNOWN               Unknown event
    def process_events(self, bbg_mnemonic):
        results = []
        excepts = []

        done = False
        while(not done):
            ev         = self.session.nextEvent(500)
            event_type =  ev.eventType()
            
            for msg in ev:
                logging.debug(msg)
                
                if event_type == blpapi.Event.PARTIAL_RESPONSE:
                    rs = self.bbg_parse_response_msg(msg, bbg_mnemonic)
                    results.append(rs[0])
                    excepts.append(rs[1])

                elif event_type == blpapi.Event.RESPONSE:
                    rs = self.bbg_parse_response_msg(msg, bbg_mnemonic)
                    results.append(rs[0])
                    excepts.append(rs[1])
                    done =True
                else:
                    pass
                    #ignore other types for now

        return (list(itertools.chain.from_iterable(results)),
                list(itertools.chain.from_iterable(excepts)))
        

    def bbg_get_ref_data(self, cusip_list, bbg_mnemonic, yellow_key):
        if self.session is None:
            self.bbg_init()

        request = self.refDataService.createRequest("ReferenceDataRequest")

        if yellow_key is not  None:
            if isinstance(yellow_key, list):
                assert len(cusip_list)==len(yellow_key)

                for c, y in zip(cusip_list, yellow_key):
                    request.append("securities", "{} {}".format(c, y))
                    
            elif isinstance(yellow_key, str):
                for c, y in zip(cusip_list, [yellow_key] * len(cusip_list)):
                    request.append("securities", "{} {}".format(c, y))

            else:
                pass
        else:
            request.append("securities", c)


#        for s in cusip_list:
#            if yellow_key is not None and yellow_key != "":
#                request.append("securities", "{} {}".format(s, yellow_key))
#            else:
#                request.append("securities", s)
#

        for m in bbg_mnemonic:
            request.append("fields", m)

        self.session.sendRequest(request)

        return self.process_events(bbg_mnemonic)


    # HistoricalDataResponse = {
    #     securityData = {
    #         security = "191216100 CUSIP"
    #         eidData[] = {
    #         }
    #         sequenceNumber = 0
    #         securityError = {
    #             source = "117::bbdbh3"
    #             code = 15
    #             category = "BAD_SEC"
    #             message = "Unknown/Invalid securityInvalid Security [nid:117] "
    #             subcategory = "INVALID_SECURITY"
    #         }
    #         fieldExceptions[] = {
    #         }
    #         fieldData[] = {
    #         }
    #     }
    # }
    def bbg_get_hist_data(self, 
                          cusip_list, 
                          start_date, 
                          end_date     = None, 
                          yellow_key   = None,
                          bbg_mnemonic = ["PX_LAST"], 
                          freq         = "DAILY", 
                          days         = "ACTUAL",
                          override_prc = False):

        if self.session is None:
            self.bbg_init()

        if end_date is None:
            end_date = start_date

        # Create and fill the request for the historical data
        request = self.refDataService.createRequest("HistoricalDataRequest")

        for m in bbg_mnemonic:
            request.append("fields", m)

        if yellow_key is not  None:
            if isinstance(yellow_key, list):
                assert len(cusip_list)==len(yellow_key)

                for c, y in zip(cusip_list, yellow_key):
                    request.append("securities", "{} {}".format(c, y))
                    
            elif isinstance(yellow_key, str):
                for c, y in zip(cusip_list, [yellow_key] * len(cusip_list)):
                    request.append("securities", "{} {}".format(c, y))

            else:
                pass
        else:
            request.append("securities", c)
                    

        request.set("periodicityAdjustment", days)
        request.set("periodicitySelection", freq)

        request.set("startDate", start_date.strftime("%Y%m%d"))
        request.set("endDate", end_date.strftime("%Y%m%d"))
        
        if override_prc:
            overrides = request.getElement("overrides")
            override1 = overrides.appendElement()
            override1.setElement("fieldId", "PRICING_SOURCE")
            override1.setElement("value", "MSG1")
            
        # request.set("maxDataPoints", 100)

        logging.debug(request)

        self.session.sendRequest(request)

        return self.process_events(bbg_mnemonic)




#
#if __name__ == "__main__":
#
#    try:
#        logging.basicConfig(filename='bbg_api_wrap.log',
#                            filemode="w",
#                            level=logging.DEBUG,
#                            datefmt='%Y-%m-%d %H:%M:%S',
#                            format="%(asctime)s:%(levelname)s:%(message)s")
#
##        logging.info(os.environ['TZ'])
##        os.environ.setdefault('TZ', 'US/Eastern')
##        logging.info(os.environ['TZ'])
#
#        bh = bbg_helper()
#        bh.bbg_init()
##        (res, exc) = bh.bbg_get_hist_data(["TFCIX", "AAPL"],
##                                        datetime.date(2013,12,31),
##                                        datetime.date(2014,1,31),
##                                        "Equity",
##                                        ["PX_LAST","BID"],
##                                        "WEEKLY",
##                                        "ACTUAL")
#                                        
#        (res, exc) = bh.bbg_get_hist_data(["US25272KAU79"],
#                                        datetime.date(2016,7,5),
#                                        datetime.date(2016,7,11),
#                                        "Corp",
#                                        ["PX_LAST","BID", "ASK", "YAS_OAS_SPRD"],
#                                        "DAILY",
#                                        "ACTUAL",
#                                        True)
#
#        logging.debug(res)
#        logging.error(exc)
#
#        
#        for sec in res:
#            
#            for k,v in sec.iteritems():
#                print "******************* {} ***************************".format(k)
#                
#                for vv in v:
#                    print "            {}:{}".format(vv[0], vv[1])
#                    
#
#        print exc
#        
#        # # rs = bh.bbg_get_ref_data(["TFCIX", "AAPL"],
#        # #                          ["NAME", "SECURITY_DES"])
#        # # print "************"
#        # # print rs
#        # # print "************"
#
#
#        # (res, exc) = bh.bbg_get_ref_data(["00489A107"],
#        #                                  ["NAME", "SECURITY_DES", "CRNCY", "SECURITY_TYP", "MARKET_STATUS", "SEDOL_ID"],
#        #                                  yellow_key = "Equity")
#
#        # logging.debug(res)
#        # logging.debug( res[0].keys())
#
#        # for k,v in res[0].iteritems():
#        #     logging.debug(filter(lambda x: x[0]=="CRNCY", v))
#
#        #     for vv in v:
#        #         logging.debug(vv)
#
#        # with open("temp.out", mode='r') as f:
#        #     for row in f:
#        #         ids = row.strip()
#                
#        bh.bbg_shutdown()
#
#    except KeyboardInterrupt:
#        print "Ctrl+C pressed. Stopping..."
#    
