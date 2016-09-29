#!/usr/bin/env python

import os
import re
import dateutil.parser
from operator import itemgetter, attrgetter
import xml.etree.ElementTree as ET
import sys
import argparse
import datetime
from decimal import *
import HTMLParser
# from htmlentitydefs import name2codepoint
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "NBRIS.settings")
sys.path.append("//nb/corp/PST/NY1/drezniko/src/NBRIS")
django.setup()

import nbris_riskhub.models as nbris
import activist_fund.models as af

import logging

xmlns_filer = "{http://www.sec.gov/edgar/thirteenffiler}"
xmlns_tbl = "{http://www.sec.gov/edgar/document/thirteenf/informationtable}"

report_date_re= re.compile(r"CONFORMED PERIOD OF REPORT:.*(\d{8})$")
subject_re    = re.compile(r"SUBJECT COMPANY:")
filedby_re    = re.compile(r"FILED BY:|FILER:")
a_number_re   = re.compile(r"ACCESSION NUMBER:.*(\d{10}-\d{2}-\d{6})$")
filed_re      = re.compile(r"FILED AS OF DATE:.*(\d{8})$")
hdr_end       = re.compile(r"</SEC-HEADER>")
filer_name_re = re.compile(r"COMPANY CONFORMED NAME:(.*)$")
filer_cik_re  = re.compile(r"CENTRAL INDEX KEY:.*(\d{10})$")
skip_line_re  = re.compile(r"---| CALL | PUT |SHARED-$|Market Value|thousands|<.*>|COLUMN \d| CUSIP ", re.IGNORECASE)
tags_re       = re.compile(r"<S>|<C>", re.IGNORECASE)

attach_type_re= re.compile(r"<TYPE>(.*)$")
html_tag_re   = re.compile(r"<HTML>|</HTML>", re.IGNORECASE)


cusip_beg_re  = re.compile(r"Title\s+of\s+Class\s+of\s+Securities", re.IGNORECASE)
cusip_end_re  = re.compile(r"CUSIP\s+Number", re.IGNORECASE)
# cusip_re      = re.compile(r"CUSIP.*([a-zA-Z0-9]{9})", re.IGNORECASE)
cusip_re      = re.compile(r"CUSIP\s+(?:NO\.\s*)?(?:\:\s*)?([a-zA-Z0-9]{8,9})", re.IGNORECASE)
cusip_re_1    = re.compile(r"[a-zA-Z0-9]{8,9}|[a-zA-Z0-9]{6}-[a-zA-Z0-9]{2}-\d|[a-zA-Z0-9]{6} [a-zA-Z0-9]{2} \d", re.IGNORECASE)
#cusip_re_1    = re.compile(r"([a-zA-Z0-9]{8,9})", re.IGNORECASE)

pct_shares_beg_re = re.compile(r"(?:PERCENT)?\s+OF\s+CLASS\s+REPRESENTED", re.IGNORECASE) 
pct_shares_end_re = re.compile(r"TYPE\s+OF\s+REPORTING(?:\s+PERSON)?", re.IGNORECASE) 
# pct_shares_re     = re.compile(r"(\d{1,2}\.\d+)\s*\%?")
pct_shares_re     = re.compile(r"(\d+(?:\.\d+)?)\s*\%?")

no_shares_beg_re  = re.compile(r"(?<!THE\s)AGGREGATE\s+AMOUNT(?!\sOF FUNDS)(?:\s+BENEFICIALLY)?", re.IGNORECASE) 
no_shares_end_re  = re.compile(r"CHECK(?:\s*BOX)?\s+IF(?:\s+THE)?\s+AGGREGATE", re.IGNORECASE) 
no_shares_re      = re.compile(r"(\d+(?:,\d{3})*(?:\.\d+)?)")
# no_shares_re      = re.compile(r"(\d+(?:,\d{3})*(?!\.))")



def extract_pct_shares(line):
    res     = pct_shares_re.findall(line)
    res_flt = filter(lambda x: x>=0. and x<1.0 and x!=0.13 and x!=0.14 and x!=0.11, map(lambda x: float(x.replace(",", "").strip()) / 100.0, res))
    logging.debug("regex match={} filter={}".format(res, res_flt))
    return res_flt

def extract_shares(line):
    res     = no_shares_re.findall(line)
    res_flt = filter(lambda x: x==0.0 or x>100.0, map(lambda x: float(x.replace(",", "").strip()), res))
    logging.debug("regex match={} filter={}".format(res, res_flt))
    return res_flt


class Form13DParser(HTMLParser.HTMLParser):

    def __init__(self):
        HTMLParser.HTMLParser.__init__(self)
        self.no_shares_list  = []
        self.pct_shares_list = []
        self.data_list       = []
        self.cusip           = None
        
    def handle_data(self, data):
        # find the ending regexp and walk back until find beginning regexp, 
        # appending all matching share amounts to the list

        if no_shares_end_re.search(data) != None:
            logging.debug("HTML END NO SHARES BLOCK {}".format(data))
            
            for data_item in self.data_list[::-1]:

                ## the line indicator comes through 
                if no_shares_beg_re.search(data_item) != None:
                    logging.debug("HTML BEG NO SHARES BLOCK {}".format(data_item))

                    self.no_shares_list = self.no_shares_list + extract_shares(data_item)
                    break

                elif data_item != "\n":
                    logging.debug("HTML INSIDE NO SHARES BLOCK {}".format(data_item))
                    self.no_shares_list = self.no_shares_list + extract_shares(data_item)

        elif pct_shares_end_re.search(data) != None:
            logging.debug("HTML END PCT SHARES BLOCK {}".format(data))

            for data_item in self.data_list[::-1]:

                if pct_shares_beg_re.search(data_item) != None:
                    logging.debug("HTML BEG PCT SHARES BLOCK {}".format(data_item))

                    self.pct_shares_list = self.pct_shares_list + extract_pct_shares(data_item)
                    break

                elif data_item != "\n":
                    logging.debug("HTML INSIDE PCT SHARES BLOCK {}".format(data_item))
                    self.pct_shares_list = self.pct_shares_list + extract_pct_shares(data_item)

        if cusip_end_re.search(data) != None:
            logging.debug("HTML END CUSIP BLOCK {}".format(data))

            for data_item in self.data_list[::-1]:
                data_item = data_item.strip()
                logging.debug("HTML INSIDE CUSIP BLOCK {}".format(data))

                if cusip_beg_re.search(data_item) != None:
                    logging.debug("HTML BEGIN CUSIP BLOCK {}".format(data))
                    break

                elif data_item != "\n" and data_item != "" and (len(data_item)==9 or len(data_item)==8):
                    self.cusip = data_item

        self.data_list.append(data)


        
def parse_13D_lines(data, last_line_read, ishtml, filing_type, a_number, filer_name, filer_cik, subject_name, subject_cik, filed_date, report_date):

    cusip           = None
    pct_shares_list = []
    no_shares_list  = []
    isnoshares      = False
    ispctshares     = False

    if ishtml:
        ## advance to <TYPE> tag to make sure file is 13D and not another HTML attachment
        for (line_cnt,line) in enumerate(data[last_line_read:]):
            logging.debug(line)
            attachment_type = attach_type_re.findall(line);
            if len(attachment_type)>0 and attachment_type[0] in [u"SC 13D/A", u"SC 13D"]:
                logging.debug("Found correct HTML attachment with 13D data")
                break

        last_line_read += line_cnt

        html_start = 0
        html_end = 0

        for (line_cnt,line) in enumerate(data[last_line_read:]):
            html = html_tag_re.findall(line)

            if html_start == 0 and len(html)>0 and html[0].upper()=="<HTML>":
                html_start = last_line_read + line_cnt
                logging.debug("Found <HTML> tag at line {}".format(html_start))
                
            if html_end == 0 and len(html)>0 and html[0].upper()=="</HTML>":
                html_end = last_line_read + line_cnt
                logging.debug("Found </HTML> tag at line {}".format(html_end))
                break

        html_data = filter(lambda x: x.strip(), data[html_start:html_end + 1])
        # logging.debug("*"*100)
        # logging.debug(html_data)
        # logging.debug("*"*100)

        html_parser = Form13DParser()
        for line in html_data:
            html_parser.feed(line)

        pct_shares_list = html_parser.pct_shares_list
        no_shares_list  = html_parser.no_shares_list
        cusip           = html_parser.cusip

    else:

        for line in data[last_line_read:]:

            if cusip is None:
                temp = cusip_re.search(line)
                if temp is not None and temp.group is not None:
                    cusip = temp.group(1).strip()
                    if cusip.lower() == "ecurities":
                        cusip ==None

                    continue

            if None != pct_shares_end_re.search(line):
                logging.debug("END PCT SHARES BLOCK {}".format(line))
                ispctshares = False
                continue

            if None != pct_shares_beg_re.search(line):
                logging.debug("BEGIN PCT SHARES BLOCK {}".format(line))
                ispctshares = True
                pct_shares_list = pct_shares_list + extract_pct_shares(line)
                continue

            if ispctshares:
                logging.debug("IN PCT SHARES BLOCK {}".format(line))
                pct_shares_list = pct_shares_list + extract_pct_shares(line)
                continue

            if None != no_shares_end_re.search(line):
                logging.debug("END NO SHARES BLOCK {}".format(line))
                isnoshares = False
                continue

            if None != no_shares_beg_re.search(line):
                logging.debug("BEGIN NO SHARES BLOCK {}".format(line))
                isnoshares = True
                no_shares_list = no_shares_list + extract_shares(line)
                continue

            if isnoshares:
                logging.debug("IN NO SHARES BLOCK {}".format(line))
                no_shares_list = no_shares_list + extract_shares(line)
                continue

        # assert(len(pct_shares_list)==len(no_shares_list))

    matched_pair_list = zip(no_shares_list, pct_shares_list)
    # matched_pair_list = map(None, no_shares_list, pct_shares_list)
    
    logging.info("{0:12} {1:40} {2:12} {3:12} {4:40} {5:10} {6:10} {7:10} {8:>12}".format(
        filer_cik,
        filer_name,
        "xxx" if cusip is None else cusip,
        subject_cik,
        subject_name,
        filing_type,
        a_number, 
        str(filed_date),
        "xxx" if len(matched_pair_list)==0 else matched_pair_list))
    
    db_filing = af.SecFiling.objects.create(form_type     = filing_type,
                                            a_number      = a_number,
                                            filer_cik     = filer_cik,
                                            filer_name    = filer_name,
                                            report_period = report_date,
                                            filed_date    = filed_date)

    # OBSOLETE, saved the largest holding only
    # matched_pair_list = sorted(matched_pair_list, key=itemgetter(0), reverse=True)
    # sec_detail = af.SecFilingDetail.objects.create(sec_filing   = db_filing,
    #                                                cusip        = cusip,
    #                                                subject_name = subject_name,
    #                                                subject_cik  = subject_cik,
    #                                                no_shares    = matched_pair_list[0][0] if len(matched_pair_list)>0 else None,
    #                                                pct_shares   = matched_pair_list[0][1] if len(matched_pair_list)>0 else None)

    for (sh, pct_sh) in matched_pair_list:
        sec_detail = af.SecFilingDetail.objects.create(sec_filing   = db_filing,
                                                       cusip        = cusip,
                                                       subject_name = subject_name,
                                                       subject_cik  = subject_cik,
                                                       no_shares    = sh, 
                                                       pct_shares   = pct_sh)



def parse_13F_lines(data, last_line_read, isxml, filing_type, a_number, filer_name, filer_cik, filed_date, report_date):

    cusip_counter=0
    error_counter=0

    if isxml:

        for attachment in ["<TYPE>13F-HR", "<TYPE>INFORMATION TABLE"]:
            last_line_read+=1

            xml_data = data[last_line_read:]
            xml_start_idx=xml_end_idx=0

            for current_line, line in enumerate(xml_data):
                if line.find(attachment)>-1:
                    xml_start_idx=current_line
                    while xml_start_idx<len(xml_data) and xml_data[xml_start_idx].lower().find("<xml>")==-1:
                        xml_start_idx +=1
                    xml_start_idx+=1

                    xml_end_idx=xml_start_idx
                    while xml_end_idx<len(xml_data) and xml_data[xml_end_idx].lower().find("</xml>")==-1:
                        xml_end_idx +=1

            last_line_read += xml_end_idx

            #logging.debug("%s: Start xml:%d, end xml=%d, last_line_read=%s"%(attachment, xml_start_idx, xml_end_idx, last_line_read))

            xmldoc = ET.fromstringlist(xml_data[xml_start_idx:xml_end_idx])

            if attachment=="<TYPE>13F-HR":
                filer_cik = xmldoc.find("./%sheaderData/%sfilerInfo/%sfiler/%scredentials/%scik"%
                                        (xmlns_filer,xmlns_filer,xmlns_filer,xmlns_filer,xmlns_filer)).text

                filer_name = xmldoc.find("./%sformData/%scoverPage/%sfilingManager/%sname"%
                                        (xmlns_filer,xmlns_filer,xmlns_filer,xmlns_filer)).text

                report_date = dateutil.parser.parse(xmldoc.find("./%sheaderData/%sfilerInfo/%speriodOfReport"%
                                                       (xmlns_filer,xmlns_filer,xmlns_filer)).text)

                db_filing = af.SecFiling.objects.create(form_type     = filing_type,
                                                        a_number      = a_number,
                                                        filer_cik     = filer_cik,
                                                        report_period = report_date,
                                                        filer_name    = filer_name,
                                                        filed_date    = filed_date)

                logging.debug("\tXML 13F form in %s, %s,%s, %s, %s"%(a_number, str(filed_date), filer_cik, filer_name, str(report_date.date())))


            elif attachment=="<TYPE>INFORMATION TABLE":
                cusips = [c.text for c in xmldoc.findall('./%sinfoTable/%scusip'%(xmlns_tbl,xmlns_tbl))]
                issuer = [c.text for c in xmldoc.findall('./%sinfoTable/%snameOfIssuer'%(xmlns_tbl,xmlns_tbl))]

                try:
                    mv     = [float(c.text)*1000 for c in xmldoc.findall('./%sinfoTable/%svalue'%(xmlns_tbl,xmlns_tbl,))]
                except ValueError:
                    mv = None*len(cusips)
                try:
                    shrs   = [float(c.text) for c in xmldoc.findall('./%sinfoTable/%sshrsOrPrnAmt/%ssshPrnamt'%
                                                                    (xmlns_tbl,xmlns_tbl,xmlns_tbl))]
                except ValueError:
                    shrs = None*len(cusips)

                for (c,i,m,s) in zip(cusips, issuer, mv, shrs):
                    sec_detail = af.SecFilingDetail.objects.create(sec_filing   = db_filing,
                                                                   cusip        = c,
                                                                   subject_name = i,
                                                                   no_shares    = s,
                                                                   mv           = m)

                    logging.debug("\t\t{0:10}{1:30}${2:12}{3:12}".format(c,i,m,s))

                    cusip_counter+=1

        # for attachment in ["<TYPE>13F-HR", "<TYPE>INFORMATION TABLE"]:

    else:

        logging.info("\tTEXT 13F form in %s, %s,%s, %s, %s"%(a_number,filed_date,filer_cik, filer_name, report_date))

        db_filing = af.SecFiling.objects.create(form_type     = filing_type,
                                                a_number      = a_number,
                                                filer_cik     = filer_cik,
                                                report_period = report_date,
                                                filer_name    = filer_name,
                                                filed_date    = filed_date)

        last_line_read+=1

        txt_data = data[last_line_read:]
        txt_start_idx=txt_end_idx=0

        while txt_start_idx<len(txt_data) and txt_data[txt_start_idx].lower().find("<table>")==-1:
            txt_start_idx +=1
        txt_start_idx+=1

        txt_end_idx=len(txt_data)-1
        while txt_end_idx>txt_start_idx and txt_data[txt_end_idx].lower().find("</table>")==-1:
            txt_end_idx -=1

        last_line_read += txt_end_idx

        info_table = txt_data[txt_start_idx:txt_end_idx]

        founddata=False
        column_idx_left = []
        skip_next_line = False

        for cnt, line in enumerate(info_table):
            cusip=subject_name=mv=shrs=None

            if not founddata:
                ########
                ## column_idx_left[0] = subject_name
                ## column_idx_left[2] = cusip
                ## column_idx_left[3] = mv (x 1000)
                ## column_idx_left[4] = no_shrs
                
                column_idx_left = [m.start() for m in re.finditer(tags_re, line)]

                if len(column_idx_left)>0:
                    if len(column_idx_left)>5:
                        logging.debug("Tags line '{}'".format(line))
                        logging.debug("Found column indices at {}".format(column_idx_left))
                        founddata=True
                    else:
                        logging.debug("Not enough column tags %s found in '%s', keep looking"%(column_idx_left,line))
                else:
                    if cnt+1 == len(info_table):
                        logging.error("Reached end of data table area, but did not find any column tags <c>|<s>")
                        error_counter=+1
            else:
                try:
                    subject_name = line[column_idx_left[0]:column_idx_left[1]-1].strip()

                except ValueError:
                    logging.error("Unable to extract subject name from {}".format(line))
                    error_counter +1
                    continue

                if subject_name != "":
                    temp = skip_line_re.search(line)

                    if temp is not None and temp.group is not None or skip_next_line:
                        skip_next_line = False
                        logging.debug("SKIPPING line '%s'"%line)
                        continue

                    raw_cusip = line[column_idx_left[2]-1:column_idx_left[3]-1].strip()
                    logging.debug("Trying to extract cusip {}".format(raw_cusip))

                    temp = cusip_re_1.search(raw_cusip)
                    if temp and temp.group is not None:
                        #logging.debug("Captured cusip_re group {},{}".format(temp.group(0), temp.group(1)))
                        cusip = temp.group(0).replace("-","").replace(" ","").strip()
                        
                    else:
                        if cnt+1<len(info_table):
                            logging.debug("No cusip on this line looking ahead to next '%s'"%info_table[cnt+1])
                            skip_next_line=True
                            
                            temp = cusip_re_1.search(info_table[cnt+1][column_idx_left[2]-1:column_idx_left[3]-1].strip())
                            if temp and temp.group is not None:
                                cusip = temp.group(0).replace("-","").replace(" ","").strip()

                            else:
                                logging.warn("No cusip on next line, giving up {}".format(info_table[cnt+1]))
                                error_counter+=1
                        else:
                            logging.warn("Reached last line no cusip found, giving up")
                            error_counter+=1

                    try:
                        ##MV might actually start in 1 col to the left of correct index
                        mv = 1000*float(line[column_idx_left[3]-1:column_idx_left[4]-1]
                                        .replace(",","")
                                        .replace("-","0")
                                        .replace("$","")
                                        .strip())  

                    except ValueError:
                        if cnt+1<len(info_table):
                            try:
                                skip_next_line=True
                                logging.debug("Invalid MV on this line {} looking ahead to next".format(line[column_idx_left[3]-1:column_idx_left[4]-1]))

                                mv = 1000*float(info_table[cnt+1][column_idx_left[3]-1:column_idx_left[4]-1]
                                                .replace(",","")
                                                .replace("-","0")
                                                .replace("$","")
                                                .strip())
                            except ValueError:
                                logging.warn("Invalid MV on next line, giving up {}".format(info_table[cnt+1]))
                                error_counter+=1
                        else:
                            logging.warn("Reached last line no mv found, giving up")
                            error_counter+=1

                    try:
                        ##shares might actually start in 1 col to the left of correct index
                        shrs = float(line[column_idx_left[4]-1:column_idx_left[5]-1]
                                     .replace(",","")
                                     .strip())

                    except ValueError:
                        if cnt+1<len(info_table):
                            try:
                                logging.debug("Invalid shares on this line '{}' looking ahead".format(line[column_idx_left[4]-1:column_idx_left[5]-1]))
                                skip_next_line=True
                                shrs = float(info_table[cnt+1][column_idx_left[4]-1:column_idx_left[5]-1]
                                             .replace(",","")
                                             .strip())

                            except ValueError:
                                logging.warn("Invalid no shares on next line, giving up: {}".format(info_table[cnt+1][column_idx_left[4]-1:column_idx_left[5]-1]))
                                error_counter += 1

                        else:
                            logging.warn("Reached last line no shares found, giving up")
                            error_counter+=1

                    sec_detail = af.SecFilingDetail.objects.create(sec_filing   = db_filing,
                                                                   cusip        = cusip,
                                                                   subject_name = subject_name,
                                                                   no_shares    = shrs,
                                                                   mv           = mv)

                    logging.info("{0:10}{1:40}${2:12}{3:12}".format("xxx" if cusip is None else cusip,
                                                                    "xxx" if subject_name is None else subject_name, 
                                                                    "xxx" if mv is None else mv, 
                                                                    "xxx" if shrs is None else shrs))
                    cusip_counter+=1

                # else:
                #     break

        # for cnt, line in enumerate(info_table):
    # if isxml:
    return error_counter


def parse_filings(args, filers):

    manager_counter = 0
    stats=[]


    if args.type is None:
        form_pattern  = re.compile(r"CONFORMED SUBMISSION TYPE:.*(SC 13D|SC 13D/A|13F-HR|13F-HR/A)$")

    elif args.type == "13F":
        form_pattern  = re.compile(r"CONFORMED SUBMISSION TYPE:.*(13F-HR|13F-HR/A)$")

    elif args.type == "13D":
        form_pattern  = re.compile(r"CONFORMED SUBMISSION TYPE:.*(SC 13D|SC 13D/A)$")


    for (cikraw, fund) in filers:

        cik = cikraw.lstrip("0")
        manager_counter+=1

        for root, dirs, files in os.walk("//nb/corp/groups/NY/Institutional/SPA/Dmitry/13D_fund/edgar/data/%s"%cik):

            if args.anumber is not None:
                filter_file_list = filter(lambda f: f.find("{}.txt".format(args.anumber))>-1, files)
            else:
                filter_file_list=filter(lambda f: f.find("-%s-"%(args.year))>-1 and f.find(".txt")>-1, files)

            logging.debug("Manager {} of {}, name '{}', diretory: {} with {} matching files".format(manager_counter,
                                                                                                   len(filers),
                                                                                                   fund.name,
                                                                                                   root,
                                                                                                   len(filter_file_list)))

            file_counter = 0
            for fname in filter_file_list:
                report_date = None
                form_type = "UKNOWN"

                xml_form_flag   = 0
                txt_form_flag   = 0
                xml_error_flag  = 0
                error_flag      = 0

                with open(os.path.join(root, fname), 'r') as f:
                    try:
                        isfiler = issubject = False
                        file_counter += 1

                        logging.debug("File %i of %i filename:'%s'" % (file_counter, len(filter_file_list), os.path.join(root, fname)))

                        data = f.readlines()

                        #########################
                        # try to read the header to figure out what form type this is
                        ########################
                        for last_line_read, line in enumerate(data):

                            temp = form_pattern.search(line)
                            if temp is not None and temp.group is not None:
                                form_type = temp.group(1)
                                continue

                            temp = subject_re.search(line)
                            if temp is not None and temp.group is not None:
                                issubject = True
                                isfiler   = False
                                continue

                            temp = filedby_re.search(line)
                            if temp is not None and temp.group is not None:
                                issubject = False
                                isfiler   = True
                                continue

                            temp = a_number_re.search(line)
                            if temp is not None and temp.group is not None:
                                a_number = temp.group(1).strip()
                                continue

                            temp = filer_name_re.search(line)
                            if temp is not None and temp.group is not None:
                                if isfiler:
                                    filer_name = temp.group(1).strip()

                                elif issubject:
                                    subject_name = temp.group(1).strip()

                                continue

                            temp = filer_cik_re.search(line)
                            if temp is not None and temp.group is not None:
                                if isfiler:
                                    filer_cik = temp.group(1).strip()

                                elif issubject:
                                    subject_cik = temp.group(1).strip()

                                continue

                            temp = filed_re.search(line)
                            if temp is not None and temp.group is not None:
                                filed_date = dateutil.parser.parse(temp.group(1)).date()
                                continue

                            temp = report_date_re.search(line)
                            if temp is not None and temp.group is not None:
                                report_date = dateutil.parser.parse(temp.group(1)).date()
                                continue

                            if None !=  hdr_end.search(line):
                                break

                        # end for last_line_read, line

                        if form_type != "UKNOWN":

                            try:
                                rs = af.SecFiling.objects.get(a_number=a_number)

                                rrs=rs.secfilingdetail_set.all()
                                rrs.delete()
                                rs.delete()

                            except af.SecFiling.DoesNotExist:
                                pass

                                

                            filing_type = af.SecFilingType.objects.get(code=form_type)
                            
                            ##this is 13, continue parsing
                            if form_type=="13F-HR" or form_type=="13F-HR/A":

                                isxml=False
                                for line in data[last_line_read:]:
                                    if line.lower().find("<xml>")>-1:
                                        isxml=True
                                        break

                                if isxml:
                                    xml_form_flag += 1

                                else:
                                    txt_form_flag += 1

                                error_counter = parse_13F_lines(data,
                                                                last_line_read,
                                                                isxml,
                                                                filing_type,
                                                                a_number,
                                                                filer_name,
                                                                filer_cik,
                                                                filed_date,
                                                                report_date)
                                error_flag += error_counter

                            elif form_type == "SC 13D" or form_type=="SC 13D/A":

                                ishtml=False
                                for line_count, line in enumerate(data[last_line_read:]):
                                    if line.lower().find("<html>")>-1:
                                        ishtml=True
                                        break

                                if ishtml:
                                    xml_form_flag += 1
                                    
                                else:
                                    txt_form_flag += 1

                                parse_13D_lines(data,
                                                last_line_read,
                                                ishtml,
                                                filing_type,
                                                a_number,
                                                filer_name,
                                                filer_cik,
                                                subject_name,
                                                subject_cik,
                                                filed_date,
                                                report_date)
                        else:
                            txt_form_flag += 1

                    except ET.ParseError as e:
                        logging.error("Unable to parse XML in {}:{}".format(fname, e.__unicode__()))
                        xml_error_flag += 1

                    except HTMLParser.HTMLParseError as e:
                        logging.error("Unable to parse HTML in {}:{}".format(fname, e.__unicode__()))
                        xml_error_flag += 1
                        
                    # except:
                    #     print "\tERROR: Unknown error"
                    #     error_flag += 1

                    # print ("Processed %i files, found %i text 13F, %i xml 13F, %i other forms, XML parse error %i, other error %i"%
                    #        (file_counter, txt_form_flag, xml_form_flag, other_form_flag, xml_error_flag, error_flag))


                    stats.append((fund.name,
                                  file_counter,
                                  txt_form_flag,
                                  xml_form_flag,
                                  xml_error_flag,
                                  error_flag,
                                  "" if report_date is None else report_date.strftime("%Y-%m-%d"),
                                  cik,
                                  form_type))

                # with open(os.path.join(root, file), 'r') as f:
            # for file in filter_file_list:
        #for root, dirs, files in os.walk("edgar/data/%s"%cik):
    # for (cik, fund_name) in filers:

    logging.info("{0:10} {1:40} {2:>10} {3:>8} {4:>8} {5:>8} {6:>8}".format("FORM","FILER NAME","CIK","TEXT","HTxML","XML ERR","TXT ERR"))

    for cik in sorted(set([s[7] for s in stats])):
        for form_str in sorted(set([s[8] for s in stats if s[7]==cik])):

            logging.info("{0:10} {1:40} {2:>10} {3:>8} {4:>8} {5:>8} {6:>8}".format(
                form_str,
                [ss[0] for ss in stats if ss[7]==cik and ss[8]==form_str][0],
                cik,
                sum( [ss[2] for ss in stats if ss[7]==cik and ss[8]==form_str]),
                sum( [ss[3] for ss in stats if ss[7]==cik and ss[8]==form_str]),
                sum( [ss[4] for ss in stats if ss[7]==cik and ss[8]==form_str]),
                sum( [ss[5] for ss in stats if ss[7]==cik and ss[8]==form_str]),
            ))
    

def fill_missing_cusip():
    for i,sf in enumerate(af.SecFilingDetail.objects.filter(cusip=None, sec_filing__form_type__code__in=["SC 13D","SC 13D/A"]).all()):
        subs = af.SecFilingDetail.objects.filter(subject_name=sf.subject_name, cusip__isnull=False)
        if len(subs)>0:
            logging.debug("FOUND    :{},{},{},{},{}".format(subs.first().cusip, sf.sec_filing.form_type, sf.cusip, sf.subject_cik, sf.subject_name))
            sf.cusip = subs.first().cusip
            sf.save()



def replace_bad_cusip_from_existing_filing(bad_cusip, sec_form_prefix):
    import bbg_api_wrap
    import get_bbg_price
    
    bh = bbg_api_wrap.bbg_helper()

    type_cusip = nbris.SecurityReferenceType(sec_ref_type="CUSIP")

    ## Replace company name of the 
    # for s in sorted(set([f.subject_name.lower() for f in af.SecFilingDetail.objects.filter(cusip=bad_cusip, sec_filing__form_type__code__startswith=sec_form_prefix)])):

    for s in sorted(set([f.subject_cik for f in af.SecFilingDetail.objects.filter(cusip=bad_cusip, sec_filing__form_type__code__startswith=sec_form_prefix)])):

        ## Replace company name of the 
        # other_cusip= set([f.cusip for f in  af.SecFilingDetail.objects.filter(subject_name__iequal=s).exclude(cusip=bad_cusip) if len(f.cusip)==9])
        other_cusip= set([f.cusip for f in  af.SecFilingDetail.objects.filter(subject_cik=s).exclude(cusip=bad_cusip) if len(f.cusip)==9])

        if len(other_cusip)>0:
            print s,other_cusip

            for oc in other_cusip:
                try:
                    other_sec = nbris.SecurityReference.objects.get(sec_id_value=oc, sec_ref_type__sec_ref_type="CUSIP").security
                    print "\tSecMaster:{}, prices:{}".format(other_sec, other_sec.price_set.count())

                    res = get_bbg_price.bbg_security_market_status(bh, other_sec) 
                    print "\t+++{}:{}:{}:{}".format(*res)

                    for ff in af.SecFilingDetail.objects.filter(cusip=bad_cusip, sec_filing__form_type__code__startswith="SC 13D", subject_name=s):
                        alt_cusip = filter(lambda x: x!=bad_cusip, other_cusip)[0]
                        ff.cusip = alt_cusip

                        ## uncomment after first run 
                        ## verify results are OK
                        ##
                        ##ff.save()
                        print "\tASSIGNED VALIDATED CUSIP TO {}".format(ff)

                    break


                except nbris.SecurityReference.DoesNotExist:
                    print "\t********{} is not in SecMaster".format(oc)
                    try:
                        new_sec = get_bbg_price.bbg_create_security(bbg_helper=bh, secid=oc,secid_type=type_cusip,yellow_key="Equity")
                        res = get_bbg_price.bbg_security_market_status(bh, new_sec) 
                        print "\t********{}+++{}:{}:{}:{}".format(new_sec, *res)
                    except bbg_api_wrap.bbgException as e:
                        print "\t*****ERROR*****{}".format(str(e))



if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description='Search for 13D/13F filings and scrape contents')
    arg_parser.add_argument('--anumber', help='Process single filing by filing accession number')
    arg_parser.add_argument('--allcik', default=False, action='store_true', help='Process filings of all all SEC CIK IDs known')
    arg_parser.add_argument('--cik',  help='Process filings of single entity identified by CIK')
    arg_parser.add_argument('--strat', choices={"13F","13D"}, help='Process customized list of funds (each may have multiple CIK IDs)')
    arg_parser.add_argument('--type', choices={"13F","13D"}, help='Process filings of given type (including amendments)')
    arg_parser.add_argument('--year', default=datetime.date.today().strftime("%y"), help='Process filings for a single year in which the form was filed (YY format)')
    arg_parser.add_argument('--loglevel', default="INFO", choices={"WARN","INFO","DEBUG"}, help='Logging level verbosity')
    args = arg_parser.parse_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)

    logging.basicConfig(filename="filing_extract_LIST-{}_TYPE-{}_YEAR-{}_CIK-{}.log".format(args.strat if args.strat is not None else "ALL",
                                                                                            args.type if args.type is not None else "ALL",
                                                                                            args.year,
                                                                                            args.cik if args.cik is not None else "ALL"),
                        filemode="w",
                        level=numeric_level,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        format="%(asctime)s:%(levelname)s:%(message)s")


    if args.allcik:
        filers = [(s.sec_id_value, s.security) for s in nbris.SecurityReference.objects.filter(sec_ref_type__sec_ref_type="CIK")]

    else:
        if args.cik is not None:
            filers = [(args.cik, nbris.SecurityReference.objects.get(sec_ref_type__sec_ref_type="CIK", sec_id_value=args.cik).security)]

        else:

            filers = []
            if args.strat is not None:
                for f in af.SecFilingStrategyFundMember.objects.filter(strategy__code="{}_ONLY".format(args.strat)):

                    for f_secref in f.fund.security.securityreference_set.filter(sec_ref_type__sec_ref_type="CIK"):
                        filers.append( (f_secref.sec_id_value, f.fund.security) )
                        logging.info("Added filer {} with CIK {} to the list".format(f.fund.security, f_secref.sec_id_value))

                    #filers.append( (f.fund.security.securityreference_set.get(sec_ref_type__sec_ref_type="CIK").sec_id_value, f.fund.security) )
                    #logging.info("Added filer {} to the list".format(f.fund.security))
            else:
                cik_set = set()
                for f in af.SecFilingStrategyFundMember.objects.all():
                    for f_secref in f.fund.security.securityreference_set.filter(sec_ref_type__sec_ref_type="CIK"):
                        if f_secref.sec_id_value not in cik_set:
                            filers.append( (f_secref.sec_id_value, f.fund.security) )
                            logging.info("Added filer {} with CIK {} to the list".format(f.fund.security, f_secref.sec_id_value))
                            cik_set.add(f_secref.sec_id_value)

                    # OBSOLETE, assumes security and CIK ID are 1-1 not 1-many
                    #
                    # cik = f.fund.security.securityreference_set.get(sec_ref_type__sec_ref_type="CIK").sec_id_value
                    # if cik not in cik_set:
                    #     filers.append( (cik, f.fund.security) )
                    #     logging.info("Added filer {} to the list".format(f.fund.security))
                    #     cik_set.add(cik)

            # funds      = af.SecFilingStrategyFundMember.objects.filter(strategy__code="{}_ONLY".format(args.strat))

            # filers = [(s.sec_id_value, s.security.name) for s in 
            #           nbris.SecurityReference.objects.filter(sec_ref_type__sec_ref_type="CIK",
            #                                                  security__sec_type__sec_type="HedgeFund")]
            # cik_list   = [(nbris.SecurityReference.objects.get(sec_ref_type__sec_ref_type="CIK", security=a.fund.security).sec_id_value,
            #                f.security) for a in funds]

    parse_filings(args, filers)
