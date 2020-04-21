import io
import json
import os
import re
import sys
import time
import datetime
import random
import xml.etree.ElementTree as ET
from collections import defaultdict
from io import BytesIO
from pprint import pprint

import pandas as pd
import pdfminer
import requests
from pdfminer.converter import XMLConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage

'''
Better Automated PDF Text Extraction for Research Papers (using PDF Miner)

Retrieve and extract text from full text PDFs of COVID-19 related research

For more info on using pdfminer, see : https://readthedocs.org/projects/pdfminer-docs/downloads/pdf/latest/

'''


def download_preprint_metadata():
    url = 'https://connect.biorxiv.org/relate/collection_json.php?grp=181'
    max_attempts = 4
    attempts = 0
    print(url)
    while attempts < max_attempts:
        r = requests.get(url)
        if r.status_code != 429:
            break
        # If rate limited, wait and try again (in seconds)
        time.sleep((2 ** attempts) + random.random())
        attempts = attempts + 1
    data = json.loads(r.content)
    return data


def get_df(site=None):
    data = download_preprint_metadata()
    df = pd.DataFrame.from_dict(data['rels'])
    srcs = df.groupby('rel_site')
    if site=='bio':
        return srcs.get_group('biorxiv')
    elif site=='med':
        return srcs.get_group('medrxiv')
    else:
        return df


# Download all pdfs

def get_text(url, parse=True, laparams=laparams):
    url += 'v1.full.pdf'
    max_attempts = 4
    attempts = 0
    print(url)
    while attempts < max_attempts:
        r = requests.get(url)
        if r.status_code != 429:
            break
        # If rate limited, wait and try again (in seconds)
        time.sleep((2 ** attempts) + random.random())
        attempts = attempts + 1
    data = r.content

    try:
        f = io.BytesIO(data)

        rsrcmgr = PDFResourceManager()
        retstr = BytesIO()
        codec = 'utf-8'
        device = XMLConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)  # , rect_colors=rect_colors)

        interpreter = PDFPageInterpreter(rsrcmgr, device)
        password = ""
        maxpages = 0  # is for all
        caching = True
        pagenos = set()

        for page in PDFPage.get_pages(f, pagenos, maxpages=maxpages, password=password, caching=caching,
                                      check_extractable=True):
            interpreter.process_page(page)
        device.close()
        pdf_data = retstr.getvalue()
        retstr.close()
    except:
        return ('.raw.txt', data)
    try:
        if parse == False:
            return ('.xml', pdf_data)
        else:

            # xmltest = convert_pdf_to_xml(pdf_data)
            root = ET.fromstring(pdf_data)

            temp = root.find('.//text')
            curr_font = temp.get('font')
            curr_size = float(temp.get('size'))
            text = ''

            rmargin = 70

            i = 0
            newline_pos = []
            for l in root.iterfind('.//textline'):
                for t in l.findall('./text'):
                    if (t.get('font') or t.get('size')) is None:
                        if t.text[0] == ' ':
                            text += ' '
                        else:
                            text += '<<NEWLINE>>'
                            newline_pos.append([])

                    else:
                        x0, y0, x1, y1 = [float(z) for z in t.get('bbox').split(',')]
                        char_size = float(t.get('size', 0))
                        char_font = t.get('font', '')
                        if y0 > 750 or y0 < 75:
                            continue
                        if x0 < rmargin:
                            if re.search('[A-Za-z]+', t.text) is not None:
                                print('changing rmargin to ', str(x0 - 1))
                                rmargin = x0 - 1
                                text += t.text
                            continue

                        else:
                            if (char_size != curr_size) or (char_font != curr_font):
                                if (char_size) <= 8.:
                                    continue
                                text += '<<NEWFONT>>' + t.text
                                curr_font = t.get('font')
                                curr_size = float(t.get('size'))
                            else:
                                text += t.text
            lines = text.split('<<NEWLINE>>')
            [print(l) for l in lines[:min(len(lines), 5)]]

            doc = lines[0]
            open_parens = False
            parens = []

            if len(re.findall(r'\(', doc)) > len(re.findall(r'\)', doc)):
                parens.append(True)
            else:
                parens.append(False)
            for i, t in enumerate(lines):

                if (i == 0):
                    if re.search(r'^\s*[a-z(]', lines[1]) is None:
                        doc += '\n'
                    continue
                if len(t) < 1:
                    if open_parens == False:
                        doc += '\n'
                    else:
                        continue
                    continue
                else:
                    o = len(re.findall(r'\(', t))
                    if open_parens == True:
                        o += 1
                    c = len(re.findall(r'\)', t))
                    if o > c:
                        open_parens = True
                    else:
                        open_parens = False

                    if open_parens == False:
                        if t.startswith(' '):
                            t = re.sub(r'^ +', '<<PARAGRAPH>>', t)
                        if t.lstrip(' ').startswith('<<NEWFONT>>') and lines[i - 1].rstrip(' ').endswith('.'):
                            t = re.sub(r'^<<NEWFONT>>', '<<PARAGRAPH>>', t.lstrip(' '))
                        if t.rstrip(' ').endswith('.'):
                            t += '<<PARAGRAPH>>'
                        if re.match(r'^\d{1,3}\.<<NEWFONT>>', t):
                            t = '<<PARAGRAPH>>' + t
                    doc += t
                    parens.append(open_parens)
            doc = re.sub(r'(?<=[^.])\n+', '', doc)
            doc = re.sub(r' {3,}', '<<PARAGRAPH>>', doc)
            print(doc[:50])

            parsed = []
            for _text in doc.split(r'<<PARAGRAPH>>'):
                _text = re.sub('(<<NEWLINE>>)+', '\n', _text)
                _text = re.sub(r'  ', r'\n', _text)
                _text = re.sub(r'<<NEWFONT>>(?P<url>http[a-zA-Z0-9./+?_=:-]+)( <<NEWFONT>>)?', r'\g<url>', _text)
                _text = re.sub(r'<<NEWFONT>> <<NEWFONT>>', r' ', _text)
                _text = re.sub(r'\(<<NEWFONT>>(.+)<<NEWFONT>>\)', r'(\g<1>)', _text, re.M)

                pattern = re.compile(r'<<NEWFONT>>(((\W|\d)+)|([A-Za-z_-]{1,2}\n?))<<NEWFONT>>')

                _text = pattern.sub(r'\g<1>', _text)

                pat2 = re.compile(r'<<NEWFONT>>([A-Za-z- :]+)<<NEWFONT>>([.:]?)')
                _text = pat2.sub(r'\g<1>\g<2>\n', _text)
                pat3 = re.compile(r'<<NEWFONT>>([A-Za-z_-]{1,3} *\n?)<<NEWFONT>>')

                _text = pat3.sub(r'\g<1>', _text)

                _text = re.sub(r'<<NEWFONT>>(.+)<<NEWFONT>>([a-z]+)', r'\g<1> \g<2>', _text)
                _text = re.sub(r'<<NEWFONT>>(.+)<<NEWFONT>>(\W*)\.?', r'\g<1> \g<2>', _text)
                _text = re.sub(r'-\n', r'-', _text)
                _text = re.sub(r'\((.+)(?:\n)(.+)\)', r'(\g<1>\g<2>))', _text)
                _text = re.sub(r'\((.+)<<NEWFONT>>(.+)\)', r'(\g<1>\g<2>)', _text, re.M)

                if len(_text.strip(' \n')) > 0:
                    if len(re.findall(r'<<NEWFONT>>', _text)) == 1:
                        _text = re.sub(r'<<NEWFONT>>', '\n', _text)
                    parsed.append(_text)

            parsed2 = [parsed[0], ]
            for i, p in enumerate(parsed):
                if i > 0:
                    if re.search(r'^\s*\n*[a-z]', p) is not None:
                        parsed2[i - 1] += p
                        p = ''
                    parsed2.append(p)
            parsed2 = '\n===================================\n'.join([p for p in parsed2 if p != ''])

            print(parsed2[:50])
            return ('.txt', parsed2)
    except:
        return ('.raw.xml', pdf_data)

def update_log(_processed, data_dir=None, errs=None, laparams=None):
    if laparams is None:
        laparams='pdfminer.layout.LAParams(line_overlap=0.1, char_margin=2.0, line_margin=1., word_margin=0.5, boxes_flow=1, detect_vertical=False, all_texts=True'
    if errs is None:
        errs = []
    if data_dir is None:
        data_dir = './medrxiv-fulltext-xml/'
    info=[{'date':str(datetime.datetime.today()), 'laparams':laparams,'files': _processed ,'err': errs}]
    if not os.path.isfile(os.path.join(data_dir,'log.json')):
        with open(os.path.join(data_dir,'log.json'),"w") as output_file:
            json.dump(info,output_file)
        print('Created new log file: ', os.path.join(data_dir,'log.json'))
        print('Processed {} new files:'.format(len(_processed)))
    else:
        entries=json.load(open(os.path.join(data_dir,'log.json'),'r'))
        entries.append(info)
        with open(os.path.join(med_dir,'log.json'),"w") as output_file:
            json.dump(entries,output_file)
        print('Logged {} new parsed papers:'.format(len(_processed)))


if __name__=='__main__':

    med_dir = './medrxiv-fulltext-xml/'
    med = get_df('med')
    links = med['rel_link'].tolist()
    parse = True
    laparams = pdfminer.layout.LAParams(line_overlap=0.1, char_margin=2.0, line_margin=1.5, word_margin=0.25,
                                        boxes_flow=.5,
                                        detect_vertical=False, all_texts=True)

    #     laparams=pdfminer.layout.LAParams(line_overlap=0.1, char_margin=2.0, line_margin=1., word_margin=0.5, boxes_flow=1, detect_vertical=False, all_texts=True)
    #     laparams=pdfminer.layout.LAParams(line_overlap=0.1, char_margin=4.0, line_margin=1., word_margin=0.5, boxes_flow=.5, detect_vertical=False, all_texts=True)

    errs = defaultdict(list)

    if not os.path.isdir(med_dir):
        os.makedirs(med_dir)
        print('Created directory: ', med_dir)
    else:
        print('Adding data to ', med_dir)

    processed = os.listdir(med_dir)
    unprocessed = [link for link in links if link.split('/')[-1] + ".txt" not in processed]
    print('Previously processed: ', len(links) - len(unprocessed))
    print('To process: ', len(unprocessed))


    for i, link in enumerate(unprocessed):
        try:
            ext, txt = get_text(link, parse)
            if parse == False:
                with open(os.path.join(med_dir, link.split('/')[-1]) + ext, "wb+") as output_file:
                    print(os.path.join(med_dir, link.split('/')[-1]) + ext)
                    output_file.write(txt)

            else:
                if type(txt) == str:
                    with open(os.path.join(med_dir, link.split('/')[-1]) + ext, "w+") as output_file:
                        output_file.write(txt)
                else:
                    with open(os.path.join(med_dir, link.split('/')[-1]) + ext, "wb") as output_file:
                        output_file.write(txt)
            time.sleep(5)
            if i % 100 == 0:
                print('========== Processed {}/{} papers =========='.format(i, len(links)))

        except OSError as err:
            print("OS error: {0}".format(err))
            errs['OSError'].append(link)
        except ValueError:
            print("Could not convert data to an integer.")
            errs['ValueError'].append(link)
        except KeyboardInterrupt:  # , SystemExit):
            print('Processed {} files before interrupt'.format(i))
            raise
        except:
            print("Unexpected error:", sys.exc_info()[0])
            errs['UnknownError'].append(link)
            print('Waiting 30 sec before continuing')
            time.sleep(30)

    print('Done')
    print('Errors: \n', errs)
    _processed = [item for item in os.listdir(med_dir) if item not in processed]
