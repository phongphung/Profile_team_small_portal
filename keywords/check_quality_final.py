__author__ = 'sunary'


from text_processing.sorted_list import SortedWords
from utils import my_connection, my_text, my_helper
import pandas as pd
import yaml


class CheckQuality():

    def __init__(self):
        self.ticker_split = ['.', ':', '_', '-', ' ']
        self.get_name_legalname = False
        self.counted_messages = True

    def read_db(self):
        self.con_psql1 = my_connection.ellytran_connection()

        self.get_keywords_check_dup()
        self.get_company_type()
        self.get_deleted_itemid()
        self.get_abbreviation()

    def get_keywords_check_dup(self):
        cur_psql1 = self.con_psql1.cursor()

        self.sorted_words = SortedWords()
        last_item_id = 0
        while True:
            cur_psql1.execute('''
                SELECT lower(trim(triggering_term)) AS triggering_term,
                    array_agg(DISTINCT item_id) AS dup_item
                FROM (SELECT item_id, lower(trim(att->>'word')) AS triggering_term
                    FROM (SELECT item_id, json_array_elements(active_triggering_term) AS att
                        FROM term
                        WHERE active_triggering_term IS NOT NULL
                             AND active_triggering_term::text != ''
                             AND active_triggering_term::text != '[]'
                             AND active_triggering_term::text != 'null'
                             AND item_id > %s
                        ORDER BY item_id
                        LIMIT 1000) AS term1
                    WHERE term1.att IS NOT NULL
                        AND term1.att::text != ''
                        AND term1.att::text != '[]'
                        AND term1.att::text != 'null') AS term
                GROUP BY lower(trim(triggering_term))
                HAVING COUNT(DISTINCT item_id) > 1
            ''', [last_item_id])
            self.con_psql1.commit()

            self._dup_item = []

            if not cur_psql1.rowcount:
                break

            for doc in cur_psql1:
                normalize = my_text.normalize(doc[0])
                if max(doc[1]) > last_item_id:
                    last_item_id = max(doc[1])

                if len(self._dup_item) == 0 or self._dup_item[-1] != normalize:
                    self._dup_item.append(doc[1])
                    self.sorted_words.add(word=normalize)

    def get_company_type(self):
        cur_psql1 = self.con_psql1.cursor()
        cur_psql1.execute('''
            SELECT type_name, type_abbreviation
            FROM company_type
            WHERE status = 1
        ''')
        self.con_psql1.commit()

        self.removed_company_type = set()
        for doc in cur_psql1:
            try:
                self.removed_company_type.add(my_text.normalize(doc[0]))
                self.removed_company_type |= set([my_text.normalize(w) for w in doc[1]])
            except:
                pass

    def get_deleted_itemid(self):
        with open('keywords/deleted_itemid.yml', 'r') as fo:
            self.set_deleted_itemid = yaml.load(fo)

        self.set_deleted_itemid = set(self.set_deleted_itemid)

    def get_abbreviation(self):
        self.abbreviation = {}
        with open('keywords/abbreviation.yml', 'r') as fo:
            self.abbreviation = yaml.load(fo)
            for key, value in self.abbreviation.iteritems():
                self.abbreviation[key] = [my_text.normalize(x) for x in value]

            self.abbreviation['&'] = ['and']

    def save_company_type(self):
        self.con_psql1 = my_connection.psql1_connection()

        cur_psql1 = self.con_psql1.cursor()
        cur_psql1.execute('''
            SELECT country, language, type_name, type_abbreviation
            FROM company_type
            WHERE status = 1
        ''')
        self.con_psql1.commit()

        company_types = []
        for doc in cur_psql1:
            company_types.append({'country': doc[0],
                                  'language': doc[1],
                                  'name': doc[2],
                                  'abbreviations': doc[3]})

        with open('company_types.yml', 'w') as fo:
            fo.write(yaml.dump(company_types))

        self.close_connection()

    def get_dup_item(self, keyword):
        normalize = my_text.normalize(keyword)
        position = self.sorted_words.find(normalize)
        if self.sorted_words.exist(position_word=position):
            return self._dup_item[position]

        return ''

    def query_by_item(self, item_id):
        '''
        get tickers, legalname from public.ticker1 by item_id
        '''

        cur_psql1 = self.con_psql1.cursor()
        self.root_tickers = set()
        self.screen_name = ''
        self.legal_name = ''
        self.url = ''

        cur_psql1.execute('''
            SELECT bloomberg_ticker,
                google_finance_ticker,
                reuters_ticker,
                local_ticker,
                bloomberg_exchange,
                google_finance_exchange,
                reuters_exchange
            FROM public.ticker1
            WHERE item_id = %s
                AND status = 1
        ''', [item_id])
        self.con_psql1.commit()

        for doc in cur_psql1:
            if doc[0]: self.root_tickers |= set(map(lambda x: my_text.normalize(x), doc[0]))
            if doc[1]: self.root_tickers |= set(map(lambda x: my_text.normalize(x), doc[1]))
            if doc[2]: self.root_tickers |= set(map(lambda x: my_text.normalize(x), doc[2]))
            if doc[3]: self.root_tickers |= set(map(lambda x: my_text.normalize(x), doc[3]))
            if doc[4]: self.root_tickers |= set(map(lambda x: my_text.normalize(x.split(':')[0]) if x and len(x.split(':')) > 0 else None, doc[4])) # bloomberg_exchange
            if doc[5]: self.root_tickers |= set(map(lambda x: my_text.normalize(x.split(':')[1]) if x and len(x.split(':')) > 1 else my_text.normalize(x), doc[5])) # google_finance_exchange
            if doc[6]: self.root_tickers |= set(map(lambda x: my_text.normalize(x.split('.')[0]) if x and len(x.split('.')) > 0 else None, doc[6])) # reuters_exchange

        cur_psql1.execute('''
            SELECT ticker, www
            FROM item_audit
            WHERE id = %s
        ''', [item_id])
        self.con_psql1.commit()

        doc = cur_psql1.fetchone()
        if doc:
            self.root_tickers |= set(map(lambda x: my_text.normalize(x), doc[0].split(',')))
            self.url = my_text.normalize(doc[1])

        if self.get_name_legalname:
            cur_psql1.execute('''
                SELECT name
                FROM public.item
                WHERE id = %s
            ''',[item_id])
            self.con_psql1.commit()

            doc = cur_psql1.fetchone()
            if doc:
                self.screen_name = doc[0]

        cur_psql1.execute('''
            SELECT legalname
            FROM public.item_audit
            WHERE id = %s
        ''',[item_id])
        self.con_psql1.commit()

        doc = cur_psql1.fetchone()
        if doc:
            self.legal_name = doc[0]

    def extract_root_ticker(self, keyword):
        for sp in self.ticker_split:
            if sp in keyword:
                keyword_splited = keyword.split(sp)
                if all([my_text.normalize(x, '$') == x.lower() for x in keyword_splited]):
                    return [my_text.normalize(x) for x in keyword_splited]
        return [my_text.normalize(keyword)]

    def process(self, file_source, file_dest):
        self.read_db()

        original_fields = my_helper.get_dataframe_columns(file_source)

        generate_fields = original_fields[::]
        generate_fields.append('__case' if '_case' in generate_fields else '_case')
        generate_fields.append('__status'  if '_status' in generate_fields else '_status')
        generate_fields.append('__value'  if '_value' in generate_fields else '_value')
        generate_fields.append('__dup_item'  if '_dup_item' in generate_fields else '_dup_item')

        original_dataframe = pd.read_csv(file_source)
        generate_dataframe = {}
        for field in generate_fields:
            generate_dataframe[field] = []

        for i in range(len(original_dataframe[original_fields[0]])):
            item_id = original_dataframe['item_id'][i]
            if int(item_id) in self.set_deleted_itemid:
                continue

            self.query_by_item(item_id)
            keyword = original_dataframe['keyword'][i]
            try:
                value = self.difference_percent(original_dataframe['msg_hit_without_and'][i], original_dataframe['msg_hit_with_and'][i])
            except:
                value = 1.0

            case = self.twitter_account(keyword)
            if not case:
                case = self.keywords_match_legalname(keyword, value)
            if not case:
                case = self.keywords_from_url(keyword)
            if not case:
                case = self.keywords_contain_ticker(keyword)
            if not case:
                case = 'manual6'

            for j in range(len(original_fields)):
                if original_fields[j] == 'legalname':
                    # override new legal name
                    generate_dataframe[generate_fields[j]].append(self.legal_name)
                else:
                    generate_dataframe[generate_fields[j]].append(my_helper.except_pandas_value(original_dataframe[original_fields[j]][i]))

            if case.startswith('pass'):
                generate_dataframe[generate_fields[len(original_fields)]].append(case[4:])
                generate_dataframe[generate_fields[len(original_fields) + 1]].append('Pass')
            elif case.startswith('fail'):
                generate_dataframe[generate_fields[len(original_fields)]].append(case[4:])
                generate_dataframe[generate_fields[len(original_fields) + 1]].append('Fail')
            elif case.startswith('notd'):
                generate_dataframe[generate_fields[len(original_fields)]].append(case[4:])
                generate_dataframe[generate_fields[len(original_fields) + 1]].append('Not decide')
            elif case.startswith('manual'):
                generate_dataframe[generate_fields[len(original_fields)]].append(case[6:])
                generate_dataframe[generate_fields[len(original_fields) + 1]].append('Manual check')

            generate_dataframe[generate_fields[len(original_fields) + 2]].append(value)
            generate_dataframe[generate_fields[len(original_fields) + 3]].append(self.get_dup_item(keyword))

        original_dataframe = pd.DataFrame(data=my_helper.dataframe_same_length(generate_dataframe, generate_fields), index=None, columns=generate_fields)
        original_dataframe.to_csv(file_dest, index= False)

        self.close_connection()

    def one_case(self, keyword, legal_name, root_ticker, url, removed_company_type):
        self.legal_name = legal_name
        self.url = url
        self.root_tickers = root_ticker
        self.removed_company_type = removed_company_type
        self.sorted_words = SortedWords()
        self.sorted_words.set(['anyword'])
        self.abbreviation = {'&':['and'],
                            'limited': '[ltd]',
                            'Corporation': ['corporate', 'corp']}

        case = self.twitter_account(keyword)
        if not case:
            case = self.keywords_match_legalname(keyword, 0)
        if not case:
            case = self.keywords_from_url(keyword)
        if not case:
            case = self.keywords_contain_ticker(keyword)
        if not case:
            case = 'manual6'

        return keyword, case

    def de_abbreviation(self, keyword):
        keyword = my_text.split_text(keyword)
        for i in range(len(keyword)):
            for key, value in self.abbreviation.iteritems():
                if my_text.normalize(keyword[i], '&') in value:
                    keyword[i] = key

        return ' '.join(keyword)

    def startswith_condition(self, str_contain, str_start):
        '''
        Examples:
            >>> self.startswith_condition('Communication', 'Com')
            True
            >>> self.startswith_condition('F&C', 'F')
            False
        '''
        if str_contain.startswith(str_start):
            if len(str_contain) == len(str_start) or my_text.normalize(str_contain[len(str_start)]):
                return True

        return False

    def keywords_match_legalname(self, keyword, value):
        keyword = self.de_abbreviation(keyword)
        legal_name = self.de_abbreviation(self.legal_name)

        if my_text.normalize(keyword, '&') == my_text.normalize(legal_name, '&'):
            return 'pass1'

        legal_name_cutting = my_text.split_text(legal_name)
        legal_name_cutting = filter(lambda x: my_text.normalize(x, '&'), legal_name_cutting)
        legal_name_cutting = map(lambda x: my_text.normalize(x, '&'), legal_name_cutting)

        keyword_cutting = my_text.split_text(keyword)
        keyword_cutting = filter(lambda x: my_text.normalize(x, '&'), keyword_cutting)
        keyword_cutting = map(lambda x: my_text.normalize(x, '&'), keyword_cutting)
        len_keyword = len(keyword_cutting)

        if len_keyword <= len(legal_name_cutting) and all([self.startswith_condition(legal_name_cutting[i], keyword_cutting[i]) for i in range(len_keyword)]):
            if len_keyword == 1:
                return 'manual4.1'
            elif len_keyword == 2:
                if value <= 0.1:
                    return 'pass4.2'
                return 'manual4.2'
            elif len_keyword == 3:
                if value <= 0.1:
                    return 'pass4.3'
                return 'manual4.3'
            elif len_keyword >= 4:
                return 'pass4.4'

        return ''

    def keywords_contain_ticker(self, keyword):
        if keyword.startswith('$'):
            if any([x in keyword for x in self.ticker_split]):
                if any([x in self.root_tickers for x in self.extract_root_ticker(keyword)]):
                    if self.get_dup_item(keyword):
                        return 'fail2.1'
                    return 'pass2.1'
            elif my_text.normalize(keyword) in self.root_tickers:
                if len(keyword) > 2:
                    if self.get_dup_item(keyword):
                        return 'fail2.2a'
                    return 'pass2.2a'
                elif len(keyword) == 2:
                    return 'manual2.2b'
        else:
            if any([x in keyword for x in self.ticker_split]):
                if any([x in self.root_tickers for x in self.extract_root_ticker(keyword)]):
                    if self.get_dup_item(keyword):
                        return 'fail2.1'
                    return 'pass2.1'
            elif any([x in self.root_tickers for x in self.extract_root_ticker(keyword)]):
                return 'fail2.3'

        return ''

    def keywords_from_url(self, keyword):
        normalize_url = my_text.normalize(keyword, ' ')
        if self.url.startswith(normalize_url + '.') or ('.' + normalize_url + '.') in self.url:
            return 'notd3'
        return ''

    def twitter_account(self, keyword):
        if keyword.startswith('@') and keyword[1:].lower() == my_text.normalize(keyword, '_'):
            return 'manual5'
        return ''

    def difference_percent(self, before, after):
        try:
            return (before - after)*1.0/before
        except:
            return 1.0

    def close_connection(self):
        self.con_psql1.close()


if __name__ == '__main__':
    check_quality = CheckQuality()
    # print check_quality.one_case(keyword='LN.B8F', legal_name='BIOFRONTERA AG', root_ticker=['b8f'], url='', removed_company_type=['ag'])
    # print check_quality.one_case(keyword='$Cybits Holding', legal_name='CYBITS HOLDING AG', root_ticker=[], url='', removed_company_type=['ag', 'holding'])
    # print check_quality.one_case(keyword='KhdHumboldtWedAg', legal_name='KHD Humboldt Wedag Industrial Services AG', root_ticker=[], url='', removed_company_type=['ag'])
    # print check_quality.one_case(keyword='Schroder Real Investment Trust', legal_name='Schroder Real Estate Investment Trust Limited', root_ticker=[], url='', removed_company_type=['limited', 'trust'])
    # print check_quality.one_case(keyword='Schroder Estate Investment Trust', legal_name='Schroder Real Estate Investment Trust Limited', root_ticker=[], url='', removed_company_type=['limited', 'trust'])
    # print check_quality.one_case(keyword='TRY LON', legal_name='', root_ticker=['try'], url='', removed_company_type=['limited', 'trust'])
    # print check_quality.one_case(keyword='$TRY LON', legal_name='', root_ticker=['try'], url='', removed_company_type=['limited', 'trust'])
    # print check_quality.one_case(keyword='$Advancedcomputersoftware', legal_name='Advanced Computer Software Group PLC', root_ticker=['try'], url='', removed_company_type=['group', 'plc'])
    # print check_quality.one_case(keyword='LN:ABZA', legal_name='LN:ABZA', root_ticker=['ln'], url='', removed_company_type=['group', 'plc'])
    # print check_quality.one_case(keyword='$Compass Group', legal_name='COMPASS GROUP PLC', root_ticker=['ln'], url='', removed_company_type=['group', 'plc'])
    # print check_quality.one_case(keyword='CompassGroupPlc', legal_name='COMPASS GROUP PLC', root_ticker=['ln'], url='', removed_company_type=['group', 'plc'])
    # print check_quality.one_case(keyword='CompGroupPlc', legal_name='COMPASS GROUP PLC', root_ticker=['ln'], url='', removed_company_type=['group', 'plc'])
    # print check_quality.one_case(keyword='$Abzena', legal_name='Abzena Ltd', root_ticker=['ln'], url='', removed_company_type=['ltd', 'plc'])
    # print check_quality.one_case(keyword='Dragon Ukrainian Prop & Development', legal_name='Dragon Ukrainian Properties & Development plc', root_ticker=['ln'], url='', removed_company_type=['ltd', 'plc'])
    # print check_quality.one_case(keyword='Compass Ltd', legal_name='COMPASS Limited', root_ticker=['ln'], url='', removed_company_type=['group', 'plc'])
    # print check_quality.one_case(keyword='UnitechCorpParks', legal_name='Unitech Corporate Parks PLC', root_ticker=['ln'], url='', removed_company_type=['group', 'plc'])
    # print check_quality.one_case(keyword='DragonUkrainianPropertiesAndDevelopmentPlc', legal_name='Dragon Ukrainian Properties & Development plc', root_ticker=['ln'], url='', removed_company_type=['group', 'plc'])
    check_quality.process('/home/nhat.vo/messages_hit_counts_for_japanese_and_indian_listed_companies.csv', '/home/nhat.vo/quality_hk.csv')