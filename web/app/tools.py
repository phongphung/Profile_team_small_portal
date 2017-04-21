# -*- coding: utf-8 -*-
import time

__author__ = 'sunary'


import os
from datetime import datetime, timedelta
from multiprocessing import Process
from werkzeug.utils import secure_filename
from web.app import flask_app
from flask import render_template, request, make_response
from utils import my_helper
from other_tool.gnip_rule_generate import RuleGenerate
from database_access.cassandra_get_scores_following import GetScoresFollowing
from keywords.messages_deleted_contain_ticker import MsgDeletedContainTicker
from keywords.check_quality_final import CheckQuality
from keywords.count_release_messages import CountReleaseMessages
from keywords.wrong_legalname_generate import WrongLegalnameGenerate
from keywords.mixed_box_counting import MixedBoxCounting
from utils.my_helper import init_logger
from bloomberg_crawler.processor import BloombergCrawler


logger = init_logger(__name__)

from python.entity_resolution import submit_matching_tasks
from python.check_financial_terms import check_financial_terms


@flask_app.route('/gnip', methods=['GET', 'POST'])
def gnip():
    manager_gnip = '<a href="https://sys.sentifi.com:4035/admin-group/index?group=rawData4">gnip-processor_rawData4</a>'

    url_request = 'https://api.gnip.com:443/accounts/Sentifi/publishers/twitter/streams/track/prod/rules.json'
    user_name = 'anh.le@sentifi.com'
    password = 's1822013!'
    file_name_json = 'gnip.json'

    if request.method == 'POST':
        file = request.files.get('file', None)
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(flask_app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            generate = RuleGenerate()
            try:
                execute_date, json = generate.generate(file_path, os.path.join(flask_app.config['UPLOAD_FOLDER'], file_name_json))
            except:
                os.remove(os.path.join(flask_app.config['UPLOAD_FOLDER'], filename))
                return render_template('gnip.html',
                                        show_upload=True,
                                        request_info='<strong>Wrong format!</strong>',
                                        date='',
                                        json='')
            os.remove(os.path.join(flask_app.config['UPLOAD_FOLDER'], filename))
            if execute_date:
                return render_template('gnip.html',
                                       show_upload=True,
                                       request_info='''Monitor gnip (worker5): %s<br>''' % manager_gnip,
                                       date='Execute date: ' + str(', '.join(execute_date)),
                                       json=json.decode('utf-8').replace('\n', '<br>').replace('  ', '&nbsp; '))
            else:
                return render_template('gnip.html',
                                       show_upload=True,
                                       request_info='''Monitor gnip (worker5): %s<br>''' % manager_gnip,
                                       date='the Rule below has length lager than 2048, please fix it!',
                                       json=json.decode('utf-8').replace('\n', '<br>').replace('  ', '&nbsp; '))
        elif request.form.get('action', '') == 'post':
            if os.path.isfile(flask_app.config['UPLOAD_FOLDER'] + file_name_json):
                output_curl = my_helper.subprocess_output('curl -v -X POST -u %s:%s %s -d @%s' %(user_name, password, url_request, file_name_json),
                                                          cwd=flask_app.config['UPLOAD_FOLDER'])

                if not output_curl:
                    output_curl = '<strong>Posted</strong>'
                os.remove(flask_app.config['UPLOAD_FOLDER'] + file_name_json)
            else:
                output_curl = '<strong>Have an error!</strong>'
            return render_template('gnip.html',
                                   show_upload=False,
                                   request_info='''Monitor gnip (worker5): ''',
                                   date='',
                                   json=output_curl.decode('utf-8').replace('\n', '<br>').replace('  ', '&nbsp; '))
        elif request.form.get('action', '') == 'delete':
            if os.path.isfile(flask_app.config['UPLOAD_FOLDER'] + 'gnip.json'):
                output_curl = my_helper.subprocess_output('curl -v -X DELETE -u %s:%s %s -d @%s' %(user_name, password, url_request, file_name_json),
                                                          cwd=flask_app.config['UPLOAD_FOLDER'])
                if not output_curl:
                    output_curl = '<strong>Deleted<strong>'
                os.remove(flask_app.config['UPLOAD_FOLDER'] + file_name_json)
            else:
                output_curl = '<strong>Have an error!</strong>'
            return render_template('gnip.html',
                                   show_upload = False,
                                   request_info='''Monitor gnip (worker5): %s''' % manager_gnip,
                                   date='',
                                   json=output_curl.decode('utf-8').replace('\n', '<br>').replace('  ', '&nbsp; '))

    return render_template('gnip.html',
                           show_upload=True,
                           request_info='''Monitor gnip (worker5): %s''' % manager_gnip,
                           date ='',
                           json ='')


@flask_app.route('/messages_deleted', methods=['GET', 'POST'])
def messages_deleted():
    result_file_name = 'messages_deleted.csv'
    result_file = os.path.join(flask_app.config['UPLOAD_FOLDER'], result_file_name)
    field_description = [['id', 'Item id']]

    file_info = ''
    if os.path.isfile(result_file):
        file_info = 'Completed at: %s' % (datetime.fromtimestamp(os.stat(result_file).st_mtime) + timedelta(hours=7))

    if request.method == 'POST':
        file = request.files.get('file', None)
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(flask_app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            try:
                if file_info:
                    # os.remove(result_file)
                    file_info = ''
                num_row = my_helper.len_dataframe(file_path)
                est_time_process = 1.0*num_row + 30
                est_time_str = '%s mins' % (int(est_time_process/60))

                messages_deleted_pro = MsgDeletedContainTicker()
                process = Process(target=messages_deleted_pro.process, args=(file_path, result_file, ))
                process.start()
                return render_template('messages_deleted.html',
                           file_info=file_info,
                           status='Processing<br><h4 style= "color: green">It take about %s</h4>' % (est_time_str),
                           field_description=field_description)
            except Exception as e:
                logger.error('Something error: %s' % e)
                return render_template('messages_deleted.html',
                           file_info=file_info,
                           status='Have an error!',
                           field_description=field_description)
        elif request.form.get('action', '') == 'download':
            with open(result_file) as fo:
                file_contents = fo.read()
            response = make_response(file_contents)
            response.headers["Content-Disposition"] = "attachment; filename=%s" % (result_file_name)
            return response

    return render_template('messages_deleted.html',
                           file_info=file_info,
                           status='',
                           field_description=field_description)

@flask_app.route('/bloomberg_crawler', methods = ['GET', 'POST'])
def bloomberg_crawler():
    date = time.strftime('%Y-%m-%d')
    result_file_name = '%s_bloomberg.csv' % date
    result_file = os.path.join(flask_app.config['UPLOAD_FOLDER'], result_file_name)
    field_description = [
        ['Name', 'Company name (i.e. 3M Company)'],
        ['Exchange', 'Exchange Name (i.e. New York Stock Exchange)'],
        ['root_ticker', 'Root ticker (i.e. AAPL)'],
        ['Bloomberg_ticker', 'Bloomberg ticker (i.e. AAPL:US)']
    ]

    file_info = ''
    if os.path.isfile(result_file):
        file_info = 'Completed at: %s' % (datetime.fromtimestamp(os.stat(result_file).st_mtime) + timedelta(hours=7))

    if request.method == 'POST':
        file = request.files.get('file', None)
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(flask_app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            try:
                if file_info:
                    # os.remove(result_file)
                    file_info = ''

                num_row = my_helper.len_dataframe(file_path)
                est_time_process = 0.5*num_row + 30
                est_time_str = '%s mins' % (int(est_time_process/60))

                bloomberg_crawler = BloombergCrawler()
                process = Process(target=bloomberg_crawler.process, args=(file_path, result_file))
                process.start()
                return render_template('bloomberg_crawler.html',
                           file_info=file_info,
                           status='<br> Processing<br><h4 style= "color: green">It takes about %s</h4>' %(est_time_str),
                           field_description=field_description)
            except:
                return render_template('bloomberg_crawler.html',
                           file_info=file_info,
                           status='Have an error!',
                           field_description=field_description)
        elif request.form.get('action', '') == 'download':
            with open(result_file) as fo:
                file_contents = fo.read()
            response = make_response(file_contents)
            response.headers["Content-Disposition"] = "attachment; filename=%s" % (result_file_name)
            return response

    return render_template('bloomberg_crawler.html',
                           file_info=file_info,
                           status='',
                           field_description=field_description)


@flask_app.route('/check_ner_duplication', methods = ['GET', 'POST'])
def check_ner_duplication():
    date = time.strftime('%Y-%m-%d')
    result_file_name = '%s_check_ner_duplication.csv' % date
    result_file = os.path.join(flask_app.config['UPLOAD_FOLDER'], result_file_name)
    field_description = [
        ['new_id', 'id (i.e. 1, 2, 3)'],
        ['name', 'Company name (i.e. 3M Company)'],
        ['www', 'URL (i.e. http://www.abbvie.com)'],
        ['root_ticker', 'Root ticker (i.e. MMM)'],
        ['exchange', 'Exchange Name (i.e. New York Stock Exchange)'],
        ['address', 'Country Code (i.e. 1 N Waukegan Rd NORTH CHICAGO, IL 60064-1802 United States)'],
        ['executives', 'Executives (i.e. Richard A. Gonzalez\nWilliam J. Chase)'],
        ['category', 'Category to match (i.e. O/Media/Publishing)']
    ]

    file_info = ''
    if os.path.isfile(result_file):
        file_info = 'Completed at: %s' % (datetime.fromtimestamp(os.stat(result_file).st_mtime) + timedelta(hours=7))

    if request.method == 'POST':
        file = request.files.get('file', None)
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(flask_app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            try:
                if file_info:
                    # os.remove(result_file)
                    file_info = ''

                num_row = my_helper.len_dataframe(file_path)
                est_time_process = 1*num_row + 30
                est_time_str = '%s mins' % (int(est_time_process/60))

                process = Process(target=submit_matching_tasks.match_new_named_entities, args=(file_path, result_file))
                process.start()
                return render_template('check_ner_duplication.html',
                           file_info=file_info,
                           status='<br> Processing<br><h4 style= "color: green">It takes about %s</h4>' %(est_time_str),
                           field_description=field_description)
            except:
                return render_template('check_ner_duplication.html',
                           file_info=file_info,
                           status='Have an error!',
                           field_description=field_description)
        elif request.form.get('action', '') == 'download':
            with open(result_file) as fo:
                file_contents = fo.read()
            response = make_response(file_contents)
            response.headers["Content-Disposition"] = "attachment; filename=%s" % (result_file_name)
            return response

    return render_template('check_ner_duplication.html',
                           file_info=file_info,
                           status='',
                           field_description=field_description)


@flask_app.route('/check_financial_terms', methods = ['GET', 'POST'])
def check_ft():
    date = time.strftime('%Y-%m-%d')
    result_file_name = '%s_check_financial_terms.csv' % date
    result_file = os.path.join(flask_app.config['UPLOAD_FOLDER'], result_file_name)
    field_description = [['dup_id', 'Group duplication id'], ['instance', 'Instance'], ['alias_en', 'Alias English'],
                         ['alias_de', 'Alias German']]

    file_info = ''
    if os.path.isfile(result_file):
        file_info = 'Completed at: %s' % (datetime.fromtimestamp(os.stat(result_file).st_mtime) + timedelta(hours=7))

    if request.method == 'POST':
        file = request.files.get('file', None)
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(flask_app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            try:
                if file_info:
                    # os.remove(result_file)
                    file_info = ''

                num_row = my_helper.len_dataframe(file_path)
                est_time_process = 0.5*num_row + 30
                est_time_str = '%s mins' % (int(est_time_process/60))

                process = Process(target=check_financial_terms.check_financial_terms, args=(file_path, result_file))
                process.start()
                return render_template('check_financial_terms.html',
                           file_info=file_info,
                           status='<br> Processing<br><h4 style= "color: green">It takes about %s</h4>' %(est_time_str),
                           field_description=field_description)
            except:
                return render_template('check_financial_terms.html',
                           file_info=file_info,
                           status='Have an error!',
                           field_description=field_description)
        elif request.form.get('action', '') == 'download':
            with open(result_file) as fo:
                file_contents = fo.read()
            response = make_response(file_contents)
            response.headers["Content-Disposition"] = "attachment; filename=%s" % (result_file_name)
            return response

    return render_template('check_financial_terms.html',
                           file_info=file_info,
                           status='',
                           field_description=field_description)


@flask_app.route('/count_messages', methods=['GET', 'POST'])
def count_messages():
    result_file_name = 'counted_messages.csv'
    result_file = os.path.join(flask_app.config['UPLOAD_FOLDER'], result_file_name)
    field_description = [['id', 'Item id']]

    file_info = ''
    if os.path.isfile(result_file):
        file_info = 'Completed at: %s' % (datetime.fromtimestamp(os.stat(result_file).st_mtime) + timedelta(hours=7))

    if request.method == 'POST':
        file = request.files.get('file', None)
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(flask_app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            try:
                if file_info:
                    # os.remove(result_file)
                    file_info = ''

                num_row = my_helper.len_dataframe(file_path)
                est_time_process = 3.96*num_row + 30
                est_time_str = '%s mins' % (int(est_time_process/60))

                count_release_messages = CountReleaseMessages()
                process = Process(target=count_release_messages.from_csv, args=(file_path, result_file, ))
                process.start()
                return render_template('count_messages.html',
                           file_info=file_info,
                           status='Processing<br><h4 style= "color: green">It take about %s</h4>' %(est_time_str),
                           field_description=field_description)
            except Exception as e:
                logger.error('Something error: %s' % e)
                return render_template('count_messages.html',
                           file_info=file_info,
                           status='Have an error!',
                           field_description=field_description)
        elif request.form.get('action', '') == 'download':
            with open(result_file) as fo:
                file_contents = fo.read()
            response = make_response(file_contents)
            response.headers["Content-Disposition"] = "attachment; filename=%s" % (result_file_name)
            return response

    return render_template('count_messages.html',
                           file_info=file_info,
                           status='',
                           field_description=field_description)


@flask_app.route('/quality_keywords', methods=['GET', 'POST'])
def quality_keywords():
    result_file_name = 'qualitied_keywords.csv'
    result_file = os.path.join(flask_app.config['UPLOAD_FOLDER'], result_file_name)
    field_description = [['keyword', 'Keyword need to be quality'],
                         ['item_id', 'Item id of keyword'],
                         ['legalname', 'Legal name'],
                         ['msg_hit_with_and', 'Num msg WITH and'],
                         ['msg_hit_without_and', 'Num msg WITHOUT and']]

    file_info = ''
    if os.path.isfile(result_file):
        file_info = 'Completed at: %s' % (datetime.fromtimestamp(os.stat(result_file).st_mtime) + timedelta(hours=7))

    if request.method == 'POST':
        file = request.files.get('file', None)
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(flask_app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            try:
                if file_info:
                    # os.remove(result_file)
                    file_info = ''
                num_row = my_helper.len_dataframe(file_path)
                est_time_process = 0.005*num_row + 60
                est_time_str = '%s mins' % (int(est_time_process/60))

                quality = CheckQuality()
                process = Process(target=quality.process, args=(file_path, result_file, ))
                process.start()
                return render_template('quality_keywords.html',
                           file_info=file_info,
                           status='Processing<br><h4 style= "color: green">It take about %s</h4>' %(est_time_str),
                           field_description=field_description)
            except Exception as e:
                logger.error('Something error: %s' % e)
                return render_template('quality_keywords.html',
                           file_info=file_info,
                           status='Have an error!',
                           field_description=field_description)
        elif request.form.get('action', '') == 'download':
            with open(result_file) as fo:
                file_contents = fo.read()
            response = make_response(file_contents)
            response.headers["Content-Disposition"] = "attachment; filename=%s" % (result_file_name)
            return response

    return render_template('quality_keywords.html',
                           file_info=file_info,
                           status='',
                           field_description=field_description)


@flask_app.route('/wrong_legalnames', methods=['GET', 'POST'])
def wrong_legalnames():
    result_file_name = 'wrong_legalname.csv'
    result_file = os.path.join(flask_app.config['UPLOAD_FOLDER'], result_file_name)
    field_description = [['name', 'Name'],
                         ['legalname', 'Legal name']]

    file_info = ''
    if os.path.isfile(result_file):
        file_info = 'Completed at: %s' % (datetime.fromtimestamp(os.stat(result_file).st_mtime) + timedelta(hours=7))

    if request.method == 'POST':
        file = request.files.get('file', None)
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(flask_app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            try:
                if file_info:
                    # os.remove(result_file)
                    file_info = ''
                num_row = my_helper.len_dataframe(file_path)
                est_time_process = 0.001*num_row + 30
                est_time_str = '%s mins' % (int(est_time_process/60))

                check_wrong_legalname = WrongLegalnameGenerate()
                process = Process(target=check_wrong_legalname.process, args=(file_path, result_file, ))
                process.start()
                return render_template('wrong_legalnames.html',
                           file_info=file_info,
                           status='Processing<br><h4 style= "color: green">It take about %s</h4>' %(est_time_str),
                           field_description=field_description)
            except Exception as e:
                logger.error('Something error: %s' % e)
                return render_template('wrong_legalnames.html',
                           file_info=file_info,
                           status='Have an error!',
                           field_description=field_description)
        elif request.form.get('action', '') == 'download':
            with open(result_file) as fo:
                file_contents = fo.read()
            response = make_response(file_contents)
            response.headers["Content-Disposition"] = "attachment; filename=%s" % (result_file_name)
            return response

    return render_template('wrong_legalnames.html',
                           file_info=file_info,
                           status='',
                           field_description=field_description)


@flask_app.route('/mixed_box', methods=['GET', 'POST'])
def mixed_box():
    result_file_name = 'mixed_box.csv'
    result_file = os.path.join(flask_app.config['UPLOAD_FOLDER'], result_file_name)
    field_description = [['id', 'Item id']]

    file_info = ''
    if os.path.isfile(result_file):
        file_info = 'Completed at: %s' % (datetime.fromtimestamp(os.stat(result_file).st_mtime) + timedelta(hours=7))

    if request.method == 'POST':
        file = request.files.get('file', None)
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(flask_app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            try:
                if file_info:
                    # os.remove(result_file)
                    file_info = ''
                num_row = my_helper.len_dataframe(file_path)
                est_time_process = 0.9*num_row + 30
                est_time_str = '%s mins' % (int(est_time_process/60))

                mixed_box_pro = MixedBoxCounting()
                process = Process(target=mixed_box_pro.process, args=(file_path, result_file, ))
                process.start()
                return render_template('wrong_legalnames.html',
                           file_info=file_info,
                           status='Processing<br><h4 style= "color: green">It take about %s</h4>' %(est_time_str),
                           field_description=field_description)
            except Exception as e:
                logger.error('Something error: %s' % e)
                return render_template('wrong_legalnames.html',
                           file_info=file_info,
                           status='Have an error!',
                           field_description=field_description)
        elif request.form.get('action', '') == 'download':
            with open(result_file) as fo:
                file_contents = fo.read()
            response = make_response(file_contents)
            response.headers["Content-Disposition"] = "attachment; filename=%s" % (result_file_name)
            return response

    return render_template('wrong_legalnames.html',
                           file_info=file_info,
                           status='',
                           field_description=field_description)


@flask_app.route('/following_score', methods=['GET', 'POST'])
def following_score():
    if request.method == 'GET':
        return render_template('following_score.html')
    else:
        try:
            user_id = int(request.form.get('user_id'))
            get_score = GetScoresFollowing()
            following_score = get_score.process(str(user_id))
            get_score.close_connection()
            if not following_score:
                return render_template('following_score.html')
        except:
            return render_template('following_score.html')
        return render_template('_following_score.html',
                               input_id=user_id,
                               following_score=following_score)