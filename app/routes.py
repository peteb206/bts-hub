import os
from app import app, db
from flask import jsonify, render_template, request, redirect, send_from_directory
import datetime
import app.utils as utils
import app.html_utils as html_utils

####################################
########### HTML Pages #############
####################################
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static/img'), 'favicon.png', mimetype='image/vnd.microsoft.icon')


@app.route('/', methods=['GET'])
def base():
    return redirect('/dashboard')


@app.route('/<path>', methods=['GET'])
def render_page(path):
    query_parameters_dict = utils.parse_request_arguments(request.args)
    if (path == 'dashboard') & ('date' not in query_parameters_dict.keys()):
        return redirect(f'/dashboard?date={utils.get_available_dates(db, max_min="max")}')
    collapse_sidebar = request.cookies.get('collapseSidebar') == 'true'
    return render_template(
        'base.html',
        collapse_sidebar=collapse_sidebar,
        loading_text=f'Loading {path}...',
        sidebar_links_html=html_utils.sidebar_links_html(db, request.path, collapse_sidebar)
    )


@app.route('/<path>/content', methods=['GET'])
def render_content(path):
    query_parameters_dict = utils.parse_request_arguments(request.args)
    query_parameters = query_parameters_dict.keys()
    filter_types, filter_values = list(), None
    if 'date' in query_parameters:
        filter_types.append('date')
        filter_values = datetime.datetime.strptime(query_parameters_dict['date'], '%Y-%m-%d')
    elif ('startDate' in query_parameters) | ('endDate' in query_parameters):
        filter_types.append('date_range')
        filter_values = list()
        collection_columns = db.collection_columns(path)['columns']
        collection_column_names = list(collection_columns.keys())
        for date_boundary in ['startDate', 'endDate']:
            if date_boundary in query_parameters:
                date_boundary_value = datetime.datetime.strptime(query_parameters_dict[date_boundary], '%Y-%m-%d')
                filter_values.append(date_boundary_value)
                for column in collection_column_names:
                    if collection_columns[column] == 'datetime':
                        operator = '$gte'
                        if date_boundary == 'endDate':
                            operator = '$lte'
                            date_boundary_value = date_boundary_value.replace(hour=23, minute=59)
                        if column not in query_parameters:
                            query_parameters_dict[column] = dict()
                        query_parameters_dict[column][operator] = date_boundary_value
                        del query_parameters_dict[date_boundary]
            else:
                filter_values.append('')
    elif 'year' in query_parameters:
        filter_types.append('year')
        filter_values = query_parameters_dict['year']
        collection_columns = db.collection_columns(path)['columns']
        collection_column_names = list(collection_columns.keys())
        if 'year' not in collection_column_names:
            for column in collection_column_names:
                if collection_columns[column] == 'datetime':
                    query_parameters_dict[column] = {
                        '$gte': datetime.datetime(query_parameters_dict['year'], 1, 1),
                        '$lte': datetime.datetime(query_parameters_dict['year'], 12, 31)
                    }
                    del query_parameters_dict['year']
    content_html = ''
    if path in ['games', 'atBats', 'players', 'teams', 'stadiums']:
        content_html = html_utils.display_html(db, path, filters=query_parameters_dict)
    return render_template(
        'content.html', # f'{path}.html'
        current_path=path,
        filters_html=html_utils.filters_html(filter_types, filter_values),
        content_html=content_html
    )
####################################
######### End HTML Pages ###########
####################################

####################################
######### JSON Endpoints ###########
####################################
@app.route('/data/availableDates')
def available_dates():
    return jsonify({'data': utils.get_available_dates(db)})


@app.route('/data/<collection>')
def data(collection):
    collection_data = db.read_collection_as_list(collection, where_dict=utils.parse_request_arguments(request.args)) if collection in db.get_db().list_collection_names() else list()
    return jsonify({'data': collection_data})


@app.route('/columns', defaults={'collection': None})
@app.route('/columns/<collection>')
def columns(collection):
    collection_columns = list()
    collections = [collection] if collection else db.get_db().list_collection_names()
    for single_collection in collections:
        collection_columns.append(db.collection_columns(single_collection))
    return jsonify({'data': collection_columns})
####################################
####### End JSON Endpoints #########
####################################