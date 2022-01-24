from numpy import where
from app import app, db
from flask import jsonify, render_template, request, redirect
import datetime
from app.utils import parse_request_arguments, sidebar_links

####################################
########### HTML Pages #############
####################################
@app.route('/', defaults={'path': 'dashboard'}, methods=['GET'])
@app.route('/<path>', methods=['GET'])
def render_page(path):
    query_parameters_dict = parse_request_arguments(request.args)
    if (path == 'dashboard') & ('date' not in query_parameters_dict.keys()):
        date_string = (datetime.datetime.now() - datetime.timedelta(hours=5)).strftime('%Y-%m-%d')
        return redirect(f'/dashboard?date={date_string}')
    collapse_sidebar = request.cookies.get('collapseSidebar') == 'true'
    return render_template(
        'base.html',
        collapse_sidebar=collapse_sidebar,
        loading_text=f'Loading {path}...',
        sidebar_links_html=sidebar_links(db, request.path, collapse_sidebar)
    )


@app.route('/<path>/content', methods=['GET'])
def render_content(path):
    query_parameters_dict = parse_request_arguments(request.args)
    if 'year' in query_parameters_dict.keys():
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
    return render_template(
        'content.html', # f'{path}.html'
        content_html=db.read_collection_to_html_table(path, where_dict=query_parameters_dict)
    )
####################################
######### End HTML Pages ###########
####################################

####################################
######### JSON Endpoints ###########
####################################
@app.route('/data/<collection>')
def data(collection):
    collection_data = db.read_collection_as_list(collection, where_dict=parse_request_arguments(request.args)) if collection in db.get_db().list_collection_names() else list()
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