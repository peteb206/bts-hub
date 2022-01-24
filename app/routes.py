from numpy import where
from app import app, db
from flask import jsonify, render_template, request, redirect
import datetime
from app.utils import parse_request_arguments, sidebar_links

####################################
########### HTML Pages #############
####################################
@app.route('/')
def redirect_to_home():
    return redirect('/dashboard')


@app.route('/dashboard')
def dashboard():
    date_arg = request.args.get('date')
    if date_arg:
        collapse_sidebar = request.cookies.get('collapseSidebar') == 'true'
        page = request.path[1:]
        return render_template(
            'dashboard.html',
            collapse_sidebar=collapse_sidebar,
            loading_text=f'Loading {page}...',
            sidebar_links_html=sidebar_links(db, request.path, collapse_sidebar)
        )
    else:
        date_string = (datetime.datetime.now() - datetime.timedelta(hours=5)).strftime('%Y-%m-%d')
        return redirect(f'/dashboard?date={date_string}')


# @app.route('/picks')
# @app.route('/leaderboard')
# @app.route('/simulations')
@app.route('/games')
@app.route('/players')
@app.route('/teams')
@app.route('/stadiums')
def content():
    collapse_sidebar, page, where_dict = request.cookies.get('collapseSidebar') == 'true', request.path[1:], parse_request_arguments(request.args)
    if 'year' in where_dict.keys():
        collection_columns = db.collection_columns(page)['columns']
        collection_column_names = list(collection_columns.keys())
        if 'year' not in collection_column_names:
            for column in collection_column_names:
                if collection_columns[column] == 'datetime':
                    where_dict[column] = {
                        '$gte': datetime.datetime(where_dict['year'], 1, 1),
                        '$lte': datetime.datetime(where_dict['year'], 12, 31)
                    }
                    del where_dict['year']
                    print(where_dict)
    return render_template(
        'content.html',
        collapse_sidebar=collapse_sidebar,
        loading_text=f'Loading {page}...',
        sidebar_links_html=sidebar_links(db, request.path, collapse_sidebar),
        content_html=db.read_collection_to_html_table(page, where_dict=where_dict)
    )


@app.route('/links')
def links():
    return render_template('links.html')
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