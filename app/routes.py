from app import app
from flask import jsonify, render_template, request, redirect
import datetime
import os
from app.data import BTSHubMongoDB
from app.utils import parseRequestArguments

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
        return render_template('dashboard.html')
    else:
        date_string = (datetime.datetime.now() - datetime.timedelta(hours=5)).strftime('%Y-%m-%d')
        return redirect(f'/dashboard?date={date_string}')


@app.route('/picks')
@app.route('/leaderboard')
@app.route('/simulations')
@app.route('/games')
@app.route('/players')
@app.route('/teams')
@app.route('/stadiums')
def picks():
    return render_template(f'data_view.html')


@app.route('/links')
def links():
    return render_template('links.html')
####################################
######### End HTML Pages ###########
####################################

####################################
######### JSON Endpoints ###########
####################################
@app.route('/loadTableData')
def load_table_data():
    date_arg = request.args.get('date')
    min_hits = int(request.args.get('hitMin'))
    is_today = bool(request.args.get('isToday'))
    from_app = bool(request.args.get('fromApp'))
    return jsonify(main.load_table_data(dates=date_arg, min_hits=min_hits, is_today=is_today, from_app=from_app))


@app.route('/pickHistory')
def pick_history():
    year = int(request.args.get('year'))
    date = request.args.get('date')
    return jsonify(main.pick_history(year=year, date=date))


@app.route('/gameLogs')
def game_logs():
    player_type = request.args.get('type')
    player_id = request.args.get(player_type)
    season = request.args.get('year')
    return jsonify(main.game_logs(player_type=player_type, player_id=player_id, season=season))


@app.route('/data/<collection>')
def data(collection):
    db = BTSHubMongoDB(os.environ.get('DATABASE_CLIENT'), 'bts-hub')
    collection_data = db.read_collection_as_list(collection, where_dict=parseRequestArguments(request.args)) if collection in db.get_db().list_collection_names() else list()
    return jsonify({'data': collection_data})


@app.route('/columns', defaults= {'collection': None})
@app.route('/columns/<collection>')
def columns(collection):
    collection_columns = list()
    db = BTSHubMongoDB(os.environ.get('DATABASE_CLIENT'), 'bts-hub')
    collections = [collection] if collection else db.get_db().list_collection_names()
    for singleCollection in collections:
        one_document = db.get_db()[singleCollection].find_one({}, {'_id': False})
        if one_document:
            columns = list(one_document.keys())
            collection_columns.append({
                'collection': singleCollection,
                'columns': columns
            })
    return jsonify({'data': collection_columns})
####################################
####### End JSON Endpoints #########
####################################