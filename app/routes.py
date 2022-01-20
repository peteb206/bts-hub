from app import app
from flask import jsonify, render_template, request, redirect
import datetime

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
####################################
####### End JSON Endpoints #########
####################################