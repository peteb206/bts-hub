import os
from app import app, db
from flask import jsonify, render_template, request, redirect, send_from_directory
from datetime import datetime, timedelta
import pandas as pd
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


@app.route('/playerDetails/<player_id>', methods=['GET'])
def player_view(player_id):
    query_parameters_dict = utils.parse_request_arguments(request.args)
    date = datetime.strptime(query_parameters_dict['date'], '%Y-%m-%d')

    player_data_dict = list(db.get_db()['players'].aggregate([
        {
            '$match': {
                'playerId': int(player_id),
                'year': date.year
            }
        }, {
            '$lookup': {
                'from': 'teams',
                'let': {
                    'teamId': '$teamId'
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$teamId',
                                            '$$teamId'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$year',
                                            date.year
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                ],
                'as': 'team'
            }
        }, {
            '$unwind': '$team'
        }, {
            '$project': {
                '_id': 0,
                'name': '$playerName',
                'team': '$team.teamAbbreviation',
                'position': '$position',
                'bats': '$bats',
                'throws': '$throws',
                'injured': '$injuredFlag'
            }
        }
    ]))
    return jsonify({'data': player_data_dict[0]})


@app.route('/summaryStats/<stat_type>/<player_id>', methods=['GET'])
def player_stats(stat_type, player_id):
    query_parameters_dict = utils.parse_request_arguments(request.args)
    date = datetime.strptime(query_parameters_dict['date'], '%Y-%m-%d')
    summary_stats = dict()

    if stat_type == 'batter':
        at_bat_details_df = pd.DataFrame(list(db.get_db()['atBats'].aggregate([
            {
                '$match': {
                    'batterId': int(player_id),
                    'gameDateTimeUTC': {
                        '$gte': datetime(date.year, 1, 1),
                        '$lte': date + timedelta(hours=5)
                    }
                }
            }, {
                '$lookup': {
                    'from': 'eventTypes',
                    'let': {
                        'eventTypeId': '$eventTypeId'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$eventTypeId',
                                        '$$eventTypeId'
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'event'
                }
            }, {
                '$unwind': '$event'
            }, {
                '$lookup': {
                    'from': 'games',
                    'let': {
                        'gamePk': '$gamePk',
                        'gameDateTimeUTC': '$gameDateTimeUTC'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$gamePk',
                                                '$$gamePk'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$gameDateTimeUTC',
                                                '$$gameDateTimeUTC'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'game'
                }
            }, {
                '$unwind': '$game'
            }, {
                '$project': {
                    '_id': 0,
                    'gamePk': '$gamePk',
                    'gameDateTimeUTC': '$gameDateTimeUTC',
                    'rightHandedBatterFlag': '$rightHandedBatterFlag',
                    'rightHandedPitcherFlag': '$rightHandedPitcherFlag',
                    'inningBottomFlag': '$inningBottomFlag',
                    'hits': {
                        '$cond': [
                            '$event.hitFlag',
                            1,
                            0
                        ]
                    },
                    'balls_in_play': {
                        '$cond': [
                            '$event.inPlayFlag',
                            1,
                            0
                        ]
                    },
                    'statcast': '$game.statcastFlag'
                }
            }
        ])))
        print(at_bat_details_df.head())
    return jsonify({'data': summary_stats})


@app.route('/gameLogs/<stat_type>/<player_id>', methods=['GET'])
def game_logs(stat_type, player_id):
    query_parameters_dict = utils.parse_request_arguments(request.args)
    date, recent_games = datetime.strptime(query_parameters_dict['date'], '%Y-%m-%d'), list()

    if stat_type == 'batter':
        recent_games = list(db.get_db()['atBats'].aggregate([
            {
                '$match': {
                    'batterId': int(player_id),
                    'gameDateTimeUTC': {
                        '$gte': datetime(date.year, 1, 1),
                        '$lte': date + timedelta(hours=5)
                    }
                }
            }, {
                '$lookup': {
                    'from': 'eventTypes',
                    'let': {
                        'eventTypeId': '$eventTypeId'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$eventTypeId',
                                        '$$eventTypeId'
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'event'
                }
            }, {
                '$unwind': '$event'
            }, {
                '$group': {
                    '_id': {
                        'batter': '$batterId',
                        'gamePk': '$gamePk',
                        'gameDateTimeUTC': '$gameDateTimeUTC'
                    },
                    'PA': {
                        '$sum': 1
                    },
                    'xH': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$gte': [
                                        '$xBA',
                                        0
                                    ]
                                },
                                '$xBA',
                                0
                            ]
                        }
                    },
                    'H': {
                        '$sum': {
                            '$cond': [
                                '$event.hitFlag',
                                1,
                                0
                            ]
                        }
                    },
                    'BIP': {
                        '$sum': {
                            '$cond': [
                                '$event.inPlayFlag',
                                1,
                                0
                            ]
                        }
                    }
                }
            }, {
                '$lookup': {
                    'from': 'games',
                    'let': {
                        'gamePk': '$_id.gamePk',
                        'gameDateTimeUTC': '$_id.gameDateTimeUTC'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$gamePk',
                                                '$$gamePk'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$gameDateTimeUTC',
                                                '$$gameDateTimeUTC'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'game'
                }
            }, {
                '$unwind': '$game'
            }, {
                '$project': {
                    '_id': 0,
                    'date': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': '$_id.gameDateTimeUTC',
                            'timezone': '-05:00'
                        }
                    },
                    'pa': '$PA',
                    'bip': '$BIP',
                    'xH': {
                        '$round': [
                            '$xH',
                            3
                        ]
                    },
                    'h': '$H',
                    'order': {
                        '$add': [
                            {
                                '$max': [
                                    {
                                        '$indexOfArray': [
                                            '$game.awayLineup',
                                            '$_id.batter'
                                        ]
                                    }, {
                                        '$indexOfArray': [
                                            '$game.homeLineup',
                                            '$_id.batter'
                                        ]
                                    }
                                ]
                            },
                            1
                        ]
                    }
                }
            }
        ]))
    elif stat_type == 'pitcher':
        recent_games = list(db.get_db()['atBats'].aggregate([
            {
                '$match': {
                    'pitcherId': int(player_id),
                    'gameDateTimeUTC': {
                        '$gte': datetime(date.year, 1, 1),
                        '$lte': date
                    }
                }
            }, {
                '$lookup': {
                    'from': 'eventTypes',
                    'let': {
                        'eventTypeId': '$eventTypeId'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$eventTypeId',
                                        '$$eventTypeId'
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'event'
                }
            }, {
                '$unwind': '$event'
            }, {
                '$group': {
                    '_id': {
                        'pitcher': '$pitcherId',
                        'gamePk': '$gamePk',
                        'gameDateTimeUTC': '$gameDateTimeUTC'
                    },
                    'BF': {
                        '$sum': 1
                    },
                    'xH': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$gte': [
                                        '$xBA',
                                        0
                                    ]
                                },
                                '$xBA',
                                0
                            ]
                        }
                    },
                    'H': {
                        '$sum': {
                            '$cond': [
                                '$event.hitFlag',
                                1,
                                0
                            ]
                        }
                    },
                    'BIP': {
                        '$sum': {
                            '$cond': [
                                '$event.inPlayFlag',
                                1,
                                0
                            ]
                        }
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'date': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': '$_id.gameDateTimeUTC',
                            'timezone': '-05:00'
                        }
                    },
                    'bf': '$BF',
                    'bip': '$BIP',
                    'xH': {
                        '$round': [
                            {
                                '$divide': [
                                    '$xH',
                                    '$BF'
                                ]
                            },
                            3
                        ]
                    },
                    'h': {
                        '$round': [
                            {
                                '$divide': [
                                    '$H',
                                    '$BF'
                                ]
                            },
                            3
                        ]
                    }
                }
            }
        ]))
    return jsonify({'data': recent_games})


@app.route('/<path>', methods=['GET'])
def render_page(path):
    query_parameters_dict = utils.parse_request_arguments(request.args)
    if (path == 'dashboard') & ('date' not in query_parameters_dict.keys()):
        return redirect(f'/dashboard?date={db.get_available_dates(max_min="max")}')
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
        date_string_as_datetime = datetime.strptime(query_parameters_dict['date'], '%Y-%m-%d')
        query_parameters_dict['date'] = date_string_as_datetime
        filter_values = date_string_as_datetime
        if path == 'dashboard':
            db.update_collection('games')
    elif ('startDate' in query_parameters) | ('endDate' in query_parameters):
        filter_types.append('date_range')
        filter_values = list()
        collection_columns = db.collection_columns(path)['columns']
        collection_column_names = list(collection_columns.keys())
        for date_boundary in ['startDate', 'endDate']:
            if date_boundary in query_parameters:
                date_boundary_value = datetime.strptime(query_parameters_dict[date_boundary], '%Y-%m-%d') + timedelta(hours=5)
                filter_values.append(date_boundary_value)
                for column in collection_column_names:
                    if collection_columns[column] == 'datetime':
                        operator = '$gte'
                        if date_boundary == 'endDate':
                            operator = '$lte'
                            date_boundary_value = date_boundary_value + timedelta(hours=24)
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
                        '$gte': datetime(query_parameters_dict['year'], 1, 1),
                        '$lte': datetime(query_parameters_dict['year'], 12, 31)
                    }
                    del query_parameters_dict['year']
    return render_template(
        'content.html', # f'{path}.html'
        current_path=path,
        filters_html=html_utils.filters_html(filter_types, filter_values),
        content_html=html_utils.display_html(db, path, filters=query_parameters_dict)
    )
####################################
######### End HTML Pages ###########
####################################

####################################
######### JSON Endpoints ###########
####################################
@app.route('/data/availableDates')
def available_dates():
    return jsonify({'data': db.get_available_dates()})


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