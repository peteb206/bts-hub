from app import db
import pandas as pd
from datetime import datetime, timedelta

def get_plot_data(plot_type, date):
    if plot_type == 'splitSummary':
        player_game_agg_list  = list(db.get_db()['atBats'].aggregate([
            {
                '$match': {
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
                        'batterId': '$batterId',
                        'gamePk': '$gamePk',
                        'gameDateTimeUTC': '$gameDateTimeUTC',
                        'inningBottomFlag': '$inningBottomFlag'
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
                    'as': 'games'
                }
            }, {
                '$unwind': '$games'
            }, {
                '$lookup': {
                    'from': 'teams',
                    'let': {
                        'pitchingTeamId': {
                            '$cond': [
                                '$_id.inningBottomFlag',
                                '$games.awayTeamId',
                                '$games.homeTeamId'
                            ]
                        },
                        'year': {
                            '$year': '$_id.gameDateTimeUTC'
                        }
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$teamId',
                                                '$$pitchingTeamId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'teamId': '$teamId',
                                'teamAbbreviation': '$teamAbbreviation'
                            }
                        }
                    ],
                    'as': 'pitchingTeam'
                }
            }, {
                '$lookup': {
                    'from': 'teams',
                    'let': {
                        'battingTeamId': {
                            '$cond': [
                                '$_id.inningBottomFlag',
                                '$games.homeTeamId',
                                '$games.awayTeamId'
                            ]
                        },
                        'year': {
                            '$year': '$_id.gameDateTimeUTC'
                        }
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$teamId',
                                                '$$battingTeamId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'teamId': '$teamId',
                                'teamAbbreviation': '$teamAbbreviation'
                            }
                        }
                    ],
                    'as': 'battingTeam'
                }
            }, {
                '$unwind': '$pitchingTeam'
            }, {
                '$unwind': '$battingTeam'
            }, {
                '$project': {
                    '_id': 0,
                    'battingTeam': '$battingTeam.teamAbbreviation',
                    'pitchingTeam': '$pitchingTeam.teamAbbreviation',
                    'PA': '$PA',
                    'xH': '$xH',
                    'H': '$H',
                    'BIP': '$BIP',
                    'H %': {
                        '$cond': [
                            '$H',
                            1,
                            0
                        ]
                    },
                    'homeAway': {
                        '$cond': [
                            '$_id.inningBottomFlag',
                            'Home',
                            'Away'
                        ]
                    },
                    'gameTime': {
                        '$cond': [
                            '$games.dayGameFlag',
                            'Day',
                            'Night'
                        ]
                    },
                    'lineupSlot': {
                        '$add': [
                            {
                                '$indexOfArray': [
                                    {
                                        '$cond': [
                                            '$_id.inningBottomFlag',
                                            '$games.homeLineup',
                                            '$games.awayLineup'
                                        ]
                                    },
                                    '$_id.batterId'
                                ]
                            },
                            1
                        ]
                    }
                }
            }, {
                '$match': {
                    'lineupSlot': {
                        '$gt': 0
                    }
                }
            }
        ]))
        player_game_agg_df = pd.DataFrame(player_game_agg_list)
        return {
            'lineupSlot': player_game_agg_df.groupby('lineupSlot')[['PA', 'xH', 'H', 'BIP', 'H %']].mean().round(2).to_dict('index'),
            'homeAway': player_game_agg_df.groupby('homeAway')[['PA', 'xH', 'H', 'BIP', 'H %']].mean().round(2).to_dict('index'),
            'gameTime': player_game_agg_df.groupby('gameTime')[['PA', 'xH', 'H', 'BIP', 'H %']].mean().round(2).to_dict('index'),
            'PA': player_game_agg_df.groupby('PA')[['H %']].mean().round(2).to_dict('index'),
            'battingTeam': player_game_agg_df.groupby('battingTeam')[['PA', 'xH', 'H', 'BIP', 'H %']].mean().round(2).to_dict('index'),
            'pitchingTeam': player_game_agg_df.groupby('pitchingTeam')[['PA', 'xH', 'H', 'BIP', 'H %']].mean().round(2).to_dict('index')
        }