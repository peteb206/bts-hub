from app import db
from datetime import datetime, timedelta

def get_plot_data(plot_type, date):
    out = list()
    if plot_type == 'seasonSummary':
        out  = list(db.get_db()['atBats'].aggregate([
            {
                '$match': {
                    'gameDateTimeUTC': {
                        '$gte': datetime(date.year, 1, 1),
                        '$lte': date + timedelta(hours=5)
                    }
                }
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
                    'as': 'games'
                }
            }, {
                '$unwind': '$games'
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
                        'gamePk': '$gamePk'
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
                    },
                    'homeAway': {
                        '$first': {
                            '$cond': [
                                '$inningBottomFlag',
                                'Home',
                                'Away'
                            ]
                        }
                    },
                    'gameTime': {
                        '$first': {
                            '$cond': [
                                '$games.dayGameFlag',
                                'Day',
                                'Night'
                            ]
                        }
                    },
                    'battingOrderIndex': {
                        '$max': {
                            '$indexOfArray': [
                                {
                                    '$cond': [
                                        '$inningBottomFlag',
                                        '$games.homeLineup',
                                        '$games.awayLineup'
                                    ]
                                },
                                '$batterId'
                            ]
                        }
                    }
                }
            }, {
                '$match': {
                    'battingOrderIndex': {
                        '$gte': 0
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'homeAway': '$homeAway',
                        'gameTime': '$gameTime',
                        'battingOrderIndex': '$battingOrderIndex',
                        'PA': '$PA'
                    },
                    'xH': {
                        '$avg': '$xH'
                    },
                    'H': {
                        '$avg': '$H'
                    },
                    'BIP': {
                        '$avg': '$BIP'
                    },
                    'HG': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$gte': [
                                        '$H',
                                        1
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    },
                    'G': {
                        '$sum': 1
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'gameTime': '$_id.gameTime',
                    'homeAway': '$_id.homeAway',
                    'lineupSlot': {
                        '$toString': {
                            '$add': [
                                '$_id.battingOrderIndex',
                                1
                            ]
                        }
                    },
                    'PA': '$_id.PA',
                    'xH': {
                        '$round': [
                            '$xH',
                            2
                        ]
                    },
                    'H': {
                        '$round': [
                            '$H',
                            2
                        ]
                    },
                    'BIP': {
                        '$round': [
                            '$BIP',
                            2
                        ]
                    },
                    'H %': {
                        '$round': [
                            {
                                '$divide': [
                                    '$HG',
                                    '$G'
                                ]
                            },
                            4
                        ]
                    },
                    'G': '$G'
                }
            }, {
                '$sort': {
                    'homeAway': 1,
                    'gameTime': 1,
                    'lineupSlot': 1,
                    'PA': -1
                }
            }
        ]))
    return out