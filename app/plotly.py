from app import db
from datetime import datetime, timedelta

def get_plot_data(plot_type, date):
    out = list()
    if plot_type == 'battingOrder':
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
                '$group': {
                    '_id': {
                        'battingOrderIndex': '$battingOrderIndex'
                    },
                    'PA': {
                        '$avg': '$PA'
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
                    'lineupSlot': {
                        '$replaceAll': {
                            'input': {
                                '$toString': {
                                    '$add': [
                                        '$_id.battingOrderIndex',
                                        1
                                    ]
                                }
                            },
                            'find': '0',
                            'replacement': 'Sub'
                        }
                    },
                    'PA': {
                        '$round': [
                            '$PA',
                            2
                        ]
                    },
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
                    }
                }
            }, {
                '$sort': {
                    'lineupSlot': 1
                }
            }
        ]))
    return out