let parseCookie = function (cookieToReturn) {
    var cookieObj = {
        'collapseSidebar': 'true'
    };
    var cookies = $(document).attr('cookie').split(';');
    for (var i = 0; i < cookies.length; i++) {
        var cookie = cookies[i].split('=');
        var cookieName = cookie[0].trim();
        var cookieValue = cookie[1].trim();
        cookieObj[cookieName] = cookieValue;
    }
    return cookieObj[cookieToReturn];
}

let formatDatePickerDate = function (date, format) {
    var dateString = '';
    if (format == 'yyyy-mm-dd') {
        year = date.getFullYear();
        month = date.getMonth() + 1;
        day = date.getDate();
        if (month < 10) {
            month = '0' + month;
        }
        if (day < 10) {
            day = '0' + day;
        }
        dateString = [year, month, day].join('-');
    }
    return dateString;
}

let adjustContentToSidebar = function () {
    $('#rightBody').css('margin-left', $('#sidebar').css('max-width'));
}

let addClass = function (element, className) {
    if (!element.hasClass(className)) {
        element.addClass(className);
    }
}

let removeClass = function (element, className) {
    if (element.hasClass(className)) {
        element.removeClass(className);
    }
}

let addTableTitle = function (table, title) {
    var tableTitle = $('<span>')
        .attr('class', 'tableTitle')
        .text(title);
    table.parent().prepend(tableTitle);
}

let dataTablesRowCallback = function (row) {
    $('td', row).each(function () {
        var cellText = $(this).text();
        if (cellText.startsWith('<') && cellText.endsWith('>'))
            $(this).html(cellText);
    });
}

let gameLogsColumns = function (playerType) {
    var columns = [];
    if (playerType == 'batter') {
        columns = [
            {
                data: 'date',
                title: 'Date'
            }, {
                data: 'order',
                title: 'Lineup Spot'
            }, {
                data: 'pa',
                title: 'PA'
            }, {
                data: 'bip',
                title: 'BIP'
            }, {
                data: 'xH',
                title: 'xH'
            }, {
                data: 'h',
                title: 'H'
            }
        ]
    } else if (playerType == 'pitcher') {
        columns = [
            {
                data: 'date',
                title: 'Date'
            }, {
                data: 'bf',
                title: 'BF'
            }, {
                data: 'bip',
                title: 'BIP'
            }, {
                data: 'xH',
                title: 'xH / BF'
            }, {
                data: 'h',
                title: 'H / BF'
            }
        ]
    }
    return columns
}

let playerView = function (anchor, playerId, viewType) {
    if ($(anchor).find('i.fa-arrow-circle-right').length) {
        $('i.fa-arrow-circle-down').removeClass('fa-arrow-circle-down')
            .addClass('fa-arrow-circle-right');
        $('img#playerImage').attr('src', 'https://securea.mlb.com/mlb/images/players/head_shot/' + playerId + '.jpg');

        // Player Info
        $.ajax({
            type: 'GET',
            url: '/playerDetails/' + playerId + $(location).attr('search'),
            dataType: 'json',
            success: function (json) {
                var playerData = json.data;
                Object.keys(playerData).forEach(function (attribute) {
                    var attributeProperCase = attribute.charAt(0).toUpperCase() + attribute.substring(1);
                    $('#player' + attributeProperCase).text((attributeProperCase == 'Name' ? '' : attributeProperCase + ': ') + playerData[attribute]);
                });
            }
        });

        // Key Stats
        $.ajax({
            type: 'GET',
            url: '/summaryStats/' + viewType + '/' + playerId + $(location).attr('search'),
            dataType: 'json',
            success: function (json) {

            }
        });

        // Game Logs
        $('div#playerGameLogs').empty();
        var gameLogsTable = $('<table></table>')
            .attr('id', 'gameLogsTable')
            .attr('class', 'display');
        $('div#playerGameLogs').append(gameLogsTable);
        gameLogsTable = $('table#gameLogsTable');
        gameLogsTable.DataTable({
            ajax: '/gameLogs/' + viewType + '/' + playerId + $(location).attr('search'),
            columns: gameLogsColumns(viewType),
            pageLength: 5,
            lengthChange: false,
            searching: false,
            info: false,
            order: [[0, 'desc']]
        });
        var tableTitle = $('<span>')
            .attr('class', 'tableTitle')
            .text('Recent Games');
        gameLogsTable.parent().prepend(tableTitle);

        $(anchor).find('i')
            .removeClass('fa-arrow-circle-right')
            .addClass('fa-arrow-circle-down');
    }
}

// Dashboard graphs
let showMainDashboard = function (anchor) {
    $('div#dashboardTabs > ul > li > a').removeClass('active');
    $(anchor).addClass('active');
    addClass($('div#seasonSummary'), 'hidden');
    removeClass($('div#mainDashboard'), 'hidden');
}

let showSeasonSummary = function (anchor) {
    $('div#dashboardTabs > ul > li > a').removeClass('active');
    $(anchor).addClass('active');
    addClass($('div#mainDashboard'), 'hidden');
    removeClass($('div#seasonSummary'), 'hidden');
    addClass($('#content'), 'hidden');
    removeClass($('#spinnerDiv'), 'hidden');
    if ($('div#seasonSummary').find('svg').length == 0) {
        $.ajax({
            type: 'GET',
            url: '/plotly/battingOrder' + $(location).attr('search'),
            dataType: 'json',
            success: function (json) {
                var group = [],
                    values = {},
                    labels = {},
                    standardPlotData = [],
                    stackedPlotData = [],
                    stats = ['xH', 'BIP', 'PA'];

                for (var lineupSlot = 0; lineupSlot < json.data.length; lineupSlot++) {
                    var slot = json.data[lineupSlot];
                    group.push(slot.lineupSlot);
                    for (var statNum = 0; statNum < stats.length; statNum++) {
                        var stat = stats[statNum];
                        if (values[stat] === undefined) {
                            values[stat] = [];
                            labels[stat] = [];
                        }
                        var value = slot[stats[statNum]];
                        labels[stat].push(value);
                        if (stat == 'BIP') {
                            value -= slot['xH'];
                        } else if (stat == 'PA') {
                            value -= slot['BIP'];
                        }
                        values[stat].push(value);
                    }
                }

                var hitPctValues = json.data.map(a => (a['H %'] * 100).toFixed(1).toString() + ' %');
                standardPlotData.push({
                    x: group,
                    y: hitPctValues,
                    name: 'H %',
                    type: 'bar',
                    text: hitPctValues,
                    textposition: 'auto',
                    textfont: {
                        color: 'white'
                    },
                    hoverinfo: 'none'
                });
                for (var statNum = 0; statNum < stats.length; statNum++) {
                    var stat = stats[statNum];
                    stackedPlotData.push({
                        x: group,
                        y: values[stat],
                        name: stat,
                        type: 'bar',
                        // orientation: 'h',
                        text: labels[stat],
                        textposition: 'auto',
                        textfont: {
                            color: 'white'
                        },
                        hoverinfo: 'none'
                    });
                }

                var plotLayout = {
                    barmode: 'stack',
                    // title: {
                    //     text: '',
                    //     color: 'var(--main-color)'
                    // },
                    // showlegend: false,
                    legend: {
                        bgcolor: '#BEBEBE',
                        x: 1,
                        xanchor: 'right',
                        y: 1
                    },
                    yaxis: {
                        gridcolor: 'black',
                        gridwidth: 0.5,
                        linecolor: 'black',
                        linewidth: 0.5,
                        mirror: true
                    },
                    xaxis: {
                        text: 'Lineup Slot',
                        type: 'category',
                        // linecolor: 'var(--main-color)',
                        // linewidth: 1,
                        // mirror: true
                    },
                    // autosize: false,
                    // width: 500,
                    // height: 400,
                    // margin: {
                    //     l: 40,
                    //     r: 40,
                    //     b: 40,
                    //     t: 40,
                    //     pad: 4
                    // },
                    paper_bgcolor: 'rgba(0, 0, 0, 0)',
                    plot_bgcolor: 'rgba(0, 0, 0, 0)'
                }

                var plotConfig = {
                    displayModeBar: false
                };

                addClass($('#spinnerDiv'), 'hidden');
                removeClass($('#content'), 'hidden');

                Plotly.newPlot('seasonSummaryPct', standardPlotData, plotLayout, plotConfig);
                // delete plotLayout.title;
                Plotly.newPlot('seasonSummaryOth', stackedPlotData, plotLayout, plotConfig);
            }
        });
    }
}