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

let loadDashboard = function () {

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
                title: 'Lineup Slot'
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
        $('table#eligibleBatters').attr('current-player', playerId);
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
                    if (attribute == 'fangraphsId') {
                        // Daily Projection
                        $.ajax({
                            type: 'GET',
                            url: '/dailyProjections/' + playerData.fangraphsId + $(location).attr('search') + '&type=' + (viewType == 'pitcher' ? 'P' : 'OF'),
                            dataType: 'json',
                            success: function (json) {
                                $('span#fangraphsProjection').text('Fangraphs Proj.: ' + json.data);
                            }
                        });
                    } else {
                        var attributeProperCase = attribute.charAt(0).toUpperCase() + attribute.substring(1);
                        $('#player' + attributeProperCase).text((attributeProperCase == 'Name' ? '' : attributeProperCase + ': ') + playerData[attribute]);
                    }
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
        removeClass($('div.selectedPlayer'), 'hidden');
    } else {
        $(anchor).find('i.fa-arrow-circle-down').each(function () {
            $(this).removeClass('fa-arrow-circle-down')
                .addClass('fa-arrow-circle-right');
        });
        addClass($('div.selectedPlayer'), 'hidden');
    }
}

// Dashboard graphs
let showMainDashboard = function (anchor) {
    $('div#dashboardTabs > ul > li > a').removeClass('active');
    $(anchor).addClass('active');
    addClass($('div#splitSummary'), 'hidden');
    removeClass($('div#mainDashboard'), 'hidden');
}

let showSummary = function (anchor) {
    $('div#dashboardTabs > ul > li > a').removeClass('active');
    $(anchor).addClass('active');
    addClass($('div#mainDashboard'), 'hidden');
    removeClass($('div#splitSummary'), 'hidden');
    addClass($('#content'), 'hidden');
    removeClass($('#spinnerDiv'), 'hidden');
    if ($('div#splitSummary').find('svg').length == 0) {
        $.ajax({
            type: 'GET',
            url: '/plotly/splitSummary' + $(location).attr('search'),
            dataType: 'json',
            success: function (json) {
                addClass($('#spinnerDiv'), 'hidden');
                removeClass($('#content'), 'hidden');

                var data= json.data;
                createBarGraph('hitPctByLineup', data, 'lineupSlot', ['H %'], 'Hit % by Lineup Slot');
                createBarGraph('otherStatsByLineup', data, 'lineupSlot', ['xH', 'BIP', 'PA'], 'Lineup Slot Breakdown');
                createBarGraph('hitPctByPAs', data, 'PA', ['H %'], 'Hit % by # of PAs');
                createBarGraph('hitPctByHomeAway', data, 'homeAway', ['H %'], 'Hit % by Home/Away');
                createBarGraph('otherStatsByHomeAway', data, 'homeAway', ['xH', 'BIP', 'PA'], 'Home/Away Breakdown');
                createBarGraph('hitPctByDayNight', data, 'gameTime', ['H %'], 'Hit % by Day/Night');
                createBarGraph('otherStatsByDayNight', data, 'gameTime', ['xH', 'BIP', 'PA'], 'Day/Night Breakdown');
                createBarGraph('hitPctByBattingTeam', data, 'battingTeam', ['H %'], 'Hit % by Batting Team');
                createBarGraph('otherStatsByBattingTeam', data, 'battingTeam', ['xH', 'BIP', 'PA'], 'Batting Team Breakdown');
                createBarGraph('hitPctByPitchingTeam', data, 'pitchingTeam', ['H %'], 'Hit % by Pitching Team');
                createBarGraph('otherStatsByPitchingTeam', data, 'pitchingTeam', ['xH', 'BIP', 'PA'], 'Pitching Team Breakdown');
            }
        });
    } else {
        addClass($('#spinnerDiv'), 'hidden');
        removeClass($('#content'), 'hidden');
    }
}

let createBarGraph = function (targetDiv, data, group, stats, title) {
    var plotData = [];
    for (var i = 0; i < stats.length; i++) {
        var stat = stats[i];
        var groupValues = [];
        var statValues = [];
        var labels = [];
        for (const [groupValue, groupValueStats] of Object.entries(data[group])) {
            groupValues.push(groupValue);
            var value = groupValueStats[stat];
            labels.push(value);
            if (stat == 'BIP')
                value -= groupValueStats.xH;
            else if (stat == 'PA')
                value -= groupValueStats.BIP;
            statValues.push(value);
        }
        plotData.push({
            x: groupValues,
            y: statValues,
            name: stat,
            type: 'bar',
            // orientation: 'h',
            text: labels,
            textposition: 'inside',
            insidetextanchor: 'middle',
            textfont: {
                color: 'white'
            },
            hoverinfo: 'none'
        });
    }

    var plotLayout = {
        barmode: 'stack',
        title: {
            text: title
        },
        showlegend: group == 'lineupSlot' && stats.length > 1,
        legend: {
            bgcolor: '#BEBEBE',
            x: 1,
            xanchor: 'right',
            y: 1
        },
        yaxis: {
            range: [0, stats.length === 1 ? 1 : 5],
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
        height: 300,
        margin: {
            l: 30,
            r: 30,
            b: 30,
            t: 30,
            pad: 3
        },
        paper_bgcolor: 'rgba(0, 0, 0, 0)',
        plot_bgcolor: 'rgba(0, 0, 0, 0)'
    }

    var plotConfig = {
        displayModeBar: false
    };

    Plotly.newPlot(targetDiv, plotData, plotLayout, plotConfig);
}