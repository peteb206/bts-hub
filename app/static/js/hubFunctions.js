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

let getIcon = function (weather) {
    var weatherKey = weather.toLowerCase();
    var iconMap =  {
        'clear': 'fa fa-sun',
        'sunny': 'fas fa-sun',
        'partly cloudy': 'fas fa-cloud-sun',
        'cloudy': 'fas fa-cloud',
        'overcast': 'fas fa-cloud',
        'roof closed': 'fas fa-landmark-dome',
        'dome': 'fas fa-landmark-dome'
    }
    var weatherIcon = weather;
    if (iconMap[weatherKey]) {
        weatherIcon = '<span title="' + weather + '"><i class="' + iconMap[weatherKey] + ' weatherIcon"></i></span>';
    }
    return weatherIcon;
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
    if ($('table#todaysGames').length) {
        $.ajax({
            type: 'GET',
            url: 'https://statsapi.mlb.com/api/v1/schedule' + $(location).attr('search') + '&lang=en&sportId=1&hydrate=team,probablePitcher,weather',
            dataType: 'json',
            success: function (json) {
                var gamesData = [];
                var dates = json.dates;
                if (dates.length) {
                    var games = dates[0].games;
                    for (var i = 0; i < games.length; i++) {
                        var game = games[i];
                        var gameDate = new Date(game.gameDate);
                        var gameTime = [gameDate.getHours(), gameDate.getMinutes() < 10 ? '0' + gameDate.getMinutes() : gameDate.getMinutes()].join(':');
                        gamesData.push([
                            gameTime,
                            game.teams.away.team.abbreviation + ' @ ' + game.teams.home.team.abbreviation,
                            game.teams.away.probablePitcher ? '<a href="javascript:void(0)" class="float-left" onclick="playerView(this, ' + game.teams.away.probablePitcher.id + ', \'pitcher\')"><i class="fas fa-arrow-circle-right rowSelectorIcon"></i></a><span class="playerText">' + game.teams.away.probablePitcher.fullName + '</span>' : '',
                            game.teams.home.probablePitcher ? '<a href="javascript:void(0)" class="float-left" onclick="playerView(this, ' + game.teams.home.probablePitcher.id + ', \'pitcher\')"><i class="fas fa-arrow-circle-right rowSelectorIcon"></i></a><span class="playerText">' + game.teams.home.probablePitcher.fullName + '</span>' : '',
                            game.status.detailedState,
                            (game.weather ? getIcon(game.weather.condition) : '') + '<span>' + game.weather.temp + ' &#186;F</span>'
                        ]);
                    }
                }
                var todaysGamesTable = $('table#todaysGames');
                todaysGamesTable.DataTable({
                    data: gamesData,
                    paging: false,
                    searching: false,
                    info: false,
                    rowCallback: dataTablesRowCallback
                });
                addTableTitle(todaysGamesTable, "Today's Games");
            },
            error: function () {
                alert('Could not get game statuses.');
            }
        });
    }
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