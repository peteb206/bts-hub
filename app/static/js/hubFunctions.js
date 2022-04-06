let parseCookie = function(cookieToReturn) {
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

let buildDataTable = function(args) {
    let collection = args.dataSource.split('/').pop();
    $.ajax({
        type: 'GET',
        url: '/columns/' + collection,
        dataType: 'json',
        success : function(columnData) {
            let columnArray = columnData.data[0].columns; 
            var columns = [];
            for (var i = 0; i < columnArray.length; i++) {
                var column = columnArray[i];
                columns.push({
                    data: column,
                    title: column.charAt(0).toUpperCase() + column.slice(1).replace(/([a-z])([A-Z])/g, '$1 $2')
                });
            }
            var config = {
                ajax: args.dataSource,
                columns: columns,
                destroy: true,
                pagingType: 'full',
                columnDefs: [
                    {
                        targets: '_all',
                        defaultContent: ''
                    }
                ]
                // dom: 'Bfrtip'
            }
            if (args.buttons) {
                config.buttons = buttons;
            }
            if (args.sortMap) {
                let sortMap = args.sortMap;
                config.order = [];
                Object.keys(sortMap).forEach(function(col) {
                    config.order.push([col, sortMap[col]]);
                });
            }
            if (args.initCompleteFunc) {
                config.initComplete = initCompleteFunc;
            }
            if (args.rowCallbackFunc) {
                config.rowCallback = rowCallbackFunc;
            }
            if (args.infoCallbackFunc) {
                config.infoCallback = infoCallbackFunc;
            }
            args.tableElement.DataTable(config);
        }
    });
}

let formatDatePickerDate = function(date, format) {
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

let adjustContentToSidebar = function() {
    $('#rightBody').css('margin-left', $('#sidebar').css('max-width'));
}

let addClass = function(element, className) {
    if (!element.hasClass(className)) {
        element.addClass(className);
    }
}

let removeClass = function(element, className) {
    if (element.hasClass(className)) {
        element.removeClass(className);
    }
}

let gameLogsColumns = function(playerType) {
    var columns = [];
    if (playerType == 'batter') {
        columns = [
            {
                data: 'date',
                title: 'Date'
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
                title: 'xH'
            }, {
                data: 'h',
                title: 'H'
            }
        ]
    }
    return columns
}

let playerView = function (anchor, playerId, viewType) {
    if ($(anchor).find('svg.fa-arrow-circle-right').length) {
        $('svg.fa-arrow-circle-down').removeClass('fa-arrow-circle-down').addClass('fa-arrow-circle-right');
        $.ajax({
            type: 'GET',
            url: '/' + viewType + 'View/' + playerId + $(location).attr('search'),
            dataType: 'json',
            success: function(json) {
                var data = json.data;
                $('img#playerImage').attr('src', 'https://securea.mlb.com/mlb/images/players/head_shot/' + playerId +'.jpg');
                $('div#keyStats').find('h4').text(data.name);

                $('div#playerGameLogs').empty();
                var gameLogsTable = $('<table></table>')
                    .attr('id', 'gameLogsTable')
                    .attr('class', 'display');
                $('div#playerGameLogs').append(gameLogsTable);
                gameLogsTable = $('table#gameLogsTable');
                gameLogsTable.DataTable({
                    data: data.recentGames,
                    columns: gameLogsColumns(viewType),
                    pageLength: 5,
                    lengthChange: false,
                    searching: false,
                    info: false,
                    order: []
                });
                var tableTitle = $('<span>')
                    .attr('class', 'tableTitle')
                    .text('Recent Games');
                gameLogsTable.parent().prepend(tableTitle);

                $(anchor).find('svg').removeClass('fa-arrow-circle-right').addClass('fa-arrow-circle-down');
            },
            error: function() {
                alert('Could not load player view');
            }
        });
    }
}