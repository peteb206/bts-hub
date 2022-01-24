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

$(window).on('load', function () {
    $('#spinnerDiv').addClass('hidden');
    $('#content').removeClass('hidden');
});