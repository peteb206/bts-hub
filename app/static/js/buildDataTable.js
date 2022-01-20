$(document).ready(function () {
    let buildDataTable = function(args) {
        var config = {
            ajax: args.dataSource,
            destroy: true,
            pagingType: 'full',
            columnDefs: [
                {
                    targets: '_all',
                    defaultContent: ''
                }
            ],
            dom: 'Bfrtip'
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
        $(tableElement).DataTable(config);
    }
});