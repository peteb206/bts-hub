$(document).ready(function () {
    buildDataTable({
        tableElement: $('table.display#dataView'),
        dataSource: 'data' + $(location).attr('pathname')
    })
});