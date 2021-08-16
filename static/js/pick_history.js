$(document).ready(function () {
   var table = $('table.display#history').DataTable({
      ajax: {
         url: '/pickHistory'
      },
      columns: [
         {
            data: 'account',
            title: 'Account'
         }, {
            data: 'game_date',
            title: 'Date'
         }, {
            data: 'name',
            title: 'Name',
            render: function(data, type, row) {
               var out = data;
               if (row.player_id) {
                  out = '<a href="https://www.mlb.com/player/' + row.player_id + '" target="_blank" style="text-decoration:none; color:#0076CE">' + data + '</a>';
               }
               return out;
            }
         }, {
            data: 'opp_descriptor',
            title: 'Opponent'
         }, {
            data: 'ab',
            title: 'AB'
         }, {
            data: 'hit',
            title: 'H'
         }, {
            data: 'status',
            title: 'Result',
            render: function(data, type, row) {
               return (data === '') ? 'In Progress' : data.charAt(0).toUpperCase() + data.substr(1).toLowerCase();
            }
         }, {
            data: 'streak',
            title: 'Streak'
         }
      ],
      order: [[ 0, 'asc'], [ 1, 'desc']],
      paging: false,
      scrollX: true,
      scrollY: '75vh',
      scrollCollapse: true,
      autoWidth: false,
      dom: 'Bfrtip',
      buttons: [
         {
            text: 'Advisor',
            action: function (e, dt, button, config) {
               $('#historyDiv').hide();
               $('#advisorDiv').show();
            }
         }
      ],
      initComplete: function(oSettings, json) {
         var th = $('#historyDiv').find('th');
         var td = $('#historyDiv').find('td');
         th.css('white-space', 'nowrap'); // Don't wrap table headers
         td.css('white-space', 'nowrap'); // Don't wrap table data
         th.css('text-align', 'center'); // Center text
         td.css('text-align', 'center'); // Center text
      }
   });
});