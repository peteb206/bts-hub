$(document).ready(function () {
   $('table.display#history').DataTable({
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
                  out = '<a class="player" href="https://www.mlb.com/player/' + row.player_id + '" target="_blank" style="text-decoration:none; color:#0076CE">' + data + '</a>';
               }
               return out;
            }
         }, {
            data: 'opp_descriptor',
            title: 'Opponent'
         }, {
            data: 'ab',
            title: 'At Bats'
         }, {
            data: 'hit',
            title: 'Hits'
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
      order: [[1, 'desc'], [0, 'asc']],
      // paging: false,
      // scrollX: true,
      // scrollY: '75vh',
      // scrollCollapse: true,
      // autoWidth: false,
      dom: 'Bfrtip',
      buttons: [
         'pageLength',
         {
            text: 'Advisor',
            action: function (e, dt, button, config) {
               $('#historyDiv').hide();
               $('#advisorDiv').show();
            }
         }
      ]
   });
});