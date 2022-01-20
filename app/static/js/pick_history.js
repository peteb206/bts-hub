$(document).ready(function () {
   var date = new Date();
   var year = date.getFullYear()
   var month = date.getMonth() + 1;
   var day = date.getDate();
   if (month < 10) {
      month = '0' + month;
   }
   if (day < 10) {
      day = '0' + day;
   }
   var yyyy_mm_dd = [year, month, day].join('-');
   $('table.display#history').DataTable({
      ajax: {
         url: '/pickHistory?year=' + year + '&date=' + yyyy_mm_dd
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
      pagingType: 'full',
      order: [[1, 'desc'], [0, 'asc']],
      // paging: false,
      // scrollX: true,
      // scrollY: '75vh',
      // scrollCollapse: true,
      // autoWidth: false,
      dom: 'Bfrtip',
      buttons: [
         // 'pageLength',
         {
            text: 'Hub',
            action: function (e, dt, button, config) {
               $('#historyDiv').hide();
               $('#hubDiv').show();
            }
         }
      ]
   });
});