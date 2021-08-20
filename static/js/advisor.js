$(document).ready(function () {
   $('#historyDiv').hide();

   var percentFunc = function (data, type, row) {
      if (data === '') {
         return data;
      } else {
         return (data * 100).toFixed(2).toString() + '%';
      }
   }

   var roundFunc = function (data, digits) {
      if (data === '' || data === undefined) {
         return data;
      } else {
         return data.toFixed(digits).toString();
      }
   }

   var roundFunc2 = function (data, type, row) {
      return roundFunc(data, 2)
   }

   var roundFunc3 = function (data, type, row) {
      return roundFunc(data, 3)
   }

   var playerLinkFunc = function(id, name) {
      if (id) {
         return '<a href="https://www.mlb.com/player/' + id + '" target="_blank" style="text-decoration:none; color:#0076CE">' + name + '</a>';
      } else {
         return ''
      }
   }

   var batterLinkFunc = function (data, type, row) {
      return playerLinkFunc(row.player_id, data);
   }

   var pitcherLinkFunc = function (data, type, row) {
      return playerLinkFunc(row.pitcher_id, data);
   }

   var gameLinkFunc = function (data, type, row) {
      return '<a href="https://www.mlb.com/gameday/' + row.game_pk + '" target="_blank" style="text-decoration:none; color:#0076CE">' + data + '</a>';
   }

   var headToHeadFunc = function (data, type, row) {
      if (row.PA_vs_SP > 0) {
         return (data / row.PA_vs_SP).toFixed(2).toString() + ' (' + data.toString() + ' / ' + row.PA_vs_SP.toString() + ')';
      }
      return ''
   }

   var cols = [
      {
         data: 'player_name',
         title: 'Name',
         render: batterLinkFunc
      }, {
         data: 'team',
         title: 'Team'
      }, {
         data: 'batter_handedness',
         title: 'B'
      }, {
         data: 'order',
         title: 'Lineup'
      }, {
         data: 'H_total',
         title: 'H',
         className: 'border_left'
      }, {
         data: 'xH_per_G_total',
         title: 'xH / G',
         render: roundFunc2
      }, {
         data: 'hit_pct_total',
         title: 'Hit %',
         render: percentFunc
      }, {
         data: 'x_hit_pct_total',
         title: 'xHit %',
         render: percentFunc
      }, {
         data: 'H_per_PA_vs_L',
         title: 'H / PA (vs. L)',
         render: roundFunc3
      }, {
         data: 'H_per_PA_vs_R',
         title: 'H / PA (vs. R)',
         render: roundFunc3
      }, {
         data: 'H_per_PA_vs_BP',
         title: 'H / PA (vs. BP)',
         render: roundFunc3
      }, {
         data: 'xH_per_G_weighted',
         title: 'xH / G*',
         render: roundFunc2,
         className: 'border_left'
      }, {
         data: 'hit_pct_weighted',
         title: 'Hit %*',
         render: percentFunc
      }, {
         data: 'x_hit_pct_weighted',
         title: 'xHit %*',
         render: percentFunc
      }, {
         data: 'opponent',
         title: 'Opponent',
         render: gameLinkFunc,
         className: 'border_left'
      }, {
         data: 'pitcher_name',
         title: 'Starter',
         render: pitcherLinkFunc
      }, {
         data: 'sp_HA_per_BF_total',
         title: 'SP: H / BF'
      }, {
         data: 'sp_xHA_per_BF_total',
         title: 'SP: xH / BF'
      }, {
         data: 'H_per_BF_vs_L',
         title: 'SP: H / BF (vs. L)',
         render: roundFunc3
      }, {
         data: 'H_per_BF_vs_R',
         title: 'SP: H / BF (vs. R)',
         render: roundFunc3
      }, {
         data: 'bp_HA_per_BF_total',
         title: 'RP: H / BF'
      }, {
         data: 'bp_xHA_per_BF_total',
         title: 'RP: xH / BF'
      }, {
         data: 'H_vs_SP',
         title: 'B vs SP: H / PA',
         render: headToHeadFunc,
         className: 'border_left'
      }, {
         data: 'xH_vs_SP',
         title: 'B vs SP: xH / PA',
         render: headToHeadFunc
      }, {
         data: 'weather',
         title: '',
         render: function (data, type, row) {return (data !== '') ? '<img style="display:block;" height="20px" height=auto src="https://rotowire.com/images/weather/' + data + '">' : ''},
         className: 'border_left'
      }
   ]

   var date = new Date();
   var day = String(date.getDate()).padStart(2, '0');
   var month = String(date.getMonth() + 1).padStart(2, '0');
   var year = date.getFullYear();
   var yyyy_mm_dd = [year, month, day].join('-');
   $('#title').html('Beat the Streak Advisor: ' + yyyy_mm_dd);

   var create_table = function() {
      return $('table.display#advisor').DataTable({
         ajax: {
            url: '/loadTableData?hitMin=10&day=' + yyyy_mm_dd
         },
         columns: cols,
         order: [[ 6, 'desc']],
         scrollX: true,
         paging: false,
         scrollY: '75vh',
         scrollCollapse: true,
         autoWidth: false,
         columnDefs: [{
            targets: '_all',
            defaultContent: ''
         }],
         fixedColumns: {
            leftColumns: 4
         },
         dom: 'Bfrtip',
         buttons: [
            'csv',
            {
               text: 'Pick History',
               action: function (e, dt, button, config) {
                  $('#advisorDiv').hide();
                  $('#historyDiv').show();
               }
            }
         ],
         initComplete: function(oSettings, json) {
            var th = $('#advisorDiv').find('th');
            var td = $('#advisorDiv').find('td');
            th.css('white-space', 'nowrap'); // Don't wrap table headers
            td.css('white-space', 'nowrap'); // Don't wrap table data
            th.css('text-align', 'center'); // Center text
            td.css('text-align', 'center'); // Center text

            var toolbarHtml = '';
            toolbarHtml += '<div style="float: left;">';
            toolbarHtml += '   <p>Most recent statcast data: ';
            toolbarHtml += '      <span style="color: red;">' + json.lastUpdated + '</span>';
            toolbarHtml += '   </p>';
            toolbarHtml += '</div>';

            $('#advisorDiv').find('toolbar').html(toolbarHtml);
         },
         rowCallback: function(row, data, index) {
            Object.keys(data).forEach(function(key, colIndex) {
               if (data[key + '_color']) { // Color scale column
                  var colDisplayIndex = cols.findIndex(item => item.data === key);
                  $(row).find('td:eq(' + colDisplayIndex + ')').css('background-color', data[key + '_color']);
               }
               if (data['order'].toString() === 'OUT') { // Red cell if player is out of lineup
                  $(row).find('td:eq(3)').css('background-color', 'red');
                  $(row).find('td:eq(3)').css('color', 'white');
               }
            });
         },
         infoCallback: function(settings, start, end, max, total, pre) {
            var info = '';
            if (settings) {
               var json = settings.json;
               if (json) { // Add footnote to table
                  var spacer = ' '.repeat(10) + '|' + ' '.repeat(10); 
                  info = 'Most recent statcast data: <span style="color: red;">' + json.lastUpdated + '</span>' + spacer + 'Showing ' + max + ' players' + spacer + '* Indicates each game weighted 10% more than the previous one to account for adjustments, streaks, slumps, etc.';
               }
            }
            return info;
         }
      })
   }

   create_table();
});