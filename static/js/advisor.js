$(document).ready(function () {
   $('#historyDiv').hide();
   // Data Picker Initialization
   var day = new Date();
   $('#datepicker').datepicker({
      autoClose: true,
      dateFormat: 'MM d, yy',
      onSelect: function(date, datePicker) {
         if (date !== datePicker.lastVal) {
            loadWithDate(new Date(date));
         }
      }
   }).datepicker('setDate', day);

   var playerLinkFunc = function(id, name, position, type) {
      var out = '';
      if (id) {
         if (type === 'link') {
            out = '<a class="playerLink text-primary" href="https://www.mlb.com/player/' + id + '" target="_blank" style="text-decoration:none">' + name + '</a>';
         } else if (type === 'selector') {
            out = '<a class="' + id + ' ' + position + ' playerSelector text-primary" href="#"  style="text-decoration:none">' + name + '</a>';
         }
      }
      return out;
   }

   var selectItem = function(type, position, id, name) {
      if (type === 'player') {
         $('#selectedImage').attr('src', 'https://securea.mlb.com/mlb/images/players/head_shot/' + id + '.jpg');
         $('#selectedName').html(playerLinkFunc(id, name, position, 'link'));
      }
   }

   $(document).on('click', 'a.playerSelector', function(e) {
      var classes = $(e.target).attr('class');
      var id = parseInt(classes);
      var name = $(e.target).text();
      var position = classes[1];
      selectItem('player', position, id, name);
      return false;
   });

   var loadWithDate = function (date) {
      $('#selectedDiv').hide();
      $('#advisorDiv').hide();
      $('#spinnerDiv').removeClass('d-none');
      var year = date.getFullYear()
      var month = date.getMonth() + 1;
      var day = date.getDate();
      if (month < 10) {
         month = '0' + month;
      }
      if (day < 10) {
         day = '0' + day;
      }

      $.ajax({
         type: 'GET',
         url: '/loadTableData?hitMin=10&date=' + [year, month, day].join('-'),
         dataType: 'json',
         // async: false,
         success : function(ajax_data) {
            $('table.display#advisor').DataTable({
               data: ajax_data.rows,
               columns: [
                  {
                     data: 'batter',
                     title: 'Name',
                     render: function (data, type, row) {
                        var out = playerLinkFunc(data, ajax_data.metrics[data].name, 'batter', 'selector');
                        if (ajax_data.metrics[data] != undefined) {
                           out += ' (' + ajax_data.metrics[data].B + ')';
                        }
                        return out;
                     }
                  }, {
                     title: 'Lineup',
                     render: function (data, type, row) {
                        var game = ajax_data.games[row.game_pk];
                        var metrics = ajax_data.metrics[row.batter]
                        var lineup = [];
                        if (metrics.team === game.away_team && game.away_lineup !== undefined) {
                           lineup = game.away_lineup;
                        } else if (game.home_lineup !== undefined) {
                           lineup = game.home_lineup;
                        }
                        var order = 'TBD';
                        if (lineup.length > 0) {
                           order = lineup.indexOf(row.batter) + 1;
                        } else if (metrics.order_total) {
                           order += ' (' + metrics.order_total.toFixed(1) + ')';
                        }
                        return (order === 0 ? 'OUT' : order);
                     }
                  }, {
                     title: 'Team',
                     render: function (data, type, row) {
                        return ajax_data.metrics[row.batter].team;
                     }
                  }, {
                     title: 'Opponent',
                     render: function (data, type, row) {
                        var game = ajax_data.games[row.game_pk];
                        var opponent = '';
                        if (ajax_data.metrics[row.batter].team === game.away_team) {
                           opponent = '@' + game.home_team;
                        } else {
                           opponent = game.away_team;
                        }
                        if (opponent !== '') {
                           opponent += ' (<a href="https://www.mlb.com/gameday/' + row.game_pk + '" target="_blank" class="text-primary" style="text-decoration:none">' + game.game_time + '</a>)';
                        }
                        return opponent;
                     }
                  }, {
                     title: 'Starter',
                     render: function (data, type, row) {
                        var game = ajax_data.games[row.game_pk];
                        var starter = '';
                        var key = '';
                        if (ajax_data.metrics[row.batter].team === game.away_team) {
                           key = game.home_starter_id;
                        } else {
                           key = game.away_starter_id;
                        }
                        if (ajax_data.opponents.starters[key] !== undefined) {
                           var starter = ajax_data.opponents.starters[key];
                           starter = playerLinkFunc(key, starter.name, 'pitcher', 'selector') + ' (' + starter.T + ')';
                        }
                        return starter;
                     }
                  }, {
                     data: 'probability',
                     title: 'Probability',
                     className: 'border_left',
                     render: function (data, type, row) {
                        var out = '';
                        if (data !== '') {
                           out = (data * 100).toFixed(2).toString() + '%';
                        }
                        return out;
                     }
                  }, {
                     title: '',
                     className: 'border_left',
                     render: function (data, type, row) {
                        var out = '';
                        var team = ajax_data.metrics[row.batter].team;
                        if (ajax_data.weather[team] !== undefined) {
                           out = '<img style="display:block;" height="20px" src="https://rotowire.com/images/weather/' + ajax_data.weather[team] + '">';
                        }
                        return out;
                     }
                  }
               ],
               destroy: true,
               order: [[5, 'desc']],
               // scrollX: true,
               // paging: false,
               // scrollY: '75vh',
               // scrollCollapse: true,
               // autoWidth: false,
               columnDefs: [{
                  targets: '_all',
                  defaultContent: ''
               }],
               // fixedColumns: {
               //    leftColumns: 1
               // },
               dom: 'Bfrtip',
               buttons: [
                  'pageLength',
                  {
                     text: 'Pick History',
                     action: function (e, dt, button, config) {
                        $('#advisorDiv').hide();
                        $('#historyDiv').show();
                     }
                  }
               ],
               initComplete: function(oSettings, json) {
                  $('#spinnerDiv').addClass('d-none');
                  $('#advisorDiv').show();
                  $('#selectedDiv').show();
               },
               rowCallback: function(row, data, index) {
                  var td = $(row).find('td:contains(OUT)');
                  td.css('background-color', 'red');
                  td.css('color', 'white');
               },
               infoCallback: function(settings, start, end, max, total, pre) {
                  if (ajax_data.lastUpdated) { // Add footnote to table
                     pre += '<br>Most recent statcast data: <span style="color: red;">' + ajax_data.lastUpdated + '</span>';
                  }
                  // pre += '<br>* Indicates each game weighted 10% more than the previous one to account for adjustments, streaks, slumps, etc.';
                  return pre;
               }
            });
      
            if (ajax_data.rows.length > 0) {
               var topBatter = ajax_data.rows[0];
               selectItem('player', 'batter', topBatter.batter, ajax_data.metrics[topBatter.batter].name);
            }
         },
         error: function() {
            console.error('Ajax call failed, so the DataTable was not initialized.');
         }
      });
   };
   loadWithDate(day);
});