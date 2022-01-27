try:
    import app.utils as utils
except ModuleNotFoundError:
    import utils
import pandas as pd
import re


def sidebar_links_html(db, current_endpoint, collapse_sidebar):
    available_dates = utils.get_available_dates(db)
    year = available_dates[-1].split('-')[0]
    game_dates = [game_date for game_date in available_dates if game_date.startswith(year)]
    list_items = [
        ['Dashboard', 'fas fa-home', '/dashboard'],
        # ['My Picks', 'fas fa-pencil-alt', '/picks'],
        # ['Leaderboard', 'fas fa-list-ol', '/leaderboard'],
        ['Splits', 'fas fa-coins' , '/splits'],
        ['Simulations', 'fas fa-chart-bar', '/simulations'],
        ['Data', 'fas fa-database', '/dataView'],
        ['Games', 'fas fa-calendar-alt', f'/games?startDate={game_dates[0]}&endDate={game_dates[-1]}'],
        ['At Bats', 'fas fa-baseball-ball', f'/atBats?startDate={available_dates[-10]}&endDate={available_dates[-1]}'],
        ['Players', 'fas fa-users', f'/players?year={year}'],
        ['Teams', 'fas fa-trophy', f'/teams?year={year}'],
        ['Stadiums', 'fas fa-university', '/stadiums'],
        # ['Links', 'fas fa-link', '/links']
    ]
    list_items_html, full_sidebar_hidden, partial_sidebar_hidden  = '', '', ''
    if collapse_sidebar:
        full_sidebar_hidden = ' hidden'
    else:
        partial_sidebar_hidden = ' hidden'
    for list_item in list_items:
        active = current_endpoint == list_item[2].split('?')[0]
        list_items_html += '<li title="' + list_item[0] + '"' + (' class="active">' if active else '>')
        list_items_html +=    '<a href="' + ('#' if active else list_item[2]) + '">'
        list_items_html +=       '<div class="fullSidebarTab' + full_sidebar_hidden + '">'
        list_items_html +=          '<i class="' + list_item[1] + '"></i>'
        list_items_html +=          '<span class="buttonText">' + list_item[0] + '</span>'
        list_items_html +=       '</div>'
        list_items_html +=       '<div class="container-fluid partialSidebarTab' + partial_sidebar_hidden + '">'
        list_items_html +=          '<i class="' + list_item[1] + ' fa-lg"></i>'
        list_items_html +=       '</div>'
        list_items_html +=    '</a>'
        list_items_html += '</li>'
    return list_items_html


def filters_html(filter_types, filter_values):
    filter_html = ''
    if 'date' in filter_types:
        date_value = filter_values.strftime('%a, %B %-d, %Y')
        filter_html += '<div class="col-auto">'
        filter_html +=    '<span class="datePickerLabel">Date:</span>'
        filter_html +=   f'<input placeholder="None selected..." type="text" id="date" class="datepicker" value="{date_value}" readonly>'
        filter_html += '</div>'
    elif 'date_range' in filter_types:
        date_value = [filter_value.strftime('%a, %B %-d, %Y') if filter_value else '' for filter_value in filter_values]
        filter_html += '<div class="col-auto">'
        filter_html +=    '<span class="datePickerLabel">Start Date:</span>'
        filter_html +=   f'<input placeholder="None selected..." type="text" id="startDate" class="datepicker" value="{date_value[0]}" readonly>'
        filter_html += '</div>'
        filter_html += '<div class="col-auto">'
        filter_html +=    '<span class="datePickerLabel">End Date:</span>'
        filter_html +=   f'<input placeholder="None selected..." type="text" id="endDate" class="datepicker" value="{date_value[1]}" readonly>'
        filter_html += '</div>'
    elif 'year' in filter_types:
        filter_html += '<div class="col-auto">'
        filter_html +=    '<span class="datePickerLabel">Year:</span>'
        filter_html +=    '<select id="yearPicker" onchange="addClass($(\'#updateFiltersButtonInactive\'), \'hidden\'); removeClass($(\'#updateFiltersButtonActive\'), \'hidden\');">'
        for year in range(2015, 2023):
            selected_year = ' selected="selected"' if year == filter_values else ''
            filter_html +=    f'<option value="{year}"{selected_year}>{year}</option>'
        filter_html +=    '</select>'
        filter_html += '</div>'
    if filter_html != '':
        filter_html += '<div class="col-auto">'
        filter_html +=    '<button type="button" id="updateFiltersButtonInactive" class="btn btn-secondary">'
        filter_html +=       '<i class="fas fa-sync"></i>'
        filter_html +=       '<span class="buttonText">Update</span>'
        filter_html +=    '</button>'
        filter_html +=    '<button type="button" id="updateFiltersButtonActive" class="btn btn-secondary hidden">'
        filter_html +=       '<i class="fas fa-sync"></i>'
        filter_html +=       '<span class="buttonText">Update</span>'
        filter_html +=    '</button>'
        filter_html += '</div>'
    return filter_html


def html_table(table_id, data):
    assert isinstance(data, (list, pd.DataFrame))
    if isinstance(data, list):
        tbody, column_list = '', list()
        for document in data:
            if len(column_list) == 0:
                column_list = list(document.keys())
            tbody += '<tr>' + ''.join([f'<td>{format_table_value(document[column])}</td>' for column in column_list]) + '</tr>'
        if len(column_list) == 0:
            column_list = [' ']
        thead = ''.join(['<th>{}</th>'.format(re.sub(r'((?<=[a-z])[A-Z]|(?<!\A)[A-Z](?=[a-z]))', r' \1', column[0].upper() + column[1:])) for column in column_list])
        return f'<table id="{table_id}" class="display"><thead>{thead}</thead><tbody>{tbody}</tbody></table>'
    else:
        return data.to_html(na_rep='', table_id=table_id, classes='display', border=None, justify='unset', index=False)


def format_table_value(value):
    if value != value:
        value = ''
    return value