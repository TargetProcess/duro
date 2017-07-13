var formatColumn = function (table, colNumber, formatter) {
    table.find('td:nth-child(' + colNumber + ')').each(function (index, el) {
        var $node = $(el),
            text = $node.text();
        if (text && text !== 'null') {
            $node.text(formatter(text))
        } else {
            $node.text('');
        }
    });
};

var formatLastCreated = function (text) {
    return moment.unix(text).fromNow();
};

var formatSeconds = function (text) {
    var time = parseInt(text);
    if (time === 0)
        return '';
    var formatMinutes = function (remainder) {
        if (remainder === 0) {
            return '';
        }
        var result = Math.floor(remainder / 60) + 'm ';
        if (remainder % 60 !== 0) {
            return result + remainder % 60 + 's';
        }
        else {
            return result;
        }
    };

    if (time < 60) {
        return time + 's'
    }

    if (time < 3600) {
        return formatMinutes(time);
    }
    if (time <= 86400) {
        var hours = Math.floor(time / 3600);
        var remainder = time % 3600;
        return hours + 'h ' + formatMinutes(remainder);
    }

    if (time > 86400) {
        var days = Math.floor(time / 86400);
        remainder = time % 86400;
        hours = Math.floor(remainder / 3600);
        remainder = remainder % 3600;
        var result = days + 'd ';
        if (hours) {
            return result + hours + '{hours}h ' + formatMinutes(remainder);
        } else {
            return result + formatMinutes(remainder);
        }
    }
};

var formatMinutes = function (text) {
    return formatSeconds(parseInt(text) * 60);
};


var displayUpdateSuccess = function (result) {
    var id = result.table.replace('.', '-');
    $('#update-' + id).text('Running...');
    $('#update-tree-' + id).text('Running...')
};

var displayUpdateFailure = function (result) {
    console.log(result);
};


var buildTable = function (tablesList) {
    var tablesSorted = tablesList.sort(function (a, b) {
        // var first = a.last_created || 1000000000,
        //     second = b.last_created || 1000000000;
        // return second - first;
        // if (a.last_created && b.last_created) {
        //     return a.last_created > b.last_created;
        // } else if (a.last_created && !b.last_created) {
        //     return 1;
        // } else if (!a.last_created && b.last_created) {
        //     return -1;
        // } else {
        //     return b.table_name > a.table_name;
        // }
    });
    var table = tablesSorted.reduce(function (acc, cur) {
        acc += '<tr><td class="align-middle"><a href="/tables/' + cur.table_name + '">' + cur.table_name + '</a></td>'
            + '<td class="align-middle">' + cur.interval + '</td>'
            + '<td class="align-middle">' + cur.last_created + '</td>'
            + '<td class="align-middle">' + cur.mean + '</td>';
        var id = cur.table_name.replace('.', '-');
        if (cur.started) {
            acc += '<td><button class="btn" id="update-' + id + '" disabled>Running...</button></td>'
                + '<td><button class="btn" id="update-tree-' + id + '" disabled>Running...</button></td></tr>'
        } else if (cur.deleted) {
            acc += '<td><button class="btn" id="update-' + id + '" disabled>Removed</button></td>'
                + '<td><button class="btn" id="update-tree-' + id + '" disabled>Removed</button></td></tr>'
        } else {
            acc += '<td><button class="btn" id="update-' + id + '"> Update table </button></td>'
                + '<td><button class="btn" id="update-tree-' + id + '">Update subtree</button></td></tr>'
        }
        return acc;
    }, '');
    var $tables = $('#tables');
    $tables.find('tbody').html(table);
    styleTable($tables);
    addListener();
};

var styleTable = function ($tables) {
    $tables.DataTable({
        "paging": false,
        "info": false,
        "order": [[2, "asc"]]
    }).draw();

    formatColumn($tables, 3, formatLastCreated);
    formatColumn($tables, 2, formatMinutes);
    formatColumn($tables, 4, formatSeconds);


};

var addListener = function () {
    $("button").click(function (e) {
        e.preventDefault();
        var id = this.id;
        var request = {
            type: 'POST',
            url: '/update',
            success: displayUpdateSuccess,
            error: displayUpdateFailure
        };
        if (id.substring(0, 11) === 'update-tree') {
            request.data = {
                table: id.substring(12),
                tree: 1
            };
        } else {
            request.data = {
                table: id.substring(7),
                tree: 0
            };
        }
        $.ajax(request);
    });
};

$(document).ready(function () {
    $.ajax({url: '/api/tables', success: buildTable});

});
