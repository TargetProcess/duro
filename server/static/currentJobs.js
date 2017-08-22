
$.ajax('/api/jobs?from=' + floor + '&to=' + ceiling).done(function(data) {
    var formattedData = data.map(function (cur) {
        if (!cur.finish) {
            cur.finish = moment.now();
            cur.finished = false;
        } else {
            cur.finished = true;
        }
        return cur;
    });
    var chart = new tauCharts.Chart({
        data: formattedData,
        type: 'horizontal-bar',
        x: 'finish',
        y: 'table',
        color: 'finished',
        dimensions: {
            start: {
                type: 'measure',
                scale: 'time'
            },
            finish: {
                type: 'measure',
                scale: 'time'
            },
            table: {
                type: 'category',
                scale: 'ordinal'
            }
        },
        guide: {
            x: {label: 'finish time'},
            y: {label: 'table'},
            color: {
                brewer: {false: '#0000ff', true: '#00ff00'}
            }
        },
        plugins: [
            tauCharts.api.plugins.get('tooltip')({
                fields: ['table', 'start', 'finish'],
                formatters: {
                    table: {
                        label: 'table'
                    },
                    start: {
                        label: 'start',
                        format: '%H:%M:%S, %b %d'
                    },
                    finish: {
                        label: 'finish',
                        format: '%H:%M:%S, %b %d'
                    }
                }
            }),
            tauCharts.api.plugins.get('bar-as-span')({x0: 'start'})
        ]
    });
    chart.renderTo('#chart');
});