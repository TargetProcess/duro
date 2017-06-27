
$.ajax('/api/jobs?from=' + floor + '&to=' + ceiling).done(function(data) {
    var chart = new tauCharts.Chart({
        data: data,
        type: 'horizontal-bar',
        x: 'finish',
        y: 'table',
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
            x: {label: 'Finish time'},
            y: {label: 'Table'}
        },
        plugins: [
            tauCharts.api.plugins.get('tooltip')(),
            tauCharts.api.plugins.get('legend')(),
            tauCharts.api.plugins.get('bar-as-span')({x0: 'start'})
        ]
    });
    chart.renderTo('#chart');
});