TESTER = document.getElementById('tester');

var total_voos = {
    x: |x_total_voos|,
    y: |y_total_voos|,
    type: 'scatter'
};

var data = [
    total_voos
];

var layout = {
    xaxis: {
        title: 'Ano',
        autotick: false,
        dtick: 1
    },
    yaxis: {
        title: 'Voos',
        autotick: false,
        dtick: 100000,
        tickformat: ",d",
        rangemode: 'tozero'
    },
    automargin: true
};

Plotly.newPlot(
    TESTER,
    data,
    layout
);