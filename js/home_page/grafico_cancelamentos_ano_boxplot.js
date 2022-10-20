var cancelamentos_ano_boxplot = document.getElementById('cancelamentos_ano_boxplot');

var data = [
    {
      y: |y_cancelamentos|,
      boxpoints: 'all',
      jitter: 0.3,
      pointpos: -1.8,
      type: 'box',
      name: 'Cancelamentos',
      customdata: |x_cancelamentos|,
      hovertemplate: 'Ano: %{customdata}',
      boxmean: true
    }
  ];

  var layout = {
    yaxis: {
        title: '% Cancelamentos',
        autotick: false,
        dtick: 1,
        tickformat: ",d",
        rangemode: 'tozero'
    },
    automargin: true
};

var config = {responsive: true}

Plotly.newPlot(
    cancelamentos_ano_boxplot,
    data,
    layout
);