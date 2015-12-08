google.load('visualization', '1', {packages: ['corechart', 'bar']});
google.setOnLoadCallback(draw_histograms);

function convert_cassandra_histo(json) {
    var total = 0;
    for (var index in json) {
        total += json[index];
    }
    var result = [['', '']];
    for(var i = 5;i > 0; i--) {
        var percent = json[i] / total;
        result.push([i + " Star", percent]);
    }
    return result;
}

function draw_histograms() {
    for(index in to_draw) {
        var label = to_draw[index][0];
        var raw_data = to_draw[index][1];
        var data = google.visualization.arrayToDataTable(convert_cassandra_histo(raw_data));
        var options = {
            chartArea: {width: '50%'},
            legend: {position: 'none'}
        };
        var chart = new google.visualization.BarChart(document.getElementById(label));
        chart.draw(data, options);
    }
}
