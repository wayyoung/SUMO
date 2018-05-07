<%--
  Created by IntelliJ IDEA.
  User: Administrator
  Date: 2018/5/5
  Time: 下午 03:03
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<!DOCTYPE HTML>
<html>
<head>
    <script src="assets/jquery-3.3.1.min.js"></script>
    <!-- add one of the jQWidgets styles -->
    <link rel="stylesheet" href="assets/jqwidgets/jqwidgets/styles/jqx.base.css" type="text/css" />
    <link rel="stylesheet" href="assets/jqwidgets/jqwidgets/styles/jqx.darkblue.css" type="text/css" />
    <script type="text/javascript" src="assets/jqwidgets/jqwidgets/jqxcore.js"></script>
    <script type="text/javascript" src="assets/jqwidgets/jqwidgets/jqxbuttons.js"></script>

    <script src="assets/highcharts/highcharts.js"></script>


    <script>
        window.onload = function () {
            var dataPointsd = [];
            $.getJSON("queryProxyConnection?sourcename=00:00:c0:0b:72:63", function(data) {
                $.each(data, function(key, value){
                    dataPointsd.push({x: new Date(value["dt"]), y: parseInt(value["connected"])});
                    console.log(value["dt"]+","+value["connected"]);
                });


                var chart = Highcharts.chart('chartContainer', {
                    chart: {
                        zoomType: 'x'
                    },
                    title: {
                        text: 'USD to EUR exchange rate over time'
                    },
                    subtitle: {
                        text: document.ontouchstart === undefined ?
                            'Click and drag in the plot area to zoom in' : 'Pinch the chart to zoom in'
                    },
                    xAxis: {
                        type: 'datetime'
                    },
                    yAxis: {
                        title: {
                            text: 'Exchange rate'
                        }
                    },
                    legend: {
                        enabled: false
                    },
                    plotOptions: {
                        area: {
                            fillColor: {
                                linearGradient: {
                                    x1: 0,
                                    y1: 0,
                                    x2: 0,
                                    y2: 1
                                },
                                stops: [
                                    [0, Highcharts.getOptions().colors[0]],
                                    [1, Highcharts.Color(Highcharts.getOptions().colors[0]).setOpacity(0).get('rgba')]
                                ]
                            },
                            marker: {
                                radius: 2
                            },
                            lineWidth: 1,
                            states: {
                                hover: {
                                    lineWidth: 1
                                }
                            },
                            threshold: null
                        }
                    },

                    series: [{
                        type: 'area',
                        name: 'USD to EUR',
                        step: true,
                        data: dataPointsd
                    }]
                });
            });

            $("#myButton").jqxButton(
                { width: '120px', height: '35px', theme: 'darkblue' }
            );
        }
    </script>
</head>
<body>
<input type="button" value="Click Me" id='myButton' />
<div id="chartContainer" style="height: 370px; width: 100%;"></div>

</body>
</html>