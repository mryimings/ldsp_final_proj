angular.module('wc_app').controller('rt_controller', function($scope, $http, $interval) {



    $scope.S_rate = 5;
    $scope.MAX_clouds = 10;
    $scope.MAX_words = 20;
    $scope.progress = 0;
    $scope.c_width = 500;
    $scope.c_height = 500;

    $scope.datas = ["sports","politics","music","dressing","technology","gaming","cars","food"];
    $scope.copyDatas = $scope.datas;
    $scope.hide = true;
    $scope.Key_words = ["sports"];



    $scope.change = function (x) {
        $scope.hide = true;
        $scope.Key_words = x;

    };

    $scope.changeKeyValue = function (v) {
        var newData = [];
        angular.forEach($scope.datas, function (data, index, array) {
            if (data.indexOf(v)>=0){
                newData.unshift(data);
            }
        });
        $scope.datas = newData;
        $scope.hide = false;

        if ($scope.datas.length==0||v==""){
            $scope.datas = $scope.copyDatas;
        }
    };


    $scope.test = function () {
        if($scope.datas.length>1){
            $scope.hide = false;
        }
    };


    $scope.begin = function() {

        var req = {
            method: 'POST',
            url: '/stats/filepipline',
            data: { S_rate: $scope.S_rate,
                MAX_clouds: $scope.MAX_clouds,
                MAX_words: $scope.MAX_words,
                Cloud_Width: $scope.c_width,
                Cloud_Height: $scope.c_height,
                keyword: $scope.Key_words
            }
        };

        $http(req).then(function(res){console.log(res)});

        $scope.slot = -1;
        tc_display();

    };




    function tc_display() {

        $scope.timer = $interval(
            function wc_show(){

                $scope.slot++ ;
                $scope.progress =  Math.round(($scope.slot +1)/$scope.MAX_clouds * 10000)/100;
                console.log("slot:",$scope.slot);

                var static_path = 'data/realtime_data/slot_';
                var file  = static_path + String($scope.slot) +".json";

                $http.get(file)
                    .then(function(res) {

                        var fill = d3.scale.category20();
                        // console.log('data', $scope.cloud_data)
                        $scope.layout = d3.layout.cloud()
                            .size([$scope.c_width, $scope.c_height])
                            .words(res.data)
                            .padding(5)
                            .rotate(function() { return (Math.random() * 40)-(Math.random() * 40); })
                            // .rotate(function() { return 30; })
                            .font("Impact")
                            .fontSize(function(d) { return d.size; })
                            .on("end", draw);

                        $scope.layout.start();

                        function draw(words) {
                            d3.select("#vis").append("svg")
                                .attr("width", $scope.layout.size()[0])
                                .attr("height", $scope.layout.size()[1])
                                .append("g")
                                .attr("transform", "translate(" + $scope.layout.size()[0] / 2 + "," + $scope.layout.size()[1] / 2 + ")")
                                .selectAll("text")
                                .data(words)
                                .enter().append("text")
                                .style("font-size", function(d) { return d.size + "px"; })
                                .style("font-family", "Impact")
                                .style("fill", function(d, i) { return fill(i); })
                                .attr("text-anchor", "middle")
                                .attr("transform", function(d) {
                                    return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
                                })
                                .text(function(d) { return d.text; });
                        }

                    })}
            , $scope.S_rate * 1000, $scope.MAX_clouds
        );

        $scope.$on(
            "$destroy",
            function() {
                $interval.cancel( $scope.timer );
            }
        );
    }



    $scope.pause = function(){

        $interval.cancel( $scope.timer );

    };

    $scope.continue = function(){

        tc_display();

    };


    $scope.clear = function(){

        d3.select("#vis").selectAll('*').remove();
        // d3.select("#vis").remove();
        $scope.layout.stop();
        $interval.cancel( $scope.timer );
        $scope.progress = 0;

    };


});