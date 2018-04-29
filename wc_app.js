var wc_app = angular.module('wc_app', ['ngRoute']);

wc_app
    .config(
        ['$locationProvider', function($locationProvider) {
            $locationProvider.hashPrefix('');
        }]
    )
    .config(function($routeProvider) {
        $routeProvider
            .when('/', {
                templateUrl: '/views/home.html',
                controller:'wc_controller'
            })
            .when('/streaming/static', {
                templateUrl: '/views/static_data.html',
                controller: 'st_controller'
                }
            )
            .when('/streaming/realtime', {
                templateUrl: '/views/realtime_data.html',
                controller: 'rt_controller'
                }
            );

    });
