var wc_app = angular.module('wc_app', ['ngRoute']);

wc_app
    .config(function($routeProvider) {
        $routeProvider
            .when('/', {
                templateUrl: '/views/index.html',
                controller:'wc_controller'
            });

    });
