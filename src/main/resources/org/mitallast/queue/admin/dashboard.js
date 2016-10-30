$(function () {
    var $main = $(".main");
    initActions(window.document);
    createRaftClusterView();

    function initActions(context) {
        $('a[href="#raftState"]', context).click(function (e) {
            e.preventDefault();
            createRaftClusterView()
        });
        $('a[href="#raftLog"]', context).click(function (e) {
            e.preventDefault();
            createRaftLogView()
        });
        $('a[href="#blobList"]', context).click(function (e) {
            e.preventDefault();
            createBlobListView()
        });
    }

    function renderMain(html) {
        $main.html(html);
        initActions($main);
    }

    function createRaftClusterView() {
        var template = Handlebars.compile($("#raftState").html());
        $.getJSON("/_raft/state", function (data) {
            renderMain(template(data));
        });
    }

    function createRaftLogView() {
        var template = Handlebars.compile($("#raftLog").html());
        $.getJSON("/_raft/log", function (data) {
            renderMain(template(data));
        });
    }
    function createBlobListView() {
        var template = Handlebars.compile($("#blobList").html());
        $.getJSON("/_blob", function (data) {
            renderMain(template(data));
        });
    }
});