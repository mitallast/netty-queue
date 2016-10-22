$(function () {
    var $main = $(".main");
    initActions(window.document);
    createRaftClusterView();

    function initActions(context) {
        $('a[href="#raftState"]', context).click(function (e) {
            e.preventDefault();
            createRaftClusterView()
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
});