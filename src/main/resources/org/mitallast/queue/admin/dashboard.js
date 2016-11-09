$(function () {
    var $main = $(".main");
    initActions(window.document);
    createRaftClusterView();

    var refresh= null;

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
        $('a[href="#putBlob"]', context).click(function (e) {
            e.preventDefault();
            createPutBlobView()
        });
        $('button[data-resource-id]', context).click(function (e) {
            e.preventDefault();
            createGetBlobView($(this).data('resource-id'))
        })
        $('form[action="#putBlob"]', context).submit(function(e) {
            e.preventDefault();
            var $form = $(this);
            var key = $form.find('input[name=key]').val();
            var data = $form.find('textarea[name=data]').val();
            $.post("/_blob/" + key, data, function(response) {
                createBlobListView();
            });
        });
    }

    function renderMain(html) {
        if(refresh != null){
            clearTimeout(refresh);
            refresh = null;
        }
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
            $.each(data.entries, function(key, value) {
                value.committed = value.index <= data.committedIndex;
            });
            renderMain(template(data));
            refresh = setTimeout(createRaftLogView, 100);
        });
    }

    function createBlobListView() {
        var template = Handlebars.compile($("#blobList").html());
        $.getJSON("/_blob", function (data) {
            renderMain(template(data));
        });
    }

    function createPutBlobView() {
        var template = Handlebars.compile($("#putBlob").html());
        renderMain(template({"key": "", "data": ""}))
    }

    function createGetBlobView(key) {
        var template = Handlebars.compile($("#putBlob").html());
        $.get("/_blob/" + key, function(data) {
            renderMain(template({"key": key, "data":data}))
        });
    }
});