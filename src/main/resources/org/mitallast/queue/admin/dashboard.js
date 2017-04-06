$(function () {
    var $main = $(".main");
    initActions(window.document);
    createRaftClusterView();

    var refresh= null;

    function initActions(context) {
        $('a[href="#settings"]', context).click(function(e) {
            e.preventDefault();
            createSettingsView();
        });
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

        $('a[href="#startBenchmark"]', context).click(function (e) {
            e.preventDefault();
            createStartBenchmarkView()
        });
        $('form[action="#startBenchmark"]', context).submit(function(e) {
            e.preventDefault();
            var $form = $(this);
            var requests = $form.find('input[name=requests]').val();
            var dataSize = $form.find('input[name=dataSize]').val();

            $.getJSON("/_benchmark?requests="+requests+"&dataSize="+dataSize, function(response) {
                createBenchmarkResultView(response);
            });
        });

        $('form[action="#crdtCreate"]', context).submit(function(e) {
            e.preventDefault();
            var $form = $(this);
            var id = $form.find('input[name=id]').val();
            $.post("/_crdt/" + id +"/lww-register", function(response) {
                createCrdtValueView(id);
            });
        });

        $('form[action="#crdtAssign"]', context).submit(function(e) {
            e.preventDefault();
            var $form = $(this);
            var id = $form.find('input[name=id]').val();
            var value = $form.find('textarea[name=value]').val();
            $.post("/_crdt/" + id + "/lww-register/value", value, function(response) {
                createCrdtValueView(id);
            });
        });

        $('a[href="#crdtCreate"]', context).click(function (e) {
            e.preventDefault();
            createCrdtCreateView()
        });
        $('a[href="#crdtAssign"]', context).click(function (e) {
            e.preventDefault();
            createCrdtAssignView()
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

    function createSettingsView() {
        var template = Handlebars.compile($("#settings").html());
        $.getJSON("/_settings", function (data) {
            renderMain(template(data));
        });
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

    function createStartBenchmarkView() {
        renderMain($("#startBenchmark").html())
    }

    function createBenchmarkResultView(response) {
        response.throughput = humanizeBytes(response.throughput)
        var template = Handlebars.compile($("#benchmarkResult").html());
        renderMain(template(response))
    }

    function createCrdtCreateView(){
        var template = Handlebars.compile($("#crdtCreate").html());
        renderMain(template())
    }

    function createCrdtAssignView(){
        var template = Handlebars.compile($("#crdtAssign").html());
        renderMain(template())
    }

    function createCrdtValueView(id){
        var template = Handlebars.compile($("#crdtValue").html());
        $.getJSON("/_crdt/" + id + "/lww-register/value", function(response) {
            renderMain(template({value:JSON.stringify(response)}))
        });
    }

    function humanizeBytes(bytes) {
        var units = ["B/s","KB/s","MB/s","GB/s","TB/s","PB/s"];
        var unit = 0;
        while(true) {
            if(unit >= units.length) {
                break;
            }
            if(bytes > 1024){
                bytes = bytes / 1024;
                unit = unit + 1;
            }else{
                break;
            }
        }
        return bytes.toFixed(3) + " " + units[unit];
    }
});