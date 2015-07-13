$(function () {
    var $main = $(".main");
    initActions(window.document);
    resourcesView();

    function initActions($context) {
        $('a[href="#resources-view"][data-path]', $context).click(function (e) {
            e.preventDefault();
            resourcesView($(this).data('path'));
        });
        $('a[href="#create-resource-view"]', $context).click(function (e) {
            e.preventDefault();
            createResourceView();
        });
    }

    function renderMain(html) {
        $main.html(html);
        initActions($main);
    }

    function resourcesView(path) {
        var template = Handlebars.compile($("#resources-view").html());
        $.getJSON("http://localhost:8080/_raft/resource", {path: path}, function (data) {
            if (path != "/") {
                data.basepath = path;
            }
            renderMain(template(data));
        });
    }

    function createResourceView() {
        var template = Handlebars.compile($("#create-resource-view").html());
        renderMain(template);
        var $form = $main.find("#create-resource-form").submit(function (e) {
            e.preventDefault();
            var path = $form.find("[name=path]").val();
            if (!/\w+/.test(path)) {
                return;
            }
            $.ajax({
                method: "POST",
                url: "http://localhost:8080/_raft/resource?" + $form.serialize(),
                success: function (data) {
                    console.log(data);
                    resourcesView("/");
                },
                error: function (xhr, status, error) {
                    console.log(xhr, status, error);
                }
            })
        });
    }
});