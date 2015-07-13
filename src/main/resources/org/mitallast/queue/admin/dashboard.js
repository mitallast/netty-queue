$(function () {
    var $main = $(".main");
    initActions(window.document);
    statsView();

    function initActions($context) {
        $('a[href="#stats-view"]', $context).click(function (e) {
            e.preventDefault();
            statsView();
        });
        $('a[href="#create-queue-view"]', $context).click(function (e) {
            e.preventDefault();
            createQueueView();
        });
        $('a[href="#queue-view"][data-queue]', $context).click(function (e) {
            e.preventDefault();
            queueView($(this).data('queue'));
        });
    }

    function renderMain(html) {
        $main.html(html);
        initActions($main);
    }

    function statsView() {
        var template = Handlebars.compile($("#stats-view").html());
        $.getJSON("http://localhost:8080/_stats", function (data) {
            renderMain(template(data));
        })
    }

    function queueView(queue) {
        var template = Handlebars.compile($("#queue-view").html());
        renderMain(template({
            queue: queue
        }));
    }

    function createQueueView() {
        var template = Handlebars.compile($("#create-queue-view").html());
        renderMain(template());
        var $form = $main.find("#create-queue-form");
        $form.submit(function (e) {
            e.preventDefault();
            var $name = $form.find("input[name=name]");
            var queue = $name.val();
            $form.find(".has-error").removeClass("has-error");
            if (!/\w+/.test(queue)) {
                $name.parent(".form-group").addClass("has-error");
                return;
            }
            $.ajax({
                url: "http://localhost:8080/" + queue,
                method: "POST",
                success: function (data) {
                    statsView();
                },
                error: function (hxr, status, error) {
                    console.log(hxr, status, error);
                }
            })
        })
    }
});