import time
from flask import Flask, render_template, request, jsonify
from dbdriver import DB

app = Flask(__name__)

@app.route("/")
def index():
    return render_template('index.html')


@app.route("/pages/list", methods=['GET'])
def pages_list():
    query = 'SELECT DISTINCT(curr_name) FROM clickstream_data;'
    return _fetch(query)


@app.route("/report/pageviews", methods=['GET'])
def pageviews_report():
    query = 'SELECT curr_name, CAST(SUM(num_requests) AS SIGNED) num_requests FROM clickstream_data'
    query = _compose_filters(request, query)
    query = query + ' GROUP BY curr_name ORDER by num_requests DESC;'

    return _fetch(query)


@app.route("/report/monthwise/pageviews", methods=['GET'])
def pageviews_monthwise_report():
    query = 'SELECT curr_name, `month`, SUM(num_requests) AS num_requests FROM clickstream_data'
    query = _compose_filters(request, query)
    query = query + ' GROUP BY curr_name, `month` ORDER by curr_name, month;'

    return _fetch(query)


@app.route("/report/referer", methods=['GET'])
def referer_report():
    query = 'SELECT prev_name, SUM(num_requests) AS num_requests FROM clickstream_data'
    query = _compose_filters(request, query)
    query = query + ' GROUP BY prev_name ORDER by num_requests DESC;'

    return _fetch(query)


@app.route("/report/referer/page", methods=['GET'])
def referer_page_report():
    query = 'SELECT curr_name, prev_name, SUM(num_requests) AS num_requests FROM clickstream_data'
    query = _compose_filters(request, query)
    query = query + ' GROUP BY curr_name, prev_name ORDER by num_requests DESC;'

    return _fetch(query)


def _fetch(query):
    print query
    start = time.time()
    results = DB().fetchall(query)
    elapsed = time.time() - start
    print('[{}] finished in {} ms'.format('DB fetch', int(elapsed * 1000)))
    app.logger.debug('%s yielded %s results', query, len(results))
    start = time.time()
    json_result = jsonify({'results' : results })
    elapsed = time.time() - start
    print('[{}] finished in {} ms'.format('jsonify', int(elapsed * 1000)))
    return json_result


def _compose_filters(request, query):
    if not request:
        return query
    if not query:
        return None

    def _join(query, clause):
        if not query:
            return None

        if not clause:
            return query

        if query.count('WHERE') > 0:
            query = query + ' AND ' + clause
        else:
            query = query + ' WHERE ' + clause
        return query

    page = request.args.get('page', None)
    if page:
        query = _join(query, "curr_name LIKE '" + page + "'")
    referer = request.args.get('referer', None)
    if referer:
        query = _join(query, "prev_name LIKE '" + referer + "'")
    project = request.args.get('project', None)
    if project:
        query = _join(query, "project = '" + project + "'")
    month = request.args.get('month', None)
    if month:
        query = _join(query, "`month` = " + month)
    min_count = request.args.get('min_count', None)
    if min_count:
        query = _join(query, "num_requests >= " + min_count)
    max_count = request.args.get('max_count', None)
    if max_count:
        query = _join(query, "num_requests <= " + max_count)
    
    return query


if __name__ == "__main__":
    app.run(debug=True)
