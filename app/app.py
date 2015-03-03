from flask import Flask, render_template, request, jsonify
from dbdriver import SQLiteDB

app = Flask(__name__)

#field_map = { "page" : "curr_name", "referer" : "prev_name", "count" : "num_requests" }
#operators = { "eq" : "=", "like" : "LIKE", "contains" : "LIKE '%%'", "gt" : ">", "lt" : "<", "gte" : ">=", "lte" : "<=", "ne" : "!=" }

@app.route("/")
def index():
    return render_template('index.html')


@app.route("/pages/list", methods=['GET'])
def pages_list():
    query = 'SELECT DISTINCT(curr_name) FROM pageviews;'
    return _fetch(query)


@app.route("/report/pageviews", methods=['GET'])
def pageviews_report():
    query = 'SELECT curr_name, SUM(num_requests) AS num_requests FROM pageviews'
    query = _compose_filters(request, query)
    query = query + ' GROUP BY curr_name ORDER by num_requests DESC;'

    return _fetch(query)


@app.route("/report/daywise/pageviews", methods=['GET'])
def pageviews_daywise_report():
    query = 'SELECT curr_name, DATE(ts), SUM(num_requests) AS num_requests FROM pageviews'
    query = _compose_filters(request, query)
    query = query + ' GROUP BY curr_name, DATE(ts) ORDER by curr_name, DATE(ts);'

    return _fetch(query)


@app.route("/report/referer", methods=['GET'])
def referer_report():
    query = 'SELECT prev_name, SUM(num_requests) AS num_requests FROM pageviews'
    query = _compose_filters(request, query)
    query = query + ' GROUP BY prev_name ORDER by num_requests DESC;'

    return _fetch(query)


@app.route("/report/referer/page", methods=['GET'])
def referer_page_report():
    query = 'SELECT curr_name, prev_name, SUM(num_requests) AS num_requests FROM pageviews'
    query = _compose_filters(request, query)
    query = query + ' GROUP BY curr_name, prev_name ORDER by num_requests DESC;'

    return _fetch(query)


def _fetch(query):
    results = SQLiteDB().fetchall(query)
    app.logger.debug('%s yielded %s results', query, len(results))
    return jsonify({'results' : results })


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
    min_count = request.args.get('min_count', None)
    if min_count:
        query = _join(query, "num_requests >= " + min_count)
    max_count = request.args.get('max_count', None)
    if max_count:
        query = _join(query, "num_requests <= " + max_count)
    lang = request.args.get('lang', None)
    if lang:
        query = _join(query, "language = '" + lang + "'")
    start_date = request.args.get('start_date', None)
    if start_date:
        query = _join(query, "DATE(ts) >= '" + start_date + "'")
    end_date = request.args.get('end_date', None)
    if end_date:
        query = _join(query, "DATE(ts) <= '" + end_date + "'")
    
    return query


if __name__ == "__main__":
    app.run(debug=True)