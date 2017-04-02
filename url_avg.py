

#Query C On average which URL produced the best story in 2010?

from google.cloud import bigquery
import webapp2
import uuid
import time

#class
class scoreAvg(webapp2.RequestHandler):
    def get(self):
        rs = self.get_result()
        text = """
        <table>
        <tr><td>url</td><td>Avg_score</td><td>TIME_STAMP</td></tr>
        """
        for i in xrange(len(rs)):
            text += '<tr>'
            text += '<td>%s</td>' % rs[i][0]
            text += '<td>%d</td>' % rs[i][1]
            text += '<td>%s</td>' % rs[i][2]
            text += '</tr>'
        text += '</table>'
        self.response.write(text)
# async wating
    def __wait_for_job(self, job):
        while True:
            job.reload()  # Refreshes the state via a GET request.
            if job.state == 'DONE':
                if job.error_result:
                    raise RuntimeError(job.errors)
                return
            time.sleep(1) 

    def get_result(self):
        sql = """ SELECT 
                    url,
                    avg(score) as avg_score, 
                    time_ts as TIME_STAMP 
                    FROM `bigquery-public-data.hacker_news.stories`
                    where EXTRACT(YEAR from time_ts) = 2010
                    GROUP BY url, TIME_STAMP 
                    ORDER BY avg_score DESC
                    LIMIT 4
              """
        client = bigquery.Client('extreme-core-158121')
        job = client.run_async_query(str(uuid.uuid4()), sql)
        ds = client.dataset('results')
        if not ds.exists():
            ds.create()
            ds.reload()

        tbl = ds.table('table_c')
        #tbl.reload()
        job.destination = tbl 
        job.use_legacy_sql = False

        job.begin()
        self.__wait_for_job(job)


        # Drain the query results by requesting a page at a time.
        query_results = job.results()
        page_token = None
        rs = []
        while True:
            rows, total_rows, page_token = query_results.fetch_data(
                max_results=500,
                page_token=page_token)

            rs += rows

            if not page_token:
                break
        return rs
