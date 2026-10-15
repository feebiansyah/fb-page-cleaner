import unittest
from unittest.mock import patch
import httpx
import app


class PostsTests(unittest.IsolatedAsyncioTestCase):
    async def load(self, payloads, **kwargs):
        calls = []
        def respond(request):
            calls.append(request)
            value = payloads[len(calls)-1]
            if isinstance(value, Exception):
                raise value
            if isinstance(value, bytes):
                return httpx.Response(200, content=value)
            return httpx.Response(value[0], json=value[1])
        app.SESSIONS['test'] = {'pages': [{'id': 'p', 'access_token': 'SECRET'}]}
        factory = httpx.AsyncClient
        with patch.object(app.httpx, 'AsyncClient', side_effect=lambda **kw: factory(transport=httpx.MockTransport(respond), **kw)):
            result = await app.get_posts('test', 'p', **kwargs)
        if isinstance(result, app.JSONResponse):
            raise app.HTTPException(result.status_code, result.body.decode())
        return result, calls

    async def test_invalid_is_not_empty(self):
        for value in [(200, {}), (200, {'data': None}), (200, {'data': [{}]}), (200, []), b'not json', (200, {'error': {'code': 190, 'message': 'SECRET'}}), (503, {}), httpx.ConnectError('SECRET')]:
            with self.subTest(value=value), self.assertRaises(app.HTTPException) as caught:
                await self.load([value])
            self.assertNotIn('SECRET', str(caught.exception.detail))

    async def test_normal_pagination(self):
        result, calls = await self.load([(200, {'data': [{'id': '1'}], 'paging': {'next': app.BASE_URL+'/p/posts?after=a'}}), (200, {'data': [{'id': '2'}]})])
        self.assertEqual(result['count'], 2)
        self.assertFalse(result['partial'])
        self.assertEqual(result['summary'], {'requests': 2, 'pages': 2, 'items_received': 2, 'stop_reason': 'no_next'})

    async def test_limit_5000_with_meta_batches_of_100(self):
        payloads = []
        for batch in range(50):
            payloads.append((200, {
                'data': [{'id': str(i)} for i in range(batch * 100, (batch + 1) * 100)],
                'paging': {'next': app.BASE_URL + f'/p/posts?after={batch + 1}&limit=100'},
            }))
        result, calls = await self.load(payloads, limit=5000)
        self.assertEqual(result['count'], 5000)
        self.assertEqual(len({p['id'] for p in result['posts']}), 5000)
        self.assertFalse(result['partial'])
        self.assertEqual(result['summary']['stop_reason'], 'limit_reached')
        self.assertEqual(len(calls), 50)
        self.assertTrue(all(int(r.url.params['limit']) <= 100 for r in calls))

    async def test_limit_5000_stops_when_source_exhausted(self):
        result, calls = await self.load([(200, {'data': [{'id': '1'}]})], limit=5000)
        self.assertEqual(result['count'], 1)
        self.assertFalse(result['partial'])
        self.assertEqual(len(calls), 1)

    async def test_empty_next_budget(self):
        payloads = [(200, {'data': [], 'paging': {'next': app.BASE_URL+f'/p/posts?after={i}'}}) for i in range(30)]
        with self.assertRaises(app.HTTPException) as caught:
            await self.load(payloads)
        self.assertIn('request_budget', caught.exception.detail)

    async def test_partial(self):
        result, calls = await self.load([(200, {'data': [{'id': '1'}], 'paging': {'next': app.BASE_URL+'/p/posts?after=a'}}), (500, {'error': {'message': 'SECRET'}})])
        self.assertEqual(result['posts'], [{'id': '1'}])
        self.assertTrue(result['partial'])
        self.assertNotIn('SECRET', str(result))

    async def test_empty_next_dedup_limit_and_params(self):
        nxt = app.BASE_URL+'/p/posts?after=a'
        result, calls = await self.load([(200, {'data': [], 'paging': {'next': nxt}}), (200, {'data': [{'id': '1'}, {'id': '1'}, {'id': '2'}, {'id': '3'}]})], limit=2, since='2026-01-01', until='2026-02-01')
        self.assertEqual([p['id'] for p in result['posts']], ['1', '2'])
        self.assertEqual(str(calls[1].url), nxt)
        self.assertEqual(calls[0].url.params['since'], '2026-01-01')
        self.assertEqual(calls[0].url.params['until'], '2026-02-01')
        self.assertEqual(calls[0].url.params['limit'], '2')

    async def test_empty_valid_and_cursor_only(self):
        result, calls = await self.load([(200, {'data': [], 'paging': {'cursors': {'after': 'a'}}})])
        self.assertEqual(result['posts'], [])
        self.assertFalse(result['partial'])
        self.assertEqual(len(calls), 1)

    async def test_repeated_cursor(self):
        result, calls = await self.load([(200, {'data': [{'id': '1'}], 'paging': {'next': app.BASE_URL+'/p/posts?after=a'}}), (200, {'data': [], 'paging': {'next': app.BASE_URL+'/p/posts?after=a&x=2'}})])
        self.assertTrue(result['partial'])
        self.assertEqual(len(calls), 2)

    async def test_validation(self):
        for kwargs in ({'limit': 0}, {'limit': 5001}, {'since': '2026-02-30'}, {'since': '2026-2-01'}, {'since': '2026-02-01', 'until': '2026-01-01'}):
            with self.subTest(kwargs=kwargs), self.assertRaises(app.HTTPException):
                await self.load([], **kwargs)


if __name__ == '__main__':
    unittest.main()
