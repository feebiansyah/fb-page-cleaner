import asyncio
import json
import unittest
from unittest.mock import patch
import httpx
import app


class PostsTests(unittest.IsolatedAsyncioTestCase):
    async def test_simultaneous_login_and_load_are_isolated(self):
        users = ['USER_A' * 12, 'USER_B' * 12]
        async def respond(request):
            await asyncio.sleep(0)
            token = request.url.params['access_token']
            if request.url.path.endswith('/me/accounts'):
                index = users.index(token)
                return httpx.Response(200, json={'data': [{'id': 'p', 'name': f'Page {index}', 'access_token': f'PAGE_{index}'}]})
            return httpx.Response(200, json={'data': [{'id': token + '_post'}]})
        factory = httpx.AsyncClient
        with patch.dict(app.SESSIONS, {}, clear=True), patch.object(app.time, 'time', return_value=1000), patch.object(app.httpx, 'AsyncClient', side_effect=lambda **kw: factory(transport=httpx.MockTransport(respond), **kw)):
            logins = await asyncio.gather(*(app.login(user) for user in users))
            self.assertNotEqual(logins[0]['session_id'], logins[1]['session_id'])
            for i, login in enumerate(logins):
                stored = app.SESSIONS[login['session_id']]
                self.assertEqual(stored['user_token'], users[i])
                self.assertEqual(stored['pages'][0]['access_token'], f'PAGE_{i}')
                self.assertEqual(login['pages'][0]['name'], f'Page {i}')
            results = await asyncio.gather(*(app.get_posts(login['session_id'], 'p') for login in logins))
            self.assertEqual([r['posts'][0]['id'] for r in results], ['PAGE_0_post', 'PAGE_1_post'])

    async def test_session_id_collision_does_not_overwrite(self):
        factory = httpx.AsyncClient
        transport = httpx.MockTransport(lambda req: httpx.Response(200, json={'data': []}))
        with patch.dict(app.SESSIONS, {'existing': {'sentinel': True}}, clear=True), patch.object(app.secrets, 'token_urlsafe', side_effect=['existing', 'fresh']) as generate, patch.object(app.httpx, 'AsyncClient', side_effect=lambda **kw: factory(transport=transport, **kw)):
            result = await app.login('X' * 60)
            self.assertEqual(result['session_id'], 'fresh')
            self.assertEqual(app.SESSIONS['existing'], {'sentinel': True})
            self.assertEqual(generate.call_args_list, [unittest.mock.call(32), unittest.mock.call(32)])

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

    async def test_upstream_http_and_graph_diagnostics(self):
        for status in (403, 429, 500, 200):
            with self.subTest(status=status), self.assertRaises(app.HTTPException) as caught:
                await self.load([(status, {'error': {'code': 190, 'error_subcode': 463, 'message': 'Permission denied SECRET https://graph.facebook.com/path?access_token=OTHER access_token=OTHER'}})])
            body = json.loads(caught.exception.detail)
            diagnostic = body['diagnosis']
            self.assertEqual(diagnostic['upstream_status'], status)
            self.assertEqual(diagnostic['meta_code'], 190)
            self.assertEqual(diagnostic['meta_subcode'], 463)
            self.assertIn('Permission denied', body['detail'])
            for secret in ('SECRET', 'OTHER', 'graph.facebook.com', 'https://'):
                self.assertNotIn(secret, str(body))
            self.assertEqual(body['summary']['stop_reason'], 'graph_error' if status == 200 else 'meta_http_error')

    async def test_timeout_connection_and_invalid_diagnostics(self):
        for payload, reason in [(httpx.ReadTimeout('SECRET'), 'timeout'), (httpx.ConnectError('SECRET'), 'connection_error'), (b'invalid', 'invalid_response'), ((200, {'data': [], 'paging': {'next': 'http://bad/SECRET'}}), 'invalid_pagination')]:
            with self.subTest(reason=reason), self.assertRaises(app.HTTPException) as caught:
                await self.load([payload])
            body = json.loads(caught.exception.detail)
            self.assertEqual(body['summary']['stop_reason'], reason)
            self.assertEqual(body['diagnosis']['kind'], reason)
            self.assertNotIn('SECRET', str(body))

    async def test_http_error_without_graph_error_retains_status(self):
        for status in (403, 429, 500):
            with self.subTest(status=status), self.assertRaises(app.HTTPException) as caught:
                await self.load([(status, {})])
            body = json.loads(caught.exception.detail)
            self.assertEqual(body['diagnosis']['upstream_status'], status)
            self.assertEqual(body['diagnosis']['kind'], 'meta_http_error')
            self.assertIsNone(body['diagnosis']['meta_code'])

    async def test_encoded_tokens_and_urls_are_redacted(self):
        message = 'Permission denied %53%45%43%52%45%54 https%3A%2F%2Fgraph.facebook.com%2Fp%3Faccess_token%3DOTHER "access_token": "PRIVATE"'
        with self.assertRaises(app.HTTPException) as caught:
            await self.load([(403, {'error': {'message': message}})])
        body = json.loads(caught.exception.detail)
        self.assertIn('Permission denied', body['detail'])
        for secret in ('SECRET', 'OTHER', 'PRIVATE', '%53', 'graph.facebook.com'):
            self.assertNotIn(secret, str(body))

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
        self.assertEqual(result['diagnosis']['upstream_status'], 500)
        self.assertEqual(result['summary']['stop_reason'], 'meta_http_error')

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
        self.assertEqual(result['diagnosis']['kind'], 'repeated_pagination')

    async def test_validation(self):
        for kwargs in ({'limit': 0}, {'limit': 5001}, {'since': '2026-02-30'}, {'since': '2026-2-01'}, {'since': '2026-02-01', 'until': '2026-01-01'}):
            with self.subTest(kwargs=kwargs), self.assertRaises(app.HTTPException):
                await self.load([], **kwargs)


if __name__ == '__main__':
    unittest.main()
