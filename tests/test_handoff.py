import json
import tempfile
import unittest
from pathlib import Path

from warehouse import handoff
from warehouse.catalog import connect, load_bundle
from warehouse.domain import overview

ROWS = [
    {'asset': 'a1', 'department': 'd1', 'status': 'active'},
    {'asset': 'a2', 'department': 'd2', 'status': 'active'},
    {'asset': 'a3', 'department': 'd1', 'status': 'retired'},
]


def make_pages(generation, rows, size=2):
    pages = []
    for index in range(0, len(rows), size):
        chunk = rows[index:index + size]
        page_no = len(pages) + 1
        pages.append({'generation': generation, 'page_no': page_no,
                      'digest': handoff.page_digest(chunk),
                      'cursor': f'g{generation}-c{page_no}', 'rows': chunk})
    manifest = [{'page_no': page['page_no'], 'digest': page['digest'], 'cursor': page['cursor']}
                for page in pages]
    return pages, manifest


def upsert(position, asset, department='d1', status='active'):
    return {'position': position, 'op': 'upsert', 'asset': asset,
            'department': department, 'status': status}


def delete(position, asset):
    return {'position': position, 'op': 'delete', 'asset': asset}


def live(connection):
    return {row['asset']: dict(row) for row in connection.execute('SELECT * FROM assets')}


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / 'warehouse.sqlite3'
        self.connection = connect(self.path)
        self.pages, self.manifest = make_pages(1, ROWS)

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def begin(self, start=0, end=0, manifest=None, generation=1):
        return handoff.begin_handoff(self.connection, 'assets', start, end,
                                     self.manifest if manifest is None else manifest, generation)

    def test_happy_path_publish_matches_end_watermark_state(self):
        self.begin(start=0, end=2)
        for page in self.pages:
            handoff.accept_page(self.connection, page)
        handoff.buffer_log(self.connection, 1, [upsert(1, 'a2', 'd9'), delete(2, 'a3')])
        result = handoff.publish(self.connection, 1)
        self.assertTrue(result['published'])
        self.assertEqual(result['watermark'], 2)
        self.assertEqual(live(self.connection), {
            'a1': {'asset': 'a1', 'department': 'd1', 'status': 'active'},
            'a2': {'asset': 'a2', 'department': 'd9', 'status': 'active'}})

    def test_out_of_order_duplicate_and_conflicting_pages(self):
        self.begin()
        result = handoff.accept_page(self.connection, self.pages[1])
        self.assertTrue(result['stored'])
        self.assertEqual(result['missing_pages'], [1])
        again = handoff.accept_page(self.connection, self.pages[1])
        self.assertFalse(again['stored'])
        self.assertTrue(again['duplicate'])
        tampered = dict(self.pages[1], rows=[{'asset': 'aX', 'department': 'd1', 'status': 'active'}])
        with self.assertRaises(ValueError):
            handoff.accept_page(self.connection, tampered)
        status = handoff.handoff_status(self.connection, 1)
        self.assertEqual(status['pages']['received'], 1)
        unknown = dict(self.pages[0], page_no=9)
        with self.assertRaises(ValueError):
            handoff.accept_page(self.connection, unknown)

    def test_publish_waits_on_missing_pages(self):
        self.begin()
        handoff.accept_page(self.connection, self.pages[0])
        waiting = handoff.publish(self.connection, 1)
        self.assertFalse(waiting['published'])
        self.assertEqual(waiting['reason'], 'pages_missing')
        self.assertEqual(waiting['missing'], [2])
        self.assertEqual(live(self.connection), {})

    def test_publish_waits_on_log_gap_then_completes(self):
        self.begin(start=0, end=3)
        for page in self.pages:
            handoff.accept_page(self.connection, page)
        handoff.buffer_log(self.connection, 1, [upsert(1, 'a1'), upsert(3, 'a4')])
        waiting = handoff.publish(self.connection, 1)
        self.assertFalse(waiting['published'])
        self.assertEqual(waiting['reason'], 'log_gap')
        self.assertEqual(waiting['missing'], [2])
        handoff.buffer_log(self.connection, 1, [delete(2, 'a3')])
        self.assertTrue(handoff.publish(self.connection, 1)['published'])
        self.assertNotIn('a3', live(self.connection))
        self.assertIn('a4', live(self.connection))

    def test_log_buffer_skips_pre_start_and_detects_conflict(self):
        self.begin(start=5, end=7)
        result = handoff.buffer_log(self.connection, 1, [upsert(3, 'aX'), upsert(6, 'a6')])
        self.assertEqual(result['stored'], 1)
        self.assertEqual(result['skipped'], 1)
        result = handoff.buffer_log(self.connection, 1, [upsert(6, 'a6')])
        self.assertEqual(result['skipped'], 1)
        with self.assertRaises(ValueError):
            handoff.buffer_log(self.connection, 1, [delete(6, 'a6')])

    def test_cursor_expired_fails_run_and_half_table_not_published(self):
        self.begin()
        handoff.accept_page(self.connection, self.pages[0])
        with self.assertRaises(ValueError):
            handoff.accept_page(self.connection, {'generation': 1, 'page_no': 2, 'error': 'cursor_expired'})
        status = handoff.handoff_status(self.connection, 1)
        self.assertEqual(status['phase'], 'failed')
        self.assertIn('cursor_expired', status['failure'])
        with self.assertRaises(ValueError):
            handoff.publish(self.connection, 1)
        self.assertEqual(live(self.connection), {})

    def test_abort_run_is_explicit_failure(self):
        self.begin()
        status = handoff.abort_run(self.connection, 1, '缺页无法补齐')
        self.assertEqual(status['phase'], 'failed')
        with self.assertRaises(ValueError):
            handoff.accept_page(self.connection, self.pages[0])
        with self.assertRaises(ValueError):
            handoff.publish(self.connection, 1)

    def test_old_table_serves_until_publish(self):
        load_bundle(self.connection, {'assets': [dict(row) for row in ROWS]}, 'baseline')
        before = overview(self.connection)
        self.begin(start=0, end=1)
        for page in self.pages:
            handoff.accept_page(self.connection, page)
        handoff.buffer_log(self.connection, 1, [delete(1, 'a1')])
        self.assertEqual(overview(self.connection), before)
        preview = handoff.preview_diff(self.connection, 1)
        self.assertEqual((preview['added'], preview['removed'], preview['updated']), (0, 1, 0))
        handoff.publish(self.connection, 1)
        self.assertNotEqual(overview(self.connection), before)
        self.assertNotIn('a1', live(self.connection))

    def test_crash_recovery_resumes_from_persisted_record(self):
        self.begin(start=0, end=1)
        handoff.accept_page(self.connection, self.pages[0])
        handoff.buffer_log(self.connection, 1, [upsert(1, 'a9')])
        self.connection.close()
        self.connection = connect(self.path)
        resumed = handoff.active_handoff(self.connection)
        self.assertEqual(resumed['generation'], 1)
        self.assertEqual(resumed['pages']['missing'], [2])
        self.assertEqual(resumed['log']['contiguous_until'], 1)
        handoff.accept_page(self.connection, self.pages[1])
        self.assertTrue(handoff.publish(self.connection, 1)['published'])
        self.assertIn('a9', live(self.connection))
        self.assertIsNone(handoff.active_handoff(self.connection))

    def test_publish_is_idempotent_for_retry(self):
        self.begin()
        for page in self.pages:
            handoff.accept_page(self.connection, page)
        self.assertTrue(handoff.publish(self.connection, 1)['published'])
        again = handoff.publish(self.connection, 1)
        self.assertTrue(again['published'])
        self.assertTrue(again['already'])

    def test_superseded_generation_late_pages_rejected(self):
        self.begin(generation=1)
        handoff.accept_page(self.connection, self.pages[0])
        pages2, manifest2 = make_pages(2, ROWS[:1])
        plan = handoff.begin_handoff(self.connection, 'assets', 0, 0, manifest2, 2)
        self.assertEqual(plan['superseded'], 1)
        with self.assertRaises(ValueError):
            handoff.accept_page(self.connection, self.pages[1])
        handoff.accept_page(self.connection, pages2[0])
        self.assertTrue(handoff.publish(self.connection, 2)['published'])
        self.assertEqual(set(live(self.connection)), {'a1'})

    def test_regeneration_does_not_resurrect_deleted_assets(self):
        self.begin(start=0, end=0)
        for page in self.pages:
            handoff.accept_page(self.connection, page)
        handoff.publish(self.connection, 1)
        rows2 = [dict(ROWS[0]), dict(ROWS[1])]  # a3 已在来源删除，不在新快照
        pages2, manifest2 = make_pages(2, rows2)
        handoff.begin_handoff(self.connection, 'assets', 1, 2, manifest2, 2)
        for page in pages2:
            handoff.accept_page(self.connection, page)
        handoff.buffer_log(self.connection, 2, [delete(2, 'a2')])  # 分页期间删除 a2
        handoff.publish(self.connection, 2)
        self.assertEqual(set(live(self.connection)), {'a1'})
        with self.assertRaises(ValueError):
            handoff.accept_page(self.connection, self.pages[0])  # 旧代次迟到页
        self.assertEqual(set(live(self.connection)), {'a1'})

    def test_replay_old_pages_after_publish_does_not_roll_back(self):
        self.begin(start=0, end=1)
        for page in self.pages:
            handoff.accept_page(self.connection, page)
        handoff.buffer_log(self.connection, 1, [delete(1, 'a1')])
        handoff.publish(self.connection, 1)
        snapshot = live(self.connection)
        self.assertNotIn('a1', snapshot)
        with self.assertRaises(ValueError):
            handoff.accept_page(self.connection, self.pages[0])  # 含 a1 的旧页重投
        with self.assertRaises(ValueError):
            handoff.accept_page(self.connection, self.pages[1])
        self.assertEqual(live(self.connection), snapshot)

    def test_empty_table_handoff(self):
        handoff.begin_handoff(self.connection, 'assets', 0, 1, [], 1)
        status = handoff.handoff_status(self.connection, 1)
        self.assertEqual(status['pages']['total'], 0)
        self.assertEqual(status['progress'], 1.0)
        handoff.buffer_log(self.connection, 1, [upsert(1, 'a1')])
        result = handoff.publish(self.connection, 1)
        self.assertTrue(result['published'])
        self.assertEqual(result['rows'], 1)
        self.assertEqual(set(live(self.connection)), {'a1'})

    def test_incremental_requires_publish_and_contiguity(self):
        with self.assertRaises(ValueError):
            handoff.apply_incremental(self.connection, 'assets', [upsert(1, 'a1')])
        self.begin(start=0, end=1)
        for page in self.pages:
            handoff.accept_page(self.connection, page)
        handoff.buffer_log(self.connection, 1, [upsert(1, 'a1', 'd2')])
        handoff.publish(self.connection, 1)
        waiting = handoff.apply_incremental(self.connection, 'assets', [upsert(3, 'a3', 'd3')])
        self.assertEqual(waiting['applied'], 0)
        self.assertTrue(waiting['waiting'])
        self.assertEqual(waiting['expected_next'], 2)
        result = handoff.apply_incremental(self.connection, 'assets', [upsert(2, 'a2', 'd2')])
        self.assertEqual(result['applied'], 2)
        self.assertEqual(result['watermark'], 3)
        self.assertFalse(result['waiting'])
        self.assertEqual(live(self.connection)['a3']['department'], 'd3')
        replay = handoff.apply_incremental(self.connection, 'assets', [upsert(2, 'a2', 'd9')])
        self.assertEqual(replay['applied'], 0)
        self.assertEqual(live(self.connection)['a2']['department'], 'd2')

    def test_incremental_conflict_detection(self):
        self.begin()
        for page in self.pages:
            handoff.accept_page(self.connection, page)
        handoff.publish(self.connection, 1)
        handoff.apply_incremental(self.connection, 'assets', [upsert(1, 'a1')])
        with self.assertRaises(ValueError):
            handoff.apply_incremental(self.connection, 'assets', [delete(2, 'a2'), upsert(2, 'a2')])

    def test_status_reports_progress_and_gaps(self):
        self.begin(start=0, end=3)
        handoff.accept_page(self.connection, self.pages[0])
        handoff.buffer_log(self.connection, 1, [upsert(1, 'a1'), upsert(3, 'a3')])
        status = handoff.handoff_status(self.connection, 1)
        self.assertEqual(status['progress'], 0.5)
        self.assertEqual(status['pages']['missing'], [2])
        self.assertEqual(status['log']['contiguous_until'], 1)
        self.assertEqual(status['log']['gaps'], [2])
        self.assertEqual(handoff.active_handoff(self.connection)['generation'], 1)

    def test_invalid_inputs_rejected(self):
        with self.assertRaises(ValueError):
            handoff.begin_handoff(self.connection, 'unknown', 0, 0, [], 1)
        with self.assertRaises(ValueError):
            handoff.begin_handoff(self.connection, 'assets', 2, 1, [], 1)
        with self.assertRaises(ValueError):
            handoff.begin_handoff(self.connection, 'assets', 0, 0, [{'page_no': 2, 'digest': 'x' * 64, 'cursor': 'c'}], 1)
        self.begin()
        with self.assertRaises(ValueError):
            handoff.begin_handoff(self.connection, 'assets', 0, 0, [], 1)  # 代次必须递增
        with self.assertRaises(ValueError):
            handoff.accept_page(self.connection, {'generation': 99, 'page_no': 1})
        with self.assertRaises(ValueError):
            handoff.accept_page(self.connection, dict(self.pages[0], cursor='wrong'))
        with self.assertRaises(ValueError):
            handoff.buffer_log(self.connection, 1, [{'position': 0, 'op': 'upsert', 'asset': 'a1',
                                                   'department': 'd1', 'status': 'active'}])

    def test_overlapping_primary_key_across_pages_rejected(self):
        pages = [
            {'generation': 1, 'page_no': 1, 'digest': handoff.page_digest([ROWS[0]]),
             'cursor': 'c1', 'rows': [ROWS[0]]},
            {'generation': 1, 'page_no': 2, 'digest': handoff.page_digest([ROWS[0]]),
             'cursor': 'c2', 'rows': [ROWS[0]]},
        ]
        manifest = [{'page_no': 1, 'digest': pages[0]['digest'], 'cursor': 'c1'},
                    {'page_no': 2, 'digest': pages[1]['digest'], 'cursor': 'c2'}]
        handoff.begin_handoff(self.connection, 'assets', 0, 0, manifest, 1)
        handoff.accept_page(self.connection, pages[0])
        with self.assertRaises(ValueError):
            handoff.accept_page(self.connection, pages[1])
        status = handoff.handoff_status(self.connection, 1)
        self.assertEqual(status['pages']['missing'], [2])


if __name__ == '__main__':
    unittest.main()
