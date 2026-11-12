"""全量与增量交接协议。

首次接仓（或重新全量）时，来源给出起始水位与结束水位：全量页按同步代次
分页交付，起始水位之后的变更日志先缓冲；只有页清单完整且摘要通过，才把
临时 ODS 与应补日志合并发布，随后转入常规增量。各阶段状态全部持久化，
分页、缓冲、合并、切换任一处中断都可按代次恢复；重新全量时旧代次迟到页
被拒绝，已删除资产不会复活；旧表在新表发布前继续服务。
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timezone

from .catalog import encode, transaction
from .definition import SCHEMA

MAX_REPORTED_GAPS = 50

DDL = (
    'CREATE TABLE IF NOT EXISTS handoff_runs ('
    'generation INTEGER PRIMARY KEY, table_name TEXT NOT NULL, phase TEXT NOT NULL, '
    'start_watermark INTEGER NOT NULL, end_watermark INTEGER NOT NULL, '
    'manifest TEXT NOT NULL, failure TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)',
    'CREATE TABLE IF NOT EXISTS handoff_pages ('
    'generation INTEGER NOT NULL, page_no INTEGER NOT NULL, digest TEXT NOT NULL, '
    'cursor TEXT NOT NULL, payload TEXT NOT NULL, rows INTEGER NOT NULL, '
    'PRIMARY KEY (generation, page_no))',
    'CREATE TABLE IF NOT EXISTS handoff_staging ('
    'generation INTEGER NOT NULL, table_name TEXT NOT NULL, key TEXT NOT NULL, '
    'payload TEXT NOT NULL, PRIMARY KEY (generation, table_name, key))',
    'CREATE TABLE IF NOT EXISTS handoff_log_buffer ('
    'generation INTEGER NOT NULL, position INTEGER NOT NULL, payload TEXT NOT NULL, '
    'PRIMARY KEY (generation, position))',
    'CREATE TABLE IF NOT EXISTS handoff_watermarks ('
    'table_name TEXT PRIMARY KEY, generation INTEGER NOT NULL, position INTEGER NOT NULL)',
)


def ensure_tables(connection):
    for statement in DDL:
        connection.execute(statement)


def page_digest(rows):
    return hashlib.sha256(encode(rows)).hexdigest()


def _now():
    return datetime.now(timezone.utc).isoformat()


def _columns(table):
    if table not in SCHEMA:
        raise ValueError('来源包含未登记的数据集合')
    return [part.strip().split()[0].strip('"') for part in SCHEMA[table].split(',')]


def _is_digest(value):
    return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


def _check_manifest(manifest):
    if not isinstance(manifest, list):
        raise ValueError('页清单必须是数组')
    seen = set()
    for entry in manifest:
        if (not isinstance(entry, dict)
                or not isinstance(entry.get('page_no'), int) or isinstance(entry.get('page_no'), bool)
                or entry['page_no'] < 1 or not _is_digest(entry.get('digest'))
                or not isinstance(entry.get('cursor'), str) or not entry['cursor']):
            raise ValueError('页清单条目格式错误')
        if entry['page_no'] in seen:
            raise ValueError('页清单页号重复')
        seen.add(entry['page_no'])
    if seen != set(range(1, len(manifest) + 1)):
        raise ValueError('页清单页号必须从 1 连续编号')


def _check_row(row, columns):
    if not isinstance(row, dict) or set(row) != set(columns):
        raise ValueError('记录字段与已登记结构不一致')
    if not all(isinstance(row[column], str) for column in columns):
        raise ValueError('记录字段类型必须为文本')


def _check_entry(entry, columns):
    key = columns[0]
    if not isinstance(entry, dict) or not set(entry) <= ({'position', 'op'} | set(columns)):
        raise ValueError('日志条目格式错误')
    position = entry.get('position')
    if not isinstance(position, int) or isinstance(position, bool) or position < 1:
        raise ValueError('日志位置必须为正整数')
    if entry.get('op') not in ('upsert', 'delete'):
        raise ValueError('日志操作类型未知')
    if entry['op'] == 'upsert':
        if not all(isinstance(entry.get(column), str) for column in columns):
            raise ValueError('更新日志缺少字段')
    elif not isinstance(entry.get(key), str):
        raise ValueError('删除日志缺少主键')


def _run(connection, generation):
    row = connection.execute('SELECT * FROM handoff_runs WHERE generation=?', (generation,)).fetchone()
    if row is None:
        raise ValueError('交接代次不存在')
    return row


def _manifest_index(run):
    return {entry['page_no']: entry for entry in json.loads(run['manifest'])}


def _missing_pages(connection, generation, index):
    received = {row['page_no'] for row in connection.execute(
        'SELECT page_no FROM handoff_pages WHERE generation=?', (generation,))}
    return [page_no for page_no in sorted(index) if page_no not in received]


def _log_coverage(connection, run):
    start, end = run['start_watermark'], run['end_watermark']
    have = {row['position'] for row in connection.execute(
        'SELECT position FROM handoff_log_buffer WHERE generation=? AND position>? AND position<=?',
        (run['generation'], start, end))}
    gaps = [position for position in range(start + 1, end + 1) if position not in have]
    contiguous = start
    while contiguous + 1 in have:
        contiguous += 1
    return contiguous, gaps


def _merged_view(connection, run, upto):
    columns = _columns(run['table_name'])
    key = columns[0]
    state = {}
    for row in connection.execute(
            'SELECT key, payload FROM handoff_staging WHERE generation=? AND table_name=?',
            (run['generation'], run['table_name'])):
        state[row['key']] = json.loads(row['payload'])
    for row in connection.execute(
            'SELECT payload FROM handoff_log_buffer WHERE generation=? AND position<=? ORDER BY position',
            (run['generation'], upto)):
        entry = json.loads(row['payload'])
        if entry['op'] == 'upsert':
            state[entry[key]] = {column: entry[column] for column in columns}
        else:
            state.pop(entry[key], None)
    return state


def begin_handoff(connection, table, start_watermark, end_watermark, manifest, generation=None):
    """开启一次全量交接；若同表有进行中的旧代次，旧代次被替换（superseded）。"""
    ensure_tables(connection)
    _columns(table)
    if (not isinstance(start_watermark, int) or isinstance(start_watermark, bool)
            or not isinstance(end_watermark, int) or isinstance(end_watermark, bool)
            or start_watermark < 0 or end_watermark < start_watermark):
        raise ValueError('水位范围无效')
    _check_manifest(manifest)
    now = _now()
    with transaction(connection):
        row = connection.execute(
            'SELECT MAX(generation) AS latest FROM handoff_runs WHERE table_name=?', (table,)).fetchone()
        latest = row['latest'] or 0
        if generation is None:
            generation = latest + 1
        elif not isinstance(generation, int) or isinstance(generation, bool) or generation <= latest:
            raise ValueError('同步代次必须大于已有代次')
        active = connection.execute(
            "SELECT generation FROM handoff_runs WHERE table_name=? AND phase='paging'", (table,)).fetchone()
        superseded = None
        if active:
            superseded = active['generation']
            connection.execute(
                "UPDATE handoff_runs SET phase='superseded', updated_at=? WHERE generation=?",
                (now, superseded))
        connection.execute(
            'INSERT INTO handoff_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (generation, table, 'paging', start_watermark, end_watermark,
             json.dumps(manifest, ensure_ascii=False, sort_keys=True), None, now, now))
    return {'generation': generation, 'table': table, 'phase': 'paging',
            'start_watermark': start_watermark, 'end_watermark': end_watermark,
            'pages_total': len(manifest), 'superseded': superseded}


def accept_page(connection, page):
    """接收一页全量数据。乱序允许；同内容重投幂等跳过；同页号不同内容报错；
    旧代次迟到页、已发布代次重投页均被拒绝。来源报告错误（如游标过期）时代次失败。"""
    ensure_tables(connection)
    if not isinstance(page, dict) or not isinstance(page.get('generation'), int):
        raise ValueError('页缺少同步代次')
    generation = page['generation']
    run = _run(connection, generation)
    columns = _columns(run['table_name'])
    key = columns[0]
    if run['phase'] == 'superseded':
        raise ValueError('旧代次已被新全量替换，迟到页拒绝接收')
    if run['phase'] == 'published':
        raise ValueError('代次已发布，重投旧页拒绝接收')
    if run['phase'] == 'failed':
        raise ValueError('代次已失败：' + (run['failure'] or ''))
    index = _manifest_index(run)
    if 'error' in page:
        reason = str(page['error'])
        with transaction(connection):
            connection.execute(
                "UPDATE handoff_runs SET phase='failed', failure=?, updated_at=? WHERE generation=?",
                (reason, _now(), generation))
        raise ValueError(f'来源报告分页读取失败：{reason}')
    page_no = page.get('page_no')
    if page_no not in index:
        raise ValueError('页号不在页清单内')
    rows = page.get('rows')
    if not isinstance(rows, list):
        raise ValueError('页记录必须是数组')
    for row in rows:
        _check_row(row, columns)
    expected = index[page_no]
    if page.get('digest') != expected['digest'] or page_digest(rows) != expected['digest']:
        raise ValueError('同页号内容与清单摘要不一致')
    if page.get('cursor') != expected['cursor']:
        raise ValueError('续读游标与清单不一致')
    duplicate = False
    with transaction(connection):
        existing = connection.execute(
            'SELECT digest FROM handoff_pages WHERE generation=? AND page_no=?',
            (generation, page_no)).fetchone()
        if existing:
            duplicate = True
        else:
            payload = json.dumps(rows, ensure_ascii=False, sort_keys=True)
            connection.execute(
                'INSERT INTO handoff_pages VALUES (?, ?, ?, ?, ?, ?)',
                (generation, page_no, expected['digest'], expected['cursor'], payload, len(rows)))
            for row in rows:
                try:
                    connection.execute(
                        'INSERT INTO handoff_staging VALUES (?, ?, ?, ?)',
                        (generation, run['table_name'], row[key],
                         json.dumps(row, ensure_ascii=False, sort_keys=True)))
                except sqlite3.IntegrityError:
                    raise ValueError('页间主键重叠') from None
            connection.execute(
                'UPDATE handoff_runs SET updated_at=? WHERE generation=?', (_now(), generation))
    return {'stored': not duplicate, 'duplicate': duplicate, 'generation': generation,
            'page_no': page_no, 'rows': len(rows),
            'missing_pages': _missing_pages(connection, generation, index)}


def buffer_log(connection, generation, entries):
    """缓冲起始水位之后的变更日志（含更新与删除）。位置可比较、严格递增；
    同位置同内容幂等跳过，同位置不同内容报错。"""
    ensure_tables(connection)
    run = _run(connection, generation)
    if run['phase'] != 'paging':
        raise ValueError('当前阶段不再缓冲日志，发布后请走常规增量')
    if not isinstance(entries, list):
        raise ValueError('日志必须是数组')
    columns = _columns(run['table_name'])
    stored = skipped = 0
    with transaction(connection):
        for entry in entries:
            _check_entry(entry, columns)
            if entry['position'] <= run['start_watermark']:
                skipped += 1
                continue
            existing = connection.execute(
                'SELECT payload FROM handoff_log_buffer WHERE generation=? AND position=?',
                (generation, entry['position'])).fetchone()
            if existing:
                if json.loads(existing['payload']) != entry:
                    raise ValueError('日志位置内容冲突')
                skipped += 1
                continue
            connection.execute(
                'INSERT INTO handoff_log_buffer VALUES (?, ?, ?)',
                (generation, entry['position'], json.dumps(entry, ensure_ascii=False, sort_keys=True)))
            stored += 1
        connection.execute(
            'UPDATE handoff_runs SET updated_at=? WHERE generation=?', (_now(), generation))
    contiguous, gaps = _log_coverage(connection, _run(connection, generation))
    return {'stored': stored, 'skipped': skipped, 'contiguous_until': contiguous,
            'gaps': gaps[:MAX_REPORTED_GAPS], 'gaps_total': len(gaps)}


def handoff_status(connection, generation=None, table='assets'):
    """进度与缺口：页接收进度、缺失页号、日志连续覆盖位置与缺口列表。"""
    ensure_tables(connection)
    if generation is None:
        run = connection.execute(
            'SELECT * FROM handoff_runs WHERE table_name=? ORDER BY generation DESC LIMIT 1',
            (table,)).fetchone()
        if run is None:
            return {'table': table, 'phase': None}
    else:
        run = _run(connection, generation)
    index = _manifest_index(run)
    missing = _missing_pages(connection, run['generation'], index)
    contiguous, gaps = _log_coverage(connection, run)
    buffered = connection.execute(
        'SELECT COUNT(*) AS n FROM handoff_log_buffer WHERE generation=?',
        (run['generation'],)).fetchone()['n']
    total = len(index)
    return {'table': run['table_name'], 'generation': run['generation'], 'phase': run['phase'],
            'failure': run['failure'],
            'start_watermark': run['start_watermark'], 'end_watermark': run['end_watermark'],
            'pages': {'received': total - len(missing), 'total': total, 'missing': missing},
            'log': {'buffered': buffered, 'contiguous_until': contiguous,
                    'gaps': gaps[:MAX_REPORTED_GAPS], 'gaps_total': len(gaps)},
            'progress': (total - len(missing)) / total if total else 1.0}


def preview_diff(connection, generation):
    """预览差异：假设现在发布，新表相对当前服务中旧表的新增、删除、更新。"""
    ensure_tables(connection)
    run = _run(connection, generation)
    columns = _columns(run['table_name'])
    key = columns[0]
    merged = _merged_view(connection, run, run['end_watermark'])
    live = {row[key]: dict(row) for row in connection.execute(f'SELECT * FROM "{run["table_name"]}"')}
    added = sorted(k for k in merged if k not in live)
    removed = sorted(k for k in live if k not in merged)
    updated = sorted(k for k in merged if k in live and live[k] != merged[k])
    return {'generation': generation, 'phase': run['phase'], 'final_rows': len(merged),
            'added': len(added), 'removed': len(removed), 'updated': len(updated),
            'samples': {'added': added[:3], 'removed': removed[:3],
                        'updated': [{'key': k, 'from': live[k], 'to': merged[k]} for k in updated[:3]]}}


def publish(connection, generation):
    """页清单完整且摘要通过、结束水位前日志无缺口时，合并临时 ODS 与应补日志并
    原子切换发布；缺页或日志缺口返回等待状态，绝不把半表当成功。"""
    ensure_tables(connection)
    run = _run(connection, generation)
    if run['phase'] == 'published':
        watermark = connection.execute(
            'SELECT position FROM handoff_watermarks WHERE table_name=?',
            (run['table_name'],)).fetchone()
        return {'published': True, 'already': True, 'generation': generation,
                'watermark': watermark['position'] if watermark else None}
    if run['phase'] == 'superseded':
        raise ValueError('旧代次已被替换，不能发布')
    if run['phase'] == 'failed':
        raise ValueError('代次已失败：' + (run['failure'] or ''))
    index = _manifest_index(run)
    missing = _missing_pages(connection, generation, index)
    if missing:
        return {'published': False, 'reason': 'pages_missing', 'missing': missing}
    for entry in index.values():
        stored = connection.execute(
            'SELECT digest, payload FROM handoff_pages WHERE generation=? AND page_no=?',
            (generation, entry['page_no'])).fetchone()
        if (stored['digest'] != entry['digest']
                or page_digest(json.loads(stored['payload'])) != entry['digest']):
            raise ValueError('页摘要校验失败，拒绝合并')
    contiguous, gaps = _log_coverage(connection, run)
    if gaps:
        return {'published': False, 'reason': 'log_gap', 'contiguous_until': contiguous,
                'missing': gaps[:MAX_REPORTED_GAPS], 'missing_total': len(gaps)}
    table = run['table_name']
    columns = _columns(table)
    merged = _merged_view(connection, run, run['end_watermark'])
    names = ', '.join(f'"{column}"' for column in columns)
    placeholders = ', '.join('?' for column in columns)
    with transaction(connection):
        connection.execute(f'DELETE FROM "{table}"')
        for key in sorted(merged):
            connection.execute(f'INSERT INTO "{table}" ({names}) VALUES ({placeholders})',
                               [merged[key][column] for column in columns])
        connection.execute(
            "UPDATE handoff_runs SET phase='published', updated_at=? WHERE generation=?",
            (_now(), generation))
        connection.execute(
            'INSERT OR REPLACE INTO handoff_watermarks VALUES (?, ?, ?)',
            (table, generation, run['end_watermark']))
        connection.execute(
            'DELETE FROM handoff_log_buffer WHERE generation=? AND position<=?',
            (generation, run['end_watermark']))
    return {'published': True, 'already': False, 'generation': generation, 'table': table,
            'rows': len(merged), 'watermark': run['end_watermark']}


def apply_incremental(connection, table='assets', entries=()):
    """发布后的常规增量：从当前水位起严格连续应用；缺口等待，不跳跃。"""
    ensure_tables(connection)
    columns = _columns(table)
    key = columns[0]
    watermark = connection.execute(
        'SELECT * FROM handoff_watermarks WHERE table_name=?', (table,)).fetchone()
    if watermark is None:
        raise ValueError('尚未发布全量，不能转常规增量')
    generation, position = watermark['generation'], watermark['position']
    if not isinstance(entries, (list, tuple)):
        raise ValueError('日志必须是数组')
    for entry in entries:
        _check_entry(entry, columns)
    with transaction(connection):
        for entry in entries:
            if entry['position'] <= position:
                continue
            existing = connection.execute(
                'SELECT payload FROM handoff_log_buffer WHERE generation=? AND position=?',
                (generation, entry['position'])).fetchone()
            if existing:
                if json.loads(existing['payload']) != entry:
                    raise ValueError('日志位置内容冲突')
                continue
            connection.execute(
                'INSERT INTO handoff_log_buffer VALUES (?, ?, ?)',
                (generation, entry['position'], json.dumps(entry, ensure_ascii=False, sort_keys=True)))
        pending = [json.loads(row['payload']) for row in connection.execute(
            'SELECT payload FROM handoff_log_buffer WHERE generation=? AND position>? ORDER BY position',
            (generation, position))]
        expected = position + 1
        applied = 0
        for entry in pending:
            if entry['position'] != expected:
                break
            if entry['op'] == 'upsert':
                names = ', '.join(f'"{column}"' for column in columns)
                placeholders = ', '.join('?' for column in columns)
                connection.execute(f'INSERT OR REPLACE INTO "{table}" ({names}) VALUES ({placeholders})',
                                   [entry[column] for column in columns])
            else:
                connection.execute(f'DELETE FROM "{table}" WHERE "{key}"=?', (entry[key],))
            connection.execute(
                'DELETE FROM handoff_log_buffer WHERE generation=? AND position=?',
                (generation, expected))
            expected += 1
            applied += 1
        if applied:
            connection.execute(
                'UPDATE handoff_watermarks SET position=? WHERE table_name=?', (expected - 1, table))
    waiting = connection.execute(
        'SELECT COUNT(*) AS n FROM handoff_log_buffer WHERE generation=? AND position>=?',
        (generation, expected)).fetchone()['n'] > 0
    return {'applied': applied, 'watermark': expected - 1, 'waiting': waiting,
            'expected_next': expected}


def abort_run(connection, generation, reason):
    """明确失败：缺口不可恢复或游标过期重试无果时中止当前代次，半表不发布。"""
    ensure_tables(connection)
    run = _run(connection, generation)
    if run['phase'] != 'paging':
        raise ValueError('只有进行中的交接可以中止')
    with transaction(connection):
        connection.execute(
            "UPDATE handoff_runs SET phase='failed', failure=?, updated_at=? WHERE generation=?",
            (str(reason), _now(), generation))
    return handoff_status(connection, generation)


def active_handoff(connection, table='assets'):
    """恢复入口：返回进行中代次的持久交接记录（进度与缺口），无则返回 None。"""
    ensure_tables(connection)
    row = connection.execute(
        "SELECT generation FROM handoff_runs WHERE table_name=? AND phase='paging' "
        'ORDER BY generation DESC LIMIT 1', (table,)).fetchone()
    if row is None:
        return None
    return handoff_status(connection, row['generation'])
