"""全量与增量交接协议本地演示。

模拟来源端资产台账：分页快照（同步代次、页号、摘要、续读游标）+ 严格递增
的变更日志（更新与删除）。演示：更新发生在分页前后、删除、空表、续读恢复、
代次替换、游标过期、缺页与日志缺口等待，以及发布后重投旧页不倒退。每次
运行使用全新临时目录，最终状态与来源结束水位完整状态逐一核对。
"""

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from warehouse import handoff
from warehouse.catalog import connect
from warehouse.domain import overview

PAGE_SIZE = 2


class Source:
    """模拟来源：当前状态 + 位置严格递增的变更日志 + 分页快照读取。"""

    def __init__(self, rows=()):
        self.rows = {row['asset']: dict(row) for row in rows}
        self.clock = 0
        self.log = []
        self.generation = 0
        self.snapshots = {}

    def upsert(self, asset, department, status):
        self.rows[asset] = {'asset': asset, 'department': department, 'status': status}
        self.clock += 1
        self.log.append({'position': self.clock, 'op': 'upsert', 'asset': asset,
                         'department': department, 'status': status})

    def delete(self, asset):
        self.rows.pop(asset, None)
        self.clock += 1
        self.log.append({'position': self.clock, 'op': 'delete', 'asset': asset})

    def begin_snapshot(self):
        """固定起始水位，按当前状态切页并给出页清单（页号、摘要、续读游标）。"""
        self.generation += 1
        ordered = [dict(self.rows[key]) for key in sorted(self.rows)]
        pages = []
        for index in range(0, len(ordered), PAGE_SIZE):
            chunk = ordered[index:index + PAGE_SIZE]
            page_no = len(pages) + 1
            pages.append({'generation': self.generation, 'page_no': page_no,
                          'digest': handoff.page_digest(chunk),
                          'cursor': f'g{self.generation}-cursor-{page_no}', 'rows': chunk})
        manifest = [{'page_no': page['page_no'], 'digest': page['digest'], 'cursor': page['cursor']}
                    for page in pages]
        self.snapshots[self.generation] = {
            'start': self.clock, 'start_state': {key: dict(row) for key, row in self.rows.items()},
            'pages': pages, 'manifest': manifest, 'valid': True}
        return self.generation, self.clock, manifest

    def read_page(self, generation, page_no):
        snapshot = self.snapshots[generation]
        if not snapshot['valid']:
            return {'generation': generation, 'page_no': page_no, 'error': 'cursor_expired'}
        return dict(snapshot['pages'][page_no - 1])

    def invalidate(self, generation):
        self.snapshots[generation]['valid'] = False

    def read_log(self, after, upto=None):
        return [dict(entry) for entry in self.log
                if entry['position'] > after and (upto is None or entry['position'] <= upto)]

    def expected_at(self, generation, watermark):
        """结束水位时的完整状态：快照起始状态叠加 (start, watermark] 日志。"""
        snapshot = self.snapshots[generation]
        state = {key: dict(row) for key, row in snapshot['start_state'].items()}
        for entry in self.log:
            if snapshot['start'] < entry['position'] <= watermark:
                if entry['op'] == 'upsert':
                    state[entry['asset']] = {'asset': entry['asset'], 'department': entry['department'],
                                             'status': entry['status']}
                else:
                    state.pop(entry['asset'], None)
        return state


def live_state(connection):
    return {row['asset']: dict(row) for row in connection.execute('SELECT * FROM assets')}


def check(condition, message):
    if not condition:
        raise RuntimeError('校验失败：' + message)
    print('  ✓ ' + message, flush=True)


def expect_rejected(action, message):
    try:
        action()
    except ValueError as error:
        print(f'  ✓ {message}（拒绝：{error}）', flush=True)
        return
    raise RuntimeError('校验失败：' + message + '（未被拒绝）')


def scenario_main():
    print('场景一：分页前后更新、删除、乱序/重复/冲突页、崩溃续读、日志缺口等待、发布后增量与旧页重投', flush=True)
    source = Source([
        {'asset': 'a1', 'department': 'd1', 'status': 'active'},
        {'asset': 'a2', 'department': 'd2', 'status': 'active'},
        {'asset': 'a3', 'department': 'd1', 'status': 'active'},
        {'asset': 'a4', 'department': 'd3', 'status': 'retired'},
        {'asset': 'a5', 'department': 'd2', 'status': 'active'},
    ])
    temporary = tempfile.TemporaryDirectory()
    connection = connect(Path(temporary.name) / 'warehouse.sqlite3')

    source.upsert('a1', 'd1', 'retired')  # 分页前的更新：位置1，进入快照页
    generation, start, manifest = source.begin_snapshot()
    # 分页读取期间来源继续变更（位置2..4）
    source.upsert('a2', 'd9', 'active')
    source.delete('a4')
    source.upsert('a6', 'd3', 'active')
    end = source.clock  # 来源给出结束水位
    plan = handoff.begin_handoff(connection, 'assets', start, end, manifest, generation)
    check(plan['phase'] == 'paging' and plan['pages_total'] == 3,
          f'交接开启：代次 {generation}，起始水位 {start}，结束水位 {end}，页清单 3 页')

    result = handoff.accept_page(connection, source.read_page(generation, 2))
    check(result['stored'] and result['missing_pages'] == [1, 3], '乱序页（第2页先到）被接受，缺页 [1, 3]')
    result = handoff.accept_page(connection, source.read_page(generation, 2))
    check(not result['stored'] and result['duplicate'], '同内容重复页幂等跳过')
    tampered = dict(source.read_page(generation, 2), rows=[{'asset': 'aX', 'department': 'dX', 'status': 'active'}])
    expect_rejected(lambda: handoff.accept_page(connection, tampered), '同页号不同内容被拒绝且不计入')
    handoff.accept_page(connection, source.read_page(generation, 1))

    connection.close()  # 模拟分页中途崩溃
    connection = connect(Path(temporary.name) / 'warehouse.sqlite3')
    resumed = handoff.active_handoff(connection)
    check(resumed is not None and resumed['pages']['missing'] == [3],
          '崩溃后从持久交接记录恢复：已收 2/3 页，缺第 3 页')
    handoff.accept_page(connection, source.read_page(generation, 3))
    check(handoff.handoff_status(connection, generation)['progress'] == 1.0, '续读完成，页清单完整')

    handoff.buffer_log(connection, generation, source.read_log(after=start, upto=3))
    waiting = handoff.publish(connection, generation)
    check(not waiting['published'] and waiting['reason'] == 'log_gap' and waiting['missing'] == [4],
          '结束水位前日志缺口：发布等待并报告缺口 [4]，半表未当成功')
    handoff.buffer_log(connection, generation, source.read_log(after=3, upto=end))
    preview = handoff.preview_diff(connection, generation)
    check(preview['added'] == 5 and preview['final_rows'] == 5,
          f"预览差异：新增 {preview['added']}，删除 {preview['removed']}，更新 {preview['updated']}（旧表为空）")
    published = handoff.publish(connection, generation)
    check(published['published'] and published['rows'] == 5 and published['watermark'] == end,
          f'摘要通过、日志连续，合并发布 {published["rows"]} 行，水位推进到 {end}')
    check(live_state(connection) == source.expected_at(generation, end),
          '发布后与来源结束水位完整状态一致（含分页期间更新 a2、删除 a4、新增 a6）')

    source.upsert('a7', 'd1', 'active')   # 位置5
    source.delete('a1')                   # 位置6
    result = handoff.apply_incremental(connection, 'assets', source.read_log(after=end))
    check(result['applied'] == 2 and result['watermark'] == 6 and not result['waiting'],
          '发布后转常规增量：连续应用 2 条（含删除），水位推进到 6')
    result = handoff.apply_incremental(connection, 'assets', source.read_log(after=end))
    check(result['applied'] == 0, '增量重投幂等，不重复应用')
    check(live_state(connection) == source.rows, '增量后与来源当前状态一致')

    before = live_state(connection)
    serving = overview(connection)
    expect_rejected(lambda: handoff.accept_page(connection, source.read_page(generation, 1)),
                    '已发布后重投旧页被拒绝')
    check(live_state(connection) == before and overview(connection) == serving,
          '旧快照不倒退，服务中数据未变化')
    connection.close()
    temporary.cleanup()
    return True


def scenario_regeneration():
    print('场景二：重新全量与代次替换——旧表继续服务、旧代次迟到页被拒、删除不复活', flush=True)
    source = Source([
        {'asset': 'b1', 'department': 'd1', 'status': 'active'},
        {'asset': 'b2', 'department': 'd2', 'status': 'active'},
        {'asset': 'b3', 'department': 'd1', 'status': 'retired'},
    ])
    temporary = tempfile.TemporaryDirectory()
    connection = connect(Path(temporary.name) / 'warehouse.sqlite3')

    generation, start, manifest = source.begin_snapshot()
    handoff.begin_handoff(connection, 'assets', start, start, manifest, generation)
    for page_no in (1, 2):
        handoff.accept_page(connection, source.read_page(generation, page_no))
    handoff.publish(connection, generation)
    serving = live_state(connection)
    check(len(serving) == 3, '首次全量已发布，3 行资产对外服务')

    source.upsert('b4', 'd3', 'active')   # 位置1：进入下一代快照页
    gen2, start2, manifest2 = source.begin_snapshot()
    source.delete('b2')                   # 位置2：分页期间删除
    source.upsert('b1', 'd9', 'active')   # 位置3：分页期间更新
    end2 = source.clock
    handoff.begin_handoff(connection, 'assets', start2, end2, manifest2, gen2)
    for page_no in (1, 2):
        handoff.accept_page(connection, source.read_page(gen2, page_no))
    handoff.buffer_log(connection, gen2, source.read_log(after=start2, upto=end2))
    check(live_state(connection) == serving, '重新全量分页期间旧表继续服务，内容不变')
    preview = handoff.preview_diff(connection, gen2)
    check(preview['added'] == 1 and preview['removed'] == 1 and preview['updated'] == 1,
          f"预览差异：新增 {preview['added']}（b4）、删除 {preview['removed']}（b2）、更新 {preview['updated']}（b1）")

    # 代次替换：gen2 未完成时来源重新全量
    source.upsert('b5', 'd1', 'active')   # 位置4
    gen3, start3, manifest3 = source.begin_snapshot()
    source.delete('b3')                   # 位置5
    end3 = source.clock
    plan = handoff.begin_handoff(connection, 'assets', start3, end3, manifest3, gen3)
    check(plan['superseded'] == gen2, f'代次 {gen2} 被新代次 {gen3} 替换')
    expect_rejected(lambda: handoff.accept_page(connection, source.read_page(gen2, 2)),
                    '旧代次迟到页被拒绝，不污染新代次')
    expect_rejected(lambda: handoff.accept_page(connection, source.read_page(generation, 1)),
                    '更早已发布代次的重投页同样被拒绝')
    for page_no in (1, 2):
        handoff.accept_page(connection, source.read_page(gen3, page_no))
    handoff.buffer_log(connection, gen3, source.read_log(after=start3, upto=end3))
    published = handoff.publish(connection, gen3)
    check(published['published'] and published['watermark'] == end3, f'新代次发布，水位 {end3}')
    final = live_state(connection)
    check(final == source.expected_at(gen3, end3) == source.rows, '最终与来源结束水位完整状态相同')
    check('b2' not in final and 'b3' not in final, '已删除资产 b2、b3 未复活')
    connection.close()
    temporary.cleanup()


def scenario_empty():
    print('场景三：空表交接——空页清单直接发布，仅补日志', flush=True)
    source = Source()
    temporary = tempfile.TemporaryDirectory()
    connection = connect(Path(temporary.name) / 'warehouse.sqlite3')
    generation, start, manifest = source.begin_snapshot()
    source.upsert('c1', 'd1', 'active')   # 快照之后唯一的变更
    end = source.clock
    handoff.begin_handoff(connection, 'assets', start, end, manifest, generation)
    status = handoff.handoff_status(connection, generation)
    check(status['pages']['total'] == 0 and status['progress'] == 1.0, '空表页清单为空，页进度即完成')
    handoff.buffer_log(connection, generation, source.read_log(after=start, upto=end))
    published = handoff.publish(connection, generation)
    check(published['published'] and published['rows'] == 1, '空快照叠加应补日志后发布 1 行')
    check(live_state(connection) == source.rows, '与来源结束水位状态一致')
    connection.close()
    temporary.cleanup()


def scenario_failure():
    print('场景四：游标过期与缺页——半表不当成功，显式失败后可重全量恢复', flush=True)
    source = Source([
        {'asset': 'e1', 'department': 'd1', 'status': 'active'},
        {'asset': 'e2', 'department': 'd2', 'status': 'active'},
        {'asset': 'e3', 'department': 'd3', 'status': 'active'},
    ])
    temporary = tempfile.TemporaryDirectory()
    connection = connect(Path(temporary.name) / 'warehouse.sqlite3')

    generation, start, manifest = source.begin_snapshot()
    handoff.begin_handoff(connection, 'assets', start, start, manifest, generation)
    handoff.accept_page(connection, source.read_page(generation, 1))
    source.invalidate(generation)  # 来源侧快照失效，续读游标过期
    expect_rejected(lambda: handoff.accept_page(connection, source.read_page(generation, 2)),
                    '游标过期使代次失败')
    status = handoff.handoff_status(connection, generation)
    check(status['phase'] == 'failed' and 'cursor_expired' in status['failure'], '失败原因持久化，可审计')
    expect_rejected(lambda: handoff.publish(connection, generation), '失败代次不能发布')
    check(live_state(connection) == {}, '半表未发布，旧表（空）继续服务')

    gen2, start2, manifest2 = source.begin_snapshot()
    handoff.begin_handoff(connection, 'assets', start2, start2, manifest2, gen2)
    handoff.accept_page(connection, source.read_page(gen2, 1))
    waiting = handoff.publish(connection, gen2)
    check(not waiting['published'] and waiting['reason'] == 'pages_missing' and waiting['missing'] == [2],
          '缺页时发布返回等待，不把半表当成功')
    handoff.abort_run(connection, gen2, '缺页无法补齐，明确失败')
    expect_rejected(lambda: handoff.accept_page(connection, source.read_page(gen2, 2)),
                    '已中止代次不再接收任何页')

    gen3, start3, manifest3 = source.begin_snapshot()
    handoff.begin_handoff(connection, 'assets', start3, start3, manifest3, gen3)
    for page_no in (1, 2):
        handoff.accept_page(connection, source.read_page(gen3, page_no))
    published = handoff.publish(connection, gen3)
    check(published['published'] and live_state(connection) == source.rows, '重新全量后成功发布并恢复服务')
    again = handoff.publish(connection, gen3)
    check(again['published'] and again['already'], '发布幂等：崩溃重试安全')
    connection.close()
    temporary.cleanup()


def main():
    scenario_main()
    scenario_regeneration()
    scenario_empty()
    scenario_failure()
    print('全部交接场景验证通过：最终状态均与来源结束水位完整状态相同', flush=True)


if __name__ == '__main__':
    main()
