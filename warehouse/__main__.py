import argparse
import json
from pathlib import Path

from . import handoff
from .catalog import connect, load_file
from .domain import overview
from .store import ObjectStore


def main():
    parser = argparse.ArgumentParser(description='本地数据集合装载与查询')
    parser.add_argument('command', choices=[
        'import', 'inspect', 'export',
        'handoff-begin', 'handoff-page', 'handoff-log', 'handoff-status',
        'handoff-preview', 'handoff-publish', 'handoff-incremental', 'handoff-abort'])
    parser.add_argument('--db', required=True)
    parser.add_argument('--input')
    parser.add_argument('--batch')
    parser.add_argument('--objects')
    parser.add_argument('--table', default='assets')
    parser.add_argument('--plan')
    parser.add_argument('--page')
    parser.add_argument('--log')
    parser.add_argument('--generation', type=int)
    parser.add_argument('--reason')
    args = parser.parse_args()
    if args.command == 'import' and (not args.input or not args.batch):
        parser.error('导入需要 --input 和 --batch')
    if args.command == 'export' and not args.objects:
        parser.error('导出需要 --objects')
    if args.command == 'handoff-begin' and not args.plan:
        parser.error('开始交接需要 --plan（含水位与页清单的 JSON）')
    if args.command == 'handoff-page' and not args.page:
        parser.error('接收页需要 --page')
    if args.command in ('handoff-log', 'handoff-incremental') and not args.log:
        parser.error('缓冲或增量需要 --log')
    if args.command in ('handoff-log', 'handoff-preview', 'handoff-publish', 'handoff-abort') \
            and args.generation is None:
        parser.error('该命令需要 --generation')
    if args.command == 'handoff-abort' and not args.reason:
        parser.error('中止需要 --reason')
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    connection = connect(args.db)
    try:
        if args.command == 'import':
            result = {'changed': load_file(connection, args.input, args.batch)}
        elif args.command == 'handoff-begin':
            plan = json.loads(Path(args.plan).read_text(encoding='utf-8'))
            result = handoff.begin_handoff(
                connection, plan.get('table', args.table), plan['start_watermark'],
                plan['end_watermark'], plan['manifest'], plan.get('generation'))
        elif args.command == 'handoff-page':
            result = handoff.accept_page(connection, json.loads(Path(args.page).read_text(encoding='utf-8')))
        elif args.command == 'handoff-log':
            result = handoff.buffer_log(
                connection, args.generation, json.loads(Path(args.log).read_text(encoding='utf-8')))
        elif args.command == 'handoff-status':
            result = handoff.active_handoff(connection, args.table) if args.generation is None \
                else handoff.handoff_status(connection, args.generation, args.table)
        elif args.command == 'handoff-preview':
            result = handoff.preview_diff(connection, args.generation)
        elif args.command == 'handoff-publish':
            result = handoff.publish(connection, args.generation)
        elif args.command == 'handoff-incremental':
            result = handoff.apply_incremental(
                connection, args.table, json.loads(Path(args.log).read_text(encoding='utf-8')))
        elif args.command == 'handoff-abort':
            result = handoff.abort_run(connection, args.generation, args.reason)
        else:
            result = overview(connection)
            if args.command == 'export':
                records = result if isinstance(result, list) else [result]
                result = {'object': ObjectStore(args.objects).put(records), 'rows': len(records)}
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
