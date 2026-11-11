import argparse
import json
import sys
from pathlib import Path

from .association import load_dataset
from .records import inspect_bundle
from .storage import RevisionStore, save_json, save_jsonl


def _join_summary(result,output,error_output):
    summary={key:value for key,value in result.items() if key not in ('facts','errors')}
    summary['output_files']={'joins':str(output),'errors':str(error_output)}
    return summary


def main():
    parser=argparse.ArgumentParser(description='数据包内容检查与双时间关联')
    commands=parser.add_subparsers(dest='command',required=True)

    p=commands.add_parser('inspect');p.add_argument('bundle',type=Path)
    p=commands.add_parser('catalog');p.add_argument('database',type=Path);p.add_argument('stream')

    p=commands.add_parser('join',description='在固定获知截止下连接事实')
    p.add_argument('bundle',type=Path)
    p.add_argument('--cutoff',default='latest',help='显式ISO时刻或 latest')
    p.add_argument('--output',type=Path,default=Path('state/fact-joins.jsonl'))
    p.add_argument('--errors',type=Path,default=Path('state/fact-errors.jsonl'))

    p=commands.add_parser('impact',description='比较两个固定获知截止的连接结果')
    p.add_argument('bundle',type=Path)
    p.add_argument('--from-cutoff',required=True)
    p.add_argument('--to-cutoff',required=True)
    p.add_argument('--output',type=Path)

    args=parser.parse_args()
    try:
        if args.command=='inspect':
            value=inspect_bundle(args.bundle)
        elif args.command=='catalog':
            value=RevisionStore(args.database).latest(args.stream)
        elif args.command=='join':
            dataset=load_dataset(args.bundle)
            result=dataset.join(args.cutoff)
            save_jsonl(args.output,result['facts'])
            save_jsonl(args.errors,result['errors'])
            value=_join_summary(result,args.output,args.errors)
        else:
            dataset=load_dataset(args.bundle)
            value=dataset.impact(args.from_cutoff,args.to_cutoff)
            if args.output is not None:
                save_json(args.output,value)
        print(json.dumps(value,ensure_ascii=False,indent=2));return 0
    except (ValueError,KeyError,OSError) as error:
        print('输入资料或目录无效: '+str(error),file=sys.stderr);return 2


if __name__=='__main__':
    raise SystemExit(main())
