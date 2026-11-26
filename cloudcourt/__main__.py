import argparse,json
from pathlib import Path
from . import MODULES
from .store import connect,replay,many

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('command',choices=[*MODULES,'demo','replay','report'])
    parser.add_argument('--db',default='demo.sqlite')
    parser.add_argument('--fixture')
    args=parser.parse_args()
    if args.command=='replay':
        result=replay(args.db)
    else:
        db=connect(args.db)
        if args.command=='report':result=many(db,'SELECT * FROM events ORDER BY seq')
        elif args.command=='demo':
            result=[]
            path_fixture=Path('fixtures/path.json')
            for index,name in enumerate(MODULES):
                path=Path('fixtures')/f'{name}-normal.json'
                commands=json.loads(path.read_text(encoding='utf-8'))
                result.append({'fixture':str(path),'results':[MODULES[name].run(db,x) for x in commands]})
                # 在席位就绪后指定 C0 主站，随后再跑演练与事故交接。
                if index==1 and path_fixture.exists():
                    commands=json.loads(path_fixture.read_text(encoding='utf-8'))
                    result.append({'fixture':str(path_fixture),'results':[MODULES[x['subsystem']].run(db,x['payload']) for x in commands]})
                failure_path=Path('fixtures')/f'{name}-failure.json'
                if failure_path.exists():
                    commands=json.loads(failure_path.read_text(encoding='utf-8'))
                    result.append({'fixture':str(failure_path),'results':[MODULES[name].run(db,x) for x in commands]})
        else:
            if not args.fixture:parser.error('--fixture required')
            commands=json.loads(Path(args.fixture).read_text(encoding='utf-8'))
            result=[MODULES[args.command].run(db,x) for x in commands]
        db.close()
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if args.command=='replay' and not result['verified']:raise SystemExit(2)

if __name__=='__main__':main()
