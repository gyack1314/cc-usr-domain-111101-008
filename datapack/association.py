"""双时间（bitemporal）维度关联引擎。

维度记录同时携带两套左闭右开 UTC 区间：

* ``[valid_from, valid_to)``：业务生效时间；
* ``[known_from, known_to)``：这条知识可被系统使用的时间，空终点为无穷。

事实只有 ``event_at``，本模块从不读取或推断入库时间。查询先按固定的
``knowledge_cutoff`` 选出该时点已知的维度版本，再用事实的 ``event_at``
在有效区间内连接。``latest`` 模式在进入计算前解析成一个固定的获知边界，
整个计算过程中不会重新读取“最新版本”。

每个事实最多产生一条成功连接；断档、同版本重叠、主键冲突都走错误旁路，
并标明具体是哪一段链路以及候选记录，不会放大成多条成功事实。
"""

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .domain import utc
from .records import load_rows
from .storage import canonical, confined, fingerprint, read_json

SEGMENTS=('vehicle','operator','software')
SEGMENT_VALUE={'vehicle':'operator','operator':'region','software':'software'}
_SCHEMA_ROLES={'facts','vehicle','operator','software'}


class DomainError(ValueError):
    """输入资料不满足双时间字段约定时抛出。"""


def parse_utc(value,label='时刻'):
    """解析必须带明确偏移的 ISO 时刻并归一化为 UTC。

    拒绝无偏移的本地时间、纯日期和无法解析的值；不使用运行环境时区猜测。
    """
    if not isinstance(value,str) or not value.strip():
        raise DomainError(label+'必须是非空ISO时刻字符串')
    try:
        return utc(value.strip())
    except (ValueError,TypeError):
        raise DomainError(label+'必须包含明确的UTC偏移（拒绝无偏移或模糊的本地时间）') from None


def _as_utc(value,label):
    if isinstance(value,datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise DomainError(label+'必须包含明确的UTC偏移（拒绝无偏移或模糊的本地时间）')
        return value.astimezone(timezone.utc)
    return parse_utc(value,label)


def format_utc(value):
    """把 UTC 时刻输出为稳定的 ISO 字符串。"""
    if isinstance(value,str):
        value=parse_utc(value)
    elif isinstance(value,datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise DomainError('时刻必须包含明确的UTC偏移（拒绝无偏移或模糊的本地时间）')
    return value.astimezone(timezone.utc).isoformat().replace('+00:00','Z')


def _interval(row,start_key,end_key,label):
    if start_key not in row or row[start_key] is None:
        raise DomainError(label+' 缺少 '+start_key)
    start=parse_utc(row[start_key],label+'.'+start_key)
    raw_end=row.get(end_key)
    end=(None if raw_end is None or (isinstance(raw_end,str) and not raw_end.strip())
         else parse_utc(raw_end,label+'.'+end_key))
    if end is not None and start>=end:
        raise DomainError(label+' 的区间倒置或为空')
    return (start,end)


class DimensionRow:
    """已校验、已归一化的维度记录。"""
    __slots__=('segment','row_id','entity_id','value','valid','known')

    def __init__(self,row,segment):
        rid=row.get('row_id')
        if not isinstance(rid,str) or not rid:
            raise DomainError(segment+' 维度记录的 row_id 必须为非空字符串')
        entity=row.get('entity_id')
        if not isinstance(entity,str) or not entity:
            raise DomainError(segment+' 维度记录 '+rid+' 的 entity_id 必须为非空字符串')
        value_key=SEGMENT_VALUE[segment]
        value=row.get(value_key)
        if not isinstance(value,str) or not value:
            raise DomainError(segment+' 维度记录 '+rid+' 的 '+value_key+' 必须为非空字符串')
        self.segment=segment
        self.row_id=rid
        self.entity_id=entity
        self.value=value
        self.valid=_interval(row,'valid_from','valid_to',segment+' '+rid)
        self.known=_interval(row,'known_from','known_to',segment+' '+rid)

    def reference(self,lookup_entity_id):
        """返回引用链中的一条稳定引用。"""
        return {
            'segment':self.segment,
            'lookup_entity_id':lookup_entity_id,
            'row_id':self.row_id,
            'entity_id':self.entity_id,
            'value_key':SEGMENT_VALUE[self.segment],
            'value':self.value,
            'valid_from':format_utc(self.valid[0]),
            'valid_to':None if self.valid[1] is None else format_utc(self.valid[1]),
            'known_from':format_utc(self.known[0]),
            'known_to':None if self.known[1] is None else format_utc(self.known[1]),
        }


def _load_dimension(rows,segment):
    if not isinstance(rows,(list,tuple)):
        raise DomainError(segment+' 维度记录必须是数组')
    first={};signatures={};counts=defaultdict(int);conflicts=set()
    for row in rows:
        if not isinstance(row,dict):
            raise DomainError(segment+' 维度记录必须是对象')
        rid=row.get('row_id')
        if not isinstance(rid,str) or not rid:
            raise DomainError(segment+' 维度记录的 row_id 必须为非空字符串')
        signature=canonical(row)
        counts[rid]+=1
        if rid in first:
            if signatures[rid]!=signature:
                conflicts.add(rid)
        else:
            first[rid]=row
            signatures[rid]=signature
    if conflicts:
        raise DomainError(segment+' 维度主键相同但内容不同: '+','.join(sorted(conflicts)))
    parsed=[DimensionRow(first[rid],segment) for rid in first]
    duplicates=sum(counts[rid]-1 for rid in first)
    return parsed,duplicates


def _normalize_fact(row):
    vehicle=row.get('vehicle')
    if not isinstance(vehicle,str) or not vehicle:
        raise DomainError('vehicle 必须为非空字符串')
    raw_event=row.get('event_at')
    event=_as_utc(raw_event,'event_at')
    partition=row.get('partition')
    if partition is not None and not isinstance(partition,str):
        raise DomainError('partition 必须为字符串或空')
    return {
        'fact_id':row['fact_id'],
        'vehicle':vehicle,
        'partition':partition,
        'event_at':raw_event if isinstance(raw_event,str) else format_utc(event),
        'event_at_utc':format_utc(event),
        '_event':event,
    }


def _prepare_facts(rows):
    """按主键归并事实：完全相同的重复输入幂等去重；内容不同则进入冲突。"""
    if not isinstance(rows,(list,tuple)):
        raise DomainError('事实记录必须是数组')
    first={};signatures={};digests=defaultdict(set);counts=defaultdict(int);conflicts=set()
    for row in rows:
        if not isinstance(row,dict):
            raise DomainError('事实记录必须是对象')
        fid=row.get('fact_id')
        if not isinstance(fid,str) or not fid:
            raise DomainError('事实记录的 fact_id 必须为非空字符串')
        signature=canonical(row)
        counts[fid]+=1
        digests[fid].add(fingerprint(row))
        if fid in first:
            if signatures[fid]!=signature:
                conflicts.add(fid)
        else:
            first[fid]=row
            signatures[fid]=signature

    facts=[];errors=[];duplicate_ids=[];duplicate_rows=0;invalid_ids=[]
    for fid,row in first.items():
        partition=row.get('partition') if isinstance(row.get('partition'),str) else None
        if fid in conflicts:
            errors.append({
                'fact_id':fid,
                'partition':partition,
                'kind':'fact_conflict',
                'stage':'fact',
                'content_digests':sorted(digests[fid]),
                'detail':'事实主键相同但内容不同，拒绝静默覆盖',
            })
            continue
        if counts[fid]>1:
            duplicate_ids.append(fid)
            duplicate_rows+=counts[fid]-1
        try:
            facts.append(_normalize_fact(row))
        except DomainError as exc:
            invalid_ids.append(fid)
            errors.append({
                'fact_id':fid,
                'partition':partition,
                'kind':'fact_invalid',
                'stage':'fact',
                'detail':str(exc),
            })
    return facts,errors,duplicate_ids,duplicate_rows,sorted(conflicts),sorted(invalid_ids)


class BitemporalDataset:
    """不可变的双时间资料快照。

    构造时校验并归一化全部维度与事实。查询只读取这份快照，因此传入固定
    cutoff 的输出在后续导入新知识后仍可复现；需要新知识时请构造新的数据集。
    """

    def __init__(self,vehicle,operator,software,facts):
        self.inputs={'vehicle':list(vehicle),'operator':list(operator),
                     'software':list(software),'facts':list(facts)}
        self.tables={};self.dimension_duplicates={}
        for segment,rows in (('vehicle',vehicle),('operator',operator),('software',software)):
            parsed,duplicates=_load_dimension(rows,segment)
            self.tables[segment]=parsed
            self.dimension_duplicates[segment]=duplicates
        self._by_entity={segment:defaultdict(list) for segment in SEGMENTS}
        for segment in SEGMENTS:
            for row in self.tables[segment]:
                self._by_entity[segment][row.entity_id].append(row)
        (self.facts,self._fact_errors,self._duplicate_fact_ids,
         self._duplicate_fact_rows,self._conflict_fact_ids,
         self._invalid_fact_ids)=_prepare_facts(facts)
        self.fact_input_rows=len(facts)
        self._unique_fact_ids={row['fact_id'] for row in facts
                               if isinstance(row,dict) and isinstance(row.get('fact_id'),str)}

    def resolve_cutoff(self,mode=None):
        """把 ``latest`` 或显式时刻解析成一次固定的获知截止。

        ``latest`` 取资料中已出现的最晚 ``known_from``，不依赖系统时钟，
        也不在计算过程中改变。返回值中的 ``_utc`` 仅供内部比较使用。
        """
        if mode is None or (isinstance(mode,str) and mode.strip().lower()=='latest'):
            points=[row.known[0] for segment in SEGMENTS for row in self.tables[segment]]
            if not points:
                raise DomainError('没有可用维度知识，无法解析 latest 获知截止')
            cutoff=max(points)
            return {'mode':'latest','cutoff':format_utc(cutoff),'_utc':cutoff}
        cutoff=_as_utc(mode,'knowledge_cutoff')
        return {'mode':'explicit','cutoff':format_utc(cutoff),'_utc':cutoff}

    def _match(self,segment,entity_id,event_at,cutoff):
        """先取 cutoff 时点已知的维度，再按事实发生时点筛选有效记录。"""
        known=[row for row in self._by_entity[segment].get(entity_id,())
               if row.known[0]<=cutoff and (row.known[1] is None or cutoff<row.known[1])]
        return sorted([row for row in known
                       if row.valid[0]<=event_at and (row.valid[1] is None or event_at<row.valid[1])],
                      key=lambda row:row.row_id)

    @staticmethod
    def _failure(kind,segment,entity_id,matches,resolved):
        detail=('在固定获知截止下匹配到多条可见记录' if kind=='conflict'
                else '在事实发生时点没有可用的有效记录')
        return {
            'kind':kind,
            'stage':segment,
            'entity_id':entity_id,
            'candidates':[row.row_id for row in matches],
            'resolved':[dict(reference) for reference in resolved],
            'detail':segment+' 维度'+detail,
        }

    def _resolve(self,fact,cutoff):
        """解析单条事实，返回（引用链，错误列表）。成功和错误互斥。"""
        chain=[];errors=[]
        event=fact['_event']

        vehicle_rows=self._match('vehicle',fact['vehicle'],event,cutoff)
        vehicle_row=None
        if not vehicle_rows:
            errors.append(self._failure('gap','vehicle',fact['vehicle'],vehicle_rows,chain))
        elif len(vehicle_rows)>1:
            errors.append(self._failure('conflict','vehicle',fact['vehicle'],vehicle_rows,chain))
        else:
            vehicle_row=vehicle_rows[0]
            chain.append(vehicle_row.reference(fact['vehicle']))

        if vehicle_row is not None:
            operator_rows=self._match('operator',vehicle_row.value,event,cutoff)
            if not operator_rows:
                errors.append(self._failure('gap','operator',vehicle_row.value,operator_rows,chain))
            elif len(operator_rows)>1:
                errors.append(self._failure('conflict','operator',vehicle_row.value,operator_rows,chain))
            else:
                operator_row=operator_rows[0]
                chain.append(operator_row.reference(vehicle_row.value))

        software_rows=self._match('software',fact['vehicle'],event,cutoff)
        if not software_rows:
            errors.append(self._failure('gap','software',fact['vehicle'],software_rows,chain))
        elif len(software_rows)>1:
            errors.append(self._failure('conflict','software',fact['vehicle'],software_rows,chain))
        else:
            chain.append(software_rows[0].reference(fact['vehicle']))

        return chain,errors

    @staticmethod
    def _attribution(chain):
        values={reference['segment']:reference['value'] for reference in chain}
        return {
            'operator':values['vehicle'],
            'region':values['operator'],
            'software':values['software'],
        }

    def _success_payload(self,fact,cutoff_info,chain):
        return {
            'fact_id':fact['fact_id'],
            'partition':fact['partition'],
            'vehicle':fact['vehicle'],
            'event_at':fact['event_at'],
            'event_at_utc':fact['event_at_utc'],
            'knowledge_cutoff_utc':cutoff_info['cutoff'],
            'status':'ok',
            'attribution':self._attribution(chain),
            'references':{reference['segment']:reference['row_id'] for reference in chain},
            'chain':chain,
        }

    def _error_payload(self,fact,cutoff_info,error):
        result={
            'fact_id':fact['fact_id'],
            'partition':fact['partition'],
            'vehicle':fact['vehicle'],
            'event_at':fact['event_at'],
            'event_at_utc':fact['event_at_utc'],
            'knowledge_cutoff_utc':cutoff_info['cutoff'],
            'status':'error',
        }
        result.update(error)
        return result

    def _compact(self,chain,errors):
        """用于影响分析的紧凑结果，保留引用链但不重复事实字段。"""
        if not errors:
            return {
                'status':'ok',
                'attribution':self._attribution(chain),
                'references':{reference['segment']:reference['row_id'] for reference in chain},
                'chain':chain,
            }
        return {'status':'error','errors':[dict(error) for error in errors]}

    @staticmethod
    def _fact_error_payload(error,cutoff_info):
        result={'status':'error','knowledge_cutoff_utc':cutoff_info['cutoff']}
        result.update(error)
        return result

    def join(self,knowledge_cutoff=None):
        """在惰性解析出的固定获知截止下连接全部事实。

        返回 ``facts``（成功连接，最多每个事实一条）、``errors``（错误旁路）
        和计数摘要。事实主键冲突或字段非法不会进入成功连接。
        """
        cutoff_info=self.resolve_cutoff(knowledge_cutoff)
        cutoff=cutoff_info['_utc']
        successes=[];errors=[]
        for fact in self.facts:
            chain,problems=self._resolve(fact,cutoff)
            if problems:
                errors.extend(self._error_payload(fact,cutoff_info,problem) for problem in problems)
            else:
                successes.append(self._success_payload(fact,cutoff_info,chain))
        for error in self._fact_errors:
            errors.append(self._fact_error_payload(error,cutoff_info))

        error_fact_ids=sorted({error['fact_id'] for error in errors})
        counts={
            'fact_input_rows':self.fact_input_rows,
            'fact_unique':len(self._unique_fact_ids),
            'joinable_facts':len(self.facts),
            'joined':len(successes),
            'error_entries':len(errors),
            'error_fact_count':len(error_fact_ids),
            'duplicate_fact_rows':self._duplicate_fact_rows,
            'dimension_duplicate_rows':sum(self.dimension_duplicates.values()),
        }
        return {
            'knowledge_cutoff':{'mode':cutoff_info['mode'],'cutoff':cutoff_info['cutoff']},
            'facts':successes,
            'errors':errors,
            'counts':counts,
            'duplicate_fact_ids':self._duplicate_fact_ids,
            'conflict_fact_ids':self._conflict_fact_ids,
            'invalid_fact_ids':self._invalid_fact_ids,
        }

    @staticmethod
    def _semantic(compact):
        if compact['status']=='ok':
            attribution=compact['attribution']
            return ('ok',attribution['operator'],attribution['region'],attribution['software'])
        return ('error',tuple(sorted((error['kind'],error['stage'],
                                     error['entity_id'],tuple(error['candidates']))
                                    for error in compact['errors'])))

    @staticmethod
    def _reference_signature(compact):
        if compact['status']=='ok':
            return tuple(reference['row_id'] for reference in compact['chain'])
        return tuple(sorted({reference['row_id'] for error in compact['errors']
                             for reference in error['resolved']}))

    def impact(self,from_cutoff,to_cutoff):
        """比较两个固定 knowledge_cutoff 下的实际连接结果。

        只有语义结果（成功归属或错误类别/段/候选）确实改变的事实才进入
        ``changed``；仅引用行变化、归属不变的事实进入 ``reference_only_changes``。
        范围逐事实计算，不使用“某车辆所有月份”之类的粗粒度集合。
        """
        before_info=self.resolve_cutoff(from_cutoff)
        after_info=self.resolve_cutoff(to_cutoff)
        before_cutoff=before_info['_utc']
        after_cutoff=after_info['_utc']

        changed=[];reference_only=[];unchanged=[]
        for fact in self.facts:
            before_chain,before_errors=self._resolve(fact,before_cutoff)
            after_chain,after_errors=self._resolve(fact,after_cutoff)
            before=self._compact(before_chain,before_errors)
            after=self._compact(after_chain,after_errors)
            if self._semantic(before)!=self._semantic(after):
                record={
                    'fact_id':fact['fact_id'],
                    'partition':fact['partition'],
                    'before':before,
                    'after':after,
                }
                if before['status']!=after['status']:
                    record['changed_fields']=['status']
                elif before['status']=='ok':
                    record['changed_fields']=[key for key in ('operator','region','software')
                                              if before['attribution'][key]!=after['attribution'][key]]
                else:
                    record['changed_fields']=['errors']
                changed.append(record)
            elif self._reference_signature(before)!=self._reference_signature(after):
                reference_only.append({
                    'fact_id':fact['fact_id'],
                    'partition':fact['partition'],
                    'before':before,
                    'after':after,
                })
            else:
                unchanged.append(fact['fact_id'])

        partitions=sorted({record['partition'] for record in changed
                           if record['partition'] is not None})
        changed_by_partition={}
        for record in changed:
            key=record['partition']
            if key is not None:
                changed_by_partition.setdefault(key,[]).append(record['fact_id'])
        return {
            'from_cutoff':{'mode':before_info['mode'],'cutoff':before_info['cutoff']},
            'to_cutoff':{'mode':after_info['mode'],'cutoff':after_info['cutoff']},
            'changed':changed,
            'reference_only_changes':reference_only,
            'unchanged_fact_ids':unchanged,
            'partitions':partitions,
            'changed_by_partition':changed_by_partition,
            'excluded_fact_ids':sorted(set(self._conflict_fact_ids)|set(self._invalid_fact_ids)),
            'counts':{
                'compared_facts':len(self.facts),
                'changed':len(changed),
                'reference_only_changes':len(reference_only),
                'unchanged':len(unchanged),
            },
        }


def _infer_role(item):
    role=item.get('role')
    if role in _SCHEMA_ROLES:
        return role
    stem=Path(item.get('path','')).stem.lower()
    if 'fact' in stem:
        return 'facts'
    if 'vehicle' in stem:
        return 'vehicle'
    if 'operator' in stem:
        return 'operator'
    if 'software' in stem:
        return 'software'
    return None


def load_dataset(root):
    """从 bundle.json 与记录文件构造不可变双时间资料快照。"""
    root=Path(root)
    config=read_json(root/'bundle.json')
    files={}
    for item in config.get('files',[]):
        role=_infer_role(item)
        if role is None:
            raise DomainError('无法确定资料文件角色: '+str(item.get('path')))
        if role in files:
            raise DomainError('bundle 重复声明文件角色: '+role)
        files[role]=confined(root,item['path'])
    missing=[role for role in _SCHEMA_ROLES if role not in files]
    if missing:
        raise DomainError('bundle 缺少资料角色: '+','.join(sorted(missing)))
    rows={role:load_rows(path) for role,path in files.items()}
    return BitemporalDataset(rows['vehicle'],rows['operator'],rows['software'],rows['facts'])
